/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.distributed.internal.locks;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.PooledDistributionMessage;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Handles messaging to all members of a lock service for the purposes of recoverying from the loss
 * of the lock grantor. The <code>DLockRecoverGrantorMessage</code> is sent out by the new lock
 * grantor and all members reply with details on held and pending locks.
 *
 */
public class DLockRecoverGrantorProcessor extends ReplyProcessor21 {
  private static final Logger logger = LogService.getLogger();

  @Immutable
  protected static final DefaultMessageProcessor nullServiceProcessor =
      new DefaultMessageProcessor();

  private final DistributionManager dm;

  private final DLockGrantor newGrantor;

  // -------------------------------------------------------------------------
  // Static operations for recovering from loss of the lock grantor
  // -------------------------------------------------------------------------

  /**
   * Sends DLockRecoverGrantorMessage to all participants of the DLockService in order to recover
   * from the loss of the previous lock grantor. Each member will reply with details on currently
   * held locks, pending locks, and known stuck locks.
   * <p>
   * This method should block until transfer of lock grantor has completed.
   */
  static boolean recoverLockGrantor(Set members, DLockService service, DLockGrantor newGrantor,
      DistributionManager dm, InternalDistributedMember elder) {
    // proc will wait for replies from everyone including THIS member...
    DLockRecoverGrantorProcessor processor =
        new DLockRecoverGrantorProcessor(dm, members, newGrantor);

    DLockRecoverGrantorMessage msg = new DLockRecoverGrantorMessage();
    msg.serviceName = service.getName();
    msg.processorId = processor.getProcessorId();
    msg.grantorVersion = newGrantor.getVersionId();
    msg.grantorSerialNumber = service.getSerialNumber();
    msg.elder = elder;

    // send msg to all members EXCEPT this member...
    Set recipients = new HashSet(members);
    recipients.remove(dm.getId());
    if (!recipients.isEmpty()) {
      msg.setRecipients(recipients);
      dm.putOutgoing(msg);
    }

    // process msg and reply from this VM...
    if (msg.getSender() == null) {
      msg.setSender(dm.getId());
    }
    msg.scheduleMessage(dm);

    // keep waiting even if interrupted
    try {
      processor.waitForRepliesUninterruptibly();
    } catch (ReplyException e) {
      e.handleCause();
    }
    return !processor.error;

    // return newGrantor.makeReady(false);
  }

  // -------------------------------------------------------------------------
  // Constructors
  // -------------------------------------------------------------------------

  /** Creates a new instance of DLockRecoverGrantorProcessor */
  private DLockRecoverGrantorProcessor(DistributionManager dm, Set members,
      DLockGrantor newGrantor) {
    super(dm, members);
    this.dm = dm;
    this.newGrantor = newGrantor;
  }

  // -------------------------------------------------------------------------
  // Instance methods
  // -------------------------------------------------------------------------

  private volatile boolean error = false;

  @Override
  protected boolean canStopWaiting() {
    return error;
  }

  @Override
  public void process(DistributionMessage msg) {
    try {
      Assert.assertTrue(msg instanceof DLockRecoverGrantorReplyMessage,
          "DLockRecoverGrantorProcessor is unable to process message of type " + msg.getClass());

      DLockRecoverGrantorReplyMessage reply = (DLockRecoverGrantorReplyMessage) msg;
      // build grantTokens from each reply...
      switch (reply.replyCode) {
        case DLockRecoverGrantorReplyMessage.GRANTOR_DISPUTE:
          if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
            logger.trace(LogMarker.DLS_VERBOSE, "Failed DLockRecoverGrantorReplyMessage: '{}'",
                reply);
          }
          error = true;
          break;
        case DLockRecoverGrantorReplyMessage.OK:
          // collect results...
          if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
            logger.trace(LogMarker.DLS_VERBOSE, "Processing DLockRecoverGrantorReplyMessage: '{}'",
                reply);
          }

          Set lockSet = new HashSet();
          DLockRemoteToken[] heldLocks = reply.heldLocks;
          if (heldLocks.length > 0) {
            for (int i = 0; i < heldLocks.length; i++) {
              lockSet.add(heldLocks[i]);
            }
            try {
              newGrantor.initializeHeldLocks(msg.getSender(), lockSet);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              dm.getCancelCriterion().checkCancelInProgress(e);
            }
          }
          break;
        default:
          throw new IllegalStateException("Invalid reply.replyCode " + reply.replyCode);
      }
      // maybe build up another reply to indicate lock recovery status?
    } catch (IllegalStateException e) {
      if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
        logger.trace(LogMarker.DLS_VERBOSE,
            "Processing of DLockRecoverGrantorReplyMessage {} resulted in {}", msg, e.getMessage(),
            e);
      }
    } finally {
      super.process(msg);
    }
  }

  @Override
  protected boolean allowReplyFromSender() {
    return true;
  }

  // -------------------------------------------------------------------------
  // DLockRecoverGrantorMessage
  // -------------------------------------------------------------------------
  public static class DLockRecoverGrantorMessage extends PooledDistributionMessage
      implements MessageWithReply {

    /** The name of the DistributedLockService */
    protected String serviceName;

    /** The reply processor to route replies to */
    protected int processorId;

    /** The version, from the elder, of the grantor doing the recovery. */
    protected long grantorVersion;

    /** The DLS serial number of the grantor doing the recovery. */
    protected int grantorSerialNumber;

    /** The elder that made this member the grantor */
    protected InternalDistributedMember elder;

    public String getServiceName() {
      return serviceName;
    }

    public void setServiceName(final String serviceName) {
      this.serviceName = serviceName;
    }

    @Override
    public int getProcessorId() {
      return processorId;
    }

    public void setProcessorId(final int processorId) {
      this.processorId = processorId;
    }

    public long getGrantorVersion() {
      return grantorVersion;
    }

    public int getGrantorSerialNumber() {
      return grantorSerialNumber;
    }

    public InternalDistributedMember getElder() {
      return elder;
    }

    @Override
    protected void process(ClusterDistributionManager dm) {
      processMessage(dm);
    }

    /**
     * For unit testing we need to push the message through scheduleAction so that message observers
     * are invoked
     *
     * @param dm the distribution manager
     */
    protected void scheduleMessage(DistributionManager dm) {
      if (dm instanceof ClusterDistributionManager) {
        super.scheduleAction((ClusterDistributionManager) dm);
      } else {
        processMessage(dm);
      }
    }

    protected void processMessage(DistributionManager dm) {
      MessageProcessor processor = nullServiceProcessor;

      DLockService svc = DLockService.getInternalServiceNamed(serviceName);
      if (svc != null) {
        if (svc.getDLockRecoverGrantorMessageProcessor() == null) {
          svc.setDLockRecoverGrantorMessageProcessor(new DefaultMessageProcessor());
        }
        processor = svc.getDLockRecoverGrantorMessageProcessor();
      }

      processor.process(dm, this);
    }

    @Override
    public int getDSFID() {
      return DLOCK_RECOVER_GRANTOR_MESSAGE;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      serviceName = DataSerializer.readString(in);
      processorId = in.readInt();
      grantorSerialNumber = in.readInt();
      grantorVersion = in.readLong();
      elder = DataSerializer.readObject(in);
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      DataSerializer.writeString(serviceName, out);
      out.writeInt(processorId);
      out.writeInt(grantorSerialNumber);
      out.writeLong(grantorVersion);
      DataSerializer.writeObject(elder, out);
    }

    @Override
    public String toString() {
      StringBuilder buff = new StringBuilder();
      buff.append("DLockRecoverGrantorMessage (service='");
      buff.append(serviceName);
      buff.append("'; processorId=");
      buff.append(processorId);
      buff.append("'; grantorVersion=");
      buff.append(grantorVersion);
      buff.append("'; grantorSerialNumber=");
      buff.append(grantorSerialNumber);
      buff.append("'; elder=");
      buff.append(elder);
      buff.append(")");
      return buff.toString();
    }
  }

  // -------------------------------------------------------------------------
  // DLockRecoverGrantorReplyMessage
  // -------------------------------------------------------------------------
  public static class DLockRecoverGrantorReplyMessage extends ReplyMessage {

    public static final int OK = 0;
    public static final int GRANTOR_DISPUTE = 1;

    protected int replyCode;

    /**
     * Locks that are currently held... Serializable owner, Object name, long leaseTime
     */
    protected DLockRemoteToken[] heldLocks;

    // /** Identifies who the responder believes the lock grantor currently is */
    // private InternalDistributedMember grantor;

    public int getReplyCode() {
      return replyCode;
    }

    public void setReplyCode(final int replyCode) {
      this.replyCode = replyCode;
    }

    public DLockRemoteToken[] getHeldLocks() {
      return heldLocks;
    }

    public void setHeldLocks(final DLockRemoteToken[] heldLocks) {
      this.heldLocks = heldLocks;
    }

    @Override
    public int getDSFID() {
      return DLOCK_RECOVER_GRANTOR_REPLY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      replyCode = in.readByte();
      heldLocks = (DLockRemoteToken[]) DataSerializer.readObjectArray(in);
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      out.writeByte(replyCode);
      DataSerializer.writeObjectArray(heldLocks, out);
    }

    @Override
    public String toString() {
      String response = null;
      switch (replyCode) {
        case OK:
          response = "OK";
          break;
        case GRANTOR_DISPUTE:
          response = "GRANTOR_DISPUTE";
          break;
        default:
          response = String.valueOf(replyCode);
          break;
      }
      return "DLockRecoverGrantorReplyMessage (processorId=" + processorId + "; replyCode="
          + replyCode + "=" + response + "; heldLocks=" + Arrays.asList(heldLocks)
          + "; sender=" + getSender() + ")";
    }
  }

  public interface MessageProcessor {
    void process(DistributionManager dm, DLockRecoverGrantorMessage msg);
  }

  static class DefaultMessageProcessor implements MessageProcessor {
    @Override
    public void process(DistributionManager dm, DLockRecoverGrantorMessage msg) {
      ReplyException replyException = null;
      int replyCode = DLockRecoverGrantorReplyMessage.OK;
      DLockRemoteToken[] heldLocks = new DLockRemoteToken[0];

      try {
        // get the service from the name
        DLockService svc = DLockService.getInternalServiceNamed(msg.getServiceName());

        if (svc != null) {
          replyCode = DLockRecoverGrantorReplyMessage.OK;

          LockGrantorId lockGrantorId = new LockGrantorId(dm, msg.getSender(),
              msg.getGrantorVersion(), msg.getGrantorSerialNumber());

          Set heldLockSet = svc.getLockTokensForRecovery(lockGrantorId);

          if (heldLockSet == null) {
            replyCode = DLockRecoverGrantorReplyMessage.GRANTOR_DISPUTE;
          } else {
            heldLocks =
                (DLockRemoteToken[]) heldLockSet.toArray(new DLockRemoteToken[0]);
          }
        }
      } catch (RuntimeException e) {
        logger.warn("[DLockRecoverGrantorMessage.process] throwable: ",
            e);
        replyException = new ReplyException(e);
      } finally {
        DLockRecoverGrantorReplyMessage replyMsg = new DLockRecoverGrantorReplyMessage();
        replyMsg.replyCode = replyCode;
        replyMsg.heldLocks = heldLocks;
        replyMsg.setProcessorId(msg.getProcessorId());
        replyMsg.setRecipient(msg.getSender());
        replyMsg.setException(replyException);
        if (msg.getSender().equals(dm.getId())) {
          // process in-line in this VM
          if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
            logger.trace(LogMarker.DLS_VERBOSE,
                "[DLockRecoverGrantorMessage.process] locally process reply");
          }
          replyMsg.setSender(dm.getId());
          replyMsg.dmProcess(dm);
        } else {
          if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
            logger.trace(LogMarker.DLS_VERBOSE, "[DLockRecoverGrantorMessage.process] send reply");
          }
          dm.putOutgoing(replyMsg);
        }
      }
    }
  }
}
