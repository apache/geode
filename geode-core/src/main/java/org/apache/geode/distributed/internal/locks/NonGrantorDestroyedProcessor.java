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

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.LockServiceDestroyedException;
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
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;

/**
 * A processor for telling the grantor that a lock service participant has shutdown. The grantor
 * should release all locks which are currently held by the calling member.
 *
 * @since GemFire 4.0
 */
public class NonGrantorDestroyedProcessor extends ReplyProcessor21 {
  private static final Logger logger = LogService.getLogger();

  private NonGrantorDestroyedReplyMessage reply;

  ////////// Public static entry point /////////

  /**
   *
   * Send a message to grantor telling it that we've shutdown the named lock service for this
   * member.
   * <p>
   * Caller should loop, getting the grantor, calling <code>send</code>, and checking
   * <code>informedGrantor()</code> until the grantor has acknowledged being informed.
   */
  static boolean send(String serviceName, LockGrantorId theLockGrantorId, DistributionManager dm) {
    InternalDistributedMember recipient = theLockGrantorId.getLockGrantorMember();
    NonGrantorDestroyedProcessor processor = new NonGrantorDestroyedProcessor(dm, recipient);
    NonGrantorDestroyedMessage.send(serviceName, recipient, dm, processor);
    try {
      processor.waitForRepliesUninterruptibly();
    } catch (ReplyException e) {
      e.handleCause();
    }
    return processor.informedGrantor();
  }

  //////////// Instance methods //////////////

  /** Creates a new instance of NonGrantorDestroyedProcessor */
  private NonGrantorDestroyedProcessor(DistributionManager dm, InternalDistributedMember grantor) {
    super(dm, grantor);
  }

  @Override
  public void process(DistributionMessage msg) {
    try {
      Assert.assertTrue(msg instanceof NonGrantorDestroyedReplyMessage,
          "NonGrantorDestroyedProcessor is unable to process message of type " + msg.getClass());

      this.reply = (NonGrantorDestroyedReplyMessage) msg;
    } finally {
      super.process(msg);
    }
  }

  /** Returns true if the grantor acknowledged the msg with OK */
  public boolean informedGrantor() {
    return this.reply != null && this.reply.isOK();
  }

  @Override
  protected boolean allowReplyFromSender() {
    return true;
  }

  /////////////// Inner message classes //////////////////

  public static class NonGrantorDestroyedMessage extends PooledDistributionMessage
      implements MessageWithReply {

    private int processorId;

    /** The name of the DistributedLockService */
    private String serviceName;

    protected static void send(String serviceName, InternalDistributedMember grantor,
        DistributionManager dm, ReplyProcessor21 proc) {
      Assert.assertTrue(grantor != null, "Cannot send NonGrantorDestroyedMessage to null grantor");

      NonGrantorDestroyedMessage msg = new NonGrantorDestroyedMessage();
      msg.serviceName = serviceName;
      msg.processorId = proc.getProcessorId();
      msg.setRecipient(grantor);

      if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
        logger.trace(LogMarker.DLS_VERBOSE, "NonGrantorDestroyedMessage sending {} to {}", msg,
            grantor);
      }

      if (grantor.equals(dm.getId())) {
        msg.setSender(dm.getId());
        msg.processLocally(dm);
      } else {
        dm.putOutgoing(msg);
      }
    }

    @Override
    public int getProcessorId() {
      return this.processorId;
    }

    private void reply(byte replyCode, DistributionManager dm) {
      NonGrantorDestroyedReplyMessage.send(this, replyCode, dm);
    }

    @Override
    protected void process(ClusterDistributionManager dm) {
      basicProcess(dm);
    }

    /** Process locally without using messaging */
    protected void processLocally(final DistributionManager dm) {
      basicProcess(dm);
    }

    /** Perform basic processing of this message */
    private void basicProcess(final DistributionManager dm) {
      boolean replied = false;
      try {
        DLockService svc = DLockService.getInternalServiceNamed(this.serviceName);
        if (svc != null && svc.isCurrentlyOrIsMakingLockGrantor()) {
          DLockGrantor grantor = DLockGrantor.waitForGrantor(svc);
          if (grantor != null) {
            grantor.handleDepartureOf(getSender());
            if (!grantor.isDestroyed()) {
              reply(NonGrantorDestroyedReplyMessage.OK, dm);
              replied = true;
            }
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
          logger.trace(LogMarker.DLS_VERBOSE,
              "Processing of NonGrantorDestroyedMessage resulted in InterruptedException", e);
        }
      } catch (LockServiceDestroyedException e) {
        if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
          logger.trace(LogMarker.DLS_VERBOSE,
              "Processing of NonGrantorDestroyedMessage resulted in LockServiceDestroyedException",
              e);
        }
      } catch (LockGrantorDestroyedException e) {
        if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
          logger.trace(LogMarker.DLS_VERBOSE,
              "Processing of NonGrantorDestroyedMessage resulted in LockGrantorDestroyedException",
              e);
        }
      } finally {
        if (!replied) {
          reply(NonGrantorDestroyedReplyMessage.NOT_GRANTOR, dm);
        }
      }
    }

    public int getDSFID() {
      return NON_GRANTOR_DESTROYED_MESSAGE;
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.processorId = in.readInt();
      this.serviceName = DataSerializer.readString(in);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeInt(this.processorId);
      DataSerializer.writeString(this.serviceName, out);
    }

    @Override
    public String toString() {
      StringBuffer buff = new StringBuffer();
      buff.append("NonGrantorDestroyedMessage (serviceName='").append(this.serviceName)
          .append("' processorId=").append(this.processorId).append(")");
      return buff.toString();
    }
  }

  public static class NonGrantorDestroyedReplyMessage extends ReplyMessage {

    public static final byte OK = 0;
    public static final byte NOT_GRANTOR = 1;

    private byte replyCode;

    public static void send(MessageWithReply destroyedMsg, byte replyCode, DistributionManager dm) {
      NonGrantorDestroyedReplyMessage m = new NonGrantorDestroyedReplyMessage();
      m.processorId = destroyedMsg.getProcessorId();
      m.setRecipient(destroyedMsg.getSender());
      m.replyCode = replyCode;

      if (dm.getId().equals(destroyedMsg.getSender())) {
        m.setSender(destroyedMsg.getSender());
        m.dmProcess(dm);
      } else {
        dm.putOutgoing(m);
      }
    }

    public boolean isOK() {
      return this.replyCode == OK;
    }

    public static String replyCodeToString(int replyCode) {
      String s = null;
      switch (replyCode) {
        case OK:
          s = "OK";
          break;
        case NOT_GRANTOR:
          s = "NOT_GRANTOR";
          break;
        default:
          s = "UNKNOWN:" + String.valueOf(replyCode);
          break;
      }
      return s;
    }

    @Override
    public int getDSFID() {
      return NON_GRANTOR_DESTROYED_REPLY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.replyCode = in.readByte();
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeByte(this.replyCode);
    }

    @Override
    public String toString() {
      StringBuffer buff = new StringBuffer();
      buff.append("NonGrantorDestroyedReplyMessage").append("; sender=").append(getSender())
          .append("; processorId=").append(super.processorId).append("; replyCode=")
          .append(replyCodeToString(this.replyCode)).append(")");
      return buff.toString();
    }
  }
}
