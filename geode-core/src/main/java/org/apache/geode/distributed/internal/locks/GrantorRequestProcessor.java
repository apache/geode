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
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.PooledDistributionMessage;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.util.concurrent.StoppableCondition;
import org.apache.geode.internal.util.concurrent.StoppableReentrantLock;

/**
 * A processor for sending a message to the elder asking it for the grantor of a dlock service.
 *
 * @since GemFire 4.0
 */
public class GrantorRequestProcessor extends ReplyProcessor21 {
  private static final Logger logger = LogService.getLogger();

  private GrantorInfo result;

  ////////// Public static entry point /////////

  /**
   * The number of milliseconds to sleep for elder change if current elder is departing (and already
   * sent shutdown msg) but is still in the View.
   */
  public static final long ELDER_CHANGE_SLEEP =
      Long.getLong("GrantorRequestProcessor.ELDER_CHANGE_SLEEP", 100).longValue();

  private static final byte GET_OP = 0;
  private static final byte BECOME_OP = 1;
  private static final byte CLEAR_OP = 2;
  private static final byte PEEK_OP = 3;
  private static final byte CLEAR_WITH_LOCKS_OP = 4;

  private static final GrantorInfo CLEAR_COMPLETE = new GrantorInfo(null, 0, 0, false);

  /**
   * Encapsulates the context necessary for processing a given grantor request for a given
   * InternalDistributedSystem
   *
   */
  public static class GrantorRequestContext {
    /**
     * Locks access to elders
     */
    final StoppableReentrantLock elderLock;

    /**
     * Subservient condition to {@link #elderLock}
     */
    final StoppableCondition elderLockCondition;

    /**
     * Our notion of the current elder
     *
     * guarded.By {@link #elderLock}
     */
    InternalDistributedMember currentElder = null;

    /**
     * Count of the elder calls in-flight
     *
     * guarded.By {@link #elderLock}
     */
    int elderCallsInProgress = 0;

    /**
     * If true, we're cooling our heels waiting for the elders to pass the baton
     *
     * guarded.By {@link #elderLock}
     */
    boolean waitingToChangeElder = false;

    public GrantorRequestContext(CancelCriterion cancelCriterion) {
      elderLock = new StoppableReentrantLock(cancelCriterion);
      elderLockCondition = elderLock.newCondition();
    }
  }

  private static boolean basicStartElderCall(InternalDistributedSystem sys, ElderState es,
      InternalDistributedMember elder, DLockService dls) {
    GrantorRequestContext grc = sys.getGrantorRequestContext();
    grc.elderLock.lock();
    try {
      if (es != null) {
        // elder is in our vm
        if (grc.elderCallsInProgress > 0) {
          // wait until all the calls in progress to an old rmt elder complete.
          // We know it is some other elder because we don't count the
          // calls in progress to a local elder.
          elderSyncWait(sys, elder, dls);
        }
      } else {
        // elder is in remote vm
        if (grc.elderCallsInProgress > 0) {
          if (elder == grc.currentElder) {
            grc.elderCallsInProgress += 1;
          } else if (elder != null && elder.equals(grc.currentElder)) {
            grc.elderCallsInProgress += 1;
          } else {
            elderSyncWait(sys, elder, dls);
            return false;
          }
        } else {
          grc.currentElder = elder;
          grc.elderCallsInProgress = 1;
        }
      }
      return true;
    } finally {
      grc.elderLock.unlock();
    }
  }

  /**
   * Waits until elder recovery can proceed safely. Currently this is done by waiting until any in
   * progress calls to an old elder are complete
   *
   * @param elderId the member id of the new elder; null if new elder is local
   */
  static void readyForElderRecovery(InternalDistributedSystem sys,
      InternalDistributedMember elderId, DLockService dls) {
    GrantorRequestContext grc = sys.getGrantorRequestContext();
    if (elderId != null) {
      grc.elderLock.lock();
      try {
        if (grc.elderCallsInProgress > 0) {
          // make sure they are not going to the new elder
          if (elderId != grc.currentElder && !elderId.equals(grc.currentElder)) {
            elderSyncWait(sys, elderId, dls);
          }
        }
      } finally {
        grc.elderLock.unlock();
      }
    } else {
      grc.elderLock.lock();
      try {
        if (grc.elderCallsInProgress > 0) {
          // wait until all the calls in progress to an old rmt elder complete.
          // We know it is some other elder because we don't count the
          // calls in progress to a local elder.
          elderSyncWait(sys, /* elderId */ null, dls);
        }
      } finally {
        grc.elderLock.unlock();
      }
    }
  }

  private static void elderSyncWait(InternalDistributedSystem sys,
      InternalDistributedMember newElder, DLockService dls) {
    GrantorRequestContext grc = sys.getGrantorRequestContext();
    grc.waitingToChangeElder = true;
    final String message = String.format(
        "GrantorRequestProcessor.elderSyncWait: The current Elder %s is waiting for the new Elder %s.",
        new Object[] {grc.currentElder, newElder});
    while (grc.waitingToChangeElder) {
      logger.info(LogMarker.DLS_MARKER, message);
      boolean interrupted = Thread.interrupted();
      try {
        grc.elderLockCondition.await(sys.getConfig().getMemberTimeout());
      } catch (InterruptedException ignore) {
        interrupted = true;
        sys.getCancelCriterion().checkCancelInProgress(ignore);
      } finally {
        if (interrupted)
          Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Sets currentElder to the memberId of the current elder if elder is remote; null if elder is in
   * our vm.
   */
  private static ElderState startElderCall(InternalDistributedSystem sys, DLockService dls) {
    InternalDistributedMember elder;
    ElderState es = null;

    final DistributionManager dm = sys.getDistributionManager();
    boolean elderCallStarted = false;
    while (!elderCallStarted) {
      dm.throwIfDistributionStopped();
      elder = dm.getElderId(); // call this before getElderState
      Assert.assertTrue(elder != null, "starting an elder call with no valid elder");
      if (dm.getId().equals(elder)) {
        try {
          es = dm.getElderState(false);
        } catch (IllegalStateException e) {
          // loop back around to reacquire Collaboration and try elder lock again
          continue;
        }
      } else {
        es = null;
      }
      elderCallStarted = basicStartElderCall(sys, es, elder, dls);
    }

    return es;
  }

  private static void finishElderCall(GrantorRequestContext grc, ElderState es) {
    if (es == null) {
      grc.elderLock.lock();
      try {
        Assert.assertTrue(grc.elderCallsInProgress > 0);
        grc.elderCallsInProgress -= 1;
        if (grc.elderCallsInProgress == 0) {
          grc.currentElder = null;
          if (grc.waitingToChangeElder) {
            grc.waitingToChangeElder = false;
            grc.elderLockCondition.signalAll();
          }
        }
      } finally {
        grc.elderLock.unlock();
      }
    }
  }

  /**
   * Asks the elder who the grantor is for the specified service. If no grantor exists then makes us
   * the grantor.
   *
   * @param service the service we want to know the grantor of.
   * @param sys the distributed system
   * @return information describing the current grantor of this service and if it needs recovery.
   */
  public static GrantorInfo getGrantor(DLockService service, int dlsSerialNumber,
      InternalDistributedSystem sys) {
    return basicOp(-1, service, dlsSerialNumber, sys, null, GET_OP);
  }

  /**
   * Asks the elder who the grantor is for the specified service.
   *
   * @param service the service we want to know the grantor of.
   * @param sys th distributed system
   * @return information describing the current grantor of this service and if recovery is needed
   */
  static GrantorInfo peekGrantor(DLockService service, InternalDistributedSystem sys) {
    return basicOp(-1, service, -1, sys, null, PEEK_OP);
  }

  static GrantorInfo peekGrantor(String serviceName, InternalDistributedSystem sys) {
    return basicOp(-1, serviceName, null, -1, sys, null, PEEK_OP);
  }

  /**
   * Tells the elder we want to become the grantor
   *
   * @param service the service we want to be the grantor of.
   * @param oldTurk if non-null then only become grantor if it is currently oldTurk.
   * @param sys the distributed system
   * @return information describing the previous grantor, if any, and if we need to do a grantor
   *         recovery
   */
  static GrantorInfo becomeGrantor(DLockService service, int dlsSerialNumber,
      InternalDistributedMember oldTurk, InternalDistributedSystem sys) {
    return basicOp(-1, service, dlsSerialNumber, sys, oldTurk, BECOME_OP);
  }

  /**
   * Tells the elder we are doing a clean destroy of our grantor
   *
   * @param service the service we are no longer the grantor of.
   * @param sys the distributed system
   */
  static void clearGrantor(long grantorVersion, DLockService service, int dlsSerialNumber,
      InternalDistributedSystem sys, boolean withLocks) {
    basicOp(grantorVersion, service, dlsSerialNumber, sys, null,
        withLocks ? CLEAR_WITH_LOCKS_OP : CLEAR_OP);
  }

  /**
   * @param opCode encodes what operation we are doing
   */
  private static GrantorInfo basicOp(long grantorVersion, DLockService service, int dlsSerialNumber,
      InternalDistributedSystem sys, InternalDistributedMember oldTurk, byte opCode) {
    return basicOp(grantorVersion, service.getName(), service, dlsSerialNumber, sys, oldTurk,
        opCode);
  }

  private static GrantorInfo basicOp(long grantorVersion, String serviceName, DLockService service,
      int dlsSerialNumber, InternalDistributedSystem system, InternalDistributedMember oldTurk,
      byte opCode) {
    GrantorInfo result = null;
    DistributionManager dm = system.getDistributionManager();
    GrantorRequestContext grc = system.getGrantorRequestContext();
    boolean tryNewElder;
    boolean interrupted = false;
    try {
      do {
        tryNewElder = false;
        final ElderState es = startElderCall(system, service);
        dm.throwIfDistributionStopped();
        try {
          if (es != null) {
            // local elder so do it without messaging
            switch (opCode) {
              case GET_OP:
                result = es.getGrantor(serviceName, dm.getId(), dlsSerialNumber);
                break;
              case PEEK_OP:
                result = es.peekGrantor(serviceName);
                break;
              case BECOME_OP:
                result = es.becomeGrantor(serviceName, dm.getId(), dlsSerialNumber, oldTurk);
                break;
              case CLEAR_OP:
                es.clearGrantor(grantorVersion, serviceName, dlsSerialNumber, dm.getId(), false);
                result = CLEAR_COMPLETE;
                break;
              case CLEAR_WITH_LOCKS_OP:
                es.clearGrantor(grantorVersion, serviceName, dlsSerialNumber, dm.getId(), true);
                result = CLEAR_COMPLETE;
                break;
              default:
                throw new IllegalStateException("Unknown opCode " + opCode);
            }
          } else {
            // remote elder so send message
            GrantorRequestProcessor processor =
                new GrantorRequestProcessor(system, grc.currentElder);
            boolean sent = GrantorRequestMessage.send(grantorVersion, dlsSerialNumber, serviceName,
                grc.currentElder, dm, processor, oldTurk, opCode);
            if (!sent) {
              if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
                logger.trace(LogMarker.DLS_VERBOSE, "Unable to communicate with elder {}",
                    grc.currentElder);
              }
            }
            try {
              processor.waitForRepliesUninterruptibly();
            } catch (ReplyException e) {
              e.handleCause();
            }
            if (processor.result != null) {
              result = processor.result;
            } else {
              // no result and no longer waiting...

              // sleep if targeted elder still in view but not activeMembers
              if (!dm.getDistributionManagerIds().contains(grc.currentElder)
                  && dm.getViewMembers().contains(grc.currentElder)) {
                // if true then elder no longer in DM activeMembers
                // but elder is still in the View
                // elder probably sent shutdown msg but may not yet left View
                try {
                  Thread.sleep(ELDER_CHANGE_SLEEP);
                } catch (InterruptedException e) {
                  interrupted = true;
                  dm.getCancelCriterion().checkCancelInProgress(e);
                }
              }

              // targetted elder either died or already sent us a shutdown msg
              if (opCode != CLEAR_OP && opCode != CLEAR_WITH_LOCKS_OP) {
                // Note we do not try a new elder if doing a clear because
                // the new elder will not have anything for us to clear.
                // He will have done an ElderInit.
                tryNewElder = true;
              }
            }
          }
        } finally {
          finishElderCall(grc, es);
        }
      } while (tryNewElder);
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
    return result;
  }
  //////////// Instance methods //////////////

  /**
   * Creates a new instance of GrantorRequestProcessor
   */
  private GrantorRequestProcessor(InternalDistributedSystem system,
      InternalDistributedMember elder) {
    super(system, elder);
  }

  @Override
  public void process(DistributionMessage msg) {
    if (msg instanceof GrantorInfoReplyMessage) {
      GrantorInfoReplyMessage giMsg = (GrantorInfoReplyMessage) msg;
      this.result = giMsg.getGrantorInfo();
    } else if (msg instanceof ReplyMessage) {
      if (((ReplyMessage) msg).getException() == null) {
        // must be a reply sent back from a CLEAR_OP
        this.result = CLEAR_COMPLETE;
      }
    } else {
      Assert.assertTrue(false,
          "Expected instance of GrantorInfoReplyMessage or CReplyMessage but got "
              + msg.getClass());
    }
    super.process(msg);
  }

  /////////////// Inner message classes //////////////////

  public static class GrantorRequestMessage extends PooledDistributionMessage
      implements MessageWithReply {
    private long grantorVersion;
    private int dlsSerialNumber;
    private String serviceName;
    private int processorId;
    private byte opCode;
    private InternalDistributedMember oldTurk;

    /**
     * @return true if the message was sent
     */
    protected static boolean send(long grantorVersion, int dlsSerialNumber, String serviceName,
        InternalDistributedMember elder, DistributionManager dm, ReplyProcessor21 proc,
        InternalDistributedMember oldTurk, byte opCode) {

      GrantorRequestMessage msg = new GrantorRequestMessage();
      msg.grantorVersion = grantorVersion;
      msg.dlsSerialNumber = dlsSerialNumber;
      msg.serviceName = serviceName;
      msg.oldTurk = oldTurk;
      msg.opCode = opCode;
      msg.processorId = proc.getProcessorId();
      msg.setRecipient(elder);
      if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
        logger.trace(LogMarker.DLS_VERBOSE, "GrantorRequestMessage sending {} to {}", msg, elder);
      }
      Set failures = dm.putOutgoing(msg);
      return failures == null || failures.size() == 0;
    }

    @Override
    public int getProcessorId() {
      return this.processorId;
    }

    private void replyGrantorInfo(DistributionManager dm, GrantorInfo gi) {
      GrantorInfoReplyMessage.send(this, dm, gi);
    }

    private void replyClear(DistributionManager dm) {
      ReplyMessage.send(this.getSender(), this.getProcessorId(), null, dm);
    }

    @Override
    protected void process(ClusterDistributionManager dm) {
      basicProcess(dm);
    }

    protected void basicProcess(final DistributionManager dm) {
      // we should be in the elder
      ElderState es = dm.getElderState(true);
      switch (this.opCode) {
        case GET_OP:
          replyGrantorInfo(dm, es.getGrantor(this.serviceName, getSender(), this.dlsSerialNumber));
          break;
        case PEEK_OP:
          replyGrantorInfo(dm, es.peekGrantor(this.serviceName));
          break;
        case BECOME_OP:
          replyGrantorInfo(dm,
              es.becomeGrantor(this.serviceName, getSender(), this.dlsSerialNumber, this.oldTurk));
          break;
        case CLEAR_OP:
          es.clearGrantor(this.grantorVersion, this.serviceName, this.dlsSerialNumber, getSender(),
              false);
          replyClear(dm);
          break;
        case CLEAR_WITH_LOCKS_OP:
          es.clearGrantor(this.grantorVersion, this.serviceName, this.dlsSerialNumber, getSender(),
              true);
          replyClear(dm);
          break;
        default:
          throw new IllegalStateException("Unknown opCode " + this.opCode);
      }
    }

    public int getDSFID() {
      return GRANTOR_REQUEST_MESSAGE;
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.grantorVersion = in.readLong();
      this.dlsSerialNumber = in.readInt();
      this.serviceName = DataSerializer.readString(in);
      this.processorId = in.readInt();
      this.opCode = in.readByte();
      if (this.opCode == BECOME_OP) {
        this.oldTurk = (InternalDistributedMember) DataSerializer.readObject(in);
      }
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeLong(this.grantorVersion);
      out.writeInt(this.dlsSerialNumber);
      DataSerializer.writeString(this.serviceName, out);
      out.writeInt(this.processorId);
      out.writeByte(this.opCode);
      if (this.opCode == BECOME_OP) {
        DataSerializer.writeObject(this.oldTurk, out);
      }
    }

    public static String opCodeToString(int opCode) {
      String string = null;
      switch (opCode) {
        case GET_OP:
          string = "GET_OP";
          break;
        case BECOME_OP:
          string = "BECOME_OP";
          break;
        case CLEAR_OP:
          string = "CLEAR_OP";
          break;
        case PEEK_OP:
          string = "PEEK_OP";
          break;
        case CLEAR_WITH_LOCKS_OP:
          string = "CLEAR_WITH_LOCKS_OP";
          break;
        default:
          string = "UNKNOWN:" + String.valueOf(opCode);
          break;
      }
      return string;
    }

    @Override
    public String toString() {
      String opCodeString = opCodeToString(this.opCode);
      StringBuffer buff = new StringBuffer();
      buff.append("GrantorRequestMessage (service='").append(this.serviceName)
          .append("'; grantorVersion=").append(this.grantorVersion).append("'; dlsSerialNumber=")
          .append(this.dlsSerialNumber).append("'; processorId=").append(this.processorId)
          .append("'; opCode=").append(opCodeString).append("'; oldT=").append(this.oldTurk)
          .append(")");
      return buff.toString();
    }
  }

  public static class GrantorInfoReplyMessage extends ReplyMessage {
    private InternalDistributedMember grantor;
    private long elderVersionId;
    private int grantorSerialNumber;
    private boolean needsRecovery;

    public static void send(MessageWithReply reqMsg, DistributionManager dm, GrantorInfo gi) {
      GrantorInfoReplyMessage m = new GrantorInfoReplyMessage();
      m.grantor = gi.getId();
      m.needsRecovery = gi.needsRecovery();
      m.elderVersionId = gi.getVersionId();
      m.grantorSerialNumber = gi.getSerialNumber();
      m.processorId = reqMsg.getProcessorId();
      m.setRecipient(reqMsg.getSender());
      dm.putOutgoing(m);
    }

    public GrantorInfo getGrantorInfo() {
      return new GrantorInfo(this.grantor, this.elderVersionId, this.grantorSerialNumber,
          this.needsRecovery);
    }

    @Override
    public int getDSFID() {
      return GRANTOR_INFO_REPLY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.grantor = (InternalDistributedMember) DataSerializer.readObject(in);
      this.elderVersionId = in.readLong();
      this.grantorSerialNumber = in.readInt();
      this.needsRecovery = in.readBoolean();
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      DataSerializer.writeObject(this.grantor, out);
      out.writeLong(this.elderVersionId);
      out.writeInt(this.grantorSerialNumber);
      out.writeBoolean(this.needsRecovery);
    }

    @Override
    public String toString() {
      StringBuffer buff = new StringBuffer();
      buff.append("GrantorInfoReplyMessage").append("; sender=").append(getSender())
          .append("; processorId=").append(super.processorId).append("; grantor=")
          .append(this.grantor).append("; elderVersionId=").append(this.elderVersionId)
          .append("; grantorSerialNumber=").append(this.grantorSerialNumber)
          .append("; needsRecovery=").append(this.needsRecovery).append(")");
      return buff.toString();
    }
  }
}
