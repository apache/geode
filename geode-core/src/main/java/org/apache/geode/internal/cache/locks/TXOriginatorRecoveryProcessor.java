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

package org.apache.geode.internal.cache.locks;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.PooledDistributionMessage;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.locks.DLockGrantor;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.TXCommitMessage;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * Sends <code>TXOriginatorRecoveryMessage</code> to all participants of a given transaction when
 * the originator departs. The participants delay reply until the commit has finished. Once all
 * replies have come in, the transaction lock (<code>TXLockId</code>) will be released.
 *
 */
public class TXOriginatorRecoveryProcessor extends ReplyProcessor21 {

  private static final Logger logger = LogService.getLogger();

  static void sendMessage(Set members, InternalDistributedMember originator, TXLockId txLockId,
      DLockGrantor grantor, DistributionManager dm) {
    TXOriginatorRecoveryProcessor processor = new TXOriginatorRecoveryProcessor(dm, members);

    TXOriginatorRecoveryMessage msg = new TXOriginatorRecoveryMessage();
    msg.processorId = processor.getProcessorId();
    msg.txLockId = txLockId;

    // send msg to all members EXCEPT this member...
    Set recipients = new HashSet(members);
    recipients.remove(dm.getId());
    msg.setRecipients(members);
    if (logger.isDebugEnabled()) {
      logger.debug("Sending TXOriginatorRecoveryMessage: {}", msg);
    }
    dm.putOutgoing(msg);

    // process msg and reply directly if this VM is a participant...
    if (members.contains(dm.getId())) {
      if (msg.getSender() == null)
        msg.setSender(dm.getId());
      msg.process((ClusterDistributionManager) dm);
    }

    // keep waiting even if interrupted
    // for() loop removed for bug 36983 - you can't loop on waitForReplies()
    dm.getCancelCriterion().checkCancelInProgress(null);
    try {
      processor.waitForRepliesUninterruptibly();
    } catch (ReplyException e) {
      e.handleCause();
    }

    // release txLockId...
    if (logger.isDebugEnabled()) {
      logger.debug("TXOriginatorRecoveryProcessor releasing: {}", txLockId);
    }
    // dtls.release(txLockId);
    try {
      grantor.releaseLockBatch(txLockId, originator);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  // -------------------------------------------------------------------------
  // Constructors
  // -------------------------------------------------------------------------

  /** Creates a new instance of TXOriginatorRecoveryProcessor */
  private TXOriginatorRecoveryProcessor(DistributionManager dm, Set members) {
    super(dm, members);
  }

  @Override
  protected boolean allowReplyFromSender() {
    return true;
  }

  /**
   * IllegalStateException is an anticipated reply exception. Receiving multiple replies with this
   * exception is normal.
   */
  @Override
  protected boolean logMultipleExceptions() {
    return false;
  }

  // -------------------------------------------------------------------------
  // TXOriginatorRecoveryMessage
  // -------------------------------------------------------------------------
  public static class TXOriginatorRecoveryMessage extends PooledDistributionMessage
      implements MessageWithReply {

    /** The transaction lock for which the originator orphaned */
    protected TXLockId txLockId;

    /** The reply processor to route replies to */
    protected int processorId;

    @Override
    public int getProcessorId() {
      return this.processorId;
    }

    @Override
    protected void process(final ClusterDistributionManager dm) {
      final TXOriginatorRecoveryMessage msg = this;

      try {
        dm.getExecutors().getWaitingThreadPool().execute(new Runnable() {
          @Override
          public void run() {
            processTXOriginatorRecoveryMessage(dm, msg);
          }
        });
      } catch (RejectedExecutionException e) {
        logger.debug("Rejected processing of <{}>", msg, e);
      }
    }

    protected void processTXOriginatorRecoveryMessage(final ClusterDistributionManager dm,
        final TXOriginatorRecoveryMessage msg) {

      ReplyException replyException = null;
      logger.info("[processTXOriginatorRecoveryMessage]");
      try {
        // Wait for the transaction associated with this lockid to finish processing
        TXCommitMessage.getTracker().waitToProcess(msg.txLockId, dm);

        // when the grantor receives reply it will release txLock...
        /*
         * TODO: implement waitToReleaseTXLockId here testTXOriginatorRecoveryProcessor in
         * org.apache.geode.internal.cache.locks.TXLockServiceTest should be expanded upon also...
         */
      } catch (RuntimeException t) {
        logger.warn("[processTXOriginatorRecoveryMessage] throwable:",
            t);
        // if (replyException == null) (can only be null)
        {
          replyException = new ReplyException(t);
        }
        // else {
        // log.warning(LocalizedStrings.TXOriginatorRecoveryProcessor_MORE_THAN_ONE_EXCEPTION_THROWN_IN__0,
        // this, t);
        // }
        // }
        // catch (VirtualMachineError err) {
        // SystemFailure.initiateFailure(err);
        // // If this ever returns, rethrow the error. We're poisoned
        // // now, so don't let this thread continue.
        // throw err;
        // }
        // catch (Throwable t) {
        // // Whenever you catch Error or Throwable, you must also
        // // catch VirtualMachineError (see above). However, there is
        // // _still_ a possibility that you are dealing with a cascading
        // // error condition, so you also need to check to see if the JVM
        // // is still usable:
        // SystemFailure.checkFailure();
        // if (replyException == null) {
        // replyException = new ReplyException(t);
        // }
        // }
        // catch (VirtualMachineError err) {
        // SystemFailure.initiateFailure(err);
        // // If this ever returns, rethrow the error. We're poisoned
        // // now, so don't let this thread continue.
        // throw err;
        // }
        // catch (Throwable t) {
        // // Whenever you catch Error or Throwable, you must also
        // // catch VirtualMachineError (see above). However, there is
        // // _still_ a possibility that you are dealing with a cascading
        // // error condition, so you also need to check to see if the JVM
        // // is still usable:
        // SystemFailure.checkFailure();
        // if (replyException == null) {
        // replyException = new ReplyException(t);
        // }
        // }
      } finally {
        TXOriginatorRecoveryReplyMessage replyMsg = new TXOriginatorRecoveryReplyMessage();
        replyMsg.txLockId = txLockId;
        replyMsg.setProcessorId(getProcessorId());
        replyMsg.setRecipient(getSender());
        replyMsg.setException(replyException);

        if (getSender().equals(dm.getId())) {
          // process in-line in this VM
          logger.info("[processTXOriginatorRecoveryMessage] locally process reply");
          replyMsg.setSender(dm.getId());
          replyMsg.dmProcess(dm);
        } else {
          logger.info("[processTXOriginatorRecoveryMessage] send reply");
          dm.putOutgoing(replyMsg);
        }
      }
    }

    @Override
    public int getDSFID() {
      return TX_ORIGINATOR_RECOVERY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      this.txLockId = (TXLockId) DataSerializer.readObject(in);
      this.processorId = in.readInt();
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      DataSerializer.writeObject(this.txLockId, out);
      out.writeInt(this.processorId);
    }

    @Override
    public String toString() {
      StringBuffer buff = new StringBuffer();
      buff.append("TXOriginatorRecoveryMessage (txLockId='");
      buff.append(this.txLockId);
      buff.append("'; processorId=");
      buff.append(this.processorId);
      buff.append(")");
      return buff.toString();
    }
  }

  // -------------------------------------------------------------------------
  // TXOriginatorRecoveryReplyMessage
  // -------------------------------------------------------------------------
  public static class TXOriginatorRecoveryReplyMessage extends ReplyMessage {

    /** The transaction lock for which the originator orphaned */
    protected TXLockId txLockId; // only for the toString

    @Override
    public int getDSFID() {
      return TX_ORIGINATOR_RECOVERY_REPLY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
    }

    @Override
    public String toString() {
      return "TXOriginatorRecoveryReplyMessage (processorId=" + super.processorId + "; txLockId="
          + this.txLockId + "; sender=" + getSender() + ")";
    }
  }

}
