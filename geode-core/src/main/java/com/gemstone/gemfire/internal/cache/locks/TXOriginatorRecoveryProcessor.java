/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gemstone.gemfire.internal.cache.locks;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.MessageWithReply;
import com.gemstone.gemfire.distributed.internal.PooledDistributionMessage;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.locks.DLockGrantor;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.TXCommitMessage;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;

/**
 * Sends <code>TXOriginatorRecoveryMessage</code> to all participants of
 * a given transaction when the originator departs. The participants delay
 * reply until the commit has finished. Once all replies have come in, the
 * transaction lock (<code>TXLockId</code>) will be released.
 *
 */
public class TXOriginatorRecoveryProcessor extends ReplyProcessor21  {

  private static final Logger logger = LogService.getLogger();
  
  static void sendMessage(Set members,
                          InternalDistributedMember originator,
                          TXLockId txLockId,
                          DLockGrantor grantor,
                          DM dm) {
    TXOriginatorRecoveryProcessor processor = 
        new TXOriginatorRecoveryProcessor(dm, members);

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
      if (msg.getSender() == null) msg.setSender(dm.getId());
      msg.process((DistributionManager)dm);
    }
        
    // keep waiting even if interrupted
    // for() loop removed for bug 36983 - you can't loop on waitForReplies()
      dm.getCancelCriterion().checkCancelInProgress(null);
      try { 
        processor.waitForRepliesUninterruptibly();
      }
      catch (ReplyException e) {
        e.handleAsUnexpected();
      }
    
    // release txLockId...
    if (logger.isDebugEnabled()) {
      logger.debug("TXOriginatorRecoveryProcessor releasing: {}", txLockId);
    }
    //dtls.release(txLockId);
    try {
      grantor.releaseLockBatch(txLockId, originator);
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
  
  // ------------------------------------------------------------------------- 
  //   Constructors
  // -------------------------------------------------------------------------  
  
  /** Creates a new instance of TXOriginatorRecoveryProcessor */
  private TXOriginatorRecoveryProcessor(DM dm, 
                                        Set members) {
    super(dm, members);
  }
  
  @Override
  protected boolean allowReplyFromSender() {
    return true;
  }
  
  /**
   * IllegalStateException is an anticipated reply exception.  Receiving
   * multiple replies with this exception is normal.
   */
  @Override
  protected boolean logMultipleExceptions() {
    return false;
  }
  
  // ------------------------------------------------------------------------- 
  //   TXOriginatorRecoveryMessage
  // -------------------------------------------------------------------------  
  public static final class TXOriginatorRecoveryMessage
  extends PooledDistributionMessage 
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
    protected void process(final DistributionManager dm) {
      final TXOriginatorRecoveryMessage msg = this;

      try {
        dm.getWaitingThreadPool().execute(new Runnable() {
          public void run() {
            processTXOriginatorRecoveryMessage(dm, msg);
          }
        });
      }
      catch (RejectedExecutionException e) {
        logger.debug("Rejected processing of <{}>", msg, e);
        }
    }
    
    protected void processTXOriginatorRecoveryMessage(
        final DistributionManager dm,
        final TXOriginatorRecoveryMessage msg) {
          
      ReplyException replyException = null;
      logger.info(LocalizedMessage.create(LocalizedStrings.TXOriginatorRecoveryProcessor_PROCESSTXORIGINATORRECOVERYMESSAGE));
      try {
        // Wait for the transaction associated with this lockid to finish processing
        TXCommitMessage.getTracker().waitToProcess(msg.txLockId, dm);

        // when the grantor receives reply it will release txLock...
        /* TODO: implement waitToReleaseTXLockId here
           testTXOriginatorRecoveryProcessor in 
           com.gemstone.gemfire.internal.cache.locks.TXLockServiceTest
           should be expanded upon also...
        */
      }
      catch (RuntimeException t) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.TXOriginatorRecoveryProcessor_PROCESSTXORIGINATORRECOVERYMESSAGE_THROWABLE), t);
//        if (replyException == null) (can only be null) 
        {
          replyException = new ReplyException(t);
        }
//        else {
//          log.warning(LocalizedStrings.TXOriginatorRecoveryProcessor_MORE_THAN_ONE_EXCEPTION_THROWN_IN__0, this, t);
//        }        
//      }
//      catch (VirtualMachineError err) {
//        SystemFailure.initiateFailure(err);
//        // If this ever returns, rethrow the error.  We're poisoned
//        // now, so don't let this thread continue.
//        throw err;
//      }
//      catch (Throwable t) {
//        // Whenever you catch Error or Throwable, you must also
//        // catch VirtualMachineError (see above).  However, there is
//        // _still_ a possibility that you are dealing with a cascading
//        // error condition, so you also need to check to see if the JVM
//        // is still usable:
//        SystemFailure.checkFailure();
//        if (replyException == null) {
//          replyException = new ReplyException(t);
//        }
//      }
//      catch (VirtualMachineError err) {
//        SystemFailure.initiateFailure(err);
//        // If this ever returns, rethrow the error.  We're poisoned
//        // now, so don't let this thread continue.
//        throw err;
//      }
//      catch (Throwable t) {
//        // Whenever you catch Error or Throwable, you must also
//        // catch VirtualMachineError (see above).  However, there is
//        // _still_ a possibility that you are dealing with a cascading
//        // error condition, so you also need to check to see if the JVM
//        // is still usable:
//        SystemFailure.checkFailure();
//        if (replyException == null) {
//          replyException = new ReplyException(t);
//        }
//      }
      }
      finally {
        TXOriginatorRecoveryReplyMessage replyMsg = 
            new TXOriginatorRecoveryReplyMessage();
        replyMsg.txLockId = txLockId;
        replyMsg.setProcessorId(getProcessorId());
        replyMsg.setRecipient(getSender());
        replyMsg.setException(replyException);
        
        if (getSender().equals(dm.getId())) {
          // process in-line in this VM
          logger.info(LocalizedMessage.create(LocalizedStrings.TXOriginatorRecoveryProcessor_PROCESSTXORIGINATORRECOVERYMESSAGE_LOCALLY_PROCESS_REPLY));
          replyMsg.setSender(dm.getId());
          replyMsg.dmProcess(dm);
        }
        else {
          logger.info(LocalizedMessage.create(LocalizedStrings.TXOriginatorRecoveryProcessor_PROCESSTXORIGINATORRECOVERYMESSAGE_SEND_REPLY));
          dm.putOutgoing(replyMsg);
        }
      }
    }
    
    public int getDSFID() {
      return TX_ORIGINATOR_RECOVERY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.txLockId = (TXLockId) DataSerializer.readObject(in);
      this.processorId = in.readInt();
    }
    
    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
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
  //   TXOriginatorRecoveryReplyMessage
  // -------------------------------------------------------------------------  
  public static final class TXOriginatorRecoveryReplyMessage 
  extends ReplyMessage  {
    
    /** The transaction lock for which the originator orphaned */
    protected TXLockId txLockId; // only for the toString
    
    @Override
    public int getDSFID() {
      return TX_ORIGINATOR_RECOVERY_REPLY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
      super.fromData(in);
    }
    
    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
    }

    @Override
    public String toString() {
      return
          "TXOriginatorRecoveryReplyMessage (processorId=" + super.processorId + 
          "; txLockId=" + this.txLockId + "; sender=" + getSender() + ")";
    }
  }
  
}

