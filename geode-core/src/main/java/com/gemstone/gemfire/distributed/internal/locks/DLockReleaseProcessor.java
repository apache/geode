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

package com.gemstone.gemfire.distributed.internal.locks;
  
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.distributed.LockServiceDestroyedException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.HighPriorityDistributionMessage;
import com.gemstone.gemfire.distributed.internal.MessageWithReply;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;

/**
 * Synchronously releases a lock.
 *
 */
public class DLockReleaseProcessor
extends ReplyProcessor21 {
  private static final Logger logger = LogService.getLogger();

  private DLockReleaseReplyMessage reply;
  
  protected Object objectName;
  
  public DLockReleaseProcessor(DM dm,
                               InternalDistributedMember member,
                               String serviceName,
                               Object objectName) {
    super(dm, member);
    this.objectName = objectName;
  }
  
  /** Returns true if release was acknowledged by the grantor; false means
   *  we targeted someone who is not the grantor */
  protected boolean release(InternalDistributedMember grantor,
                            String serviceName, 
                            boolean lockBatch,
                            int lockId) {

    DM dm = getDistributionManager();
    DLockReleaseMessage msg = new DLockReleaseMessage();
    msg.processorId = getProcessorId();
    msg.serviceName = serviceName;
    msg.objectName = this.objectName;
    msg.lockBatch = lockBatch;
    msg.lockId = lockId;
    
    msg.setRecipient(grantor);
    if (grantor.equals(dm.getId())) {
      // local... don't message...
      msg.setSender(grantor);
      msg.processLocally(dm);
    }
    else {
      dm.putOutgoing(msg);
    }

    // keep waiting even if interrupted
    try { 
      waitForRepliesUninterruptibly();
    }
    catch (ReplyException e) {
      e.handleAsUnexpected();
    }
    
    if (this.reply == null) return false;
    return this.reply.replyCode == DLockReleaseReplyMessage.OK;
  }
  
  @Override
  protected boolean allowReplyFromSender() {
    return true;
  }
  
  @Override
  public void process(DistributionMessage msg) {
    try {
      Assert.assertTrue(msg instanceof DLockReleaseReplyMessage, 
          "DLockReleaseProcessor is unable to process message of type " +
          msg.getClass());

      DLockReleaseReplyMessage myReply = (DLockReleaseReplyMessage) msg;
      final boolean isDebugEnabled_DLS = logger.isTraceEnabled(LogMarker.DLS);
      if (isDebugEnabled_DLS) {
        logger.trace(LogMarker.DLS, "Handling: {}", myReply);
      }
      this.reply = myReply;
      
      if (isDebugEnabled_DLS) {
        // grantor acknowledged release of lock...
        if (myReply.replyCode == DLockReleaseReplyMessage.OK) {
          logger.trace(LogMarker.DLS, "Successfully released {} in {}", this.objectName, myReply.serviceName);
        }
        // sender denies being the grantor...
        else if (myReply.replyCode == DLockReleaseReplyMessage.NOT_GRANTOR) {
          logger.trace(LogMarker.DLS, "{} has responded DLockReleaseReplyMessage.NOT_GRANTOR for {}", myReply.getSender(), myReply.serviceName);
        }
      }
    }
    finally {
      super.process(msg);
      /*if (this.log.fineEnabled()) {
        this.log.fine("Finished handling: " + msg);
      }*/
    }
  }
  
  // -------------------------------------------------------------------------
  //   DLockReleaseMessage
  // -------------------------------------------------------------------------
  public static final class DLockReleaseMessage 
  extends HighPriorityDistributionMessage
  implements MessageWithReply {
    /** The name of the DistributedLockService */
    protected String serviceName;
  
    /** The object name */
    protected Object objectName;
    
    /** True if objectName identifies a batch of locks */
    protected boolean lockBatch;
    
    /** Id of the processor that will handle replies */
    protected int processorId;
    
    /** Matches up this release with the original lock request */
    protected int lockId;
    
    // set used during processing of this msg...
    protected DLockService svc;
    protected DLockGrantor grantor;
    
    public DLockReleaseMessage() {}
  
    @Override
    public int getProcessorId() {
      return this.processorId;
    }
      
    /**
     * Processes this message - invoked on the node that is the lock grantor.
     */
    @Override
    protected void process(final DistributionManager dm) {
      boolean failed = true;
      ReplyException replyException = null;
      try {
        this.svc = DLockService.getInternalServiceNamed(this.serviceName);
        if (this.svc == null) { 
          failed = false; // basicProcess has it's own finally-block w reply
          basicProcess(dm, false); // don't have a grantor anymore
        }
        else {
          executeBasicProcess(dm); // use executor
        }
        failed = false; // nothing above threw anything
      }
      catch (RuntimeException e) {
        replyException = new ReplyException(e);
        throw e;
      }
      catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        replyException = new ReplyException(e);
        throw e;
      }
      catch (Error e) {
        SystemFailure.checkFailure();
        replyException = new ReplyException(e);
        throw e;
      }
      finally {
        if (failed) {
          // above code failed so now ensure reply is sent
          if (logger.isTraceEnabled(LogMarker.DLS)) {
            logger.trace(LogMarker.DLS, "DLockReleaseMessage.process failed for <{}>", this);
          }
          int replyCode = DLockReleaseReplyMessage.NOT_GRANTOR;
          DLockReleaseReplyMessage replyMsg = new DLockReleaseReplyMessage();
          replyMsg.serviceName = this.serviceName;
          replyMsg.replyCode = replyCode;
          replyMsg.setProcessorId(this.processorId);
          replyMsg.setRecipient(getSender());
          replyMsg.setException(replyException);
          
          if (dm.getId().equals(getSender())) {
            replyMsg.setSender(getSender());
            replyMsg.dmProcess(dm);
          }
          else {
            dm.putOutgoing(replyMsg);
          }
        }
      }
    }
    
    /** Process locally without using messaging or executor */
    protected void processLocally(final DM dm) {
      this.svc = DLockService.getInternalServiceNamed(this.serviceName);
      basicProcess(dm, true); // don't use executor
    }

    /** 
     * Execute basicProcess inside Pooled Executor because grantor may not 
     * be initializing which will require us to wait.
     * <p>
     * this.svc and this.grantor must be set before calling this method.
     */
    private void executeBasicProcess(final DM dm) {
      final DLockReleaseMessage msg = this;
      dm.getWaitingThreadPool().execute(new Runnable() {
        public void run() {
          if (logger.isTraceEnabled(LogMarker.DLS)) {
            logger.trace(LogMarker.DLS, "[executeBasicProcess] waitForGrantor {}", msg);
          }
          basicProcess(dm, true);
        }
      });
    }
    
    /** 
     * Perform basic processing of this message.
     * <p>
     * this.svc and this.grantor must be set before calling this method.
     */
    protected void basicProcess(final DM dm, final boolean waitForGrantor) {
      final boolean isDebugEnabled_DLS = logger.isTraceEnabled(LogMarker.DLS);
      if (isDebugEnabled_DLS) {
        logger.trace(LogMarker.DLS, "[basicProcess] {}", this);
      }
      int replyCode = DLockReleaseReplyMessage.NOT_GRANTOR;
      ReplyException replyException = null;
      try {
        if (svc == null || svc.isDestroyed()) return;
        
        if (waitForGrantor) {
          try {
            this.grantor = DLockGrantor.waitForGrantor(this.svc);
          }
          catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            this.grantor = null;
          }
        }
        
        if (grantor == null || grantor.isDestroyed()) {
          return;
        }
        
        try {
          if (lockBatch) {
            grantor.releaseLockBatch(objectName, getSender());
            replyCode = DLockReleaseReplyMessage.OK;
          }
          else {
            grantor.releaseIfLocked(objectName, getSender(), lockId);
            replyCode = DLockReleaseReplyMessage.OK;
          }
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          replyCode = DLockReleaseReplyMessage.NOT_GRANTOR;
        }
      }
      catch (LockGrantorDestroyedException ignore) {
      }
      catch (LockServiceDestroyedException ignore) {
      }
      catch (RuntimeException e) {
        replyException = new ReplyException(e);
        if (isDebugEnabled_DLS) {
          logger.trace(LogMarker.DLS, "[basicProcess] caught RuntimeException", e);
        }
      }
      catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error.  We're poisoned
        // now, so don't let this thread continue.
        throw err;
      }
      catch (Error e) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above).  However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        replyException = new ReplyException(e);
        if (isDebugEnabled_DLS) {
          logger.trace(LogMarker.DLS, "[basicProcess] caught Error", e);
        }
      }
      finally {
        DLockReleaseReplyMessage replyMsg = new DLockReleaseReplyMessage();
        replyMsg.serviceName = this.serviceName;
        replyMsg.replyCode = replyCode;
        replyMsg.setProcessorId(this.processorId);
        replyMsg.setRecipient(getSender());
        replyMsg.setException(replyException);
        
        if (dm.getId().equals(getSender())) {
          replyMsg.setSender(getSender());
          replyMsg.dmProcess(dm);
        }
        else {
          dm.putOutgoing(replyMsg);
        }
        if (grantor != null && !lockBatch) {
          // moved what was here into grantor...
          try {
            grantor.postRemoteReleaseLock(objectName);
          }
          catch (InterruptedException e) {
            try {
              dm.getCancelCriterion().checkCancelInProgress(e);
            } finally {
              Thread.currentThread().interrupt();
            }
          }
        } // grantor != null
        else {
          if (DLockGrantor.DEBUG_SUSPEND_LOCK && isDebugEnabled_DLS) {
            logger.trace(LogMarker.DLS, "DLockReleaseMessage, omitted postRemoteRelease lock on " + objectName + "; grantor = " + grantor + ", lockBatch = " + lockBatch + ", replyMsg = " + replyMsg);
          }
        }
      }
    }
    
    public int getDSFID() {
      return DLOCK_RELEASE_MESSAGE;
    }
    
    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      DataSerializer.writeString(this.serviceName, out);
      DataSerializer.writeObject(this.objectName, out);
      out.writeBoolean(this.lockBatch);
      out.writeInt(this.processorId);
      out.writeInt(this.lockId);
    }
  
    @Override
    public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.serviceName = DataSerializer.readString(in);
      this.objectName = DataSerializer.readObject(in);
      this.lockBatch = in.readBoolean();
      this.processorId = in.readInt();
      this.lockId = in.readInt();
    }
  
    @Override
    public String toString() {
      return new StringBuilder("DLockReleaseMessage for ").append(this.serviceName)
          .append(", ").append(this.objectName)
          .append("; processorId=").append(this.processorId) 
          .append("; lockBatch=").append(this.lockBatch) 
          .append("; lockId=").append(this.lockId).toString();
    }
  } // DLockReleaseMessage
  
  // -------------------------------------------------------------------------
  //   DLockReleaseReplyMessage
  // -------------------------------------------------------------------------
  public static final class DLockReleaseReplyMessage 
  extends ReplyMessage {
   
    static final int NOT_GRANTOR = 0;
    static final int OK = 1;
    
    /** Name of service to release the lock in; for toString only */
    protected String serviceName;
    
    /** OK or NOT_GRANTOR for the service  */
    protected int replyCode = NOT_GRANTOR;
    
    @Override
    public int getDSFID() {
      return DLOCK_RELEASE_REPLY;
    }

    @Override
    public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.serviceName = DataSerializer.readString(in);
      this.replyCode = in.readInt();
    }
    
    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      DataSerializer.writeString(this.serviceName, out);
      out.writeInt(this.replyCode);
    }

    @Override
    public String toString() {
      StringBuffer buff = new StringBuffer();
      buff.append("DLockReleaseReplyMessage");
      buff.append(" (serviceName=");
      buff.append(this.serviceName);
      buff.append("; replyCode=");
      switch (this.replyCode) {
        case NOT_GRANTOR: buff.append("NOT_GRANTOR"); break;
        case OK:          buff.append("OK"); break;
        default: buff.append(String.valueOf(this.replyCode)); break;
      }
      buff.append("; sender=");
      buff.append(getSender());
      buff.append("; processorId=");
      buff.append(super.processorId);
      buff.append(")");
      return buff.toString();
    }
  } // DLockReleaseReplyMessage
}

