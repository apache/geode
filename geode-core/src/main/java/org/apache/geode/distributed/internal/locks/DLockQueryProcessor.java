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
import org.apache.geode.SystemFailure;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.LockServiceDestroyedException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.PooledDistributionMessage;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.locks.DLockGrantor.DLockGrantToken;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * Queries the grantor for current leasing information of a lock.
 *
 */
public class DLockQueryProcessor extends ReplyProcessor21 {
  private static final Logger logger = LogService.getLogger();

  /** The reply to the query or null */
  private volatile DLockQueryReplyMessage reply;

  /**
   * Query the grantor for current leasing information of a lock.
   *
   * @param grantor the member that is the grantor
   * @param serviceName the name of the lock service
   * @param objectName the named lock
   * @param lockBatch true if the named lock is a TX lock batch
   * @param dm distribution manager to use for logging and messaging
   * @return the query reply or null if there was no reply due to membership change
   */
  static DLockQueryReplyMessage query(final InternalDistributedMember grantor,
      final String serviceName, final Object objectName, final boolean lockBatch,
      final DistributionManager dm) {
    DLockQueryProcessor processor = new DLockQueryProcessor(dm, grantor, serviceName);

    DLockQueryMessage msg = new DLockQueryMessage();
    msg.processorId = processor.getProcessorId();
    msg.serviceName = serviceName;
    msg.objectName = objectName;
    msg.lockBatch = lockBatch;

    msg.setRecipient(grantor);
    if (grantor.equals(dm.getId())) {
      // local... don't actually send message
      msg.setSender(grantor);
      msg.processLocally(dm);
    } else {
      dm.putOutgoing(msg);
    }

    // keep waiting even if interrupted
    try {
      processor.waitForRepliesUninterruptibly();
    } catch (ReplyException e) {
      e.handleCause();
    }

    if (processor.reply == null) {
      return null;
    } else {
      return processor.reply;
    }
  }

  /**
   * Instantiates a new DLockQueryProcessor.
   *
   * @param dm the distribution manager to use for logging and messaging
   * @param grantor the member to query for lock leasing info
   * @param serviceName the name of the lock service
   */
  private DLockQueryProcessor(DistributionManager dm, InternalDistributedMember grantor,
      String serviceName) {
    super(dm, grantor);
  }

  @Override
  protected boolean allowReplyFromSender() {
    return true;
  }

  @Override
  public void process(DistributionMessage msg) {
    try {
      DLockQueryReplyMessage myReply = (DLockQueryReplyMessage) msg;
      if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
        logger.trace(LogMarker.DLS_VERBOSE, "Handling: {}", myReply);
      }
      this.reply = myReply;
    } finally {
      super.process(msg);
    }
  }

  // -------------------------------------------------------------------------
  // DLockQueryMessage
  // -------------------------------------------------------------------------
  public static class DLockQueryMessage extends PooledDistributionMessage
      implements MessageWithReply {
    /** The name of the DistributedLockService */
    protected String serviceName;

    /** The object name */
    protected Object objectName;

    /** True if objectName identifies a batch of locks */
    protected boolean lockBatch;

    /** Id of the processor that will handle replies */
    protected int processorId;

    // set used during processing of this msg...
    protected transient DLockService svc;
    protected transient DLockGrantor grantor;

    public DLockQueryMessage() {}

    @Override
    public int getProcessorId() {
      return this.processorId;
    }

    /**
     * Processes this message - invoked on the node that is the lock grantor.
     */
    @Override
    protected void process(final ClusterDistributionManager dm) {
      boolean failed = true;
      ReplyException replyException = null;
      try {
        this.svc = DLockService.getInternalServiceNamed(this.serviceName);
        if (this.svc == null) {
          failed = false; // basicProcess has it's own finally-block w reply
          basicProcess(dm, false); // don't have a grantor anymore
        } else {
          executeBasicProcess(dm); // use executor
        }
        failed = false; // nothing above threw anything
      } catch (RuntimeException e) {
        replyException = new ReplyException(e);
        throw e;
      } catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        replyException = new ReplyException(e);
        throw e;
      } catch (Error e) {
        SystemFailure.checkFailure();
        replyException = new ReplyException(e);
        throw e;
      } finally {
        if (failed) {
          // above code failed so now ensure reply is sent
          if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
            logger.trace(LogMarker.DLS_VERBOSE, "DLockQueryMessage.process failed for <{}>", this);
          }
          DLockQueryReplyMessage replyMsg = new DLockQueryReplyMessage();
          replyMsg.setProcessorId(this.processorId);
          replyMsg.setRecipient(getSender());
          replyMsg.setException(replyException); // might be null

          if (dm.getId().equals(getSender())) {
            replyMsg.setSender(getSender());
            replyMsg.dmProcess(dm);
          } else {
            dm.putOutgoing(replyMsg);
          }
        }
      }
    }

    /** Process locally without using messaging or executor */
    protected void processLocally(final DistributionManager dm) {
      this.svc = DLockService.getInternalServiceNamed(this.serviceName);
      basicProcess(dm, true); // don't use executor
    }

    /**
     * Execute basicProcess inside Pooled Executor because grantor may not be initializing which
     * will require us to wait.
     * <p>
     * this.svc and this.grantor must be set before calling this method.
     */
    private void executeBasicProcess(final DistributionManager dm) {
      final DLockQueryMessage msg = this;
      dm.getExecutors().getWaitingThreadPool().execute(new Runnable() {
        @Override
        public void run() {
          if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
            logger.trace(LogMarker.DLS_VERBOSE, "[executeBasicProcess] {}", msg);
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
    protected void basicProcess(final DistributionManager dm, final boolean waitForGrantor) {
      final boolean isDebugEnabled_DLS = logger.isTraceEnabled(LogMarker.DLS_VERBOSE);
      if (isDebugEnabled_DLS) {
        logger.trace(LogMarker.DLS_VERBOSE, "[basicProcess] {}", this);
      }
      final DLockQueryReplyMessage replyMsg = new DLockQueryReplyMessage();
      replyMsg.setProcessorId(this.processorId);
      replyMsg.setRecipient(getSender());
      replyMsg.replyCode = DLockQueryReplyMessage.NOT_GRANTOR;
      replyMsg.lesseeThread = null;
      replyMsg.leaseId = DLockService.INVALID_LEASE_ID;
      replyMsg.leaseExpireTime = 0;

      try {
        if (svc == null || svc.isDestroyed())
          return;

        if (waitForGrantor) {
          try {
            this.grantor = DLockGrantor.waitForGrantor(this.svc);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            this.grantor = null;
          }
        }

        if (grantor == null || grantor.isDestroyed()) {
          return;
        }

        if (lockBatch) {
          throw new UnsupportedOperationException(
              "DLockQueryProcessor does not support lock batches");
        } else {
          DLockGrantToken grantToken;
          try {
            grantToken = grantor.handleLockQuery(this);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            grantToken = null;
          }
          if (grantToken != null) {
            synchronized (grantToken) {
              if (!grantToken.isDestroyed()) {
                replyMsg.lesseeThread = grantToken.getRemoteThread();
                replyMsg.leaseId = grantToken.getLockId();
                replyMsg.leaseExpireTime = grantToken.getLeaseExpireTime();
              }
            }
          }
        }

        replyMsg.replyCode = DLockQueryReplyMessage.OK;
      } catch (LockGrantorDestroyedException | LockServiceDestroyedException ignore) {
      } catch (RuntimeException e) {
        replyMsg.setException(new ReplyException(e));
        if (isDebugEnabled_DLS) {
          logger.trace(LogMarker.DLS_VERBOSE, "[basicProcess] caught RuntimeException", e);
        }
      } catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      } catch (Error e) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above). However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        replyMsg.setException(new ReplyException(e));
        if (isDebugEnabled_DLS) {
          logger.trace(LogMarker.DLS_VERBOSE, "[basicProcess] caught Error", e);
        }
      } finally {
        if (dm.getId().equals(getSender())) {
          replyMsg.setSender(getSender());
          replyMsg.dmProcess(dm);
        } else {
          dm.putOutgoing(replyMsg);
        }
      }
    }

    @Override
    public int getDSFID() {
      return DLOCK_QUERY_MESSAGE;
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      DataSerializer.writeString(this.serviceName, out);
      DataSerializer.writeObject(this.objectName, out);
      out.writeBoolean(this.lockBatch);
      out.writeInt(this.processorId);
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      this.serviceName = DataSerializer.readString(in);
      this.objectName = DataSerializer.readObject(in);
      this.lockBatch = in.readBoolean();
      this.processorId = in.readInt();
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer("DLockQueryMessage@");
      sb.append(Integer.toHexString(hashCode()));
      sb.append(", serviceName: ").append(this.serviceName);
      sb.append(", objectName: ").append(this.objectName);
      sb.append(", lockBatch: ").append(this.lockBatch);
      sb.append(", processorId: ").append(this.processorId);
      return sb.toString();
    }
  } // DLockQueryMessage

  // -------------------------------------------------------------------------
  // DLockQueryReplyMessage
  // -------------------------------------------------------------------------
  public static class DLockQueryReplyMessage extends ReplyMessage {

    static final int NOT_GRANTOR = 0;
    static final int OK = 1;

    /** OK or NOT_GRANTOR for the service */
    protected int replyCode = NOT_GRANTOR;

    /** Member and thread holding lease on the lock */
    protected RemoteThread lesseeThread;

    /** Lease id used to hold a lease on the lock */
    protected int leaseId;

    /** Absolute time in millis when lease will expire */
    protected long leaseExpireTime;

    public DLockQueryReplyMessage() {}

    /**
     * Returns true if the queried grantor replied with the current lease info for the named lock.
     *
     * @return true if the queried grantor replied with the current lease info
     */
    boolean repliedOK() {
      return this.replyCode == DLockQueryReplyMessage.OK;
    }

    /**
     * Returns true if the queried grantor replied NOT_GRANTOR.
     *
     * @return true if the queried grantor replied NOT_GRANTOR
     */
    boolean repliedNotGrantor() {
      return this.replyCode == DLockQueryReplyMessage.NOT_GRANTOR;
    }

    /**
     * Returns the member holding the lease or null if there was no lease.
     *
     * @return the member holding the lease or null
     */
    DistributedMember getLessee() {
      if (this.lesseeThread != null) {
        return this.lesseeThread.getDistributedMember();
      } else {
        return null;
      }
    }

    /**
     * Returns the query reply's lesseeThread or null if there was no lease.
     *
     * @return the query reply's lesseeThread or null
     */
    RemoteThread getLesseeThread() {
      return this.lesseeThread;
    }

    /**
     * Return the query reply's leaseId or -1 if there was no lease.
     *
     * @return the query reply's leaseId or -1
     */
    int getLeaseId() {
      return this.leaseId;
    }

    /**
     * Return the query reply's leaseExpireTime or 0 if there was no lease.
     *
     * @return the query reply's leaseExpireTime or 0
     */
    long getLeaseExpireTime() {
      return this.leaseExpireTime;
    }

    @Override
    public int getDSFID() {
      return DLOCK_QUERY_REPLY;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      this.replyCode = in.readInt();
      if (this.replyCode == OK) {
        InternalDistributedMember lessee =
            (InternalDistributedMember) DataSerializer.readObject(in);
        if (lessee != null) {
          this.lesseeThread = new RemoteThread(lessee, in.readInt());
        }
        this.leaseId = in.readInt();
        this.leaseExpireTime = in.readLong();
      }
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      out.writeInt(this.replyCode);
      if (this.replyCode == OK) {
        if (this.lesseeThread == null) {
          DataSerializer.writeObject(null, out);
        } else {
          DataSerializer.writeObject(this.lesseeThread.getDistributedMember(), out);
          out.writeInt(this.lesseeThread.getThreadId());
        }
        out.writeInt(this.leaseId);
        out.writeLong(this.leaseExpireTime);
      }
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer("DLockQueryReplyMessage@");
      sb.append(Integer.toHexString(hashCode()));
      sb.append(", replyCode: ");
      switch (this.replyCode) {
        case NOT_GRANTOR:
          sb.append("NOT_GRANTOR");
          break;
        case OK:
          sb.append("OK");
          break;
        default:
          sb.append(String.valueOf(this.replyCode));
          break;
      }
      sb.append(", lesseeThread: ").append(this.lesseeThread);
      sb.append(", leaseId: ").append(this.leaseId);
      sb.append(", leaseExpireTime: ").append(this.leaseExpireTime);
      sb.append(", processorId: ").append(this.processorId);
      return sb.toString();
    }
  } // DLockQueryReplyMessage
}
