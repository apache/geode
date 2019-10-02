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
package org.apache.geode.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.OperationExecutors;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

public class DistributedRegionFunctionStreamingMessage extends DistributionMessage
    implements TransactionMessage {

  private static final Logger logger = LogService.getLogger();

  private Object result;

  transient int replyMsgNum = 0;

  transient boolean replyLastMsg;

  transient int numObjectsInChunk = 0;

  private Function functionObject;

  private String functionName;

  Object args;

  private String regionPath;

  private Set filter;

  private int processorId;

  private boolean isReExecute;

  private boolean isFnSerializationReqd;

  private int txUniqId = TXManagerImpl.NOTX;

  private InternalDistributedMember txMemberId = null;

  private static final short IS_REEXECUTE = UNRESERVED_FLAGS_START;

  /** default exception to ensure a false-positive response is never returned */
  @Immutable
  static final ForceReattemptException UNHANDLED_EXCEPTION =
      (ForceReattemptException) new ForceReattemptException("Unknown exception").fillInStackTrace();

  public DistributedRegionFunctionStreamingMessage() {}

  public DistributedRegionFunctionStreamingMessage(final String regionPath, Function function,
      int procId, final Set filter, Object args, boolean isReExecute,
      boolean isFnSerializationReqd) {
    this.functionObject = function;
    this.processorId = procId;
    this.args = args;
    this.regionPath = regionPath;
    this.filter = filter;
    this.isReExecute = isReExecute;
    this.isFnSerializationReqd = isFnSerializationReqd;
    this.txUniqId = TXManagerImpl.getCurrentTXUniqueId();
    TXStateProxy txState = TXManagerImpl.getCurrentTXState();
    if (txState != null && txState.isMemberIdForwardingRequired()) {
      this.txMemberId = txState.getOriginatingMember();
    }
  }

  private TXStateProxy prepForTransaction(ClusterDistributionManager dm)
      throws InterruptedException {
    if (this.txUniqId == TXManagerImpl.NOTX) {
      return null;
    } else {
      InternalCache cache = dm.getCache();
      if (cache == null) {
        // ignore and return, we are shutting down!
        return null;
      }
      TXManagerImpl mgr = cache.getTXMgr();
      return mgr.masqueradeAs(this);
    }
  }

  private void cleanupTransaction(TXStateProxy tx) {
    if (this.txUniqId != TXManagerImpl.NOTX) {
      InternalCache cache = GemFireCacheImpl.getInstance();
      if (cache == null) {
        // ignore and return, we are shutting down!
        return;
      }
      TXManagerImpl mgr = cache.getTXMgr();
      mgr.unmasquerade(tx);
    }
  }

  @Override
  protected void process(final ClusterDistributionManager dm) {
    Throwable thr = null;
    boolean sendReply = true;
    DistributedRegion dr = null;
    TXStateProxy tx = null;

    try {
      if (checkCacheClosing(dm) || checkDSClosing(dm)) {
        InternalCache cache = dm.getCache();
        if (cache != null) {
          thr = cache
              .getCacheClosedException(String.format("Remote cache is closed: %s",
                  dm.getId()));
        } else {
          thr = new CacheClosedException(String.format("Remote cache is closed: %s",
              dm.getId()));
        }
        return;
      }
      dr = (DistributedRegion) dm.getCache().getRegion(this.regionPath);
      if (dr == null) {
        // if the distributed system is disconnecting, don't send a reply saying
        // the partitioned region can't be found (bug 36585)
        thr = new ForceReattemptException(dm.getDistributionManagerId().toString()
            + ": could not find Distributed region " + regionPath);
        return; // reply sent in finally block below
      }
      thr = UNHANDLED_EXCEPTION;
      tx = prepForTransaction(dm);
      sendReply = operateOnDistributedRegion(dm, dr); // need to take care of
                                                      // it...
      thr = null;
    } catch (CancelException se) {
      // bug 37026: this is too noisy...
      // throw new CacheClosedException("remote system shutting down");
      // thr = se; cache is closed, no point trying to send a reply
      thr = null;
      sendReply = false;
      if (logger.isDebugEnabled()) {
        logger.debug("shutdown caught, abandoning message: {}", se.getMessage(), se);
      }
    } catch (RegionDestroyedException rde) {
      // [bruce] RDE does not always mean that the sender's region is also
      // destroyed, so we must send back an exception. If the sender's
      // region is also destroyed, who cares if we send it an exception
      if (dr != null && dr.isClosed()) {
        thr = new ForceReattemptException("Region is destroyed in " + dm.getDistributionManagerId(),
            rde);
      }
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Throwable t) {
      logger.debug("{} exception occurred while processing message: {}", this, t.getMessage(), t);
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above). However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      // log the exception at fine level if there is no reply to the message
      thr = null;
      if (sendReply && this.processorId != 0) {
        if (!checkDSClosing(dm)) {
          thr = t;
        } else {
          // don't pass arbitrary runtime exceptions and errors back if this
          // cache/vm is closing
          thr = new ForceReattemptException("Distributed system is disconnecting");
        }
      }
      if (this.processorId == 0) {
        logger.debug("{} exception while processing message: {}", this, t.getMessage(), t);
      } else if (logger.isTraceEnabled(LogMarker.DM_VERBOSE) && (t instanceof RuntimeException)) {
        logger.trace(LogMarker.DM_VERBOSE, "Exception caught while processing message", t);
      }
    } finally {
      cleanupTransaction(tx);
      if (sendReply && this.processorId != 0) {
        ReplyException rex = null;
        if (thr != null) {
          // don't transmit the exception if this message was to a listener
          // and this listener is shutting down
          boolean excludeException = false;
          if (!this.functionObject.isHA()) {
            excludeException =
                (thr instanceof CacheClosedException || (thr instanceof ForceReattemptException));
          } else {
            excludeException = thr instanceof ForceReattemptException;
          }
          if (!excludeException) {
            rex = new ReplyException(thr);
          }
        }
        // Send the reply if the operateOnPartitionedRegion returned true
        sendReply(getSender(), this.processorId, dm, rex, null, 0, true, false);
      }
    }
  }

  protected boolean operateOnDistributedRegion(final ClusterDistributionManager dm,
      DistributedRegion r) throws ForceReattemptException {
    if (this.functionObject == null) {
      ReplyMessage.send(getSender(), this.processorId,
          new ReplyException(new FunctionException(
              String.format("Function named %s is not registered to FunctionService",
                  this.functionName))),
          dm, r.isInternalRegion());

      return false;
    }


    if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
      logger.trace(LogMarker.DM_VERBOSE, "FunctionMessage operateOnRegion: {}", r.getFullPath());
    }
    try {
      r.executeOnRegion(this, this.functionObject, this.args, this.processorId, this.filter,
          this.isReExecute);
      if (!this.replyLastMsg && this.functionObject.hasResult()) {
        ReplyMessage.send(getSender(), this.processorId,
            new ReplyException(new FunctionException(
                String.format("The function, %s, did not send last result",
                    functionObject.getId()))),
            dm, r.isInternalRegion());
        return false;
      }
    } catch (IOException e) {
      ReplyMessage
          .send(getSender(), this.processorId,
              new ReplyException(
                  "Operation got interrupted due to shutdown in progress on remote VM", e),
              dm, r.isInternalRegion());
      return false;
    } catch (CancelException sde) {
      ReplyMessage.send(getSender(), this.processorId,
          new ReplyException(new ForceReattemptException(
              "Operation got interrupted due to shutdown in progress on remote VM", sde)),
          dm, r.isInternalRegion());
      return false;
    }

    // Unless there was an exception thrown, this message handles sending the
    // response
    return false;
  }

  /**
   * check to see if the cache is closing
   */
  private boolean checkCacheClosing(ClusterDistributionManager dm) {
    InternalCache cache = dm.getCache();
    return cache == null || cache.getCancelCriterion().isCancelInProgress();
  }

  /**
   * check to see if the distributed system is closing
   *
   * @return true if the distributed system is closing
   */
  private boolean checkDSClosing(ClusterDistributionManager dm) {
    InternalDistributedSystem ds = dm.getSystem();
    return (ds == null || ds.isDisconnecting());
  }

  @Override
  public int getProcessorId() {
    return this.processorId;
  }

  @Override
  public int getDSFID() {
    return DR_FUNCTION_STREAMING_MESSAGE;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);

    short flags = in.readShort();
    if ((flags & HAS_PROCESSOR_ID) != 0) {
      this.processorId = in.readInt();
      ReplyProcessor21.setMessageRPId(this.processorId);
    }
    if ((flags & HAS_TX_ID) != 0)
      this.txUniqId = in.readInt();
    if ((flags & HAS_TX_MEMBERID) != 0) {
      this.txMemberId = DataSerializer.readObject(in);
    }

    Object object = DataSerializer.readObject(in);
    if (object instanceof String) {
      this.isFnSerializationReqd = false;
      this.functionObject = FunctionService.getFunction((String) object);
      if (this.functionObject == null) {
        this.functionName = (String) object;
      }
    } else {
      this.functionObject = (Function) object;
      this.isFnSerializationReqd = true;
    }
    this.args = (Serializable) DataSerializer.readObject(in);
    this.filter = (HashSet) DataSerializer.readHashSet(in);
    this.regionPath = DataSerializer.readString(in);
    this.isReExecute = (flags & IS_REEXECUTE) != 0;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);

    short flags = 0;
    if (this.processorId != 0)
      flags |= HAS_PROCESSOR_ID;
    if (this.txUniqId != TXManagerImpl.NOTX)
      flags |= HAS_TX_ID;
    if (this.txMemberId != null)
      flags |= HAS_TX_MEMBERID;
    if (this.isReExecute)
      flags |= IS_REEXECUTE;
    out.writeShort(flags);
    if (this.processorId != 0)
      out.writeInt(this.processorId);
    if (this.txUniqId != TXManagerImpl.NOTX)
      out.writeInt(this.txUniqId);
    if (this.txMemberId != null) {
      DataSerializer.writeObject(this.txMemberId, out);
    }

    if (this.isFnSerializationReqd) {
      DataSerializer.writeObject(this.functionObject, out);
    } else {
      DataSerializer.writeObject(functionObject.getId(), out);
    }
    DataSerializer.writeObject(this.args, out);
    DataSerializer.writeHashSet((HashSet) this.filter, out);
    DataSerializer.writeString(this.regionPath, out);
  }

  public synchronized boolean sendReplyForOneResult(DistributionManager dm, Object oneResult,
      boolean lastResult, boolean sendResultsInOrder)
      throws CacheException, ForceReattemptException, InterruptedException {
    if (this.replyLastMsg) {
      return false;
    }
    if (Thread.interrupted())
      throw new InterruptedException();
    int msgNum = this.replyMsgNum;
    this.replyLastMsg = lastResult;

    sendReply(getSender(), this.processorId, dm, null, oneResult, msgNum, lastResult,
        sendResultsInOrder);

    if (logger.isDebugEnabled()) {
      logger.debug("Sending reply message count: {} to co-ordinating node", replyMsgNum);
    }
    this.replyMsgNum++;
    return false;
  }

  protected void sendReply(InternalDistributedMember member, int procId, DistributionManager dm,
      ReplyException ex, Object result, int msgNum, boolean lastResult,
      boolean sendResultsInOrder) {
    // if there was an exception, then throw out any data
    if (ex != null) {
      this.result = null;
      this.replyMsgNum = 0;
      this.replyLastMsg = true;
    }
    if (sendResultsInOrder) {
      FunctionStreamingOrderedReplyMessage.send(member, procId, ex, dm, result, msgNum, lastResult);
    } else {
      FunctionStreamingReplyMessage.send(member, procId, ex, dm, result, msgNum, lastResult);
    }
  }

  @Override
  public int getProcessorType() {
    return OperationExecutors.REGION_FUNCTION_EXECUTION_EXECUTOR;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.TransactionMessage#canStartRemoteTransaction()
   */
  @Override
  public boolean canStartRemoteTransaction() {
    return true;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.TransactionMessage#getTXUniqId()
   */
  @Override
  public int getTXUniqId() {
    return txUniqId;
  }

  @Override
  public InternalDistributedMember getMemberToMasqueradeAs() {
    if (txMemberId == null) {
      return getSender();
    } else {
      return txMemberId;
    }
  }

  @Override
  public InternalDistributedMember getTXOriginatorClient() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean canParticipateInTransaction() {
    return true;
  }

  @Override
  public boolean isTransactionDistributed() {
    return false;
  }
}
