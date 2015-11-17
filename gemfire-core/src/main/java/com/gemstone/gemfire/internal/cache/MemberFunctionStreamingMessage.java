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
package com.gemstone.gemfire.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionInvocationTargetException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.MessageWithReply;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.execute.FunctionContextImpl;
import com.gemstone.gemfire.internal.cache.execute.FunctionStats;
import com.gemstone.gemfire.internal.cache.execute.MemberFunctionResultSender;
import com.gemstone.gemfire.internal.cache.execute.MultiRegionFunctionContextImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * 
 * @author ymahajan
 * 
 */
public class MemberFunctionStreamingMessage extends DistributionMessage implements TransactionMessage, MessageWithReply {

  private static final Logger logger = LogService.getLogger();
  
  transient int replyMsgNum = 0;

  transient boolean replyLastMsg;

  private Function functionObject;

  private String functionName;

  Object args;

  private int processorId;

  private int txUniqId = TXManagerImpl.NOTX;
  private InternalDistributedMember txMemberId = null;

  private boolean isFnSerializationReqd;

  private Set<String> regionPathSet;
  
  private boolean isReExecute;

  //private final Object lastResultLock = new Object();

  private static final short IS_REEXECUTE = UNRESERVED_FLAGS_START;

  public MemberFunctionStreamingMessage() {
  }

  public MemberFunctionStreamingMessage(Function function, int procId,
      Object ar, boolean isFnSerializationReqd, boolean isReExecute) {
    this.functionObject = function;
    this.processorId = procId;
    this.args = ar;
    this.isFnSerializationReqd = isFnSerializationReqd;
    this.isReExecute = isReExecute;
    this.txUniqId = TXManagerImpl.getCurrentTXUniqueId();
    TXStateProxy txState = TXManagerImpl.getCurrentTXState();
    if(txState!=null && txState.isMemberIdForwardingRequired()) {
      this.txMemberId = txState.getOriginatingMember();
    }
  }

  // For Multi region function execution
  public MemberFunctionStreamingMessage(Function function, int procId,
      Object ar, boolean isFnSerializationReqd, Set<String> regions,
      boolean isReExecute) {
    this.functionObject = function;
    this.processorId = procId;
    this.args = ar;
    this.isFnSerializationReqd = isFnSerializationReqd;
    this.regionPathSet = regions;
    this.isReExecute = isReExecute;
    this.txUniqId = TXManagerImpl.getCurrentTXUniqueId();
    TXStateProxy txState = TXManagerImpl.getCurrentTXState();
    if(txState!=null && txState.isMemberIdForwardingRequired()) {
      this.txMemberId = txState.getOriginatingMember();
    }
  }

  public MemberFunctionStreamingMessage(DataInput in) throws IOException,
      ClassNotFoundException {
    fromData(in);
  }

  private TXStateProxy prepForTransaction() throws InterruptedException {
    if (this.txUniqId == TXManagerImpl.NOTX) {
      return null;
    } else {
      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      if(cache==null) {
        // ignore and return, we are shutting down!
        return null;
      }
      TXManagerImpl mgr = cache.getTXMgr();
      return mgr.masqueradeAs(this);
    }
  }

  private void cleanupTransasction(TXStateProxy tx) {
    if (this.txUniqId != TXManagerImpl.NOTX) {
      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      if(cache==null) {
        // ignore and return, we are shutting down!
        return;
      }
      TXManagerImpl mgr = cache.getTXMgr();
      mgr.unmasquerade(tx);
    }
  }

  @Override
  protected void process(final DistributionManager dm) {
    Throwable thr = null;
    ReplyException rex = null;
    if (this.functionObject == null) {
      rex = new ReplyException(new FunctionException(
          LocalizedStrings.ExecuteFunction_FUNCTION_NAMED_0_IS_NOT_REGISTERED
              .toLocalizedString(this.functionName)));

      replyWithException(dm, rex);
      return;
    }

    FunctionStats stats = FunctionStats.getFunctionStats(this.functionObject
        .getId(), dm.getSystem());
    TXStateProxy tx = null;
    try {
      tx = prepForTransaction();
      ResultSender resultSender = new MemberFunctionResultSender(dm, this,
          this.functionObject);
      Set<Region> regions = new HashSet<Region>();
      if (this.regionPathSet != null) {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        for (String regionPath : this.regionPathSet) {
          if (checkCacheClosing(dm) || checkDSClosing(dm)) {
            thr = new CacheClosedException(LocalizedStrings.PartitionMessage_REMOTE_CACHE_IS_CLOSED_0.toLocalizedString(dm.getId()));
            return;
          }
          regions.add(cache.getRegion(regionPath));
        }
      }
      FunctionContextImpl context = new MultiRegionFunctionContextImpl(
          this.functionObject.getId(), this.args, resultSender, regions,
          isReExecute);

      
      long start = stats.startTime();
      stats.startFunctionExecution(this.functionObject.hasResult());
      if (logger.isDebugEnabled()) {
        logger.debug("Executing Function: {} on remote member with context: {}", this.functionObject.getId(), context.toString());
      }
      this.functionObject.execute(context);
      if (!this.replyLastMsg && this.functionObject.hasResult()) {
        throw new FunctionException(
            LocalizedStrings.ExecuteFunction_THE_FUNCTION_0_DID_NOT_SENT_LAST_RESULT
                .toString(functionObject.getId()));
      }
      stats.endFunctionExecution(start, this.functionObject.hasResult());
    }
    catch (FunctionException functionException) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "FunctionException occured on remote member while executing Function: {}", this.functionObject.getId(), functionException);
      }
      stats.endFunctionExecutionWithException(this.functionObject.hasResult());
      rex = new ReplyException(functionException);
      replyWithException(dm, rex);
      // thr = functionException.getCause();
    }
    catch (CancelException exception) {
      // bug 37026: this is too noisy...
      // throw new CacheClosedException("remote system shutting down");
      // thr = se; cache is closed, no point trying to send a reply
      thr = new FunctionInvocationTargetException(exception);
      stats.endFunctionExecutionWithException(this.functionObject.hasResult());
      rex = new ReplyException(thr);
      replyWithException(dm, rex);
    }
    catch (Exception exception) {
      if (logger.isDebugEnabled()) {
        logger.debug("Exception occured on remote member while executing Function: {}", this.functionObject.getId(), exception);
      }
      stats.endFunctionExecutionWithException(this.functionObject.hasResult());
      rex = new ReplyException(exception);
      replyWithException(dm, rex);
      // thr = e.getCause();
    }
    catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw err;
    }
    catch (Throwable t) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above). However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      thr = t;
    }
    finally {
      cleanupTransasction(tx);
      if (thr != null) {
        rex = new ReplyException(thr);
        replyWithException(dm, rex);
      }
    }
  }

  private void replyWithException(DistributionManager dm, ReplyException rex) {
    ReplyMessage.send(getSender(), this.processorId, rex, dm);
  }

  @Override
  public int getProcessorId() {
    return this.processorId;
  }

  public int getDSFID() {
    return MEMBER_FUNCTION_STREAMING_MESSAGE;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);

    short flags = in.readShort();
    if ((flags & HAS_PROCESSOR_ID) != 0) {
      this.processorId = in.readInt();
      ReplyProcessor21.setMessageRPId(this.processorId);
    }
    if ((flags & HAS_TX_ID) != 0) this.txUniqId = in.readInt();
    if ((flags & HAS_TX_MEMBERID) != 0) {
      this.txMemberId = (InternalDistributedMember) DataSerializer.readObject(in);
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
    this.args = DataSerializer.readObject(in);
    this.regionPathSet = DataSerializer.readObject(in);
    this.isReExecute = (flags & IS_REEXECUTE) != 0;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);

    short flags = 0;
    if (this.processorId != 0) flags |= HAS_PROCESSOR_ID;
    if (this.txUniqId != TXManagerImpl.NOTX) flags |= HAS_TX_ID;
    if (this.txMemberId != null) flags |= HAS_TX_MEMBERID;
    if (this.isReExecute) flags |= IS_REEXECUTE;
    out.writeShort(flags);

    if (this.processorId != 0) out.writeInt(this.processorId);
    if (this.txUniqId != TXManagerImpl.NOTX) out.writeInt(this.txUniqId);
    if (this.txMemberId != null) DataSerializer.writeObject(this.txMemberId,out);

    if (this.isFnSerializationReqd) {
      DataSerializer.writeObject(this.functionObject, out);
    }
    else {
      DataSerializer.writeObject(functionObject.getId(), out);
    }
    DataSerializer.writeObject(this.args, out);
    DataSerializer.writeObject(this.regionPathSet, out);
  }

  public synchronized boolean sendReplyForOneResult(DM dm,
      Object oneResult, boolean lastResult, boolean sendResultsInOrder)
      throws CacheException, QueryException, ForceReattemptException,
      InterruptedException {

    if(this.replyLastMsg) {
      return false;
    }
    
    if (Thread.interrupted())
      throw new InterruptedException();
    int msgNum = this.replyMsgNum;
    this.replyLastMsg = lastResult;

    sendReply(getSender(), this.processorId, dm, oneResult, msgNum,
        lastResult, sendResultsInOrder);

    if (logger.isDebugEnabled()) {
      logger.debug("Sending reply message count: {} to co-ordinating node", replyMsgNum);
    }
    this.replyMsgNum++;
    return false;
  }

  protected void sendReply(InternalDistributedMember member, int procId, DM dm, Object oneResult, int msgNum, boolean lastResult,
      boolean sendResultsInOrder) {
    if (sendResultsInOrder) {
      FunctionStreamingOrderedReplyMessage.send(member, procId, null, dm, oneResult,
          msgNum, lastResult);
    }
    else {
      FunctionStreamingReplyMessage.send(member, procId, null, dm, oneResult,
          msgNum, lastResult);
    }
  }

  @Override
  public int getProcessorType() {
    return DistributionManager.REGION_FUNCTION_EXECUTION_EXECUTOR;
  }
  
  /**
   * check to see if the cache is closing
   */
  final public boolean checkCacheClosing(DistributionManager dm) {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    return (cache == null || cache.getCancelCriterion().cancelInProgress() != null);
  }

  /**
   * check to see if the distributed system is closing
   * 
   * @return true if the distributed system is closing
   */
  final public boolean checkDSClosing(DistributionManager dm) {
    InternalDistributedSystem ds = dm.getSystem();
    return (ds == null || ds.isDisconnecting());
  }
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.TransactionMessage#canStartRemoteTransaction()
   */
  public boolean canStartRemoteTransaction() {
    return true;
  }
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.TransactionMessage#getTXUniqId()
   */
  public int getTXUniqId() {
    return this.txUniqId;
  }

  public final InternalDistributedMember getMemberToMasqueradeAs() {
	  if(txMemberId==null) {
		  return getSender();
	  } else {
		  return txMemberId;
	  }
  }

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
