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

package com.gemstone.gemfire.cache.client.internal;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.cache.client.ServerConnectivityException;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.cache.client.internal.ExecuteRegionFunctionSingleHopOp.ExecuteRegionFunctionSingleHopOpImpl;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.execute.AbstractExecution;
import com.gemstone.gemfire.internal.cache.execute.FunctionStats;
import com.gemstone.gemfire.internal.cache.execute.InternalFunctionException;
import com.gemstone.gemfire.internal.cache.execute.MemberMappedArgument;
import com.gemstone.gemfire.internal.cache.execute.InternalFunctionInvocationTargetException;
import com.gemstone.gemfire.internal.cache.execute.ServerRegionFunctionExecutor;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.ChunkedMessage;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * Does a Execution of function on server region.<br>
 * It alos gets result from the server
 * @since GemFire 5.8Beta
 */
public class ExecuteRegionFunctionOp {

  private static final Logger logger = LogService.getLogger();
  private ExecuteRegionFunctionOp() {
    // no instances allowed
  }

  /**
   * Does a execute Function on a server using connections from the given pool
   * to communicate with the server.
   * @param pool the pool to use to communicate with the server.
   * @param region the name of the region to do the put on
   * @param function to be executed
   * @param serverRegionExecutor which will return argument and filter
   * @param resultCollector is used to collect the results from the Server
   */
  public static void execute(ExecutablePool pool, String region,
      Function function, ServerRegionFunctionExecutor serverRegionExecutor,
      ResultCollector resultCollector, byte hasResult, int mRetryAttempts) {
    AbstractOp op = new ExecuteRegionFunctionOpImpl(region, function, serverRegionExecutor,
        resultCollector, hasResult, new HashSet<String>());

    int retryAttempts = 0;
    boolean reexecute = false;
    boolean reexecuteForServ = false;
    Set<String> failedNodes = new HashSet<String>();
    AbstractOp reexecOp = null; 
    int maxRetryAttempts = 0;
    if (function.isHA()) {
      maxRetryAttempts = mRetryAttempts;
    }
    
    final boolean isDebugEnabled =logger.isDebugEnabled();
    do {
    try {
        if (reexecuteForServ) {
          reexecOp = new ExecuteRegionFunctionOpImpl(
              (ExecuteRegionFunctionOpImpl)op, (byte)1/* isReExecute */,
              failedNodes);
          pool.execute(reexecOp, 0);
        }
        else {
          pool.execute(op, 0);
        }
        reexecute = false;
        reexecuteForServ = false;
      }
    catch (InternalFunctionInvocationTargetException e) {
      if (isDebugEnabled) {
        logger.debug("ExecuteRegionFunctionOp#execute : Received InternalFunctionInvocationTargetException. The failed nodes are {}", e.getFailedNodeSet());
      }
      reexecute = true;
      resultCollector.clearResults();
      Set<String> failedNodesIds = e.getFailedNodeSet();
      failedNodes.clear();
      if (failedNodesIds != null) {
        failedNodes.addAll(failedNodesIds);
      }
    }
    catch (ServerConnectivityException se) {
      retryAttempts++;
      if (isDebugEnabled) {
        logger.debug("ExecuteRegionFunctionOp#execute : Received ServerConnectivityException. The exception is {} The retryattempt is : {} maxRetryAttempts {}", se, retryAttempts, maxRetryAttempts );
      }
      if(se instanceof ServerOperationException){
        throw se;
      }
      if ((retryAttempts > maxRetryAttempts && maxRetryAttempts !=-1) /*|| !function.isHA()*/)
        throw se;

      reexecuteForServ = true;
      resultCollector.clearResults();
      failedNodes.clear();
    }
    }
    while(reexecuteForServ);

    if ( reexecute && function.isHA()) {
      ExecuteRegionFunctionOp.reexecute(pool, region, function,
          serverRegionExecutor, resultCollector, hasResult, failedNodes,
          maxRetryAttempts - 1);
    }
  }
  
  public static void execute(ExecutablePool pool, String region,
      String function, ServerRegionFunctionExecutor serverRegionExecutor,
      ResultCollector resultCollector, byte hasResult, int mRetryAttempts, boolean isHA, boolean optimizeForWrite) {
    AbstractOp op = new ExecuteRegionFunctionOpImpl(region, function, serverRegionExecutor,
        resultCollector, hasResult, new HashSet<String>(), isHA, optimizeForWrite, true);

    int retryAttempts = 0;
    boolean reexecute = false;
    boolean reexecuteForServ = false;
    Set<String> failedNodes = new HashSet<String>();
    AbstractOp reexecOp = null; 
    int maxRetryAttempts = 0;
    if (isHA) {
      maxRetryAttempts = mRetryAttempts;
    }
    final boolean isDebugEnabled = logger.isDebugEnabled();
    do{
    try {
      if (reexecuteForServ) {
        reexecOp = new ExecuteRegionFunctionOpImpl(
            (ExecuteRegionFunctionOpImpl)op, (byte)1/* isReExecute */,
            failedNodes);
        pool.execute(reexecOp, 0);
      }
      else {
        pool.execute(op, 0);
      }
      reexecute = false;
      reexecuteForServ = false;
    }
    catch (InternalFunctionInvocationTargetException e) {
      if (isDebugEnabled) {
        logger.debug("ExecuteRegionFunctionOp#execute : Received InternalFunctionInvocationTargetException. The failed nodes are {}", e.getFailedNodeSet());
      }
      reexecute = true;
      resultCollector.clearResults();
      Set<String> failedNodesIds = e.getFailedNodeSet();
      failedNodes.clear();
      if (failedNodesIds != null) {
        failedNodes.addAll(failedNodesIds);
      }
    }
    catch (ServerConnectivityException se) {
      if (isDebugEnabled) {
        logger.debug("ExecuteRegionFunctionOp#execute : Received ServerConnectivityException. The exception is {} The retryattempt is : {} maxRetryAttempts {}", se, retryAttempts, maxRetryAttempts);
      }
      if(se instanceof ServerOperationException){
        throw se;
      }
      retryAttempts++;
      if ((retryAttempts >  maxRetryAttempts && maxRetryAttempts != -1) /*|| !isHA*/)
        throw se;

      reexecute = true;
      resultCollector.clearResults();
      failedNodes.clear();
    }
  }
  while(reexecuteForServ);
  
    if ( reexecute && isHA) {
      ExecuteRegionFunctionOp.reexecute(pool, region, function,
          serverRegionExecutor, resultCollector, hasResult, failedNodes,
          maxRetryAttempts - 1, isHA, optimizeForWrite);
    }
  }
  
  public static void reexecute(ExecutablePool pool, String region,
      Function function, ServerRegionFunctionExecutor serverRegionExecutor,
      ResultCollector resultCollector, byte hasResult, Set<String> failedNodes, int maxRetryAttempts) {
    AbstractOp op = new ExecuteRegionFunctionOpImpl(region, function, serverRegionExecutor,
        resultCollector, hasResult, new HashSet<String>());

    boolean reexecute = true;
    int retryAttempts = 0;
    final boolean isDebugEnabled = logger.isDebugEnabled();
    do {
      reexecute = false;
     AbstractOp reExecuteOp = new ExecuteRegionFunctionOpImpl(
          (ExecuteRegionFunctionOpImpl)op, (byte)1/*isReExecute*/, failedNodes);
      if (isDebugEnabled) {
        logger.debug("ExecuteRegionFunction#reexecute: Sending Function Execution Message: {} to Server using pool: {} with failed nodes: {}", reExecuteOp.getMessage(), pool, failedNodes);
      }
      try {
          pool.execute(reExecuteOp,0);
      }
      catch (InternalFunctionInvocationTargetException e) {
        if (isDebugEnabled) {
          logger.debug("ExecuteRegionFunctionOp#reexecute : Received InternalFunctionInvocationTargetException. The failed nodes are {}", e.getFailedNodeSet());
        }
        reexecute = true;
        resultCollector.clearResults();
        Set<String> failedNodesIds = e.getFailedNodeSet();
        failedNodes.clear();
        if (failedNodesIds != null) {
          failedNodes.addAll(failedNodesIds);
        }
      }
      catch (ServerConnectivityException se) {
        if (isDebugEnabled) {
          logger.debug("ExecuteRegionFunctionOp#reexecute : Received ServerConnectivity Exception.");
        }
        
        if(se instanceof ServerOperationException){
          throw se;
        }
        retryAttempts++;
        if (retryAttempts > maxRetryAttempts && maxRetryAttempts != -2) 
          throw se;

        reexecute = true;
        resultCollector.clearResults();
        failedNodes.clear();
      }
    } while (reexecute);
  }
  
  public static void reexecute(ExecutablePool pool, String region,
      String function, ServerRegionFunctionExecutor serverRegionExecutor,
      ResultCollector resultCollector, byte hasResult, Set<String> failedNodes, int maxRetryAttempts, boolean isHA, boolean optimizeForWrite) {
    AbstractOp op = new ExecuteRegionFunctionOpImpl(region, function, serverRegionExecutor,
        resultCollector, hasResult, new HashSet<String>(), isHA, optimizeForWrite, true);

    boolean reexecute = true;
    int retryAttempts = 0;
    final boolean isDebugEnabled = logger.isDebugEnabled();
    do {
      reexecute = false;
      
      AbstractOp reExecuteOp = new ExecuteRegionFunctionOpImpl(
          (ExecuteRegionFunctionOpImpl)op, (byte)1/*isReExecute*/, failedNodes);
      if (isDebugEnabled) {
        logger.debug("ExecuteRegionFunction#reexecute : Sending Function Execution Message: {} to Server using pool: {}", reExecuteOp.getMessage(), pool);
      }
      try {
          pool.execute(reExecuteOp,0);
      }
      catch (InternalFunctionInvocationTargetException e) {
        if (isDebugEnabled) {
          logger.debug("ExecuteRegionFunctionOp#reexecute : Received InternalFunctionInvocationTargetException. The failed nodes are {}", e.getFailedNodeSet());
        }
        reexecute = true;
        resultCollector.clearResults();
        Set<String> failedNodesIds = e.getFailedNodeSet();
        failedNodes.clear();
        if (failedNodesIds != null) {
          failedNodes.addAll(failedNodesIds);
        }
      }
      catch (ServerConnectivityException se) {
        if (isDebugEnabled) { 
          logger.debug("ExecuteRegionFunctionOp#reexecute : Received ServerConnectivityException. The exception is {} The retryattempt is : {} maxRetryAttempts {}", se, retryAttempts, maxRetryAttempts);
        }
        if(se instanceof ServerOperationException){
          throw se;
        }
        retryAttempts++;
        if (retryAttempts > maxRetryAttempts && maxRetryAttempts != -2) 
          throw se;

        reexecute = true;
        resultCollector.clearResults();
        failedNodes.clear();
      }
    } while (reexecute);
  }
  static class ExecuteRegionFunctionOpImpl extends AbstractOp {

    // To collect the results from the server
    private final ResultCollector resultCollector;

    //To get the instance of the Function Statistics we need the function name or instance
    private Function function;

    private byte isReExecute = 0;

    private final String regionName;

    private final ServerRegionFunctionExecutor executor;

    private final byte hasResult;

    private Set<String> failedNodes = new HashSet<String>();

    private final String functionId;
    private final boolean executeOnBucketSet;

    /**
     * @param removedNodes TODO
     * @throws com.gemstone.gemfire.SerializationException if serialization fails
     */
    public ExecuteRegionFunctionOpImpl(String region, Function function,
        ServerRegionFunctionExecutor serverRegionExecutor, ResultCollector rc,
        byte hasResult, Set<String> removedNodes) {
      super(MessageType.EXECUTE_REGION_FUNCTION, 8
          + serverRegionExecutor.getFilter().size() + removedNodes.size());
      Set routingObjects = serverRegionExecutor.getFilter();
      Object args = serverRegionExecutor.getArguments();
      byte functionState = AbstractExecution.getFunctionState(function.isHA(),
          function.hasResult(), function.optimizeForWrite());
      MemberMappedArgument memberMappedArg = serverRegionExecutor
          .getMemberMappedArgument();

      addBytes(functionState);
      getMessage().addStringPart(region);
      if (serverRegionExecutor.isFnSerializationReqd()) {
        getMessage().addStringOrObjPart(function);
      }
      else {
        getMessage().addStringOrObjPart(function.getId());
      }
      getMessage().addObjPart(args);
      getMessage().addObjPart(memberMappedArg);
      this.executeOnBucketSet = serverRegionExecutor.getExecuteOnBucketSetFlag(); 
      byte flags = ExecuteFunctionHelper.createFlags(executeOnBucketSet, isReExecute);

      getMessage().addBytesPart(new byte[] {flags});
      getMessage().addIntPart(routingObjects.size());
      for (Object key : routingObjects) {
        getMessage().addStringOrObjPart(key);
      }
      getMessage().addIntPart(removedNodes.size());
      for (Object nodes : removedNodes) {
        getMessage().addStringOrObjPart(nodes);
      }

      this.resultCollector = rc;
      this.regionName = region;
      this.function = function;
      this.functionId = function.getId();
      this.executor = serverRegionExecutor;
      this.hasResult = functionState;
      this.failedNodes = removedNodes;
    }
    
    public ExecuteRegionFunctionOpImpl(String region, String function,
        ServerRegionFunctionExecutor serverRegionExecutor, ResultCollector rc,
        byte hasResult, Set<String> removedNodes, boolean isHA, boolean optimizeForWrite, boolean calculateFnState ) {
      super(MessageType.EXECUTE_REGION_FUNCTION, 8
          + serverRegionExecutor.getFilter().size() + removedNodes.size());
      Set routingObjects = serverRegionExecutor.getFilter();
      byte functionState = hasResult;
      if(calculateFnState){
         functionState = AbstractExecution.getFunctionState(isHA,
          hasResult == (byte)1 ? true : false, optimizeForWrite);
      }
      Object args = serverRegionExecutor.getArguments();
      MemberMappedArgument memberMappedArg = serverRegionExecutor
          .getMemberMappedArgument();
      addBytes(functionState);
      getMessage().addStringPart(region);
      getMessage().addStringOrObjPart(function);
      getMessage().addObjPart(args);
      getMessage().addObjPart(memberMappedArg);
      
      this.executeOnBucketSet = serverRegionExecutor.getExecuteOnBucketSetFlag(); 
//      byte flags = this.executeOnBucketSet ? 
//          (byte)(0x00 | ExecuteFunctionHelper.BUCKETS_AS_FILTER_MASK) : 0x00;
//      flags = isReExecute == 1? (byte)(flags | ExecuteFunctionHelper.IS_REXECUTE_MASK) : flags;
      byte flags = ExecuteFunctionHelper.createFlags(executeOnBucketSet, isReExecute);
      
      getMessage().addBytesPart(new byte[] { flags });
      getMessage().addIntPart(routingObjects.size());
      for (Object key : routingObjects) {
        getMessage().addStringOrObjPart(key);
      }
      getMessage().addIntPart(removedNodes.size());
      for (Object nodes : removedNodes) {
        getMessage().addStringOrObjPart(nodes);
      }

      this.resultCollector = rc;
      this.regionName = region;
      this.functionId = function;
      this.executor = serverRegionExecutor;
      this.hasResult = functionState;
      this.failedNodes = removedNodes;
    }

    public ExecuteRegionFunctionOpImpl(
        ExecuteRegionFunctionSingleHopOpImpl newop) {
      this(newop.getRegionName(), newop.getFunctionId(), newop.getExecutor(),
          newop.getResultCollector(), newop.getHasResult(),
          new HashSet<String>(), newop.isHA(), newop
              .optimizeForWrite(), false);
    }

    public ExecuteRegionFunctionOpImpl(ExecuteRegionFunctionOpImpl op,
        byte isReExecute, Set<String> removedNodes) {
      super(MessageType.EXECUTE_REGION_FUNCTION, 8
          + op.executor.getFilter().size() + removedNodes.size());
      this.isReExecute = isReExecute;
      this.resultCollector = op.resultCollector;
      this.function = op.function;
      this.functionId = op.functionId;
      this.regionName = op.regionName;
      this.executor = op.executor;
      this.hasResult = op.hasResult;
      this.failedNodes = op.failedNodes;
      this.executeOnBucketSet = op.executeOnBucketSet;
      if (isReExecute == 1) {
        this.resultCollector.endResults();
        this.resultCollector.clearResults();
      }

      Set routingObjects = executor.getFilter();
      Object args = executor.getArguments();
      MemberMappedArgument memberMappedArg = executor.getMemberMappedArgument();
      getMessage().clear();
      addBytes(this.hasResult);
      getMessage().addStringPart(this.regionName);
      if (executor.isFnSerializationReqd()) {
        getMessage().addStringOrObjPart(function);
      }
      else {
        getMessage().addStringOrObjPart(functionId);
      }
      getMessage().addObjPart(args);
      getMessage().addObjPart(memberMappedArg);
//      byte flags = this.executeOnBucketSet ?
//          (byte)(0x00 | Op.BUCKETS_AS_FILTER_MASK) : 0x00;
//      flags = isReExecute == 1? (byte)(flags | Op.IS_REXECUTE_MASK) : flags;
      byte flags = ExecuteFunctionHelper.createFlags(executeOnBucketSet, isReExecute);

      getMessage().addBytesPart(new byte[] {flags });
      getMessage().addIntPart(routingObjects.size());
      for (Object key : routingObjects) {
        getMessage().addStringOrObjPart(key);
      }
      getMessage().addIntPart(removedNodes.size());
      for (Object nodes : removedNodes) {
        getMessage().addStringOrObjPart(nodes);
      }
    }

    private void addBytes(byte functionStateOrHasResult) {
      if (GemFireCacheImpl.getClientFunctionTimeout() == GemFireCacheImpl.DEFAULT_CLIENT_FUNCTION_TIMEOUT) {
        getMessage().addBytesPart(new byte[] { functionStateOrHasResult });
      } else {
        byte[] bytes = new byte[5];
        bytes[0] = functionStateOrHasResult;
        Part.encodeInt(GemFireCacheImpl.getClientFunctionTimeout(), bytes, 1);
        getMessage().addBytesPart(bytes);
      }
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {
      ChunkedMessage executeFunctionResponseMsg = (ChunkedMessage)msg;
      // Read the header which describes the type of message following
      try {
        executeFunctionResponseMsg.readHeader();
        switch (executeFunctionResponseMsg.getMessageType()) {
          case MessageType.EXECUTE_REGION_FUNCTION_RESULT:
            final boolean isDebugEnabled = logger.isDebugEnabled();
            if (isDebugEnabled) {
              logger.debug("ExecuteRegionFunctionOpImpl#processResponse: received message of type EXECUTE_REGION_FUNCTION_RESULT. The number of parts are : {}", executeFunctionResponseMsg.getNumberOfParts());
            }
            // Read the chunk
            do {
              executeFunctionResponseMsg.receiveChunk();
              Object resultResponse = executeFunctionResponseMsg.getPart(0)
                  .getObject();
              Object result;
              if (resultResponse instanceof ArrayList) {
                result = ((ArrayList)resultResponse).get(0);
              }
              else {
                result = resultResponse;
              }
              if (result instanceof FunctionException) {
                FunctionException ex = ((FunctionException)result);
                if (ex instanceof InternalFunctionException) {
                  Throwable cause = ex.getCause();
                  DistributedMember memberID = (DistributedMember)((ArrayList)resultResponse)
                      .get(1);
                  this.resultCollector
                      .addResult(memberID, cause);
                  FunctionStats.getFunctionStats(this.functionId,
                      this.executor.getRegion().getSystem())
                      .incResultsReceived();
                  continue;
                }
                else if (((FunctionException)result).getCause() instanceof InternalFunctionInvocationTargetException) {
                  InternalFunctionInvocationTargetException ifite = (InternalFunctionInvocationTargetException)ex
                      .getCause();
                  this.failedNodes.addAll(ifite.getFailedNodeSet());
                }
                executeFunctionResponseMsg.clear();
                throw ex;
              }
              else if (result instanceof Throwable) {
                String s = "While performing a remote " + getOpName();
                executeFunctionResponseMsg.clear();
                throw new ServerOperationException(s, (Throwable)result);
              }
              else {
                DistributedMember memberID = (DistributedMember)((ArrayList)resultResponse)
                    .get(1);
                this.resultCollector.addResult(memberID, result);
                FunctionStats.getFunctionStats(this.functionId,
                    this.executor.getRegion().getSystem()).incResultsReceived();
              }
            } while (!executeFunctionResponseMsg.isLastChunk());
            if (isDebugEnabled) {
              logger.debug("ExecuteRegionFunctionOpImpl#processResponse: received all the results from server successfully.");
            }
            this.resultCollector.endResults();
            return null;

          case MessageType.EXCEPTION:
            if (logger.isDebugEnabled()) {
              logger.debug("ExecuteRegionFunctionOpImpl#processResponse: received message of type EXCEPTION. The number of parts are : {}", executeFunctionResponseMsg.getNumberOfParts());
            }

            // Read the chunk
            executeFunctionResponseMsg.receiveChunk();
            Part part0 = executeFunctionResponseMsg.getPart(0);
            Object obj = part0.getObject();
            if (obj instanceof FunctionException) {
              FunctionException ex = ((FunctionException)obj);
              if (((FunctionException)obj).getCause() instanceof InternalFunctionInvocationTargetException) {
                InternalFunctionInvocationTargetException ifite = (InternalFunctionInvocationTargetException)ex
                    .getCause();
                this.failedNodes.addAll(ifite.getFailedNodeSet());
              }
              executeFunctionResponseMsg.clear();
              throw ex;
            }
            else if (obj instanceof Throwable) {
              executeFunctionResponseMsg.clear();
              String s = "While performing a remote " + getOpName();
              throw new ServerOperationException(s, (Throwable)obj);
            }
            break;
          case MessageType.EXECUTE_REGION_FUNCTION_ERROR:
            if (logger.isDebugEnabled()) {
              logger.debug("ExecuteRegionFunctionOpImpl#processResponse: received message of type EXECUTE_REGION_FUNCTION_ERROR");
            }
            // Read the chunk
            executeFunctionResponseMsg.receiveChunk();
            String errorMessage = executeFunctionResponseMsg.getPart(0)
                .getString();
            executeFunctionResponseMsg.clear();
            throw new ServerOperationException(errorMessage);
          default:
            throw new InternalGemFireError("Unknown message type "
                + executeFunctionResponseMsg.getMessageType());
        }
      }
      finally {
        executeFunctionResponseMsg.clear();
      }
      return null;
    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return msgType == MessageType.EXECUTE_REGION_FUNCTION_ERROR;
    }

    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startExecuteFunction();
    }

    protected String getOpName() {
      return "executeRegionFunction";
    }

    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endExecuteFunctionSend(start, hasFailed());
    }

    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endExecuteFunction(start, hasTimedOut(), hasFailed());
    }

    @Override
    protected Message createResponseMessage() {
      return new ChunkedMessage(1, Version.CURRENT);
    }

  }
}
