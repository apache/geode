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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.execute.AbstractExecution;
import com.gemstone.gemfire.internal.cache.execute.FunctionStats;
import com.gemstone.gemfire.internal.cache.execute.InternalFunctionException;
import com.gemstone.gemfire.internal.cache.execute.InternalFunctionInvocationTargetException;
import com.gemstone.gemfire.internal.cache.execute.MemberMappedArgument;
import com.gemstone.gemfire.internal.cache.execute.ServerRegionFunctionExecutor;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.ChunkedMessage;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * @since GemFire 6.5
 */
public class ExecuteRegionFunctionSingleHopOp {

  private static final Logger logger = LogService.getLogger();
  
  private ExecuteRegionFunctionSingleHopOp() {
  }

  public static void execute(ExecutablePool pool, Region region,
      Function function, ServerRegionFunctionExecutor serverRegionExecutor,
      ResultCollector resultCollector, byte hasResult,
      Map<ServerLocation, ? extends HashSet> serverToFilterMap, int mRetryAttempts, boolean allBuckets) {

    boolean reexecute = false;
    Set<String> failedNodes = new HashSet<String>();
    int maxRetryAttempts = 0;
    if (function.isHA()) {
      maxRetryAttempts = mRetryAttempts;
    }
    ClientMetadataService cms = ((GemFireCacheImpl)region.getCache())
        .getClientMetadataService();

    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("ExecuteRegionFunctionSingleHopOp#execute : The serverToFilterMap is : {}", serverToFilterMap);
    }
    List<SingleHopOperationCallable> callableTasks = constructAndGetExecuteFunctionTasks(region.getFullPath(),
        serverRegionExecutor, serverToFilterMap, (PoolImpl)pool, function,
        hasResult, resultCollector, cms, allBuckets);

    reexecute = SingleHopClientExecutor.submitAllHA(callableTasks,
        (LocalRegion)region, function.isHA(), resultCollector, failedNodes);

    if (isDebugEnabled) {
      logger.debug("ExecuteRegionFunctionSingleHopOp#execute : The size of callableTask is : {}", callableTasks.size());
    }

    if (reexecute ) {
      resultCollector.clearResults();
      if(function.isHA()) {
      ExecuteRegionFunctionOp.reexecute(pool, region.getFullPath(), function,
          serverRegionExecutor, resultCollector, hasResult, failedNodes,
          maxRetryAttempts - 1);
      }
//      else {
//        ExecuteRegionFunctionOp.execute(pool, region.getFullPath(), function,
//            serverRegionExecutor, resultCollector, hasResult,
//            maxRetryAttempts - 1);
//      }
    }

    resultCollector.endResults();
  }

  public static void execute(ExecutablePool pool, Region region,
      String functionId, ServerRegionFunctionExecutor serverRegionExecutor,
      ResultCollector resultCollector, byte hasResult,
      Map<ServerLocation, ? extends HashSet> serverToFilterMap,
      int mRetryAttempts, boolean allBuckets, boolean isHA,
      boolean optimizeForWrite) {

    boolean reexecute = false;
    Set<String> failedNodes = new HashSet<String>();
    int maxRetryAttempts = 0;
    if (isHA) {
      maxRetryAttempts = mRetryAttempts;
    }
    ClientMetadataService cms = ((GemFireCacheImpl)region.getCache())
        .getClientMetadataService();

    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("ExecuteRegionFunctionSingleHopOp#execute : The serverToFilterMap is : {}", serverToFilterMap);
    }
    List<SingleHopOperationCallable> callableTasks = constructAndGetExecuteFunctionTasks(region.getFullPath(),
        serverRegionExecutor, serverToFilterMap, (PoolImpl)pool, functionId,
        hasResult, resultCollector, cms, allBuckets, isHA,optimizeForWrite);

    reexecute = SingleHopClientExecutor.submitAllHA(callableTasks,
        (LocalRegion)region, isHA, resultCollector, failedNodes);

    if (isDebugEnabled) {
      logger.debug("ExecuteRegionFunctionSingleHopOp#execute : The size of callableTask is: {}, reexecute={}", callableTasks.size(), reexecute);
    }

    if (reexecute) {
      resultCollector.clearResults();
      if (isHA) {
        ExecuteRegionFunctionOp.reexecute(pool, region.getFullPath(),
            functionId, serverRegionExecutor, resultCollector, hasResult,
            failedNodes, maxRetryAttempts - 1, isHA, optimizeForWrite);
      }
//      else {
//        ExecuteRegionFunctionOp.execute(pool, region.getFullPath(), functionId,
//            serverRegionExecutor, resultCollector, hasResult,
//            maxRetryAttempts - 1, isHA, optimizeForWrite);
//      }
    }

    resultCollector.endResults();
  }

  
  static List<SingleHopOperationCallable> constructAndGetExecuteFunctionTasks(String region,
      ServerRegionFunctionExecutor serverRegionExecutor,
      final Map<ServerLocation, ? extends HashSet> serverToFilterMap,
      final PoolImpl pool, final Function function, byte hasResult,
      ResultCollector rc, ClientMetadataService cms, boolean allBucket) {
    final List<SingleHopOperationCallable> tasks = new ArrayList<SingleHopOperationCallable>();
    ArrayList<ServerLocation> servers = new ArrayList<ServerLocation>(
        serverToFilterMap.keySet());

    if (logger.isDebugEnabled()) {
      logger.debug("Constructing tasks for the servers {}", servers);
    }
    for (ServerLocation server : servers) {
      ServerRegionFunctionExecutor executor = (ServerRegionFunctionExecutor)serverRegionExecutor
          .withFilter(serverToFilterMap.get(server));

      AbstractOp op = new ExecuteRegionFunctionSingleHopOpImpl(region, function, executor, rc, hasResult, new HashSet<String>(),
          allBucket);
      SingleHopOperationCallable task = new SingleHopOperationCallable(
          new ServerLocation(server.getHostName(), server.getPort()), pool, op, UserAttributes.userAttributes.get());
      tasks.add(task);
    }
    return tasks;
  }

  static List<SingleHopOperationCallable> constructAndGetExecuteFunctionTasks(String region,
      ServerRegionFunctionExecutor serverRegionExecutor,
      final Map<ServerLocation, ? extends HashSet> serverToFilterMap,
      final PoolImpl pool, final String functionId, byte hasResult,
      ResultCollector rc, ClientMetadataService cms, boolean allBucket, boolean isHA, boolean optimizeForWrite) {
    final List<SingleHopOperationCallable> tasks = new ArrayList<SingleHopOperationCallable>();
    ArrayList<ServerLocation> servers = new ArrayList<ServerLocation>(
        serverToFilterMap.keySet());

    if (logger.isDebugEnabled()) {
      logger.debug("Constructing tasks for the servers {}", servers);
    }
    for (ServerLocation server : servers) {
      ServerRegionFunctionExecutor executor = (ServerRegionFunctionExecutor)serverRegionExecutor
          .withFilter(serverToFilterMap.get(server));

      AbstractOp op = new ExecuteRegionFunctionSingleHopOpImpl(region, functionId, executor, rc, hasResult, new HashSet<String>(),
          allBucket, isHA, optimizeForWrite);
      SingleHopOperationCallable task = new SingleHopOperationCallable(
          new ServerLocation(server.getHostName(), server.getPort()), pool, op, UserAttributes.userAttributes.get());
      tasks.add(task);
    }
    return tasks;
  }
  
  static class ExecuteRegionFunctionSingleHopOpImpl extends AbstractOp {

    private final ResultCollector resultCollector;

    private final String functionId;

    private final String regionName;

    private final ServerRegionFunctionExecutor executor;

    private final byte hasResult;

    private Set<String> failedNodes = new HashSet<String>();

    private boolean isHA; 
    
    private boolean optimizeForWrite;

    public ExecuteRegionFunctionSingleHopOpImpl(String region,
        Function function, ServerRegionFunctionExecutor serverRegionExecutor,
        ResultCollector rc, byte hasResult,
        Set<String> removedNodes, boolean allBuckets) {
      // What is this 8 that is getting added to filter and removednode sizes?
      // It should have been used as a constant and documented
      super(MessageType.EXECUTE_REGION_FUNCTION_SINGLE_HOP, 8
          + serverRegionExecutor.getFilter().size() + removedNodes.size());
      this.isHA = function.isHA();
      this.optimizeForWrite = function.optimizeForWrite();
      byte functionState = AbstractExecution.getFunctionState(function.isHA(),
          function.hasResult() , function.optimizeForWrite());
      Set routingObjects = serverRegionExecutor.getFilter();
      Object args = serverRegionExecutor.getArguments();
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
      getMessage().addBytesPart(new byte[] { allBuckets ? (byte)1 : (byte)0 });
      getMessage().addIntPart(routingObjects.size());
      for (Object key : routingObjects) {
        if(allBuckets){
          getMessage().addIntPart((Integer)key);
        }
        else {
          getMessage().addStringOrObjPart(key);
        }
      }
      getMessage().addIntPart(removedNodes.size());
      for (Object nodes : removedNodes) {
        getMessage().addStringOrObjPart(nodes);
      }

      this.resultCollector = rc;
      this.regionName = region;
      this.functionId = function.getId();
      this.executor = serverRegionExecutor;
      this.hasResult = functionState;
      this.failedNodes = removedNodes;
    }

    public ExecuteRegionFunctionSingleHopOpImpl(String region,
        String functionId, ServerRegionFunctionExecutor serverRegionExecutor,
        ResultCollector rc, byte hasResult,
        Set<String> removedNodes, boolean allBuckets, boolean isHA, boolean optimizeForWrite) {
      // What is this 8 that is getting added to filter and removednode sizes?
      // It should have been used as a constant and documented
      super(MessageType.EXECUTE_REGION_FUNCTION_SINGLE_HOP, 8
          + serverRegionExecutor.getFilter().size() + removedNodes.size());
      this.isHA = isHA;
      this.optimizeForWrite = optimizeForWrite;
      Set routingObjects = serverRegionExecutor.getFilter();
      Object args = serverRegionExecutor.getArguments();
      byte functionState = AbstractExecution.getFunctionState(isHA,
          hasResult == (byte)1 ? true : false, optimizeForWrite);
      MemberMappedArgument memberMappedArg = serverRegionExecutor
          .getMemberMappedArgument();
      addBytes(functionState);
      getMessage().addStringPart(region);
      getMessage().addStringOrObjPart(functionId);
      getMessage().addObjPart(args);
      getMessage().addObjPart(memberMappedArg);
      getMessage().addBytesPart(new byte[] { allBuckets ? (byte)1 : (byte)0 });
      getMessage().addIntPart(routingObjects.size());
      for (Object key : routingObjects) {
        if(allBuckets){
          getMessage().addIntPart((Integer)key);
        }
        else {
          getMessage().addStringOrObjPart(key);
        }
      }
      getMessage().addIntPart(removedNodes.size());
      for (Object nodes : removedNodes) {
        getMessage().addStringOrObjPart(nodes);
      }

      this.resultCollector = rc;
      this.regionName = region;
      this.functionId = functionId;
      this.executor = serverRegionExecutor;
      this.hasResult = functionState;
      this.failedNodes = removedNodes;
    }

    private void addBytes(byte functionState) {
      if (GemFireCacheImpl.getClientFunctionTimeout() == GemFireCacheImpl.DEFAULT_CLIENT_FUNCTION_TIMEOUT) {
        getMessage().addBytesPart(new byte[] { functionState });
      } else {
        byte[] bytes = new byte[5];
        bytes[0] = functionState;
        Part.encodeInt(GemFireCacheImpl.getClientFunctionTimeout(), bytes, 1);
        getMessage().addBytesPart(bytes);
      }
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {
      ChunkedMessage executeFunctionResponseMsg = (ChunkedMessage)msg;
      try {
        executeFunctionResponseMsg.readHeader();
        switch (executeFunctionResponseMsg.getMessageType()) {
          case MessageType.EXECUTE_REGION_FUNCTION_RESULT:
            final boolean isDebugEnabled = logger.isDebugEnabled();
            if (isDebugEnabled) {
              logger.debug("ExecuteRegionFunctionSingleHopOpImpl#processResponse: received message of type EXECUTE_REGION_FUNCTION_RESULT.");
            }
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
                if (isDebugEnabled) {
                  logger.debug("ExecuteRegionFunctionSingleHopOpImpl#processResponse: received Exception. {}", ex.getCause());
                }
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
                if (!ex.getMessage().equals("Buckets are null"))
                  throw ex;

                return null;
              }
              else if (result instanceof Throwable) {
                String s = "While performing a remote " + getOpName();
                throw new ServerOperationException(s, (Throwable)result);
              }
              else {
                DistributedMember memberID = (DistributedMember)((ArrayList)resultResponse)
                    .get(1);
                synchronized (this.resultCollector) {
                  this.resultCollector
                      .addResult(memberID, result);
                }
                FunctionStats.getFunctionStats(this.functionId,
                    this.executor.getRegion().getSystem()).incResultsReceived();
              }
            } while (!executeFunctionResponseMsg.isLastChunk());
            if (isDebugEnabled) {
              logger.debug("ExecuteRegionFunctionSingleHopOpImpl#processResponse: received all the results from server successfully.");
            }
            return null;
            
          case MessageType.EXCEPTION:
            if (logger.isDebugEnabled()) {
              logger.debug("ExecuteRegionFunctionSingleHopOpImpl#processResponse: received message of type EXCEPTION");
            }
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
              if (!ex.getMessage().equals("Buckets are null")) {
                throw ex;
              }
              return null;
            }
            else if (obj instanceof Throwable) {
              String s = "While performing a remote " + getOpName();
              throw new ServerOperationException(s, (Throwable)obj);
            }
            break;
          case MessageType.EXECUTE_REGION_FUNCTION_ERROR:
            if (logger.isDebugEnabled()) {
              logger.debug("ExecuteRegionFunctionSingleHopOpImpl#processResponse: received message of type EXECUTE_REGION_FUNCTION_ERROR");
            }
            executeFunctionResponseMsg.receiveChunk();
            String errorMessage = executeFunctionResponseMsg.getPart(0)
                .getString();
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

    ResultCollector getResultCollector() {
      return this.resultCollector;
    }

    String getFunctionId() {
      return this.functionId;
    }

    String getRegionName() {
      return this.regionName;
    }

    ServerRegionFunctionExecutor getExecutor() {
      return this.executor;
    }

    byte getHasResult() {
      return this.hasResult;
    }

    boolean isHA() {
      return this.isHA;
    }

     boolean optimizeForWrite() {
      return this.optimizeForWrite;
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
      return "executeRegionFunctionSingleHop";
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
