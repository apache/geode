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

package org.apache.geode.cache.client.internal;

import static org.apache.geode.internal.cache.execute.AbstractExecution.DEFAULT_CLIENT_FUNCTION_TIMEOUT;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.logging.log4j.Logger;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionInvocationTargetException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.execute.AbstractExecution;
import org.apache.geode.internal.cache.execute.BucketMovedException;
import org.apache.geode.internal.cache.execute.InternalFunctionException;
import org.apache.geode.internal.cache.execute.InternalFunctionInvocationTargetException;
import org.apache.geode.internal.cache.execute.MemberMappedArgument;
import org.apache.geode.internal.cache.execute.ServerRegionFunctionExecutor;
import org.apache.geode.internal.cache.execute.metrics.FunctionStatsManager;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * @since GemFire 6.5
 */
public class ExecuteRegionFunctionSingleHopOp {

  private static final Logger logger = LogService.getLogger();

  private ExecuteRegionFunctionSingleHopOp() {}


  public static void execute(ExecutablePool pool, Region region,
      ServerRegionFunctionExecutor serverRegionExecutor,
      ResultCollector resultCollector,
      Map<ServerLocation, ? extends Set> serverToFilterMap,
      boolean isHA,
      final java.util.function.Function<ServerRegionFunctionExecutor, AbstractOp> regionFunctionSingleHopOpFunction,
      final Supplier<AbstractOp> executeRegionFunctionOpSupplier) {

    Set<String> failedNodes = new HashSet<>();

    ClientMetadataService cms = ((InternalCache) region.getCache()).getClientMetadataService();

    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("ExecuteRegionFunctionSingleHopOp#execute : The serverToFilterMap is : {}",
          serverToFilterMap);
    }

    List<SingleHopOperationCallable> callableTasks = constructAndGetExecuteFunctionTasks(
        serverRegionExecutor, serverToFilterMap, (PoolImpl) pool,
        cms, regionFunctionSingleHopOpFunction);

    final int retryAttempts =
        SingleHopClientExecutor.submitAllHA(callableTasks, (LocalRegion) region, isHA,
            resultCollector, failedNodes, ((PoolImpl) pool));

    if (isDebugEnabled) {
      logger.debug(
          "ExecuteRegionFunctionSingleHopOp#execute : The size of callableTask is: {}, reexecute={}",
          callableTasks.size(), retryAttempts);
    }

    if (retryAttempts > 0) {
      resultCollector.clearResults();

      final ExecuteRegionFunctionOp.ExecuteRegionFunctionOpImpl executeRegionFunctionOp =
          (ExecuteRegionFunctionOp.ExecuteRegionFunctionOpImpl) executeRegionFunctionOpSupplier
              .get();

      ExecuteRegionFunctionOp.execute(pool,
          resultCollector, retryAttempts - 1,
          isHA,
          executeRegionFunctionOp, true, failedNodes);
    }

    resultCollector.endResults();
  }


  private static List<SingleHopOperationCallable> constructAndGetExecuteFunctionTasks(
      ServerRegionFunctionExecutor serverRegionExecutor,
      final Map<ServerLocation, ? extends Set> serverToFilterMap,
      final PoolImpl pool,
      ClientMetadataService cms,
      final java.util.function.Function<ServerRegionFunctionExecutor, AbstractOp> opFactory) {
    final List<SingleHopOperationCallable> tasks = new ArrayList<>();
    ArrayList<ServerLocation> servers = new ArrayList<>(serverToFilterMap.keySet());

    if (logger.isDebugEnabled()) {
      logger.debug("Constructing tasks for the servers {}", servers);
    }
    for (ServerLocation server : servers) {
      ServerRegionFunctionExecutor executor = (ServerRegionFunctionExecutor) serverRegionExecutor
          .withFilter(serverToFilterMap.get(server));

      AbstractOp op = opFactory.apply(executor);

      SingleHopOperationCallable task =
          new SingleHopOperationCallable(new ServerLocation(server.getHostName(), server.getPort()),
              pool, op, UserAttributes.userAttributes.get());
      tasks.add(task);
    }
    return tasks;
  }

  static class ExecuteRegionFunctionSingleHopOpImpl extends AbstractOpWithTimeout {

    private final ResultCollector resultCollector;

    private final String functionId;

    private final String regionName;

    private final ServerRegionFunctionExecutor executor;

    private final byte hasResult;

    private Set<String> failedNodes;

    private boolean isHA;

    private boolean optimizeForWrite;

    ExecuteRegionFunctionSingleHopOpImpl(String region, Function function,
        ServerRegionFunctionExecutor serverRegionExecutor, ResultCollector rc, byte hasResult,
        Set<String> removedNodes, boolean allBuckets, final int timeoutMs) {
      // What is this 8 that is getting added to filter and removednode sizes?
      // It should have been used as a constant and documented
      super(MessageType.EXECUTE_REGION_FUNCTION_SINGLE_HOP,
          8 + serverRegionExecutor.getFilter().size() + removedNodes.size(), timeoutMs);
      isHA = function.isHA();
      optimizeForWrite = function.optimizeForWrite();
      byte functionState = AbstractExecution.getFunctionState(function.isHA(), function.hasResult(),
          function.optimizeForWrite());
      Set routingObjects = serverRegionExecutor.getFilter();
      Object args = serverRegionExecutor.getArguments();
      MemberMappedArgument memberMappedArg = serverRegionExecutor.getMemberMappedArgument();
      addBytes(functionState);
      getMessage().addStringPart(region, true);
      if (serverRegionExecutor.isFnSerializationReqd()) {
        getMessage().addStringOrObjPart(function);
      } else {
        getMessage().addStringOrObjPart(function.getId());
      }
      getMessage().addObjPart(args);
      getMessage().addObjPart(memberMappedArg);
      getMessage().addBytesPart(new byte[] {(byte) (allBuckets ? 1 : 0)});
      getMessage().addIntPart(routingObjects.size());
      for (Object key : routingObjects) {
        if (allBuckets) {
          getMessage().addIntPart((Integer) key);
        } else {
          getMessage().addStringOrObjPart(key);
        }
      }
      getMessage().addIntPart(removedNodes.size());
      for (Object nodes : removedNodes) {
        getMessage().addStringOrObjPart(nodes);
      }

      resultCollector = rc;
      regionName = region;
      functionId = function.getId();
      executor = serverRegionExecutor;
      this.hasResult = functionState;
      failedNodes = removedNodes;
    }

    ExecuteRegionFunctionSingleHopOpImpl(String region, String functionId,
        ServerRegionFunctionExecutor serverRegionExecutor, ResultCollector rc, byte hasResult,
        Set<String> removedNodes, boolean allBuckets, boolean isHA, boolean optimizeForWrite,
        final int timeoutMs) {
      // What is this 8 that is getting added to filter and removednode sizes?
      // It should have been used as a constant and documented
      super(MessageType.EXECUTE_REGION_FUNCTION_SINGLE_HOP,
          8 + serverRegionExecutor.getFilter().size() + removedNodes.size(), timeoutMs);
      this.isHA = isHA;
      this.optimizeForWrite = optimizeForWrite;
      Set routingObjects = serverRegionExecutor.getFilter();
      Object args = serverRegionExecutor.getArguments();
      byte functionState = AbstractExecution.getFunctionState(isHA,
          hasResult == (byte) 1, optimizeForWrite);
      MemberMappedArgument memberMappedArg = serverRegionExecutor.getMemberMappedArgument();
      addBytes(functionState);
      getMessage().addStringPart(region, true);
      getMessage().addStringOrObjPart(functionId);
      getMessage().addObjPart(args);
      getMessage().addObjPart(memberMappedArg);
      getMessage().addBytesPart(new byte[] {(byte) (allBuckets ? 1 : 0)});
      getMessage().addIntPart(routingObjects.size());
      for (Object key : routingObjects) {
        if (allBuckets) {
          getMessage().addIntPart((Integer) key);
        } else {
          getMessage().addStringOrObjPart(key);
        }
      }
      getMessage().addIntPart(removedNodes.size());
      for (Object nodes : removedNodes) {
        getMessage().addStringOrObjPart(nodes);
      }

      resultCollector = rc;
      regionName = region;
      this.functionId = functionId;
      executor = serverRegionExecutor;
      this.hasResult = functionState;
      failedNodes = removedNodes;
    }

    private void addBytes(byte functionState) {
      if (getTimeoutMs() == DEFAULT_CLIENT_FUNCTION_TIMEOUT) {
        getMessage().addBytesPart(new byte[] {functionState});
      } else {
        byte[] bytes = new byte[5];
        bytes[0] = functionState;
        Part.encodeInt(getTimeoutMs(), bytes, 1);
        getMessage().addBytesPart(bytes);
      }
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {
      ChunkedMessage executeFunctionResponseMsg = (ChunkedMessage) msg;
      try {
        executeFunctionResponseMsg.readHeader();
        switch (executeFunctionResponseMsg.getMessageType()) {
          case MessageType.EXECUTE_REGION_FUNCTION_RESULT:
            final boolean isDebugEnabled = logger.isDebugEnabled();
            if (isDebugEnabled) {
              logger.debug(
                  "ExecuteRegionFunctionSingleHopOpImpl#processResponse: received message of type EXECUTE_REGION_FUNCTION_RESULT.");
            }
            Exception exception = null;
            do {
              executeFunctionResponseMsg.receiveChunk();
              Object resultResponse = executeFunctionResponseMsg.getPart(0).getObject();
              Object result;
              if (resultResponse instanceof ArrayList) {
                result = ((List) resultResponse).get(0);
              } else {
                result = resultResponse;
              }
              if (result instanceof FunctionException) {
                FunctionException ex = ((FunctionException) result);
                if (isDebugEnabled) {
                  logger.debug(
                      "ExecuteRegionFunctionSingleHopOpImpl#processResponse: received Exception.",
                      ex.getCause());
                }
                if (ex instanceof InternalFunctionException) {
                  Throwable cause = ex.getCause();
                  DistributedMember memberID = (DistributedMember) ((List) resultResponse).get(1);
                  resultCollector.addResult(memberID, cause);
                  FunctionStatsManager
                      .getFunctionStats(functionId, executor.getRegion().getSystem())
                      .incResultsReceived();
                  continue;
                } else if (((FunctionException) result)
                    .getCause() instanceof InternalFunctionInvocationTargetException) {
                  InternalFunctionInvocationTargetException ifite =
                      (InternalFunctionInvocationTargetException) ex.getCause();
                  failedNodes.addAll(ifite.getFailedNodeSet());
                }
                if (!ex.getMessage().equals("Buckets are null"))
                  exception = ex;
              } else if (result instanceof BucketMovedException) {
                FunctionInvocationTargetException fite =
                    new InternalFunctionInvocationTargetException(
                        ((BucketMovedException) result).getMessage());
                exception = new FunctionException(fite);
              } else if (result instanceof CacheClosedException) {
                FunctionInvocationTargetException fite =
                    new InternalFunctionInvocationTargetException(
                        ((CacheClosedException) result).getMessage());
                if (resultResponse instanceof ArrayList) {
                  DistributedMember memberID = (DistributedMember) ((List) resultResponse).get(1);
                  failedNodes.add(memberID.getId());
                }
                exception = new FunctionException(fite);
              } else if (result instanceof Throwable) {
                String s = "While performing a remote " + getOpName();
                exception = new ServerOperationException(s, (Throwable) result);
              } else {
                DistributedMember memberID = (DistributedMember) ((List) resultResponse).get(1);
                resultCollector.addResult(memberID, result);
                FunctionStatsManager
                    .getFunctionStats(functionId, executor.getRegion().getSystem())
                    .incResultsReceived();
              }
            } while (!executeFunctionResponseMsg.isLastChunk());

            if (exception != null) {
              throw exception;
            }

            if (isDebugEnabled) {
              logger.debug(
                  "ExecuteRegionFunctionSingleHopOpImpl#processResponse: received all the results from server successfully.");
            }
            return null;

          case MessageType.EXCEPTION:
            if (logger.isDebugEnabled()) {
              logger.debug(
                  "ExecuteRegionFunctionSingleHopOpImpl#processResponse: received message of type EXCEPTION");
            }
            executeFunctionResponseMsg.receiveChunk();
            Part part0 = executeFunctionResponseMsg.getPart(0);
            Object obj = part0.getObject();

            if (obj instanceof FunctionException) {
              FunctionException ex = ((FunctionException) obj);
              if (((FunctionException) obj)
                  .getCause() instanceof InternalFunctionInvocationTargetException) {
                InternalFunctionInvocationTargetException ifite =
                    (InternalFunctionInvocationTargetException) ex.getCause();
                failedNodes.addAll(ifite.getFailedNodeSet());
              }
              if (!ex.getMessage().equals("Buckets are null")) {
                throw ex;
              }
              return null;
            } else if (obj instanceof Throwable) {
              String s = "While performing a remote " + getOpName();
              throw new ServerOperationException(s, (Throwable) obj);
            }
            break;
          case MessageType.EXECUTE_REGION_FUNCTION_ERROR:
            if (logger.isDebugEnabled()) {
              logger.debug(
                  "ExecuteRegionFunctionSingleHopOpImpl#processResponse: received message of type EXECUTE_REGION_FUNCTION_ERROR");
            }
            executeFunctionResponseMsg.receiveChunk();
            String errorMessage = executeFunctionResponseMsg.getPart(0).getString();
            throw new ServerOperationException(errorMessage);

          default:
            throw new InternalGemFireError(
                "Unknown message type " + executeFunctionResponseMsg.getMessageType());
        }
      } finally {
        executeFunctionResponseMsg.clear();
      }
      return null;
    }

    ResultCollector getResultCollector() {
      return resultCollector;
    }

    String getFunctionId() {
      return functionId;
    }

    String getRegionName() {
      return regionName;
    }

    ServerRegionFunctionExecutor getExecutor() {
      return executor;
    }

    byte getHasResult() {
      return hasResult;
    }

    boolean isHA() {
      return isHA;
    }

    boolean optimizeForWrite() {
      return optimizeForWrite;
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
