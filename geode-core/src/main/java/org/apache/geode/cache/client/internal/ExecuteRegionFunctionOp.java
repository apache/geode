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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.client.NoAvailableServersException;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.client.internal.ExecuteRegionFunctionSingleHopOp.ExecuteRegionFunctionSingleHopOpImpl;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionInvocationTargetException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.execute.AbstractExecution;
import org.apache.geode.internal.cache.execute.BucketMovedException;
import org.apache.geode.internal.cache.execute.FunctionStats;
import org.apache.geode.internal.cache.execute.InternalFunctionException;
import org.apache.geode.internal.cache.execute.InternalFunctionInvocationTargetException;
import org.apache.geode.internal.cache.execute.MemberMappedArgument;
import org.apache.geode.internal.cache.execute.ServerRegionFunctionExecutor;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.logging.LogService;

/**
 * Does a Execution of function on server region.<br>
 * It alos gets result from the server
 *
 * @since GemFire 5.8Beta
 */
public class ExecuteRegionFunctionOp {

  private static final Logger logger = LogService.getLogger();

  private ExecuteRegionFunctionOp() {
    // no instances allowed
  }

  private static void execute(ExecutablePool pool, String region, String function,
      ServerRegionFunctionExecutor serverRegionExecutor, ResultCollector resultCollector,
      byte hasResult, int maxRetryAttempts, boolean isHA, boolean optimizeForWrite,
      ExecuteRegionFunctionOpImpl op, boolean isReexecute, Set<String> failedNodes) {

    if (!isHA) {
      maxRetryAttempts = 0;
    }

    do {
      try {
        if (isReexecute) {
          failedNodes = ensureMutability(failedNodes);
          op = new ExecuteRegionFunctionOpImpl(op,
              (byte) 1/* isReExecute */, failedNodes);
        }
        pool.execute(op, 0);
        return;
      } catch (InternalFunctionInvocationTargetException e) {
        resultCollector.clearResults();
        if (!isHA) {
          return;
        }
        isReexecute = true;
        Set<String> failedNodesIds = e.getFailedNodeSet();
        failedNodes = ensureMutability(failedNodes);
        failedNodes.clear();
        if (failedNodesIds != null) {
          failedNodes.addAll(failedNodesIds);
        }
      } catch (ServerOperationException | NoAvailableServersException failedException) {
        throw failedException;
      } catch (ServerConnectivityException se) {

        if (maxRetryAttempts == PoolFactory.DEFAULT_RETRY_ATTEMPTS) {
          // If the retryAttempt is set to default(-1). Try it on all servers once.
          // Calculating number of servers when function is re-executed as it involves
          // messaging locator.
          maxRetryAttempts = ((PoolImpl) pool).getConnectionSource().getAllServers().size() - 1;
        }

        if ((maxRetryAttempts--) < 1) {
          throw se;
        }

        isReexecute = true;
        resultCollector.clearResults();
        failedNodes = ensureMutability(failedNodes);
        failedNodes.clear();
      }
    } while (true);
  }

  private static Set<String> ensureMutability(final Set<String> failedNodes) {
    if (failedNodes == Collections.EMPTY_SET) {
      return new HashSet<>();
    }
    return failedNodes;
  }

  /**
   * Does a execute Function on a server using connections from the given pool to communicate with
   * the server.
   *
   * @param pool the pool to use to communicate with the server.
   * @param region the name of the region to do the put on
   * @param function to be executed
   * @param serverRegionExecutor which will return argument and filter
   * @param resultCollector is used to collect the results from the Server
   * @param hasResult is used to collect the results from the Server
   * @param maxRetryAttempts Maximum number of retry attempts
   * @param timeoutMs timeout in milliseconds
   */
  static void execute(ExecutablePool pool, String region, Function function,
      ServerRegionFunctionExecutor serverRegionExecutor, ResultCollector resultCollector,
      byte hasResult, int maxRetryAttempts, final int timeoutMs) {

    ExecuteRegionFunctionOpImpl op = new ExecuteRegionFunctionOpImpl(region, function,
        serverRegionExecutor, resultCollector, timeoutMs);

    execute(pool, region, function.getId(), serverRegionExecutor, resultCollector, hasResult,
        maxRetryAttempts, function.isHA(), function.optimizeForWrite(), op, false,
        Collections.emptySet());
  }

  static void execute(ExecutablePool pool, String region, String function,
      ServerRegionFunctionExecutor serverRegionExecutor, ResultCollector resultCollector,
      byte hasResult, int maxRetryAttempts, boolean isHA, boolean optimizeForWrite,
      final int timeoutMs) {

    ExecuteRegionFunctionOpImpl op = new ExecuteRegionFunctionOpImpl(region, function,
        serverRegionExecutor, resultCollector, hasResult, isHA, optimizeForWrite,
        true, timeoutMs);

    execute(pool, region, function, serverRegionExecutor, resultCollector, hasResult,
        maxRetryAttempts, isHA, optimizeForWrite, op, false, Collections.emptySet());
  }

  static void reexecute(ExecutablePool pool, String region, Function function,
      ServerRegionFunctionExecutor serverRegionExecutor, ResultCollector resultCollector,
      byte hasResult, Set<String> failedNodes, int retryAttempts, final int timeoutMs) {

    ExecuteRegionFunctionOpImpl op = new ExecuteRegionFunctionOpImpl(region, function,
        serverRegionExecutor, resultCollector, timeoutMs);

    execute(pool, region, function.getId(), serverRegionExecutor, resultCollector, hasResult,
        retryAttempts, function.isHA(), function.optimizeForWrite(), op, true, failedNodes);
  }

  static void reexecute(ExecutablePool pool, String region, String function,
      ServerRegionFunctionExecutor serverRegionExecutor, ResultCollector resultCollector,
      byte hasResult, Set<String> failedNodes, int retryAttempts, boolean isHA,
      boolean optimizeForWrite, final int timeoutMs) {

    ExecuteRegionFunctionOpImpl op = new ExecuteRegionFunctionOpImpl(region, function,
        serverRegionExecutor, resultCollector, hasResult, isHA,
        optimizeForWrite, true, timeoutMs);

    execute(pool, region, function, serverRegionExecutor, resultCollector, hasResult,
        retryAttempts, isHA, optimizeForWrite, op, true, failedNodes);
  }

  static class ExecuteRegionFunctionOpImpl extends AbstractOpWithTimeout {

    // To collect the results from the server
    private final ResultCollector resultCollector;

    // To get the instance of the Function Statistics we need the function name or instance
    private Function function;

    private byte isReExecute = 0;

    private final String regionName;

    private final ServerRegionFunctionExecutor executor;

    private final byte hasResult;

    private Set<String> failedNodes;

    private final String functionId;

    private final boolean executeOnBucketSet;

    private final boolean isHA;

    private FunctionException functionException;

    private static final int PART_COUNT = 8;

    private static int getMessagePartCount(int filterSize, int removedNodesSize) {
      return PART_COUNT + filterSize + removedNodesSize;
    }

    private void fillMessage(String region, Function function, String functionId,
        ServerRegionFunctionExecutor serverRegionExecutor,
        Set<String> removedNodes, byte functionState, byte flags) {
      Set routingObjects = serverRegionExecutor.getFilter();
      Object args = serverRegionExecutor.getArguments();
      MemberMappedArgument memberMappedArg = serverRegionExecutor.getMemberMappedArgument();
      addBytes(functionState);
      getMessage().addStringPart(region, true);
      if (function != null && serverRegionExecutor.isFnSerializationReqd()) {
        getMessage().addStringOrObjPart(function);
      } else {
        getMessage().addStringOrObjPart(functionId);
      }

      getMessage().addObjPart(args);
      getMessage().addObjPart(memberMappedArg);

      getMessage().addBytesPart(new byte[] {flags});
      getMessage().addIntPart(routingObjects.size());
      for (Object key : routingObjects) {
        getMessage().addStringOrObjPart(key);
      }
      getMessage().addIntPart(removedNodes.size());
      for (Object nodes : removedNodes) {
        getMessage().addStringOrObjPart(nodes);
      }
    }

    ExecuteRegionFunctionOpImpl(String region, Function function,
        ServerRegionFunctionExecutor serverRegionExecutor, ResultCollector rc,
        final int timeoutMs) {
      super(MessageType.EXECUTE_REGION_FUNCTION,
          getMessagePartCount(serverRegionExecutor.getFilter().size(), 0), timeoutMs);
      executeOnBucketSet = serverRegionExecutor.getExecuteOnBucketSetFlag();
      byte flags = ExecuteFunctionHelper.createFlags(executeOnBucketSet, isReExecute);
      byte functionState = AbstractExecution.getFunctionState(function.isHA(), function.hasResult(),
          function.optimizeForWrite());
      failedNodes = Collections.emptySet();
      fillMessage(region,
          function, function.getId(),
          serverRegionExecutor, failedNodes, functionState, flags);
      resultCollector = rc;
      regionName = region;
      this.function = function;
      functionId = function.getId();
      executor = serverRegionExecutor;
      hasResult = functionState;
      isHA = function.isHA();
    }

    // For testing only
    ExecuteRegionFunctionOpImpl() {
      super(MessageType.EXECUTE_REGION_FUNCTION, 0, DEFAULT_CLIENT_FUNCTION_TIMEOUT);
      resultCollector = null;
      function = null;
      isReExecute = (byte) 0;
      regionName = "";
      executor = null;
      hasResult = (byte) 0;
      failedNodes = null;
      functionId = null;
      executeOnBucketSet = true;
      isHA = true;
    }

    ExecuteRegionFunctionOpImpl(String region, String functionId,
        ServerRegionFunctionExecutor serverRegionExecutor, ResultCollector rc, byte hasResult,
        boolean isHA, boolean optimizeForWrite,
        boolean calculateFnState, final int timeoutMs) {
      super(MessageType.EXECUTE_REGION_FUNCTION,
          getMessagePartCount(serverRegionExecutor.getFilter().size(), 0), timeoutMs);

      byte functionState = hasResult;
      if (calculateFnState) {
        functionState = AbstractExecution.getFunctionState(isHA,
            hasResult == (byte) 1, optimizeForWrite);
      }

      executeOnBucketSet = serverRegionExecutor.getExecuteOnBucketSetFlag();
      byte flags = ExecuteFunctionHelper.createFlags(executeOnBucketSet, isReExecute);

      failedNodes = Collections.emptySet();
      fillMessage(region, null, functionId, serverRegionExecutor, failedNodes, functionState,
          flags);

      resultCollector = rc;
      regionName = region;
      this.functionId = functionId;
      executor = serverRegionExecutor;
      this.hasResult = functionState;
      this.isHA = isHA;
    }

    ExecuteRegionFunctionOpImpl(ExecuteRegionFunctionSingleHopOpImpl newop) {
      this(newop.getRegionName(), newop.getFunctionId(), newop.getExecutor(),
          newop.getResultCollector(), newop.getHasResult(), newop.isHA(),
          newop.optimizeForWrite(), false, newop.getTimeoutMs());
    }

    ExecuteRegionFunctionOpImpl(ExecuteRegionFunctionOpImpl op, byte isReExecute,
        Set<String> removedNodes) {
      super(MessageType.EXECUTE_REGION_FUNCTION,
          getMessagePartCount(op.executor.getFilter().size(), removedNodes.size()),
          op.getTimeoutMs());
      this.isReExecute = isReExecute;
      resultCollector = op.resultCollector;
      function = op.function;
      functionId = op.functionId;
      regionName = op.regionName;
      executor = op.executor;
      hasResult = op.hasResult;
      failedNodes = op.failedNodes;
      executeOnBucketSet = op.executeOnBucketSet;
      isHA = op.isHA;
      if (isReExecute == 1) {
        resultCollector.endResults();
        resultCollector.clearResults();
      }

      byte flags = ExecuteFunctionHelper.createFlags(executeOnBucketSet, isReExecute);

      fillMessage(regionName, function, functionId,
          executor, removedNodes, hasResult, flags);
    }

    private void addBytes(byte functionStateOrHasResult) {
      if (getTimeoutMs() == DEFAULT_CLIENT_FUNCTION_TIMEOUT) {
        getMessage().addBytesPart(new byte[] {functionStateOrHasResult});
      } else {
        byte[] bytes = new byte[5];
        bytes[0] = functionStateOrHasResult;
        Part.encodeInt(getTimeoutMs(), bytes, 1);
        getMessage().addBytesPart(bytes);
      }
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {
      ChunkedMessage executeFunctionResponseMsg = (ChunkedMessage) msg;
      // Read the header which describes the type of message following
      try {
        executeFunctionResponseMsg.readHeader();
        switch (executeFunctionResponseMsg.getMessageType()) {
          case MessageType.EXECUTE_REGION_FUNCTION_RESULT:
            final boolean isDebugEnabled = logger.isDebugEnabled();
            if (isDebugEnabled) {
              logger.debug(
                  "ExecuteRegionFunctionOpImpl#processResponse: received message of type EXECUTE_REGION_FUNCTION_RESULT. The number of parts are : {}",
                  executeFunctionResponseMsg.getNumberOfParts());
            }
            // Read the chunk
            boolean throwServerOp = false;
            do {
              executeFunctionResponseMsg.receiveChunk();
              Object resultResponse = executeFunctionResponseMsg.getPart(0).getObject();
              Object result;
              if (resultResponse instanceof ArrayList) {
                result = ((ArrayList) resultResponse).get(0);
              } else {
                result = resultResponse;
              }

              // if the function is HA throw exceptions
              // if nonHA collect these exceptions and wait till you get last chunk
              if (result instanceof FunctionException) {
                FunctionException ex = ((FunctionException) result);
                if (ex instanceof InternalFunctionException) {
                  Throwable cause = ex.getCause();
                  DistributedMember memberID =
                      (DistributedMember) ((ArrayList) resultResponse).get(1);
                  resultCollector.addResult(memberID, cause);
                  FunctionStats
                      .getFunctionStats(functionId, executor.getRegion().getSystem())
                      .incResultsReceived();
                } else if (((FunctionException) result)
                    .getCause() instanceof InternalFunctionInvocationTargetException) {
                  InternalFunctionInvocationTargetException ifite =
                      (InternalFunctionInvocationTargetException) ex.getCause();
                  failedNodes = ensureMutability(failedNodes);
                  failedNodes.addAll(ifite.getFailedNodeSet());
                  addFunctionException((FunctionException) result);
                } else {
                  addFunctionException((FunctionException) result);
                }
              } else if (result instanceof Throwable) {
                Throwable t = (Throwable) result;
                if (functionException == null) {
                  if (result instanceof BucketMovedException) {
                    FunctionInvocationTargetException fite;
                    if (isHA) {
                      fite = new InternalFunctionInvocationTargetException(
                          ((BucketMovedException) result).getMessage());
                    } else {
                      fite = new FunctionInvocationTargetException(
                          ((BucketMovedException) result).getMessage());
                    }
                    functionException = new FunctionException(fite);
                    functionException.addException(fite);
                  } else if (result instanceof CacheClosedException) {
                    FunctionInvocationTargetException fite;
                    if (isHA) {
                      fite = new InternalFunctionInvocationTargetException(
                          ((CacheClosedException) result).getMessage());
                    } else {
                      fite = new FunctionInvocationTargetException(
                          ((CacheClosedException) result).getMessage());
                    }
                    if (resultResponse instanceof ArrayList) {
                      DistributedMember memberID =
                          (DistributedMember) ((ArrayList) resultResponse).get(1);
                      failedNodes = ensureMutability(failedNodes);
                      failedNodes.add(memberID.getId());
                    }
                    functionException = new FunctionException(fite);
                    functionException.addException(fite);
                  } else {
                    throwServerOp = true;
                    functionException = new FunctionException(t);
                    functionException.addException(t);
                  }
                } else {
                  functionException.addException(t);
                }
              } else {
                DistributedMember memberID =
                    (DistributedMember) ((ArrayList) resultResponse).get(1);
                resultCollector.addResult(memberID, result);
                FunctionStats
                    .getFunctionStats(functionId, executor.getRegion().getSystem())
                    .incResultsReceived();
              }
            } while (!executeFunctionResponseMsg.isLastChunk());
            if (isDebugEnabled) {
              logger.debug(
                  "ExecuteRegionFunctionOpImpl#processResponse: received all the results from server successfully.");
            }


            if (isHA && throwServerOp) {
              String s = "While performing a remote " + getOpName();
              throw new ServerOperationException(s, functionException);
            }

            // add all the exceptions here.
            if (functionException != null) {
              throw functionException;
            }
            resultCollector.endResults();
            return null;

          case MessageType.EXCEPTION:
            if (logger.isDebugEnabled()) {
              logger.debug(
                  "ExecuteRegionFunctionOpImpl#processResponse: received message of type EXCEPTION. The number of parts are : {}",
                  executeFunctionResponseMsg.getNumberOfParts());
            }

            // Read the chunk
            executeFunctionResponseMsg.receiveChunk();
            Part part0 = executeFunctionResponseMsg.getPart(0);
            Object obj = part0.getObject();
            if (obj instanceof FunctionException) {
              FunctionException ex = ((FunctionException) obj);
              if (((FunctionException) obj)
                  .getCause() instanceof InternalFunctionInvocationTargetException) {
                InternalFunctionInvocationTargetException ifite =
                    (InternalFunctionInvocationTargetException) ex.getCause();
                failedNodes = ensureMutability(failedNodes);
                failedNodes.addAll(ifite.getFailedNodeSet());
              }
              throw ex;
            } else if (obj instanceof Throwable) {
              String s = "While performing a remote " + getOpName();
              throw new ServerOperationException(s, (Throwable) obj);
            }
            break;
          case MessageType.EXECUTE_REGION_FUNCTION_ERROR:
            if (logger.isDebugEnabled()) {
              logger.debug(
                  "ExecuteRegionFunctionOpImpl#processResponse: received message of type EXECUTE_REGION_FUNCTION_ERROR");
            }
            // Read the chunk
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

    void addFunctionException(final FunctionException result) {
      if (result.getCause() instanceof FunctionInvocationTargetException) {
        if (functionException == null) {
          functionException = result;
        }
        functionException.addException(result.getCause());
      } else if (result instanceof FunctionInvocationTargetException) {
        if (functionException == null) {
          functionException = new FunctionException(result);
        }
        functionException.addException(result);
      } else {
        if (functionException == null) {
          functionException = result;
        }
        functionException.addException(result);
      }
    }

    FunctionException getFunctionException() {
      return functionException;
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
