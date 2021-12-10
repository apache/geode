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
import java.util.List;
import java.util.function.Supplier;

import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.client.NoAvailableServersException;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.execute.AbstractExecution;
import org.apache.geode.internal.cache.execute.InternalFunctionException;
import org.apache.geode.internal.cache.execute.InternalFunctionInvocationTargetException;
import org.apache.geode.internal.cache.execute.MemberMappedArgument;
import org.apache.geode.internal.cache.execute.metrics.FunctionStatsManager;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Executes the function on server (possibly without region/cache).<br>
 * Also gets the result back from the server
 *
 * @since GemFire 5.8
 */

public class ExecuteFunctionOp {

  private static final Logger logger = LogService.getLogger();

  /** index of allMembers in flags[] */
  public static final int ALL_MEMBERS_INDEX = 0;
  /** index of ignoreFailedMembers in flags[] */
  public static final int IGNORE_FAILED_MEMBERS_INDEX = 1;

  private static final int MAX_RETRY_INITIAL_VALUE = -1;

  private ExecuteFunctionOp() {
    // no instances allowed
  }

  public static void execute(final PoolImpl pool,
      boolean allServers, ResultCollector rc,
      final boolean isHA, UserAttributes attributes, String[] groups,
      final ExecuteFunctionOpImpl executeFunctionOp,
      final Supplier<ExecuteFunctionOpImpl> executeFunctionOpSupplier,
      final Supplier<ExecuteFunctionOpImpl> reExecuteFunctionOpSupplier) {

    final AbstractOp op = executeFunctionOp;

    if (allServers && groups.length == 0) {

      List callableTasks = constructAndGetFunctionTasks(pool,
          attributes, executeFunctionOpSupplier);

      SingleHopClientExecutor.submitAll(callableTasks);

    } else {

      boolean reexecute = false;
      int maxRetryAttempts = MAX_RETRY_INITIAL_VALUE;

      if (!isHA) {
        maxRetryAttempts = 0;
      }

      do {
        try {
          if (reexecute) {
            pool.execute(reExecuteFunctionOpSupplier.get(), 0);
          } else {
            pool.execute(op, 0);
          }
          reexecute = false;
        } catch (InternalFunctionInvocationTargetException e) {
          if (isHA) {
            reexecute = true;
          }
          rc.clearResults();
        } catch (ServerOperationException serverOperationException) {
          throw serverOperationException;

        } catch (ServerConnectivityException se) {

          if (maxRetryAttempts == MAX_RETRY_INITIAL_VALUE) {
            maxRetryAttempts = pool.calculateRetryAttempts(se);
          }

          if ((maxRetryAttempts--) < 1) {
            throw se;
          }

          reexecute = true;
          rc.clearResults();
        }
      } while (reexecute);
    }
  }

  private static List constructAndGetFunctionTasks(final PoolImpl pool,
      UserAttributes attributes,
      final Supplier<ExecuteFunctionOpImpl> executeFunctionOpSupplier) {
    final List<SingleHopOperationCallable> tasks = new ArrayList<>();
    List<ServerLocation> servers = pool.getConnectionSource().getAllServers();
    if (servers == null) {
      throw new NoAvailableServersException();
    }
    for (ServerLocation server : servers) {
      final AbstractOp op = executeFunctionOpSupplier.get();
      SingleHopOperationCallable task =
          new SingleHopOperationCallable(server, pool, op, attributes);
      tasks.add(task);
    }
    return tasks;
  }

  static byte[] getByteArrayForFlags(boolean... flags) {
    byte[] retVal = null;
    if (flags.length > 0) {
      retVal = new byte[flags.length];
      for (int i = 0; i < flags.length; i++) {
        if (flags[i]) {
          retVal[i] = 1;
        } else {
          retVal[i] = 0;
        }
      }
    }
    return retVal;
  }

  public static class ExecuteFunctionOpImpl extends AbstractOpWithTimeout {

    private ResultCollector resultCollector;

    // To get the instance of the Function Statistics we need the function name or instance
    private String functionId;

    private Function function;

    private Object args;

    private MemberMappedArgument memberMappedArg;

    private byte hasResult;

    private boolean isFnSerializationReqd;

    private String[] groups;

    /**
     * [0] = allMembers [1] = ignoreFailedMembers
     */
    private byte[] flags;

    /**
     * number of parts in the request message
     */
    private static final int MSG_PARTS = 6;

    /**
     * @throws org.apache.geode.SerializationException if serialization fails
     */
    public ExecuteFunctionOpImpl(Function function, Object args,
        MemberMappedArgument memberMappedArg,
        ResultCollector rc, boolean isFnSerializationReqd,
        byte isReexecute,
        String[] groups, boolean allMembers, boolean ignoreFailedMembers,
        final int timeoutMs) {
      super(MessageType.EXECUTE_FUNCTION, MSG_PARTS, timeoutMs);
      byte fnState = AbstractExecution.getFunctionState(function.isHA(), function.hasResult(),
          function.optimizeForWrite());

      addBytes(isReexecute, fnState);
      if (isFnSerializationReqd) {
        getMessage().addStringOrObjPart(function);
      } else {
        getMessage().addStringOrObjPart(function.getId());
      }
      getMessage().addObjPart(args);
      getMessage().addObjPart(memberMappedArg);
      getMessage().addObjPart(groups);
      flags = getByteArrayForFlags(allMembers, ignoreFailedMembers);
      getMessage().addBytesPart(flags);
      resultCollector = rc;
      if (isReexecute == 1) {
        resultCollector.clearResults();
      }
      functionId = function.getId();
      this.function = function;
      this.args = args;
      this.memberMappedArg = memberMappedArg;
      this.hasResult = fnState;
      this.isFnSerializationReqd = isFnSerializationReqd;
      this.groups = groups;
    }

    public ExecuteFunctionOpImpl(String functionId, Object args2,
        MemberMappedArgument memberMappedArg, byte hasResult, ResultCollector rc,
        boolean isFnSerializationReqd, boolean isHA, boolean optimizeForWrite, byte isReexecute,
        String[] groups, boolean allMembers, boolean ignoreFailedMembers, final int timeoutMs) {
      super(MessageType.EXECUTE_FUNCTION, MSG_PARTS, timeoutMs);
      byte fnState = AbstractExecution.getFunctionState(isHA, hasResult == (byte) 1,
          optimizeForWrite);

      addBytes(isReexecute, fnState);
      getMessage().addStringOrObjPart(functionId);
      getMessage().addObjPart(args2);
      getMessage().addObjPart(memberMappedArg);
      getMessage().addObjPart(groups);
      flags = getByteArrayForFlags(allMembers, ignoreFailedMembers);
      getMessage().addBytesPart(flags);
      resultCollector = rc;
      if (isReexecute == 1) {
        resultCollector.clearResults();
      }
      this.functionId = functionId;
      args = args2;
      this.memberMappedArg = memberMappedArg;
      this.hasResult = fnState;
      this.isFnSerializationReqd = isFnSerializationReqd;
      this.groups = groups;
    }

    ExecuteFunctionOpImpl(ExecuteFunctionOpImpl op, byte isReexecute) {
      super(MessageType.EXECUTE_FUNCTION, MSG_PARTS, op.getTimeoutMs());
      resultCollector = op.resultCollector;
      function = op.function;
      functionId = op.functionId;
      hasResult = op.hasResult;
      args = op.args;
      memberMappedArg = op.memberMappedArg;
      isFnSerializationReqd = op.isFnSerializationReqd;
      groups = op.groups;
      flags = op.flags;

      addBytes(isReexecute, hasResult);
      if (isFnSerializationReqd) {
        getMessage().addStringOrObjPart(function);
      } else {
        getMessage().addStringOrObjPart(function.getId());
      }
      getMessage().addObjPart(args);
      getMessage().addObjPart(memberMappedArg);
      getMessage().addObjPart(groups);
      getMessage().addBytesPart(flags);
      if (isReexecute == 1) {
        resultCollector.clearResults();
      }

    }

    private void addBytes(byte isReexecute, byte fnStateOrHasResult) {
      if (getTimeoutMs() == DEFAULT_CLIENT_FUNCTION_TIMEOUT) {
        if (isReexecute == 1) {
          getMessage().addBytesPart(
              new byte[] {AbstractExecution.getReexecuteFunctionState(fnStateOrHasResult)});
        } else {
          getMessage().addBytesPart(new byte[] {fnStateOrHasResult});
        }
      } else {
        byte[] bytes = new byte[5];
        if (isReexecute == 1) {
          bytes[0] = AbstractExecution.getReexecuteFunctionState(fnStateOrHasResult);
        } else {
          bytes[0] = fnStateOrHasResult;
        }
        Part.encodeInt(getTimeoutMs(), bytes, 1);
        getMessage().addBytesPart(bytes);
      }
    }

    /**
     * ignoreFaileMember flag is at index 1
     */
    private boolean getIgnoreFailedMembers() {
      boolean ignoreFailedMembers = false;
      if (flags != null && flags.length > 1) {
        if (flags[IGNORE_FAILED_MEMBERS_INDEX] == 1) {
          ignoreFailedMembers = true;
        }
      }
      return ignoreFailedMembers;
    }

    @Override
    protected Object processResponse(final @NotNull Message msg) throws Exception {
      ChunkedMessage executeFunctionResponseMsg = (ChunkedMessage) msg;
      try {
        // Read the header which describes the type of message following
        executeFunctionResponseMsg.readHeader();
        switch (executeFunctionResponseMsg.getMessageType()) {
          case MessageType.EXECUTE_FUNCTION_RESULT:
            if (logger.isDebugEnabled()) {
              logger.debug(
                  "ExecuteFunctionOpImpl#processResponse: received message of type EXECUTE_FUNCTION_RESULT.");
            }
            Exception exception = null;
            // Read the chunk
            do {
              executeFunctionResponseMsg.receiveChunk();
              Object resultResponse = executeFunctionResponseMsg.getPart(0).getObject();
              Object result;
              if (resultResponse instanceof ArrayList) {
                result = ((ArrayList) resultResponse).get(0);
              } else {
                result = resultResponse;
              }
              if (result instanceof FunctionException) {
                FunctionException ex = ((FunctionException) result);
                if (ex instanceof InternalFunctionException || getIgnoreFailedMembers()) {
                  Throwable cause = ex.getCause() == null ? ex : ex.getCause();
                  DistributedMember memberID =
                      (DistributedMember) ((ArrayList) resultResponse).get(1);
                  resultCollector.addResult(memberID, cause);
                  FunctionStatsManager.getFunctionStats(functionId).incResultsReceived();
                } else {
                  exception = ex;
                }
              } else if (result instanceof Throwable) {
                String s = "While performing a remote " + getOpName();
                exception = new ServerOperationException(s, (Throwable) result);
                // Get the exception toString part.
                // This was added for c++ thin client and not used in java
              } else {
                DistributedMember memberID =
                    (DistributedMember) ((ArrayList) resultResponse).get(1);
                resultCollector.addResult(memberID, result);
                FunctionStatsManager.getFunctionStats(functionId).incResultsReceived();
              }
            } while (!executeFunctionResponseMsg.isLastChunk());

            if (exception != null) {
              throw exception;
            }

            if (logger.isDebugEnabled()) {
              logger.debug(
                  "ExecuteFunctionOpImpl#processResponse: received all the results from server successfully.");
            }
            return null;
          case MessageType.EXCEPTION:
            if (logger.isDebugEnabled()) {
              logger.debug(
                  "ExecuteFunctionOpImpl#processResponse: received message of type EXCEPTION");
            }
            // Read the chunk
            executeFunctionResponseMsg.receiveChunk();
            Part part0 = executeFunctionResponseMsg.getPart(0);
            Object obj = part0.getObject();
            if (obj instanceof FunctionException) {
              throw ((FunctionException) obj);
            } else {
              final Throwable t = (Throwable) obj;
              throw new ServerOperationException(
                  ": While performing a remote execute Function" + t.getMessage(), t);
            }
          case MessageType.EXECUTE_FUNCTION_ERROR:
            if (logger.isDebugEnabled()) {
              logger.debug(
                  "ExecuteFunctionOpImpl#processResponse: received message of type EXECUTE_FUNCTION_ERROR");
            }
            // Read the chunk
            executeFunctionResponseMsg.receiveChunk();
            String errorMessage = executeFunctionResponseMsg.getPart(0).getString();
            throw new ServerOperationException(errorMessage);
          default:
            throw new InternalGemFireError(String.format("Unknown message type %s",
                executeFunctionResponseMsg.getMessageType()));
        }
      } finally {
        executeFunctionResponseMsg.clear();
      }
    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return msgType == MessageType.EXECUTE_FUNCTION_ERROR;
    }

    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startExecuteFunction();
    }

    protected String getOpName() {
      return "executeFunction";
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
    protected @NotNull Message createResponseMessage() {
      return new ChunkedMessage(1, KnownVersion.CURRENT);
    }
  }
}
