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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.logging.log4j.Logger;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.execute.AbstractExecution;
import org.apache.geode.internal.cache.execute.FunctionStats;
import org.apache.geode.internal.cache.execute.InternalFunctionException;
import org.apache.geode.internal.cache.execute.InternalFunctionInvocationTargetException;
import org.apache.geode.internal.cache.execute.MemberMappedArgument;
import org.apache.geode.internal.cache.execute.ServerFunctionExecutor;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.logging.LogService;

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

  private ExecuteFunctionOp() {
    // no instances allowed
  }

  /**
   * Does a execute Function on a server using connections from the given pool to communicate with
   * the server.
   *
   * @param pool the pool to use to communicate with the server.
   * @param function of the function to be executed
   * @param args specified arguments to the application function
   */
  public static void execute(final PoolImpl pool, Function function,
      ServerFunctionExecutor executor, Object args, MemberMappedArgument memberMappedArg,
      boolean allServers, byte hasResult, ResultCollector rc, boolean isFnSerializationReqd,
      UserAttributes attributes, String[] groups) {
    final AbstractOp op = new ExecuteFunctionOpImpl(function, args, memberMappedArg, hasResult, rc,
        isFnSerializationReqd, (byte) 0, groups, allServers, executor.isIgnoreDepartedMembers());

    if (allServers && groups.length == 0) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "ExecuteFunctionOp#execute : Sending Function Execution Message:{} to all servers using pool: {}",
            op.getMessage(), pool);
      }
      List callableTasks = constructAndGetFunctionTasks(pool, function, args, memberMappedArg,
          hasResult, rc, isFnSerializationReqd, attributes);

      SingleHopClientExecutor.submitAll(callableTasks);
    } else {
      boolean reexecuteForServ = false;
      AbstractOp reexecOp = null;
      int retryAttempts = 0;
      boolean reexecute = false;
      int maxRetryAttempts = 0;
      if (function.isHA())
        maxRetryAttempts = pool.getRetryAttempts();

      final boolean isDebugEnabled = logger.isDebugEnabled();
      do {
        try {
          if (reexecuteForServ) {
            if (isDebugEnabled) {
              logger.debug(
                  "ExecuteFunctionOp#execute.reexecuteForServ : Sending Function Execution Message:{} to server using pool: {} with groups:{} all members:{} ignoreFailedMembers:{}",
                  op.getMessage(), pool, Arrays.toString(groups), allServers,
                  executor.isIgnoreDepartedMembers());
            }
            reexecOp = new ExecuteFunctionOpImpl(function, args, memberMappedArg, hasResult, rc,
                isFnSerializationReqd, (byte) 1/* isReExecute */, groups, allServers,
                executor.isIgnoreDepartedMembers());
            pool.execute(reexecOp, 0);
          } else {
            if (isDebugEnabled) {
              logger.debug(
                  "ExecuteFunctionOp#execute : Sending Function Execution Message:{} to server using pool: {} with groups:{} all members:{} ignoreFailedMembers:{}",
                  op.getMessage(), pool, Arrays.toString(groups), allServers,
                  executor.isIgnoreDepartedMembers());
            }

            pool.execute(op, 0);
          }
          reexecute = false;
          reexecuteForServ = false;
        } catch (InternalFunctionInvocationTargetException e) {
          if (isDebugEnabled) {
            logger.debug(
                "ExecuteFunctionOp#execute : Received InternalFunctionInvocationTargetException. The failed node is {}",
                e.getFailedNodeSet());
          }
          reexecute = true;
          rc.clearResults();
        } catch (ServerConnectivityException se) {
          retryAttempts++;

          if (isDebugEnabled) {
            logger.debug(
                "ExecuteFunctionOp#execute : Received ServerConnectivityException. The exception is {} The retryAttempt is : {} maxRetryAttempts  {}",
                se, retryAttempts, maxRetryAttempts);
          }
          if (se instanceof ServerOperationException) {
            throw se;
          }
          if ((retryAttempts > maxRetryAttempts && maxRetryAttempts != -1))
            throw se;

          reexecuteForServ = true;
          rc.clearResults();
        }
      } while (reexecuteForServ);

      if (reexecute && function.isHA()) {
        ExecuteFunctionOp.reexecute(pool, function, executor, rc, hasResult, isFnSerializationReqd,
            maxRetryAttempts - 1, groups, allServers);
      }
    }
  }

  public static void execute(final PoolImpl pool, String functionId,
      ServerFunctionExecutor executor, Object args, MemberMappedArgument memberMappedArg,
      boolean allServers, byte hasResult, ResultCollector rc, boolean isFnSerializationReqd,
      boolean isHA, boolean optimizeForWrite, UserAttributes properties, String[] groups) {
    final AbstractOp op = new ExecuteFunctionOpImpl(functionId, args, memberMappedArg, hasResult,
        rc, isFnSerializationReqd, isHA, optimizeForWrite, (byte) 0, groups, allServers,
        executor.isIgnoreDepartedMembers());
    if (allServers && groups.length == 0) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "ExecuteFunctionOp#execute : Sending Function Execution Message:{} to all servers using pool: {}",
            op.getMessage(), pool);
      }
      List callableTasks = constructAndGetFunctionTasks(pool, functionId, args, memberMappedArg,
          hasResult, rc, isFnSerializationReqd, isHA, optimizeForWrite, properties);

      SingleHopClientExecutor.submitAll(callableTasks);
    } else {
      boolean reexecuteForServ = false;
      AbstractOp reexecOp = null;
      int retryAttempts = 0;
      boolean reexecute = false;
      int maxRetryAttempts = 0;
      if (isHA) {
        maxRetryAttempts = pool.getRetryAttempts();
      }

      final boolean isDebugEnabled = logger.isDebugEnabled();
      do {
        try {
          if (reexecuteForServ) {
            reexecOp = new ExecuteFunctionOpImpl(functionId, args, memberMappedArg, hasResult, rc,
                isFnSerializationReqd, isHA, optimizeForWrite, (byte) 1, groups, allServers,
                executor.isIgnoreDepartedMembers());
            pool.execute(reexecOp, 0);
          } else {
            if (isDebugEnabled) {
              logger.debug(
                  "ExecuteFunctionOp#execute : Sending Function Execution Message:{} to server using pool:{} with groups:{} all members:{} ignoreFailedMembers:{}",
                  op.getMessage(), pool, Arrays.toString(groups), allServers,
                  executor.isIgnoreDepartedMembers());
            }
            pool.execute(op, 0);
          }
          reexecute = false;
          reexecuteForServ = false;
        } catch (InternalFunctionInvocationTargetException e) {
          if (isDebugEnabled) {
            logger.debug(
                "ExecuteFunctionOp#execute : Received InternalFunctionInvocationTargetException. The failed node is {}",
                e.getFailedNodeSet());
          }
          reexecute = true;
          rc.clearResults();
        } catch (ServerConnectivityException se) {
          retryAttempts++;

          if (isDebugEnabled) {
            logger.debug(
                "ExecuteFunctionOp#execute : Received ServerConnectivityException. The exception is {} The retryAttempt is : {} maxRetryAttempts {}",
                se, retryAttempts, maxRetryAttempts);
          }
          if (se instanceof ServerOperationException) {
            throw se;
          }
          if ((retryAttempts > maxRetryAttempts && maxRetryAttempts != -1))
            throw se;

          reexecuteForServ = true;
          rc.clearResults();
        }
      } while (reexecuteForServ);

      if (reexecute && isHA) {
        ExecuteFunctionOp.reexecute(pool, functionId, executor, rc, hasResult,
            isFnSerializationReqd, maxRetryAttempts - 1, args, isHA, optimizeForWrite, groups,
            allServers);
      }
    }
  }

  public static void reexecute(ExecutablePool pool, Function function,
      ServerFunctionExecutor serverExecutor, ResultCollector resultCollector, byte hasResult,
      boolean isFnSerializationReqd, int maxRetryAttempts, String[] groups, boolean allMembers) {
    boolean reexecute = true;
    int retryAttempts = 0;
    final boolean isDebugEnabled = logger.isDebugEnabled();
    do {
      reexecute = false;
      AbstractOp reExecuteOp = new ExecuteFunctionOpImpl(function, serverExecutor.getArguments(),
          serverExecutor.getMemberMappedArgument(), hasResult, resultCollector,
          isFnSerializationReqd, (byte) 1, groups, allMembers,
          serverExecutor.isIgnoreDepartedMembers());
      if (isDebugEnabled) {
        logger.debug(
            "ExecuteFunction#reexecute : Sending Function Execution Message:{} to Server using pool:{} with groups:{} all members:{} ignoreFailedMembers:{}",
            reExecuteOp.getMessage(), pool, Arrays.toString(groups), allMembers,
            serverExecutor.isIgnoreDepartedMembers());
      }
      try {
        pool.execute(reExecuteOp, 0);
      } catch (InternalFunctionInvocationTargetException e) {
        if (isDebugEnabled) {
          logger.debug(
              "ExecuteFunctionOp#reexecute : Received InternalFunctionInvocationTargetException. The failed nodes are {}",
              e.getFailedNodeSet());
        }
        reexecute = true;
        resultCollector.clearResults();
      } catch (ServerConnectivityException se) {
        if (isDebugEnabled) {
          logger.debug("ExecuteFunctionOp#reexecute : Received ServerConnectivity Exception.");
        }

        if (se instanceof ServerOperationException) {
          throw se;
        }
        retryAttempts++;
        if (retryAttempts > maxRetryAttempts && maxRetryAttempts != -2)
          throw se;

        reexecute = true;
        resultCollector.clearResults();
      }
    } while (reexecute);
  }

  public static void reexecute(ExecutablePool pool, String functionId,
      ServerFunctionExecutor serverExecutor, ResultCollector resultCollector, byte hasResult,
      boolean isFnSerializationReqd, int maxRetryAttempts, Object args, boolean isHA,
      boolean optimizeForWrite, String[] groups, boolean allMembers) {
    boolean reexecute = true;
    int retryAttempts = 0;
    final boolean isDebugEnabled = logger.isDebugEnabled();
    do {
      reexecute = false;

      final AbstractOp op =
          new ExecuteFunctionOpImpl(functionId, args, serverExecutor.getMemberMappedArgument(),
              hasResult, resultCollector, isFnSerializationReqd, isHA, optimizeForWrite, (byte) 1,
              groups, allMembers, serverExecutor.isIgnoreDepartedMembers());

      if (isDebugEnabled) {
        logger.debug(
            "ExecuteFunction#reexecute : Sending Function Execution Message:{} to Server using pool:{} with groups:{} all members:{} ignoreFailedMembers:{}",
            op.getMessage(), pool, Arrays.toString(groups), allMembers,
            serverExecutor.isIgnoreDepartedMembers());
      }
      try {
        pool.execute(op, 0);
      } catch (InternalFunctionInvocationTargetException e) {
        if (isDebugEnabled) {
          logger.debug(
              "ExecuteFunctionOp#reexecute : Received InternalFunctionInvocationTargetException. The failed nodes are {}",
              e.getFailedNodeSet());
        }
        reexecute = true;
        resultCollector.clearResults();
      } catch (ServerConnectivityException se) {
        if (isDebugEnabled) {
          logger.debug("ExecuteFunctionOp#reexecute : Received ServerConnectivity Exception.");
        }

        if (se instanceof ServerOperationException) {
          throw se;
        }
        retryAttempts++;
        if (retryAttempts > maxRetryAttempts && maxRetryAttempts != -2)
          throw se;

        reexecute = true;
        resultCollector.clearResults();
      }
    } while (reexecute);
  }

  static List constructAndGetFunctionTasks(final PoolImpl pool, final Function function,
      Object args, MemberMappedArgument memberMappedArg, byte hasResult, ResultCollector rc,
      boolean isFnSerializationReqd, UserAttributes attributes) {
    final List<SingleHopOperationCallable> tasks = new ArrayList<SingleHopOperationCallable>();
    List<ServerLocation> servers = pool.getConnectionSource().getAllServers();
    for (ServerLocation server : servers) {
      final AbstractOp op = new ExecuteFunctionOpImpl(function, args, memberMappedArg, hasResult,
          rc, isFnSerializationReqd, (byte) 0, null/* onGroups does not use single-hop for now */,
          false, false);
      SingleHopOperationCallable task =
          new SingleHopOperationCallable(server, pool, op, attributes);
      tasks.add(task);
    }
    return tasks;
  }

  static List constructAndGetFunctionTasks(final PoolImpl pool, final String functionId,
      Object args, MemberMappedArgument memberMappedArg, byte hasResult, ResultCollector rc,
      boolean isFnSerializationReqd, boolean isHA, boolean optimizeForWrite,
      UserAttributes properties) {
    final List<SingleHopOperationCallable> tasks = new ArrayList<SingleHopOperationCallable>();
    List<ServerLocation> servers = pool.getConnectionSource().getAllServers();
    for (ServerLocation server : servers) {
      final AbstractOp op = new ExecuteFunctionOpImpl(functionId, args, memberMappedArg, hasResult,
          rc, isFnSerializationReqd, isHA, optimizeForWrite, (byte) 0,
          null/* onGroups does not use single-hop for now */, false, false);
      SingleHopOperationCallable task =
          new SingleHopOperationCallable(server, pool, op, properties);
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

  static class ExecuteFunctionOpImpl extends AbstractOp {

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
        MemberMappedArgument memberMappedArg, byte hasResult, ResultCollector rc,
        boolean isFnSerializationReqd, byte isReexecute, String[] groups, boolean allMembers,
        boolean ignoreFailedMembers) {
      super(MessageType.EXECUTE_FUNCTION, MSG_PARTS);
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
      this.flags = getByteArrayForFlags(allMembers, ignoreFailedMembers);
      getMessage().addBytesPart(this.flags);
      resultCollector = rc;
      if (isReexecute == 1) {
        resultCollector.clearResults();
      }
      this.functionId = function.getId();
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
        String[] groups, boolean allMembers, boolean ignoreFailedMembers) {
      super(MessageType.EXECUTE_FUNCTION, MSG_PARTS);
      byte fnState = AbstractExecution.getFunctionState(isHA, hasResult == (byte) 1 ? true : false,
          optimizeForWrite);

      addBytes(isReexecute, fnState);
      getMessage().addStringOrObjPart(functionId);
      getMessage().addObjPart(args2);
      getMessage().addObjPart(memberMappedArg);
      getMessage().addObjPart(groups);
      this.flags = getByteArrayForFlags(allMembers, ignoreFailedMembers);
      getMessage().addBytesPart(this.flags);
      resultCollector = rc;
      if (isReexecute == 1) {
        resultCollector.clearResults();
      }
      this.functionId = functionId;
      this.args = args2;
      this.memberMappedArg = memberMappedArg;
      this.hasResult = fnState;
      this.isFnSerializationReqd = isFnSerializationReqd;
      this.groups = groups;
    }

    public ExecuteFunctionOpImpl(ExecuteFunctionOpImpl op, byte isReexecute) {
      super(MessageType.EXECUTE_FUNCTION, MSG_PARTS);
      this.resultCollector = op.resultCollector;
      this.function = op.function;
      this.functionId = op.functionId;
      this.hasResult = op.hasResult;
      this.args = op.args;
      this.memberMappedArg = op.memberMappedArg;
      this.isFnSerializationReqd = op.isFnSerializationReqd;
      this.groups = op.groups;
      this.flags = op.flags;

      addBytes(isReexecute, this.hasResult);
      if (this.isFnSerializationReqd) {
        getMessage().addStringOrObjPart(function);
      } else {
        getMessage().addStringOrObjPart(function.getId());
      }
      getMessage().addObjPart(this.args);
      getMessage().addObjPart(this.memberMappedArg);
      getMessage().addObjPart(this.groups);
      getMessage().addBytesPart(this.flags);
      if (isReexecute == 1) {
        resultCollector.clearResults();
      }
    }

    private void addBytes(byte isReexecute, byte fnStateOrHasResult) {
      if (ConnectionImpl
          .getClientFunctionTimeout() == ConnectionImpl.DEFAULT_CLIENT_FUNCTION_TIMEOUT) {
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
        Part.encodeInt(ConnectionImpl.getClientFunctionTimeout(), bytes, 1);
        getMessage().addBytesPart(bytes);
      }
    }

    /**
     * ignoreFaileMember flag is at index 1
     */
    private boolean getIgnoreFailedMembers() {
      boolean ignoreFailedMembers = false;
      if (this.flags != null && this.flags.length > 1) {
        if (this.flags[IGNORE_FAILED_MEMBERS_INDEX] == 1) {
          ignoreFailedMembers = true;
        }
      }
      return ignoreFailedMembers;
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {
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
                  this.resultCollector.addResult(memberID, cause);
                  FunctionStats.getFunctionStats(this.functionId).incResultsReceived();
                  continue;
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
                FunctionStats.getFunctionStats(this.functionId).incResultsReceived();
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
              FunctionException ex = ((FunctionException) obj);
              throw ex;
            } else {
              String s =
                  ": While performing a remote execute Function" + ((Throwable) obj).getMessage();
              throw new ServerOperationException(s, (Throwable) obj);
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
                Integer.valueOf(executeFunctionResponseMsg.getMessageType())));
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
    protected Message createResponseMessage() {
      return new ChunkedMessage(1, Version.CURRENT);
    }
  }

  public static final int MAX_FE_THREADS = Integer.getInteger("DistributionManager.MAX_FE_THREADS",
      Math.max(Runtime.getRuntime().availableProcessors() * 4, 16)).intValue();
}
