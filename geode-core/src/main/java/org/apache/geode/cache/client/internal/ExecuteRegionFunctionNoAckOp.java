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

import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.execute.AbstractExecution;
import org.apache.geode.internal.cache.execute.MemberMappedArgument;
import org.apache.geode.internal.cache.execute.ServerRegionFunctionExecutor;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.logging.LogService;

/**
 * Does a Execution of function on server region It does not get the resul from the server (follows
 * Fire&Forget approch)
 *
 * @since GemFire 5.8Beta
 */
public class ExecuteRegionFunctionNoAckOp {

  private static final Logger logger = LogService.getLogger();

  private ExecuteRegionFunctionNoAckOp() {
    // no instances allowed
  }

  /**
   * Does a execute Function on a server using connections from the given pool to communicate with
   * the server.
   *
   * @param pool the pool to use to communicate with the server.
   * @param region the name of the region to do the put on
   * @param function to be executed
   * @param serverRegionExecutor which will return argument and filter
   */
  public static void execute(ExecutablePool pool, String region, Function function,
      ServerRegionFunctionExecutor serverRegionExecutor, byte hasResult) {
    AbstractOp op =
        new ExecuteRegionFunctionNoAckOpImpl(region, function, serverRegionExecutor, hasResult);
    try {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "ExecuteRegionFunctionNoAckOp#execute : Sending Function Execution Message: {} to Server using pool: {}",
            op.getMessage(), pool);
      }
      pool.execute(op);
    } catch (Exception ex) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "ExecuteRegionFunctionNoAckOp#execute : Exception occurred while Sending Function Execution Message: {} to server using pool: {}",
            op.getMessage(), pool, ex);
      }
      if (ex.getMessage() != null)
        throw new FunctionException(ex.getMessage(), ex);
      else
        throw new FunctionException("Unexpected exception during function execution:", ex);
    }
  }

  public static void execute(ExecutablePool pool, String region, String functionId,
      ServerRegionFunctionExecutor serverRegionExecutor, byte hasResult, boolean isHA,
      boolean optimizeForWrite) {
    AbstractOp op = new ExecuteRegionFunctionNoAckOpImpl(region, functionId, serverRegionExecutor,
        hasResult, isHA, optimizeForWrite);
    try {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "ExecuteRegionFunctionNoAckOp#execute : Sending Function Execution Message: {} to Server using pool: {}",
            op.getMessage(), pool);
      }
      pool.execute(op);
    } catch (Exception ex) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "ExecuteRegionFunctionNoAckOp#execute : Exception occurred while Sending Function Execution Message: {} to server using pool: {}",
            op.getMessage(), pool, ex);
      }
      if (ex.getMessage() != null)
        throw new FunctionException(ex.getMessage(), ex);
      else
        throw new FunctionException("Unexpected exception during function execution:", ex);
    }
  }

  private static class ExecuteRegionFunctionNoAckOpImpl extends AbstractOp {
    private final boolean executeOnBucketSet;

    /**
     * @throws org.apache.geode.SerializationException if serialization fails
     */
    public ExecuteRegionFunctionNoAckOpImpl(String region, Function function,
        ServerRegionFunctionExecutor serverRegionExecutor, byte hasResult) {
      super(MessageType.EXECUTE_REGION_FUNCTION, 8 + serverRegionExecutor.getFilter().size());
      byte isReExecute = 0;
      int removedNodesSize = 0;
      byte functionState = AbstractExecution.getFunctionState(function.isHA(), function.hasResult(),
          function.optimizeForWrite());
      Set routingObjects = serverRegionExecutor.getFilter();
      Object args = serverRegionExecutor.getArguments();
      MemberMappedArgument memberMappedArg = serverRegionExecutor.getMemberMappedArgument();
      getMessage().addBytesPart(new byte[] {functionState});
      getMessage().addStringPart(region);
      if (serverRegionExecutor.isFnSerializationReqd()) {
        getMessage().addStringOrObjPart(function);
      } else {
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
      getMessage().addIntPart(removedNodesSize);
    }

    public ExecuteRegionFunctionNoAckOpImpl(String region, String functionId,
        ServerRegionFunctionExecutor serverRegionExecutor, byte hasResult, boolean isHA,
        boolean optimizeForWrite) {
      super(MessageType.EXECUTE_REGION_FUNCTION, 8 + serverRegionExecutor.getFilter().size());
      byte isReExecute = 0;
      int removedNodesSize = 0;
      byte functionState = AbstractExecution.getFunctionState(isHA,
          hasResult == (byte) 1 ? true : false, optimizeForWrite);

      Set routingObjects = serverRegionExecutor.getFilter();
      Object args = serverRegionExecutor.getArguments();
      MemberMappedArgument memberMappedArg = serverRegionExecutor.getMemberMappedArgument();
      getMessage().addBytesPart(new byte[] {functionState});
      getMessage().addStringPart(region);
      getMessage().addStringOrObjPart(functionId);
      getMessage().addObjPart(args);
      getMessage().addObjPart(memberMappedArg);
      this.executeOnBucketSet = serverRegionExecutor.getExecuteOnBucketSetFlag();
      byte flags = ExecuteFunctionHelper.createFlags(executeOnBucketSet, isReExecute);

      getMessage().addBytesPart(new byte[] {flags});
      getMessage().addIntPart(routingObjects.size());
      for (Object key : routingObjects) {
        getMessage().addStringOrObjPart(key);
      }
      getMessage().addIntPart(removedNodesSize);
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {
      final int msgType = msg.getMessageType();
      if (msgType == MessageType.REPLY) {
        return null;
      } else {
        Part part = msg.getPart(0);
        if (msgType == MessageType.EXCEPTION) {
          Throwable t = (Throwable) part.getObject();
          logger.warn("Function execution without result encountered an Exception on server.", t);
        } else if (isErrorResponse(msgType)) {
          logger.warn("Function execution without result encountered an Exception on server.");
        } else {
          throw new InternalGemFireError(
              "Unexpected message type " + MessageType.getString(msgType));
        }
        return null;
      }

    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return msgType == MessageType.EXECUTE_REGION_FUNCTION_ERROR;
    }

    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startExecuteFunction();
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
      return new Message(1, Version.CURRENT);
    }
  }
}
