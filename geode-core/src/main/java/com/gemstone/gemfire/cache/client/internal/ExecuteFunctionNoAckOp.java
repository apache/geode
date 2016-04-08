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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.execute.AbstractExecution;
import com.gemstone.gemfire.internal.cache.execute.MemberMappedArgument;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;

/**
 * Does a Execution of function on server (possibly without region/cache) 
 * It does not get the resulf from the server (follows Fire&Forget approch)
 * @since 5.8Beta
 */
public class ExecuteFunctionNoAckOp {

  private static final Logger logger = LogService.getLogger();
  
  private ExecuteFunctionNoAckOp() {
    // no instances allowed
  }

  /**
   * Does a execute Function on a server using connections from the given pool
   * to communicate with the server.
   * 
   * @param pool
   *                the pool to use to communicate with the server.
   * @param function
   *                of the function to be executed
   * @param args
   *                specified arguments to the application function
   */
  public static void execute(PoolImpl pool, Function function,
      Object args, MemberMappedArgument memberMappedArg,
      boolean allServers, byte hasResult, boolean isFnSerializationReqd, String[] groups) {
    List servers = null;
    AbstractOp op = new ExecuteFunctionNoAckOpImpl(function, args, memberMappedArg,
        hasResult, isFnSerializationReqd, groups, allServers);
    try {
      // In case of allServers getCurrentServers and call
      // executeOn(ServerLocation server, Op op)
      if (allServers && groups.length == 0) {
        if (logger.isDebugEnabled()) {
          logger.debug("ExecuteFunctionNoAckOp#execute : Sending Function Execution Message:" + op.getMessage() + " to all servers using pool: " +pool);
        }
        servers = pool.getCurrentServers();
        Iterator i = servers.iterator();
        while (i.hasNext()) {          
          pool.executeOn((ServerLocation)i.next(), op);
        }
      }
      else { 
        if (logger.isDebugEnabled()) {
          logger.debug("ExecuteFunctionNoAckOp#execute : Sending Function Execution Message:" + op.getMessage() + " to server using pool: " + pool + " with groups:" + Arrays.toString(groups) + " all members:" + allServers);
        }
        pool.execute(op,0);       
      }
    }
    catch (Exception ex) {
      if (logger.isDebugEnabled()) {
        logger.debug("ExecuteFunctionNoAckOp#execute : Exception occured while Sending Function Execution Message:" + op.getMessage() + " to server using pool: " +pool, ex);
      }
      if (ex.getMessage() != null)
        throw new FunctionException(ex.getMessage(), ex);
      else
        throw new FunctionException(
            "Unexpected exception during function execution:", ex);
    }
  }

  public static void execute(PoolImpl pool, String functionId,
      Object args, MemberMappedArgument memberMappedArg,
      boolean allServers, byte hasResult, boolean isFnSerializationReqd, boolean isHA, boolean optimizeForWrite, String[] groups) {
    List servers = null;
    AbstractOp op = new ExecuteFunctionNoAckOpImpl(functionId, args, memberMappedArg,
        hasResult, isFnSerializationReqd, isHA, optimizeForWrite, groups, allServers);
    try {
      // In case of allServers getCurrentServers and call
      // executeOn(ServerLocation server, Op op)
      if (allServers && groups.length == 0) {
        if (logger.isDebugEnabled()) {
          logger.debug("ExecuteFunctionNoAckOp#execute : Sending Function Execution Message:" + op.getMessage() + " to all servers using pool: " +pool);
        }
        servers = pool.getCurrentServers();
        Iterator i = servers.iterator();
        while (i.hasNext()) {          
          pool.executeOn((ServerLocation)i.next(), op);
        }
      }
      else {        
        if (logger.isDebugEnabled()) {
          logger.debug("ExecuteFunctionNoAckOp#execute : Sending Function Execution Message:" + op.getMessage() + " to server using pool: " + pool + " with groups:" + Arrays.toString(groups) + " all members:" + allServers);
        }
        pool.execute(op,0);       
      }
    }
    catch (Exception ex) {
      if (logger.isDebugEnabled()) {
        logger.debug("ExecuteFunctionNoAckOp#execute : Exception occured while Sending Function Execution Message:" + op.getMessage() + " to server using pool: " +pool, ex);
      }
      if (ex.getMessage() != null)
        throw new FunctionException(ex.getMessage(), ex);
      else
        throw new FunctionException(
            "Unexpected exception during function execution:", ex);
    }
  }
  
  private static class ExecuteFunctionNoAckOpImpl extends AbstractOp {

    /**
     * number of parts in the request message
     */
    private static final int MSG_PARTS = 6;

    /**
     * @throws com.gemstone.gemfire.SerializationException
     *                 if serialization fails
     */
    public ExecuteFunctionNoAckOpImpl(Function function, Object args,
        MemberMappedArgument memberMappedArg, byte hasResult,
        boolean isFnSerializationReqd, String[] groups, boolean allMembers) {
      super(MessageType.EXECUTE_FUNCTION, MSG_PARTS);
      byte functionState = AbstractExecution.getFunctionState(function.isHA(),
          function.hasResult(), function.optimizeForWrite());
      getMessage().addBytesPart(new byte[]{functionState});
      if(isFnSerializationReqd){
        getMessage().addStringOrObjPart(function); 
      }
      else{
        getMessage().addStringOrObjPart(function.getId()); 
      }
      getMessage().addObjPart(args);
      getMessage().addObjPart(memberMappedArg);
      getMessage().addObjPart(groups);
      getMessage().addBytesPart(ExecuteFunctionOp.getByteArrayForFlags(allMembers));
    }

    /**
     * @param functionId
     * @param args
     * @param memberMappedArg
     * @param hasResult
     * @param isFnSerializationReqd
     * @param isHA
     * @param optimizeForWrite
     */
    public ExecuteFunctionNoAckOpImpl(String functionId, Object args,
        MemberMappedArgument memberMappedArg, byte hasResult,
        boolean isFnSerializationReqd, boolean isHA, boolean optimizeForWrite,
        String[] groups, boolean allMembers) {
      super(MessageType.EXECUTE_FUNCTION, MSG_PARTS);
      getMessage().addBytesPart(
          new byte[] { AbstractExecution.getFunctionState(isHA,
              hasResult == (byte)1 ? true : false, optimizeForWrite) });
      getMessage().addStringOrObjPart(functionId);
      getMessage().addObjPart(args);
      getMessage().addObjPart(memberMappedArg);
      getMessage().addObjPart(groups);
      getMessage().addBytesPart(ExecuteFunctionOp.getByteArrayForFlags(allMembers));
    }

    @Override  
    protected Object processResponse(Message msg) throws Exception {
      final int msgType = msg.getMessageType();
      if (msgType == MessageType.REPLY) {
        return null;
      }
      else {
        Part part = msg.getPart(0);
        if (msgType == MessageType.EXCEPTION) {
          Throwable t = (Throwable)part.getObject();
          logger.warn(LocalizedMessage.create(LocalizedStrings.EXECUTE_FUNCTION_NO_HAS_RESULT_RECEIVED_EXCEPTION), t);
        }
        else if (isErrorResponse(msgType)) {
          logger.warn(LocalizedMessage.create(LocalizedStrings.EXECUTE_FUNCTION_NO_HAS_RESULT_RECEIVED_EXCEPTION)); // TODO:LOG:FIXED: was ", part.getString());" which makes no sense
        }
        else {
          throw new InternalGemFireError("Unexpected message type "
              + MessageType.getString(msgType));
        }
        return null;
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
