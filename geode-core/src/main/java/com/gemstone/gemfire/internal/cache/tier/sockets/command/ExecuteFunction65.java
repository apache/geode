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
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import com.gemstone.gemfire.cache.LowMemoryException;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.cache.operations.ExecuteFunctionOperationContext;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.control.HeapMemoryMonitor;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;
import com.gemstone.gemfire.internal.cache.control.MemoryThresholds;
import com.gemstone.gemfire.internal.cache.execute.AbstractExecution;
import com.gemstone.gemfire.internal.cache.execute.FunctionContextImpl;
import com.gemstone.gemfire.internal.cache.execute.FunctionStats;
import com.gemstone.gemfire.internal.cache.execute.InternalFunctionInvocationTargetException;
import com.gemstone.gemfire.internal.cache.execute.MemberMappedArgument;
import com.gemstone.gemfire.internal.cache.execute.ServerToClientFunctionResultSender65;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand;
import com.gemstone.gemfire.internal.cache.tier.sockets.ChunkedMessage;
import com.gemstone.gemfire.internal.cache.tier.sockets.HandShake;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.security.AuthorizeRequest;

/**
 * @since GemFire 6.5
 */
public class ExecuteFunction65 extends BaseCommand {

  private final static ExecuteFunction65 singleton = new ExecuteFunction65();

  public static Command getCommand() {
    return singleton;
  }

  ExecuteFunction65() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start) throws IOException {
    Object function = null;
    Object args = null;
    MemberMappedArgument memberMappedArg = null;
    byte hasResult = 0;
    byte functionState = 0;
    boolean isReexecute = false;
    try {
      functionState = msg.getPart(0).getSerializedForm()[0];

      if (functionState == AbstractExecution.HA_HASRESULT_NO_OPTIMIZEFORWRITE_REEXECUTE) {
        functionState = AbstractExecution.HA_HASRESULT_NO_OPTIMIZEFORWRITE;
        isReexecute = true;
      } else if (functionState == AbstractExecution.HA_HASRESULT_OPTIMIZEFORWRITE_REEXECUTE) {
        functionState = AbstractExecution.HA_HASRESULT_OPTIMIZEFORWRITE;
        isReexecute = true;
      }

      if (functionState != 1) {
        hasResult = (byte) ((functionState & 2) - 1);
      } else {
        hasResult = functionState;
      }
      if (hasResult == 1) {
        servConn.setAsTrue(REQUIRES_RESPONSE);
        servConn.setAsTrue(REQUIRES_CHUNKED_RESPONSE);
      }
      function = msg.getPart(1).getStringOrObject();
      args = msg.getPart(2).getObject();

      Part part = msg.getPart(3);
      if (part != null) {
        memberMappedArg = (MemberMappedArgument) part.getObject();
      }
    } catch (ClassNotFoundException exception) {
      logger.warn(LocalizedMessage.create(LocalizedStrings.ExecuteFunction_EXCEPTION_ON_SERVER_WHILE_EXECUTIONG_FUNCTION_0, function), exception);
      if (hasResult == 1) {
        writeChunkedException(msg, exception, false, servConn);
        servConn.setAsTrue(RESPONDED);
        return;
      }
    }
    if (function == null) {
      final String message = LocalizedStrings.ExecuteFunction_THE_INPUT_FUNCTION_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
        .toLocalizedString();
      logger.warn("{}: {}", servConn.getName(), message);
      sendError(hasResult, msg, message, servConn);
      return;
    }

    // Execute function on the cache
    try {
      Function functionObject = null;
      if (function instanceof String) {
        functionObject = FunctionService.getFunction((String) function);
        if (functionObject == null) {
          final String message = LocalizedStrings.ExecuteFunction_FUNCTION_NAMED_0_IS_NOT_REGISTERED.toLocalizedString(function);
          logger.warn("{}: {}", servConn.getName(), message);
          sendError(hasResult, msg, message, servConn);
          return;
        } else {
          byte functionStateOnServerSide = AbstractExecution.getFunctionState(functionObject.isHA(), functionObject.hasResult(), functionObject
            .optimizeForWrite());
          if (logger.isDebugEnabled()) {
            logger.debug("Function State on server side: {} on client: {}", functionStateOnServerSide, functionState);
          }
          if (functionStateOnServerSide != functionState) {
            String message = LocalizedStrings.FunctionService_FUNCTION_ATTRIBUTE_MISMATCH_CLIENT_SERVER.toLocalizedString(function);
            logger.warn("{}: {}", servConn.getName(), message);
            sendError(hasResult, msg, message, servConn);
            return;
          }
        }
      } else {
        functionObject = (Function) function;
      }

      FunctionStats stats = FunctionStats.getFunctionStats(functionObject.getId());

      this.securityService.authorizeDataWrite();

      // check if the caller is authorized to do this operation on server
      AuthorizeRequest authzRequest = servConn.getAuthzRequest();
      ExecuteFunctionOperationContext executeContext = null;
      if (authzRequest != null) {
        executeContext = authzRequest.executeFunctionAuthorize(functionObject.getId(), null, null, args, functionObject.optimizeForWrite());
      }
      ChunkedMessage m = servConn.getFunctionResponseMessage();
      m.setTransactionId(msg.getTransactionId());
      ResultSender resultSender = new ServerToClientFunctionResultSender65(m, MessageType.EXECUTE_FUNCTION_RESULT, servConn, functionObject, executeContext);

      InternalDistributedMember localVM = (InternalDistributedMember)servConn.getCache().getDistributedSystem().getDistributedMember();
      FunctionContext context = null;

      if (memberMappedArg != null) {
        context = new FunctionContextImpl(functionObject.getId(), memberMappedArg.getArgumentsForMember(localVM.getId()), resultSender, isReexecute);
      } else {
        context = new FunctionContextImpl(functionObject.getId(), args, resultSender, isReexecute);
      }
      HandShake handShake = (HandShake) servConn.getHandshake();
      int earlierClientReadTimeout = handShake.getClientReadTimeout();
      handShake.setClientReadTimeout(0);
      try {
        long startExecution = stats.startTime();
        stats.startFunctionExecution(functionObject.hasResult());
        if (logger.isDebugEnabled()) {
          logger.debug("Executing Function on Server: {} with context: {}", servConn, context);
        }
        GemFireCacheImpl cache = (GemFireCacheImpl)servConn.getCache();
        HeapMemoryMonitor hmm = ((InternalResourceManager) cache.getResourceManager()).getHeapMonitor();
        if (functionObject.optimizeForWrite() && cache != null &&
            hmm.getState().isCritical() &&
            !MemoryThresholds.isLowMemoryExceptionDisabled()) {
          Set<DistributedMember> sm = Collections.singleton((DistributedMember) cache.getMyId());
          Exception e = new LowMemoryException(LocalizedStrings.ResourceManager_LOW_MEMORY_FOR_0_FUNCEXEC_MEMBERS_1.toLocalizedString(new Object[] {
            functionObject.getId(),
            sm
          }), sm);

          sendException(hasResult, msg, e.getMessage(), servConn, e);
          return;
        }
        functionObject.execute(context);
        if (!((ServerToClientFunctionResultSender65) resultSender).isLastResultReceived() && functionObject.hasResult()) {
          throw new FunctionException(LocalizedStrings.ExecuteFunction_THE_FUNCTION_0_DID_NOT_SENT_LAST_RESULT.toString(functionObject
            .getId()));
        }
        stats.endFunctionExecution(startExecution, functionObject.hasResult());
      } catch (FunctionException functionException) {
        stats.endFunctionExecutionWithException(functionObject.hasResult());

        throw functionException;
      } catch (Exception exception) {
        stats.endFunctionExecutionWithException(functionObject.hasResult());
        throw new FunctionException(exception);
      } finally {
        handShake.setClientReadTimeout(earlierClientReadTimeout);
      }
    } catch (IOException ioException) {
      logger.warn(LocalizedMessage.create(LocalizedStrings.ExecuteFunction_EXCEPTION_ON_SERVER_WHILE_EXECUTIONG_FUNCTION_0, function), ioException);
      String message = LocalizedStrings.ExecuteFunction_SERVER_COULD_NOT_SEND_THE_REPLY.toLocalizedString();
      sendException(hasResult, msg, message, servConn, ioException);
    } catch (InternalFunctionInvocationTargetException internalfunctionException) {
      // Fix for #44709: User should not be aware of
      // InternalFunctionInvocationTargetException. No instance of
      // InternalFunctionInvocationTargetException is giving useful
      // information to user to take any corrective action hence logging
      // this at fine level logging
      // 1> In case of HA FucntionInvocationTargetException thrown. Since
      // it is HA, function will be reexecuted on right node
      // 2> in case of HA member departed
      if (logger.isDebugEnabled()) {
        logger.debug(LocalizedMessage.create(LocalizedStrings.ExecuteFunction_EXCEPTION_ON_SERVER_WHILE_EXECUTIONG_FUNCTION_0, new Object[] {
          function
        }), internalfunctionException);
      }
      final String message = internalfunctionException.getMessage();
      sendException(hasResult, msg, message, servConn, internalfunctionException);
    } catch (Exception e) {
      logger.warn(LocalizedMessage.create(LocalizedStrings.ExecuteFunction_EXCEPTION_ON_SERVER_WHILE_EXECUTIONG_FUNCTION_0, function), e);
      final String message = e.getMessage();
      sendException(hasResult, msg, message, servConn, e);
    }
  }

  private void sendException(byte hasResult, Message msg, String message, ServerConnection servConn, Throwable e)
    throws IOException {
    if (hasResult == 1) {
      writeFunctionResponseException(msg, MessageType.EXCEPTION, message, servConn, e);
      servConn.setAsTrue(RESPONDED);
    }
  }

  private void sendError(byte hasResult, Message msg, String message, ServerConnection servConn) throws IOException {
    if (hasResult == 1) {
      writeFunctionResponseError(msg, MessageType.EXECUTE_FUNCTION_ERROR, message, servConn);
      servConn.setAsTrue(RESPONDED);
    }
  }

}
