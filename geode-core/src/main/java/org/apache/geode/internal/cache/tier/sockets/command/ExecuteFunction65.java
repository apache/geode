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
package org.apache.geode.internal.cache.tier.sockets.command;

import java.io.IOException;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.operations.ExecuteFunctionOperationContext;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.AbstractExecution;
import org.apache.geode.internal.cache.execute.FunctionContextImpl;
import org.apache.geode.internal.cache.execute.FunctionStats;
import org.apache.geode.internal.cache.execute.InternalFunctionInvocationTargetException;
import org.apache.geode.internal.cache.execute.MemberMappedArgument;
import org.apache.geode.internal.cache.execute.ServerToClientFunctionResultSender65;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.ServerSideHandshake;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.SecurityService;

/**
 * @since GemFire 6.5
 */
public class ExecuteFunction65 extends BaseCommand {

  private static final ExecuteFunction65 singleton = new ExecuteFunction65();

  public static Command getCommand() {
    return singleton;
  }

  ExecuteFunction65() {}

  @Override
  public void cmdExecute(final Message clientMessage, final ServerConnection serverConnection,
      final SecurityService securityService, long start) throws IOException {
    Object function = null;
    Object args = null;
    MemberMappedArgument memberMappedArg = null;
    byte hasResult = 0;
    byte functionState = 0;
    boolean isReexecute = false;
    try {
      functionState = clientMessage.getPart(0).getSerializedForm()[0];

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
        serverConnection.setAsTrue(REQUIRES_RESPONSE);
        serverConnection.setAsTrue(REQUIRES_CHUNKED_RESPONSE);
      }
      function = clientMessage.getPart(1).getStringOrObject();
      args = clientMessage.getPart(2).getObject();

      Part part = clientMessage.getPart(3);
      if (part != null) {
        memberMappedArg = (MemberMappedArgument) part.getObject();
      }
    } catch (ClassNotFoundException exception) {
      logger.warn(String.format("Exception on server while executing function: %s",
          function),
          exception);
      if (hasResult == 1) {
        writeChunkedException(clientMessage, exception, serverConnection);
        serverConnection.setAsTrue(RESPONDED);
        return;
      }
    }
    if (function == null) {
      final String message =
          "The input function for the execute function request is null";
      logger.warn("{}: {}", serverConnection.getName(), message);
      sendError(hasResult, clientMessage, message, serverConnection);
      return;
    }

    // Execute function on the cache
    try {
      Function<?> functionObject = null;
      if (function instanceof String) {
        functionObject = FunctionService.getFunction((String) function);
        if (functionObject == null) {
          final String message =
              String.format("Function named %s is not registered to FunctionService",
                  function);
          logger.warn("{}: {}", serverConnection.getName(), message);
          sendError(hasResult, clientMessage, message, serverConnection);
          return;
        } else {
          byte functionStateOnServerSide = AbstractExecution.getFunctionState(functionObject.isHA(),
              functionObject.hasResult(), functionObject.optimizeForWrite());
          if (logger.isDebugEnabled()) {
            logger.debug("Function State on server side: {} on client: {}",
                functionStateOnServerSide, functionState);
          }
          if (functionStateOnServerSide != functionState) {
            String message =
                "Function attributes at client and server don't match";
            logger.warn("{}: {}", serverConnection.getName(), message);
            sendError(hasResult, clientMessage, message, serverConnection);
            return;
          }
        }
      } else {
        functionObject = (Function) function;
      }

      FunctionStats stats = FunctionStats.getFunctionStats(functionObject.getId());

      // check if the caller is authorized to do this operation on server
      functionObject.getRequiredPermissions(null).forEach(securityService::authorize);

      AuthorizeRequest authzRequest = serverConnection.getAuthzRequest();
      ExecuteFunctionOperationContext executeContext = null;
      if (authzRequest != null) {
        executeContext = authzRequest.executeFunctionAuthorize(functionObject.getId(), null, null,
            args, functionObject.optimizeForWrite());
      }

      ChunkedMessage m = serverConnection.getFunctionResponseMessage();
      m.setTransactionId(clientMessage.getTransactionId());
      ResultSender resultSender = new ServerToClientFunctionResultSender65(m,
          MessageType.EXECUTE_FUNCTION_RESULT, serverConnection, functionObject, executeContext);

      FunctionContext context = null;
      InternalCache cache = serverConnection.getCache();
      InternalDistributedMember localVM =
          (InternalDistributedMember) cache.getDistributedSystem().getDistributedMember();

      if (memberMappedArg != null) {
        context = new FunctionContextImpl(cache, functionObject.getId(),
            memberMappedArg.getArgumentsForMember(localVM.getId()), resultSender, isReexecute);
      } else {
        context =
            new FunctionContextImpl(cache, functionObject.getId(), args, resultSender, isReexecute);
      }

      ServerSideHandshake handshake = serverConnection.getHandshake();
      int earlierClientReadTimeout = handshake.getClientReadTimeout();
      handshake.setClientReadTimeout(0);
      try {
        long startExecution = stats.startTime();
        stats.startFunctionExecution(functionObject.hasResult());
        if (logger.isDebugEnabled()) {
          logger.debug("Executing Function on Server: {} with context: {}", serverConnection,
              context);
        }

        Exception e = cache.getInternalResourceManager().getHeapMonitor()
            .createLowMemoryIfNeeded(functionObject, cache.getMyId());
        if (e != null) {
          sendException(hasResult, clientMessage, e.getMessage(), serverConnection, e);
          return;
        }
        functionObject.execute(context);
        if (!((ServerToClientFunctionResultSender65) resultSender).isLastResultReceived()
            && functionObject.hasResult()) {
          throw new FunctionException(
              String.format("The function, %s, did not send last result",
                  functionObject.getId()));
        }
        stats.endFunctionExecution(startExecution, functionObject.hasResult());
      } catch (FunctionException functionException) {
        stats.endFunctionExecutionWithException(functionObject.hasResult());

        throw functionException;
      } catch (Exception exception) {
        stats.endFunctionExecutionWithException(functionObject.hasResult());
        throw new FunctionException(exception);
      } finally {
        handshake.setClientReadTimeout(earlierClientReadTimeout);
      }
    } catch (IOException ioException) {
      logger.warn(String.format("Exception on server while executing function: %s",
          function),
          ioException);
      String message =
          "Server could not send the reply";
      sendException(hasResult, clientMessage, message, serverConnection, ioException);
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
        logger.debug(String.format("Exception on server while executing function: %s",
            new Object[] {function}),
            internalfunctionException);
      }
      final String message = internalfunctionException.getMessage();
      sendException(hasResult, clientMessage, message, serverConnection, internalfunctionException);
    } catch (Exception e) {
      logger.warn(String.format("Exception on server while executing function: %s",
          function),
          e);
      final String message = e.getMessage();
      sendException(hasResult, clientMessage, message, serverConnection, e);
    }
  }

  private void sendException(byte hasResult, Message msg, String message,
      ServerConnection serverConnection, Throwable e) throws IOException {
    if (hasResult == 1) {
      writeFunctionResponseException(msg, MessageType.EXCEPTION, serverConnection, e);
      serverConnection.setAsTrue(RESPONDED);
    }
  }

  private void sendError(byte hasResult, Message msg, String message,
      ServerConnection serverConnection) throws IOException {
    if (hasResult == 1) {
      writeFunctionResponseError(msg, MessageType.EXECUTE_FUNCTION_ERROR, message,
          serverConnection);
      serverConnection.setAsTrue(RESPONDED);
    }
  }

}
