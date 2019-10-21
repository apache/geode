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

import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.operations.ExecuteFunctionOperationContext;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.FunctionContextImpl;
import org.apache.geode.internal.cache.execute.InternalFunctionExecutionService;
import org.apache.geode.internal.cache.execute.InternalFunctionInvocationTargetException;
import org.apache.geode.internal.cache.execute.InternalFunctionService;
import org.apache.geode.internal.cache.execute.MemberMappedArgument;
import org.apache.geode.internal.cache.execute.ServerToClientFunctionResultSender;
import org.apache.geode.internal.cache.execute.metrics.FunctionStats;
import org.apache.geode.internal.cache.execute.metrics.FunctionStatsManager;
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
 * This is the base command which read the parts for the MessageType.EXECUTE_FUNCTION.<br>
 * If the hasResult byte is 1, then this command send back the result after the execution to the
 * client else do not send the reply back to the client
 *
 * @since GemFire 5.8Beta
 */
public class ExecuteFunction extends BaseCommand {

  @Immutable
  private static final ExecuteFunction singleton = new ExecuteFunction();

  public static Command getCommand() {
    return singleton;
  }

  private final InternalFunctionExecutionService internalFunctionExecutionService;
  private final ServerToClientFunctionResultSenderFactory serverToClientFunctionResultSenderFactory;
  private final FunctionContextImplFactory functionContextImplFactory;

  private ExecuteFunction() {
    this(InternalFunctionService.getInternalFunctionExecutionService(),
        new DefaultServerToClientFunctionResultSenderFactory(),
        new DefaultFunctionContextImplFactory());
  }

  @VisibleForTesting
  ExecuteFunction(InternalFunctionExecutionService internalFunctionExecutionService,
      ServerToClientFunctionResultSenderFactory serverToClientFunctionResultSenderFactory,
      FunctionContextImplFactory functionContextImplFactory) {
    this.internalFunctionExecutionService = internalFunctionExecutionService;
    this.serverToClientFunctionResultSenderFactory = serverToClientFunctionResultSenderFactory;
    this.functionContextImplFactory = functionContextImplFactory;
  }

  @Override
  public void cmdExecute(final Message clientMessage, final ServerConnection serverConnection,
      final SecurityService securityService, long start) throws IOException {
    Object function = null;
    Object args = null;
    MemberMappedArgument memberMappedArg = null;
    byte hasResult = 0;

    try {
      hasResult = clientMessage.getPart(0).getSerializedForm()[0];
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
    } catch (ClassNotFoundException e) {
      logger.warn("Exception on server while executing function: {}", function, e);
      if (hasResult == 1) {
        writeChunkedException(clientMessage, e, serverConnection);
        serverConnection.setAsTrue(RESPONDED);
        return;
      }
    }

    if (function == null) {
      String message = "The input function for the execute function request is null";
      logger.warn("{}: {}", serverConnection.getName(), message);
      sendError(hasResult, clientMessage, message, serverConnection);
      return;
    }

    // Execute function on the cache
    try {
      Function<?> functionObject;
      if (function instanceof String) {
        functionObject = internalFunctionExecutionService.getFunction((String) function);
        if (functionObject == null) {
          String message = String.format("Function named %s is not registered to FunctionService",
              function);
          logger.warn("{}: {}", serverConnection.getName(), message);
          sendError(hasResult, clientMessage, message, serverConnection);
          return;
        }
      } else {
        functionObject = (Function) function;
      }

      FunctionStats stats = FunctionStatsManager.getFunctionStats(functionObject.getId());

      // check if the caller is authorized to do this operation on server
      functionObject.getRequiredPermissions(null, args).forEach(securityService::authorize);

      AuthorizeRequest authzRequest = serverConnection.getAuthzRequest();
      ExecuteFunctionOperationContext executeContext = null;
      if (authzRequest != null) {
        executeContext = authzRequest.executeFunctionAuthorize(functionObject.getId(), null, null,
            args, functionObject.optimizeForWrite());
      }

      ChunkedMessage chunkedMessage = serverConnection.getFunctionResponseMessage();
      chunkedMessage.setTransactionId(clientMessage.getTransactionId());
      ResultSender resultSender = serverToClientFunctionResultSenderFactory.create(chunkedMessage,
          MessageType.EXECUTE_FUNCTION_RESULT, serverConnection, functionObject, executeContext);

      FunctionContext context;
      InternalCache cache = serverConnection.getCache();
      InternalDistributedMember localVM =
          (InternalDistributedMember) cache.getDistributedSystem().getDistributedMember();

      if (memberMappedArg != null) {
        context = functionContextImplFactory.create(cache, functionObject.getId(),
            memberMappedArg.getArgumentsForMember(localVM.getId()), resultSender);
      } else {
        context =
            functionContextImplFactory.create(cache, functionObject.getId(), args, resultSender);
      }

      ServerSideHandshake handshake = serverConnection.getHandshake();
      int earlierClientReadTimeout = handshake.getClientReadTimeout();
      handshake.setClientReadTimeout(0);

      long startExecution = stats.startFunctionExecution(functionObject.hasResult());
      try {
        if (logger.isDebugEnabled()) {
          logger.debug("Executing Function on Server: {} with context: {}", serverConnection,
              context);
        }
        cache.getInternalResourceManager().getHeapMonitor().checkForLowMemory(functionObject,
            cache.getMyId());
        functionObject.execute(context);
        stats.endFunctionExecution(startExecution, functionObject.hasResult());
      } catch (FunctionException e) {
        stats.endFunctionExecutionWithException(startExecution, functionObject.hasResult());
        throw e;
      } catch (Exception e) {
        stats.endFunctionExecutionWithException(startExecution, functionObject.hasResult());
        throw new FunctionException(e);
      } finally {
        handshake.setClientReadTimeout(earlierClientReadTimeout);
      }

    } catch (IOException e) {
      logger.warn("Exception on server while executing function: {}", function, e);
      String message = "Server could not send the reply";
      sendException(hasResult, clientMessage, message, serverConnection, e);

    } catch (InternalFunctionInvocationTargetException e) {
      /*
       * TRAC #44709: InternalFunctionInvocationTargetException should not be logged
       * Fix for #44709: User should not be aware of InternalFunctionInvocationTargetException. No
       * instance is giving useful information to user to take any corrective action hence logging
       * this at fine level logging. May occur when:
       * 1> When bucket is moved
       * 2> In case of HA FunctionInvocationTargetException thrown. Since it is HA, function will
       * be re-executed on right node
       * 3> Multiple target nodes found for single hop operation
       * 4> in case of HA member departed
       */
      if (logger.isDebugEnabled()) {
        logger.debug("Exception on server while executing function: {}", function, e);
      }
      sendException(hasResult, clientMessage, e.getMessage(), serverConnection, e);

    } catch (Exception e) {
      logger.warn("Exception on server while executing function: {}", function, e);
      sendException(hasResult, clientMessage, e.getMessage(), serverConnection, e);
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

  interface ServerToClientFunctionResultSenderFactory {
    ServerToClientFunctionResultSender create(ChunkedMessage msg, int messageType,
        ServerConnection sc, Function function, ExecuteFunctionOperationContext authzContext);
  }

  interface FunctionContextImplFactory {
    FunctionContextImpl create(Cache cache, String functionId, Object args,
        ResultSender resultSender);
  }

  private static class DefaultServerToClientFunctionResultSenderFactory
      implements ServerToClientFunctionResultSenderFactory {
    @Override
    public ServerToClientFunctionResultSender create(ChunkedMessage msg, int messageType,
        ServerConnection sc, Function function, ExecuteFunctionOperationContext authzContext) {
      return new ServerToClientFunctionResultSender(msg, messageType, sc, function, authzContext);
    }
  }

  private static class DefaultFunctionContextImplFactory implements FunctionContextImplFactory {
    @Override
    public FunctionContextImpl create(Cache cache, String functionId, Object args,
        ResultSender resultSender) {
      return new FunctionContextImpl(cache, functionId, args, resultSender);
    }
  }
}
