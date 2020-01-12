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
import org.apache.geode.cache.LowMemoryException;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.operations.ExecuteFunctionOperationContext;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.AbstractExecution;
import org.apache.geode.internal.cache.execute.FunctionContextImpl;
import org.apache.geode.internal.cache.execute.InternalFunctionExecutionService;
import org.apache.geode.internal.cache.execute.InternalFunctionInvocationTargetException;
import org.apache.geode.internal.cache.execute.InternalFunctionService;
import org.apache.geode.internal.cache.execute.MemberMappedArgument;
import org.apache.geode.internal.cache.execute.ServerToClientFunctionResultSender65;
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
 * @since GemFire 6.5
 */
public class ExecuteFunction65 extends BaseCommand {

  @Immutable
  private static final ExecuteFunction65 singleton = new ExecuteFunction65();

  public static Command getCommand() {
    return singleton;
  }

  private final InternalFunctionExecutionService internalFunctionExecutionService;
  private final ServerToClientFunctionResultSender65Factory serverToClientFunctionResultSender65Factory;
  private final FunctionContextImplFactory functionContextImplFactory;

  private ExecuteFunction65() {
    this(InternalFunctionService.getInternalFunctionExecutionService(),
        new DefaultServerToClientFunctionResultSender65Factory(),
        new DefaultFunctionContextImplFactory());
  }

  @VisibleForTesting
  ExecuteFunction65(InternalFunctionExecutionService internalFunctionExecutionService,
      ServerToClientFunctionResultSender65Factory serverToClientFunctionResultSender65Factory,
      FunctionContextImplFactory functionContextImplFactory) {
    this.internalFunctionExecutionService = internalFunctionExecutionService;
    this.serverToClientFunctionResultSender65Factory = serverToClientFunctionResultSender65Factory;
    this.functionContextImplFactory = functionContextImplFactory;
  }

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
      ResultSender resultSender = serverToClientFunctionResultSender65Factory.create(chunkedMessage,
          MessageType.EXECUTE_FUNCTION_RESULT, serverConnection, functionObject, executeContext);

      FunctionContext context;
      InternalCache cache = serverConnection.getCache();
      InternalDistributedMember localVM =
          (InternalDistributedMember) cache.getDistributedSystem().getDistributedMember();

      if (memberMappedArg != null) {
        context = functionContextImplFactory.create(cache, functionObject.getId(),
            memberMappedArg.getArgumentsForMember(localVM.getId()), resultSender, isReexecute);
      } else {
        context = functionContextImplFactory.create(cache, functionObject.getId(), args,
            resultSender, isReexecute);
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

        cache.getInternalResourceManager().getHeapMonitor().createLowMemoryIfNeeded(null,
            (DistributedMember) null);

        LowMemoryException lowMemoryException = cache.getInternalResourceManager().getHeapMonitor()
            .createLowMemoryIfNeeded(functionObject, cache.getMyId());
        if (lowMemoryException != null) {
          sendException(hasResult, clientMessage, lowMemoryException.getMessage(), serverConnection,
              lowMemoryException);
          return;
        }
        functionObject.execute(context);
        if (!((ServerToClientFunctionResultSender65) resultSender).isLastResultReceived()
            && functionObject.hasResult()) {
          throw new FunctionException(String.format("The function, %s, did not send last result",
              functionObject.getId()));
        }
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
       * 1> In case of HA FunctionInvocationTargetException thrown. Since it is HA, function will
       * be re-executed on right node
       * 2> in case of HA member departed
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

  interface ServerToClientFunctionResultSender65Factory {
    ServerToClientFunctionResultSender65 create(ChunkedMessage msg, int messageType,
        ServerConnection sc, Function function, ExecuteFunctionOperationContext authzContext);
  }

  interface FunctionContextImplFactory {
    FunctionContextImpl create(Cache cache, String functionId, Object args,
        ResultSender resultSender, boolean isPossibleDuplicate);
  }

  private static class DefaultServerToClientFunctionResultSender65Factory
      implements ServerToClientFunctionResultSender65Factory {
    @Override
    public ServerToClientFunctionResultSender65 create(ChunkedMessage msg, int messageType,
        ServerConnection sc, Function function, ExecuteFunctionOperationContext authzContext) {
      return new ServerToClientFunctionResultSender65(msg, messageType, sc, function, authzContext);
    }
  }

  private static class DefaultFunctionContextImplFactory implements FunctionContextImplFactory {
    @Override
    public FunctionContextImpl create(Cache cache, String functionId, Object args,
        ResultSender resultSender, boolean isPossibleDuplicat) {
      return new FunctionContextImpl(cache, functionId, args, resultSender, isPossibleDuplicat);
    }
  }
}
