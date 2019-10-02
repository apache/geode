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

import static org.apache.geode.internal.cache.execute.ServerFunctionExecutor.DEFAULT_CLIENT_FUNCTION_TIMEOUT;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.LowMemoryException;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.operations.ExecuteFunctionOperationContext;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxy;
import org.apache.geode.internal.cache.execute.AbstractExecution;
import org.apache.geode.internal.cache.execute.FunctionContextImpl;
import org.apache.geode.internal.cache.execute.FunctionStats;
import org.apache.geode.internal.cache.execute.InternalFunctionExecutionService;
import org.apache.geode.internal.cache.execute.InternalFunctionInvocationTargetException;
import org.apache.geode.internal.cache.execute.InternalFunctionService;
import org.apache.geode.internal.cache.execute.MemberMappedArgument;
import org.apache.geode.internal.cache.execute.ServerToClientFunctionResultSender;
import org.apache.geode.internal.cache.execute.ServerToClientFunctionResultSender65;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.ServerSideHandshake;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.logging.LoggingExecutors;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.serialization.Version;

/**
 * @since GemFire 6.6
 */
public class ExecuteFunction66 extends BaseCommand {
  @Immutable
  private static final ExecuteFunction66 singleton = new ExecuteFunction66();

  @MakeNotStatic
  private static volatile boolean asyncTxWarningIssued;

  @Immutable
  private static final ExecutorService execService =
      LoggingExecutors.newCachedThreadPool("Function Execution Thread-", true);

  public static Command getCommand() {
    return singleton;
  }

  private final InternalFunctionExecutionService internalFunctionExecutionService;
  private final ServerToClientFunctionResultSender65Factory serverToClientFunctionResultSender65Factory;
  private final FunctionContextImplFactory functionContextImplFactory;

  ExecuteFunction66() {
    this(InternalFunctionService.getInternalFunctionExecutionService(),
        new DefaultServerToClientFunctionResultSender65Factory(),
        new DefaultFunctionContextImplFactory());
  }

  ExecuteFunction66(InternalFunctionExecutionService internalFunctionExecutionService,
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
    Object args;
    MemberMappedArgument memberMappedArg = null;
    String[] groups;
    byte hasResult = 0;
    byte functionState;
    boolean isReexecute = false;
    boolean allMembers;
    boolean ignoreFailedMembers;
    int functionTimeout = DEFAULT_CLIENT_FUNCTION_TIMEOUT;

    try {
      byte[] bytes = clientMessage.getPart(0).getSerializedForm();
      functionState = bytes[0];
      if (bytes.length >= 5
          && serverConnection.getClientVersion().ordinal() >= Version.GFE_8009.ordinal()) {
        functionTimeout = Part.decodeInt(bytes, 1);
      }

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

      groups = getGroups(clientMessage);
      allMembers = getAllMembers(clientMessage);
      ignoreFailedMembers = getIgnoreFailedMembers(clientMessage);
    } catch (ClassNotFoundException e) {
      logger.warn("Exception on server while executing function: {}", function, e);
      if (hasResult == 1) {
        writeChunkedException(clientMessage, e, serverConnection);
      } else {
        writeException(clientMessage, e, false, serverConnection);
      }
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    if (function == null) {
      String message = "The input function for the execute function request is null";
      logger.warn("{} : {}", serverConnection.getName(), message);
      sendError(hasResult, clientMessage, message, serverConnection);
      return;
    }

    // Execute function on the cache
    try {
      Function<?> functionObject;
      if (function instanceof String) {
        functionObject = internalFunctionExecutionService.getFunction((String) function);
        if (functionObject == null) {
          String message = String
              .format("Function named %s is not registered to FunctionService", function);
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
            String message = String
                .format("Function attributes at client and server don't match for %s", function);
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
      functionObject.getRequiredPermissions(null, args).forEach(securityService::authorize);

      AuthorizeRequest authzRequest = serverConnection.getAuthzRequest();
      ExecuteFunctionOperationContext executeContext = null;
      if (authzRequest != null) {
        executeContext = authzRequest.executeFunctionAuthorize(functionObject.getId(), null, null,
            args, functionObject.optimizeForWrite());
      }

      ChunkedMessage chunkedMessage = serverConnection.getFunctionResponseMessage();
      chunkedMessage.setTransactionId(clientMessage.getTransactionId());
      ServerToClientFunctionResultSender resultSender =
          serverToClientFunctionResultSender65Factory.create(chunkedMessage,
              MessageType.EXECUTE_FUNCTION_RESULT,
              serverConnection, functionObject, executeContext);

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
      handshake.setClientReadTimeout(functionTimeout);

      try {
        if (logger.isDebugEnabled()) {
          logger.debug("Executing Function on Server: {} with context: {}", serverConnection,
              context);
        }

        LowMemoryException lowMemoryException = cache.getInternalResourceManager().getHeapMonitor()
            .createLowMemoryIfNeeded(functionObject, cache.getMyId());
        if (lowMemoryException != null) {
          sendException(hasResult, clientMessage, lowMemoryException.getMessage(), serverConnection,
              lowMemoryException);
          return;
        }

        // cache is never null or the above invocations would have thrown NPE
        DistributionManager dm = cache.getDistributionManager();
        if (groups != null && groups.length > 0) {
          executeFunctionOnGroups(function, args, groups, allMembers, functionObject, resultSender,
              ignoreFailedMembers);
        } else {
          executeFunctionLocally(functionObject, context,
              (ServerToClientFunctionResultSender65) resultSender, dm, stats);
        }

        if (!functionObject.hasResult()) {
          writeReply(clientMessage, serverConnection);
        }
      } catch (FunctionException e) {
        stats.endFunctionExecutionWithException(functionObject.hasResult());
        throw e;
      } catch (Exception e) {
        stats.endFunctionExecutionWithException(functionObject.hasResult());
        throw new FunctionException(e);
      } finally {
        handshake.setClientReadTimeout(earlierClientReadTimeout);
      }

    } catch (IOException e) {
      logger.warn("Exception on server while executing function: {}}", function, e);
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

  protected boolean getIgnoreFailedMembers(Message msg) {
    return false;
  }

  protected boolean getAllMembers(Message msg) {
    return false;
  }

  protected void executeFunctionOnGroups(Object function, Object args, String[] groups,
      boolean allMembers, Function functionObject, ServerToClientFunctionResultSender resultSender,
      boolean ignoreFailedMembers) {
    throw new InternalGemFireError();
  }

  protected String[] getGroups(Message msg) throws IOException, ClassNotFoundException {
    return null;
  }

  private void executeFunctionLocally(final Function fn, final FunctionContext cx,
      final ServerToClientFunctionResultSender65 sender, DistributionManager dm,
      final FunctionStats stats) throws IOException {
    long startExecution = stats.startTime();
    stats.startFunctionExecution(fn.hasResult());

    if (fn.hasResult()) {
      fn.execute(cx);
      if (sender.isOkayToSendResult() && !sender.isLastResultReceived() && fn.hasResult()) {
        throw new FunctionException(
            String.format("The function, %s, did not send last result", fn.getId()));
      }
    } else {
      /*
       * if dm is null it mean cache is also null. Transactional function without cache cannot be
       * executed.
       */
      TXStateProxy txState = TXManagerImpl.getCurrentTXState();
      Runnable functionExecution = () -> {
        InternalCache cache = null;
        try {
          if (txState != null) {
            cache = GemFireCacheImpl.getExisting("executing function");
            cache.getTxManager().masqueradeAs(txState);
            if (cache.getLogger().warningEnabled() && !asyncTxWarningIssued) {
              asyncTxWarningIssued = true;
              cache.getLogger().warning(
                  "Function invoked within transactional context, but hasResults() is false; ordering of transactional operations cannot be guaranteed.  This message is only issued once by a server.");
            }
          }
          fn.execute(cx);
        } catch (InternalFunctionInvocationTargetException e) {
          // TRAC #44709: InternalFunctionInvocationTargetException should not be logged
          stats.endFunctionExecutionWithException(fn.hasResult());
          if (logger.isDebugEnabled()) {
            logger.debug("Exception on server while executing function: {}", fn, e);
          }
        } catch (Exception e) {
          stats.endFunctionExecutionWithException(fn.hasResult());
          logger.warn("Exception on server while executing function: {}", fn, e);
        } finally {
          if (txState != null && cache != null) {
            cache.getTxManager().unmasquerade(txState);
          }
        }
      };

      if (dm == null) {
        /*
         * Executing the function in its own thread pool as FunctionExecution Thread pool of
         * DistributionManager is not yet available.
         */
        execService.execute(functionExecution);
      } else {
        ClusterDistributionManager newDM = (ClusterDistributionManager) dm;
        newDM.getExecutors().getFunctionExecutor().execute(functionExecution);
      }
    }
    stats.endFunctionExecution(startExecution, fn.hasResult());
  }

  private void sendException(byte hasResult, Message msg, String message,
      ServerConnection serverConnection, Throwable e) throws IOException {
    if (hasResult == 1) {
      writeFunctionResponseException(msg, MessageType.EXCEPTION, serverConnection, e);
    } else {
      writeException(msg, e, false, serverConnection);
    }
    serverConnection.setAsTrue(RESPONDED);
  }

  private void sendError(byte hasResult, Message msg, String message,
      ServerConnection serverConnection) throws IOException {
    if (hasResult == 1) {
      writeFunctionResponseError(msg, MessageType.EXECUTE_FUNCTION_ERROR, message,
          serverConnection);
    } else {
      writeErrorResponse(msg, MessageType.EXECUTE_FUNCTION_ERROR, message, serverConnection);
    }
    serverConnection.setAsTrue(RESPONDED);
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
