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
import java.util.concurrent.ExecutorService;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.client.internal.ConnectionImpl;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.operations.ExecuteFunctionOperationContext;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxy;
import org.apache.geode.internal.cache.execute.AbstractExecution;
import org.apache.geode.internal.cache.execute.FunctionContextImpl;
import org.apache.geode.internal.cache.execute.FunctionStats;
import org.apache.geode.internal.cache.execute.InternalFunctionInvocationTargetException;
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

/**
 * @since GemFire 6.6
 */
public class ExecuteFunction66 extends BaseCommand {

  private static final ExecuteFunction66 singleton = new ExecuteFunction66();

  protected static volatile boolean ASYNC_TX_WARNING_ISSUED = false;

  static final ExecutorService execService =
      LoggingExecutors.newCachedThreadPool("Function Execution Thread-", true);

  public static Command getCommand() {
    return singleton;
  }

  ExecuteFunction66() {}

  @Override
  public void cmdExecute(final Message clientMessage, final ServerConnection serverConnection,
      final SecurityService securityService, long start) throws IOException {
    Object function = null;
    Object args = null;
    MemberMappedArgument memberMappedArg = null;
    String[] groups = null;
    byte hasResult = 0;
    byte functionState = 0;
    boolean isReexecute = false;
    boolean allMembers = false;
    boolean ignoreFailedMembers = false;
    int functionTimeout = ConnectionImpl.DEFAULT_CLIENT_FUNCTION_TIMEOUT;
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
    } catch (ClassNotFoundException exception) {
      logger.warn(String.format("Exception on server while executing function: %s",
          function),
          exception);
      if (hasResult == 1) {
        writeChunkedException(clientMessage, exception, serverConnection);
      } else {
        writeException(clientMessage, exception, false, serverConnection);
      }
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    if (function == null) {
      final String message =
          "The input function for the execute function request is null";
      logger.warn("{} : {}",
          new Object[] {serverConnection.getName(), message});
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
                "Function attributes at client and server don't match for " +
                    function;
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
      ServerToClientFunctionResultSender resultSender = new ServerToClientFunctionResultSender65(m,
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
      handshake.setClientReadTimeout(functionTimeout);
      try {
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
        /*
         * if cache is null, then either cache has not yet been created on this node or it is a
         * shutdown scenario.
         */
        DistributionManager dm = null;
        if (cache != null) {
          dm = cache.getDistributionManager();
        }
        if (groups != null && groups.length > 0) {
          executeFunctionOnGroups(function, args, groups, allMembers, functionObject, resultSender,
              ignoreFailedMembers);
        } else {
          executeFunctionaLocally(functionObject, context,
              (ServerToClientFunctionResultSender65) resultSender, dm, stats);
        }

        if (!functionObject.hasResult()) {
          writeReply(clientMessage, serverConnection);
        }
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
      // 1> When bucket is moved
      // 2> Incase of HA FucntionInvocationTargetException thrown. Since
      // it is HA, fucntion will be reexecuted on right node
      // 3> Multiple target nodes found for single hop operation
      // 4> in case of HA member departed
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

  private void executeFunctionaLocally(final Function fn, final FunctionContext cx,
      final ServerToClientFunctionResultSender65 sender, DistributionManager dm,
      final FunctionStats stats) throws IOException {
    long startExecution = stats.startTime();
    stats.startFunctionExecution(fn.hasResult());

    if (fn.hasResult()) {
      fn.execute(cx);
      if (sender.isOkayToSendResult()
          && !((ServerToClientFunctionResultSender65) sender).isLastResultReceived()
          && fn.hasResult()) {
        throw new FunctionException(
            String.format("The function, %s, did not send last result",
                fn.getId()));
      }
    } else {
      /*
       * if dm is null it mean cache is also null. Transactional function without cache cannot be
       * executed.
       */
      final TXStateProxy txState = TXManagerImpl.getCurrentTXState();
      Runnable functionExecution = new Runnable() {
        public void run() {
          InternalCache cache = null;
          try {
            if (txState != null) {
              cache = GemFireCacheImpl.getExisting("executing function");
              cache.getTxManager().masqueradeAs(txState);
              if (cache.getLogger().warningEnabled() && !ASYNC_TX_WARNING_ISSUED) {
                ASYNC_TX_WARNING_ISSUED = true;
                cache.getLogger().warning(
                    "Function invoked within transactional context, but hasResults() is false; ordering of transactional operations cannot be guaranteed.  This message is only issued once by a server.");
              }
            }
            fn.execute(cx);
          } catch (InternalFunctionInvocationTargetException internalfunctionException) {
            // Fix for #44709: User should not be aware of
            // InternalFunctionInvocationTargetException. No instance of
            // InternalFunctionInvocationTargetException is giving useful
            // information to user to take any corrective action hence logging
            // this at fine level logging
            // 1> Incase of HA FucntionInvocationTargetException thrown. Since
            // it is HA, function will be reexecuted on right node
            // 2> in case of HA member departed
            stats.endFunctionExecutionWithException(fn.hasResult());
            if (logger.isDebugEnabled()) {
              logger.debug(String.format("Exception on server while executing function: %s",
                  new Object[] {fn}),
                  internalfunctionException);
            }
          } catch (FunctionException functionException) {
            stats.endFunctionExecutionWithException(fn.hasResult());
            logger.warn(String.format("Exception on server while executing function: %s",
                fn),
                functionException);
          } catch (Exception exception) {
            stats.endFunctionExecutionWithException(fn.hasResult());
            logger.warn(String.format("Exception on server while executing function: %s",
                fn),
                exception);
          } finally {
            if (txState != null && cache != null) {
              cache.getTxManager().unmasquerade(txState);
            }
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
        final ClusterDistributionManager newDM = (ClusterDistributionManager) dm;
        newDM.getFunctionExecutor().execute(functionExecution);
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

}
