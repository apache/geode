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
import java.util.HashSet;
import java.util.Set;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionInvocationTargetException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.operations.ExecuteFunctionOperationContext;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.execute.AbstractExecution;
import org.apache.geode.internal.cache.execute.DistributedRegionFunctionExecutor;
import org.apache.geode.internal.cache.execute.InternalFunctionInvocationTargetException;
import org.apache.geode.internal.cache.execute.MemberMappedArgument;
import org.apache.geode.internal.cache.execute.PartitionedRegionFunctionExecutor;
import org.apache.geode.internal.cache.execute.ServerToClientFunctionResultSender;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
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
 *
 *
 * @since GemFire 6.1
 */
public class ExecuteRegionFunction61 extends BaseCommand {

  private static final ExecuteRegionFunction61 singleton = new ExecuteRegionFunction61();

  public static Command getCommand() {
    return singleton;
  }

  private ExecuteRegionFunction61() {}

  @Override
  public void cmdExecute(final Message clientMessage, final ServerConnection serverConnection,
      final SecurityService securityService, long start) throws IOException {
    String regionName = null;
    Object function = null;
    Object args = null;
    MemberMappedArgument memberMappedArg = null;
    byte isReExecute = 0;
    Set filter = null;
    byte hasResult = 0;
    int removedNodesSize = 0;
    Set removedNodesSet = null;
    int filterSize = 0, partNumber = 0;
    CachedRegionHelper crHelper = serverConnection.getCachedRegionHelper();
    try {
      hasResult = clientMessage.getPart(0).getSerializedForm()[0];
      if (hasResult == 1) {
        serverConnection.setAsTrue(REQUIRES_RESPONSE);
        serverConnection.setAsTrue(REQUIRES_CHUNKED_RESPONSE);
      }
      regionName = clientMessage.getPart(1).getString();
      function = clientMessage.getPart(2).getStringOrObject();
      args = clientMessage.getPart(3).getObject();
      Part part = clientMessage.getPart(4);
      if (part != null) {
        Object obj = part.getObject();
        if (obj instanceof MemberMappedArgument) {
          memberMappedArg = (MemberMappedArgument) obj;
        }
      }
      isReExecute = clientMessage.getPart(5).getSerializedForm()[0];
      filterSize = clientMessage.getPart(6).getInt();
      if (filterSize != 0) {
        filter = new HashSet();
        partNumber = 7;
        for (int i = 0; i < filterSize; i++) {
          filter.add(clientMessage.getPart(partNumber + i).getStringOrObject());
        }
      }

      partNumber = 7 + filterSize;
      removedNodesSize = clientMessage.getPart(partNumber).getInt();

      if (removedNodesSize != 0) {
        removedNodesSet = new HashSet();
        partNumber = partNumber + 1;

        for (int i = 0; i < removedNodesSize; i++) {
          removedNodesSet.add(clientMessage.getPart(partNumber + i).getStringOrObject());
        }
      }

    } catch (ClassNotFoundException exception) {
      logger.warn(String.format("Exception on server while executing function : %s",
          function),
          exception);
      if (hasResult == 1) {
        writeChunkedException(clientMessage, exception, serverConnection);
        serverConnection.setAsTrue(RESPONDED);
        return;
      }
    }
    if (function == null || regionName == null) {
      String message = null;
      if (function == null) {
        message =
            String.format("The input %s for the execute function request is null",
                "function");
      }
      if (regionName == null) {
        message =
            String.format("The input %s for the execute function request is null",
                "region");
      }
      logger.warn("{}: {}", serverConnection.getName(), message);
      sendError(hasResult, clientMessage, message, serverConnection);
      return;
    } else {
      Region region = crHelper.getRegion(regionName);
      if (region == null) {
        String message =
            String.format("The region named %s was not found during execute Function request.",
                regionName);
        logger.warn("{}: {}", serverConnection.getName(), message);
        sendError(hasResult, clientMessage, message, serverConnection);
        return;
      }
      ServerSideHandshake handshake = serverConnection.getHandshake();
      int earlierClientReadTimeout = handshake.getClientReadTimeout();
      handshake.setClientReadTimeout(0);
      ServerToClientFunctionResultSender resultSender = null;
      Function<?> functionObject = null;
      try {
        if (function instanceof String) {
          functionObject = FunctionService.getFunction((String) function);
          if (functionObject == null) {
            String message =
                String.format("The function, %s, has not been registered",
                    function);
            logger.warn("{}: {}", serverConnection.getName(), message);
            sendError(hasResult, clientMessage, message, serverConnection);
            return;
          }
        } else {
          functionObject = (Function) function;
        }
        // check if the caller is authorized to do this operation on server
        functionObject.getRequiredPermissions(regionName).forEach(securityService::authorize);

        AuthorizeRequest authzRequest = serverConnection.getAuthzRequest();
        final String functionName = functionObject.getId();
        final String regionPath = region.getFullPath();
        ExecuteFunctionOperationContext executeContext = null;
        if (authzRequest != null) {
          executeContext = authzRequest.executeFunctionAuthorize(functionName, regionPath, filter,
              args, functionObject.optimizeForWrite());
        }

        // Construct execution
        AbstractExecution execution = (AbstractExecution) FunctionService.onRegion(region);
        ChunkedMessage m = serverConnection.getFunctionResponseMessage();
        m.setTransactionId(clientMessage.getTransactionId());
        resultSender =
            new ServerToClientFunctionResultSender(m, MessageType.EXECUTE_REGION_FUNCTION_RESULT,
                serverConnection, functionObject, executeContext);


        if (execution instanceof PartitionedRegionFunctionExecutor) {
          execution = new PartitionedRegionFunctionExecutor((PartitionedRegion) region, filter,
              args, memberMappedArg, resultSender, removedNodesSet, false);
        } else {
          execution = new DistributedRegionFunctionExecutor((DistributedRegion) region, filter,
              args, memberMappedArg, resultSender);
        }
        if (isReExecute == 1) {
          execution.setIsReExecute();
        }

        if (logger.isDebugEnabled()) {
          logger.debug("Executing Function: {} on Server: {} with Execution: {}",
              functionObject.getId(), serverConnection, execution);
        }
        if (hasResult == 1) {
          if (function instanceof String) {
            execution.execute((String) function).getResult();
          } else {
            execution.execute(functionObject).getResult();
          }
        } else {
          if (function instanceof String) {
            execution.execute((String) function);
          } else {
            execution.execute(functionObject);
          }
        }
      } catch (IOException ioe) {
        logger.warn(String.format("Exception on server while executing function : %s",
            function),
            ioe);
        final String message = "Server could not send the reply";
        sendException(hasResult, clientMessage, message, serverConnection, ioe);
      } catch (FunctionException fe) {
        String message = fe.getMessage();

        if (fe.getCause() instanceof FunctionInvocationTargetException) {
          if (fe.getCause() instanceof InternalFunctionInvocationTargetException) {
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
                  fe);
            }
          } else if (functionObject.isHA()) {
            logger.warn("Exception on server while executing function : " +
                function + " :" + message);
          } else {
            logger.warn(String.format("Exception on server while executing function : %s",
                function),
                fe);
          }
          resultSender.setException(fe);
        } else {
          if (setLastResultReceived(resultSender)) {
            logger.warn(String.format("Exception on server while executing function : %s",
                function),
                fe);
            sendException(hasResult, clientMessage, message, serverConnection, fe);
          }
        }

      } catch (Exception e) {
        if (setLastResultReceived(resultSender)) {
          logger.warn(String.format("Exception on server while executing function : %s",
              function),
              e);
          String message = e.getMessage();
          sendException(hasResult, clientMessage, message, serverConnection, e);
        }
      }

      finally {
        handshake.setClientReadTimeout(earlierClientReadTimeout);
      }
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
      writeFunctionResponseError(msg, MessageType.EXECUTE_REGION_FUNCTION_ERROR, message,
          serverConnection);
      serverConnection.setAsTrue(RESPONDED);
    }
  }
}
