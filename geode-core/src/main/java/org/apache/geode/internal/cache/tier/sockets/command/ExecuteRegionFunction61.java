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
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionInvocationTargetException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.operations.ExecuteFunctionOperationContext;
import com.gemstone.gemfire.internal.cache.DistributedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.execute.AbstractExecution;
import com.gemstone.gemfire.internal.cache.execute.DistributedRegionFunctionExecutor;
import com.gemstone.gemfire.internal.cache.execute.InternalFunctionInvocationTargetException;
import com.gemstone.gemfire.internal.cache.execute.MemberMappedArgument;
import com.gemstone.gemfire.internal.cache.execute.PartitionedRegionFunctionExecutor;
import com.gemstone.gemfire.internal.cache.execute.ServerToClientFunctionResultSender;
import com.gemstone.gemfire.internal.cache.tier.CachedRegionHelper;
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
 * 
 * 
 *  @since GemFire 6.1
 */
public class ExecuteRegionFunction61 extends BaseCommand {

  private final static ExecuteRegionFunction61 singleton = new ExecuteRegionFunction61();

  public static Command getCommand() {
    return singleton;
  }

  private ExecuteRegionFunction61() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException {
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
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();
    try {
      hasResult = msg.getPart(0).getSerializedForm()[0];
      if (hasResult == 1) {
        servConn.setAsTrue(REQUIRES_RESPONSE);
        servConn.setAsTrue(REQUIRES_CHUNKED_RESPONSE);
      }
      regionName = msg.getPart(1).getString();
      function = msg.getPart(2).getStringOrObject();
      args = msg.getPart(3).getObject();
      Part part = msg.getPart(4);
      if (part != null) {
        Object obj = part.getObject();
        if (obj instanceof MemberMappedArgument) {
          memberMappedArg = (MemberMappedArgument)obj;
        }
      }
      isReExecute = msg.getPart(5).getSerializedForm()[0];
      filterSize = msg.getPart(6).getInt();
      if (filterSize != 0) {
        filter = new HashSet();
        partNumber = 7;
        for (int i = 0; i < filterSize; i++) {
          filter.add(msg.getPart(partNumber + i).getStringOrObject());
        }
      }
      
      partNumber = 7 + filterSize;
      removedNodesSize = msg.getPart(partNumber).getInt();
      
      if(removedNodesSize != 0){
        removedNodesSet = new HashSet();
        partNumber = partNumber + 1;
        
        for (int i = 0; i < removedNodesSize; i++) {
          removedNodesSet.add(msg.getPart(partNumber + i).getStringOrObject());
        }
      }
      
    }
    catch (ClassNotFoundException exception) {
      logger.warn(LocalizedMessage.create(LocalizedStrings.ExecuteRegionFunction_EXCEPTION_ON_SERVER_WHILE_EXECUTIONG_FUNCTION_0, function), exception);
      if (hasResult == 1) {
        writeChunkedException(msg, exception, false, servConn);
        servConn.setAsTrue(RESPONDED);
        return;
      }
    }
    if (function == null || regionName == null) {
      String message = null;
      if (function == null) {
        message = LocalizedStrings.ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL.toLocalizedString("function");
      }
      if (regionName == null) {
        message = LocalizedStrings.ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL.toLocalizedString("region");
      }
      logger.warn("{}: {}", servConn.getName(), message);
      sendError(hasResult, msg, message, servConn);
      return;
    }
    else {
      Region region = crHelper.getRegion(regionName);
      if (region == null) {
        String message = 
          LocalizedStrings.ExecuteRegionFunction_THE_REGION_NAMED_0_WAS_NOT_FOUND_DURING_EXECUTE_FUNCTION_REQUEST
          .toLocalizedString(regionName);
        logger.warn("{}: {}", servConn.getName(), message);
        sendError(hasResult, msg, message, servConn);
        return;
      }
      HandShake handShake = (HandShake)servConn.getHandshake();
      int earlierClientReadTimeout = handShake.getClientReadTimeout();
      handShake.setClientReadTimeout(0);
      ServerToClientFunctionResultSender resultSender = null;
      Function functionObject = null;
      try { 
        if (function instanceof String) {
          functionObject = FunctionService.getFunction((String)function);
          if (functionObject == null) {
            String message = LocalizedStrings.
              ExecuteRegionFunction_THE_FUNCTION_0_HAS_NOT_BEEN_REGISTERED
                .toLocalizedString(function);
            logger.warn("{}: {}", servConn.getName(), message);
            sendError(hasResult, msg, message, servConn);
            return;
          }
        }
        else {
          functionObject = (Function)function;
        }
        // check if the caller is authorized to do this operation on server
        AuthorizeRequest authzRequest = servConn.getAuthzRequest();
        final String functionName = functionObject.getId();
        final String regionPath = region.getFullPath();
        ExecuteFunctionOperationContext executeContext = null;
        if (authzRequest != null) {
          executeContext = authzRequest.executeFunctionAuthorize(functionName,
              regionPath, filter, args, functionObject.optimizeForWrite());
        }
        
        //Construct execution 
        AbstractExecution execution = (AbstractExecution)FunctionService.onRegion(region);
        ChunkedMessage m = servConn.getFunctionResponseMessage();
        m.setTransactionId(msg.getTransactionId());        
        resultSender = new ServerToClientFunctionResultSender(m,
            MessageType.EXECUTE_REGION_FUNCTION_RESULT, servConn,functionObject,executeContext);
        
        
        if (execution instanceof PartitionedRegionFunctionExecutor) {
          execution = new PartitionedRegionFunctionExecutor(
              (PartitionedRegion)region, filter, args, memberMappedArg,
              resultSender, removedNodesSet, false);
        }
        else {
          execution = new DistributedRegionFunctionExecutor(
              (DistributedRegion)region, filter, args, memberMappedArg,
              resultSender);          
        }
        if (isReExecute == 1) {
          execution.setIsReExecute();
        }
        
        if (logger.isDebugEnabled()) {
          logger.debug("Executing Function: {} on Server: {} with Execution: {}", functionObject.getId(), servConn, execution);
        }
        if (hasResult == 1) {
          if (function instanceof String) {
            execution.execute((String)function).getResult();
          }
          else {
            execution.execute(functionObject).getResult();
          }
        }else {
          if (function instanceof String) {
            execution.execute((String)function);
          }
          else {
            execution.execute(functionObject);
          }
        }            
      }
      catch (IOException ioe) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.ExecuteRegionFunction_EXCEPTION_ON_SERVER_WHILE_EXECUTIONG_FUNCTION_0, function), ioe);
        final String message = LocalizedStrings.
          ExecuteRegionFunction_SERVER_COULD_NOT_SEND_THE_REPLY
            .toLocalizedString();
        sendException(hasResult, msg, message, servConn,ioe);
      }
      catch (FunctionException fe) {
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
              logger.debug(LocalizedMessage.create(LocalizedStrings.ExecuteFunction_EXCEPTION_ON_SERVER_WHILE_EXECUTIONG_FUNCTION_0, new Object[] { function }), fe);
            }
          }
          else if (functionObject.isHA()) {
            logger.warn(LocalizedMessage.create(LocalizedStrings.ExecuteRegionFunction_EXCEPTION_ON_SERVER_WHILE_EXECUTIONG_FUNCTION_0, function + " :" + message));
          }
          else {
            logger.warn(LocalizedMessage.create(LocalizedStrings.ExecuteRegionFunction_EXCEPTION_ON_SERVER_WHILE_EXECUTIONG_FUNCTION_0, function), fe);
          }
          resultSender.setException(fe);
        }
        else {
          logger.warn(LocalizedMessage.create(LocalizedStrings.ExecuteRegionFunction_EXCEPTION_ON_SERVER_WHILE_EXECUTIONG_FUNCTION_0, function), fe);
          sendException(hasResult, msg, message, servConn, fe);
        }

      }
      catch (Exception e) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.ExecuteRegionFunction_EXCEPTION_ON_SERVER_WHILE_EXECUTIONG_FUNCTION_0, function), e);
        String message = e.getMessage();
        sendException(hasResult, msg, message, servConn,e);
      }

      finally{
        handShake.setClientReadTimeout(earlierClientReadTimeout);
      }
    }
  }

  private void sendException(byte hasResult, Message msg, String message,
      ServerConnection servConn, Throwable e) throws IOException {
    if (hasResult == 1) {
      writeFunctionResponseException(msg, MessageType.EXCEPTION,
          message, servConn, e);
      servConn.setAsTrue(RESPONDED);
    }
  }
  
  private void sendError(byte hasResult, Message msg, String message,
      ServerConnection servConn) throws IOException {
    if (hasResult == 1) {
      writeFunctionResponseError(msg, MessageType.EXECUTE_REGION_FUNCTION_ERROR,
          message, servConn);
      servConn.setAsTrue(RESPONDED);
    }
  }
}

