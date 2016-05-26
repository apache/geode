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
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionInvocationTargetException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.operations.ExecuteFunctionOperationContext;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.execute.AbstractExecution;
import com.gemstone.gemfire.internal.cache.execute.InternalFunctionInvocationTargetException;
import com.gemstone.gemfire.internal.cache.execute.MemberMappedArgument;
import com.gemstone.gemfire.internal.cache.execute.PartitionedRegionFunctionExecutor;
import com.gemstone.gemfire.internal.cache.execute.ServerToClientFunctionResultSender;
import com.gemstone.gemfire.internal.cache.execute.ServerToClientFunctionResultSender65;
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
 *  @since GemFire 6.5
 */
public class ExecuteRegionFunctionSingleHop extends BaseCommand {

  private final static ExecuteRegionFunctionSingleHop singleton = new ExecuteRegionFunctionSingleHop();

  public static Command getCommand() {
    return singleton;
  }

  private ExecuteRegionFunctionSingleHop() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException {
    
    String regionName = null;
    Object function = null;
    Object args = null;
    MemberMappedArgument memberMappedArg = null;
    byte isExecuteOnAllBuckets = 0;
    Set<Object> filter = null;
    Set<Integer> buckets = null;
    byte hasResult = 0;
    byte functionState = 0;
    int removedNodesSize = 0;
    Set<Object> removedNodesSet = null;
    int filterSize = 0, bucketIdsSize = 0,  partNumber = 0;
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();
    int functionTimeout = GemFireCacheImpl.DEFAULT_CLIENT_FUNCTION_TIMEOUT;
    try {
      byte[] bytes = msg.getPart(0).getSerializedForm();
      functionState = bytes[0];
      if (bytes.length >= 5 && servConn.getClientVersion().ordinal() >= Version.GFE_8009.ordinal()) {
        functionTimeout = Part.decodeInt(bytes, 1);
      }
      if(functionState != 1) {
        hasResult = (byte) ((functionState & 2) - 1);
      }
      else {
        hasResult = functionState;
      }
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
      isExecuteOnAllBuckets = msg.getPart(5).getSerializedForm()[0];
      if(isExecuteOnAllBuckets  == 1) {
        filter = new HashSet();
        bucketIdsSize  = msg.getPart(6).getInt();
        if (bucketIdsSize != 0) {
          buckets = new HashSet<Integer>();
          partNumber = 7;
          for (int i = 0; i < bucketIdsSize; i++) {
            buckets.add(msg.getPart(partNumber + i).getInt());
          }
        }
        partNumber = 7 + bucketIdsSize;
      }
      else {
        filterSize = msg.getPart(6).getInt();
        if (filterSize != 0) {
          filter = new HashSet<Object>();
          partNumber = 7;
          for (int i = 0; i < filterSize; i++) {
            filter.add(msg.getPart(partNumber + i).getStringOrObject());
          }
        } 
        partNumber = 7 + filterSize;
      }
      
      
      removedNodesSize = msg.getPart(partNumber).getInt();
      
      if(removedNodesSize != 0){
        removedNodesSet = new HashSet<Object>();
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
      handShake.setClientReadTimeout(functionTimeout);
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
          else {
            byte functionStateOnServer = AbstractExecution.getFunctionState(
                functionObject.isHA(), functionObject.hasResult(),
                functionObject.optimizeForWrite());
            if (functionStateOnServer != functionState) {
              String message = LocalizedStrings.FunctionService_FUNCTION_ATTRIBUTE_MISMATCH_CLIENT_SERVER
                  .toLocalizedString(function);
              logger.warn("{}: {}", servConn.getName(), message);
              sendError(hasResult, msg, message, servConn);
              return;
            }
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
        resultSender = new ServerToClientFunctionResultSender65(m,
            MessageType.EXECUTE_REGION_FUNCTION_RESULT, servConn,functionObject,executeContext);
        
        if (isExecuteOnAllBuckets == 1) {
          PartitionedRegion pr = (PartitionedRegion)region;
          Set<Integer> actualBucketSet = pr.getRegionAdvisor().getBucketSet();
          try {
            buckets.retainAll(actualBucketSet);
          }
          catch (NoSuchElementException done) {
          }
          if (buckets.isEmpty()) {
            throw new FunctionException("Buckets are null");
          }
          execution = new PartitionedRegionFunctionExecutor(
              (PartitionedRegion)region, buckets, args, memberMappedArg,
              resultSender, removedNodesSet, true, true);
        }
        else {
          execution = new PartitionedRegionFunctionExecutor(
              (PartitionedRegion)region, filter, args, memberMappedArg,
              resultSender, removedNodesSet, false, true);
        }
        
        if((hasResult == 1) && filter!= null &&filter.size() == 1) {
          ServerConnection.executeFunctionOnLocalNodeOnly((byte)1);
        }
        
        if (logger.isDebugEnabled()) {
          logger.debug("Executing Function: {} on Server: {} with Execution: {}", functionObject.getId(), servConn, execution);
        }
        if (hasResult == 1) {
          if (function instanceof String) {
            switch (functionState) {
              case AbstractExecution.NO_HA_HASRESULT_NO_OPTIMIZEFORWRITE:
                execution.execute((String)function, true, false, false)
                    .getResult();
                break;
              case AbstractExecution.HA_HASRESULT_NO_OPTIMIZEFORWRITE:
                execution.execute((String)function, true, true, false)
                    .getResult();
                break;
              case AbstractExecution.HA_HASRESULT_OPTIMIZEFORWRITE:
                execution.execute((String)function, true, true, true)
                    .getResult();
                break;
              case AbstractExecution.NO_HA_HASRESULT_OPTIMIZEFORWRITE:
                execution.execute((String)function, true, false, true)
                    .getResult();
                break;
            }
          }
          else {
            execution.execute(functionObject).getResult();
          }
        }
        else {
          if (function instanceof String) {
            switch (functionState) {
              case AbstractExecution.NO_HA_NO_HASRESULT_NO_OPTIMIZEFORWRITE:
                execution.execute((String)function, false, false, false);
                break;
              case AbstractExecution.NO_HA_NO_HASRESULT_OPTIMIZEFORWRITE:
                execution.execute((String)function, false, false, true);
                break;
            }
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
          if (functionObject.isHA() && logger.isDebugEnabled()) {
            logger.debug("Exception on server while executing function: {}: {}", function, message);
          }
          else if (logger.isDebugEnabled()) {
            logger.debug("Exception on server while executing function: {}: {}", function, message, fe);
          }
          synchronized (msg) {
            resultSender.setException(fe);
          }
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
        ServerConnection.executeFunctionOnLocalNodeOnly((byte)0);
      }
    }
  }

  private void sendException(byte hasResult, Message msg, String message,
      ServerConnection servConn, Throwable e) throws IOException {
    synchronized (msg) {
      if (hasResult == 1) {
        writeFunctionResponseException(msg, MessageType.EXCEPTION, message,
            servConn, e);
        servConn.setAsTrue(RESPONDED);
      }
    }
  }
  
  private void sendError(byte hasResult, Message msg, String message,
      ServerConnection servConn) throws IOException {
    synchronized (msg) {
      if (hasResult == 1) {
        writeFunctionResponseError(msg,
            MessageType.EXECUTE_REGION_FUNCTION_ERROR, message, servConn);
        servConn.setAsTrue(RESPONDED);
      }
    }
  }
  
  protected static void writeFunctionResponseException(Message origMsg,
      int messageType, String message, ServerConnection servConn, Throwable e)
      throws IOException {
    ChunkedMessage functionResponseMsg = servConn.getFunctionResponseMessage();
    ChunkedMessage chunkedResponseMsg = servConn.getChunkedResponseMessage();
    int numParts = 0;
    if (functionResponseMsg.headerHasBeenSent()) {
      if (e instanceof FunctionException
          && e.getCause() instanceof InternalFunctionInvocationTargetException) {
        functionResponseMsg.setNumberOfParts(3);
        functionResponseMsg.addObjPart(e);
        functionResponseMsg.addStringPart(BaseCommand.getExceptionTrace(e));
        InternalFunctionInvocationTargetException fe = (InternalFunctionInvocationTargetException)e
            .getCause();
        functionResponseMsg.addObjPart(fe.getFailedNodeSet());
        numParts = 3;
      }
      else {
        functionResponseMsg.setNumberOfParts(2);
        functionResponseMsg.addObjPart(e);
        functionResponseMsg.addStringPart(BaseCommand.getExceptionTrace(e));
        numParts = 2;
      }
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Sending exception chunk while reply in progress: ", servConn.getName(), e);
      }
      functionResponseMsg.setServerConnection(servConn);
      functionResponseMsg.setLastChunkAndNumParts(true, numParts);
      functionResponseMsg.sendChunk(servConn);
    }
    else {
      chunkedResponseMsg.setMessageType(messageType);
      chunkedResponseMsg.setTransactionId(origMsg.getTransactionId());
      chunkedResponseMsg.sendHeader();
      if (e instanceof FunctionException
          && e.getCause() instanceof InternalFunctionInvocationTargetException) {
        chunkedResponseMsg.setNumberOfParts(3);
        chunkedResponseMsg.addObjPart(e);
        chunkedResponseMsg.addStringPart(BaseCommand.getExceptionTrace(e));
        InternalFunctionInvocationTargetException fe = (InternalFunctionInvocationTargetException)e
            .getCause();
        chunkedResponseMsg.addObjPart(fe.getFailedNodeSet());
        numParts = 3;
      }
      else {
        chunkedResponseMsg.setNumberOfParts(2);
        chunkedResponseMsg.addObjPart(e);
        chunkedResponseMsg.addStringPart(BaseCommand.getExceptionTrace(e));
        numParts = 2;
      }
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Sending exception chunk: ", servConn.getName(), e);
      }
      chunkedResponseMsg.setServerConnection(servConn);
      chunkedResponseMsg.setLastChunkAndNumParts(true, numParts);
      chunkedResponseMsg.sendChunk(servConn);
    }
  }
}

