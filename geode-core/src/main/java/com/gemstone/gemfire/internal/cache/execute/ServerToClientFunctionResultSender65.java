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
package com.gemstone.gemfire.internal.cache.execute;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.operations.ExecuteFunctionOperationContext;
import com.gemstone.gemfire.cache.query.QueryInvalidException;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand;
import com.gemstone.gemfire.internal.cache.tier.sockets.ChunkedMessage;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
/**
 * 
 * @since 6.5
 *
 */
public class ServerToClientFunctionResultSender65 extends
    ServerToClientFunctionResultSender {
  private static final Logger logger = LogService.getLogger();

  public ServerToClientFunctionResultSender65(ChunkedMessage msg,
      int messageType, ServerConnection sc, Function function,
      ExecuteFunctionOperationContext authzContext) {
    super(msg, messageType, sc, function, authzContext);
  }

  @Override
  public synchronized void lastResult(Object oneResult) {
    if(this.lastResultReceived){
      return;
    }
    this.lastResultReceived = true;
    if (!isOkayToSendResult()) {
      if (logger.isDebugEnabled()) {
        logger.debug(" ServerToClientFunctionResultSender65 not sending lastResult {} as the server has shutdown", oneResult);
      }
      return;
    }
    try {
      authorizeResult(oneResult);
      if (!this.fn.hasResult()) {
        throw new IllegalStateException(
            LocalizedStrings.ExecuteFunction_CANNOT_0_RESULTS_HASRESULT_FALSE
                .toLocalizedString("send"));
      }

      if (!headerSent) {
    	   sendHeader();
      }
      if (logger.isDebugEnabled()) {
        logger.debug(" ServerToClientFunctionResultSender65 sending lastResult {}", oneResult);
      }
      DistributedMember memberID = InternalDistributedSystem.getAnyInstance()
          .getDistributionManager().getId();
      List<Object> result = new ArrayList<Object>();
      result.add(oneResult);
      result.add(memberID);
      this.setBuffer();
      this.msg.setServerConnection(this.sc);
      if(oneResult instanceof InternalFunctionException) {
        this.msg.setNumberOfParts(2);
        this.msg.setLastChunkAndNumParts(true, 2);
      } else {
        this.msg.setNumberOfParts(1);
        this.msg.setLastChunkAndNumParts(true, 1);
      }      
      this.msg.addObjPart(result);      
      if(oneResult instanceof InternalFunctionException) {
        List<Object> result2 = new ArrayList<Object>();        
        result2.add(BaseCommand.getExceptionTrace((Throwable)oneResult));
        result2.add(memberID);        
        this.msg.addObjPart(result2);
      }
      this.msg.sendChunk(this.sc);
      this.sc.setAsTrue(Command.RESPONDED);
      FunctionStats.getFunctionStats(fn.getId(), null).incResultsReturned();
    }
    catch (IOException ex) {
      if (isOkayToSendResult()) {
        throw new FunctionException(
            LocalizedStrings.ExecuteFunction_IOEXCEPTION_WHILE_SENDING_LAST_CHUNK
                .toLocalizedString(), ex);
      }
    }
  }

  @Override
  public synchronized void lastResult(Object oneResult,
      DistributedMember memberID) {
    this.lastResultReceived = true;
    if (!isOkayToSendResult()) {
      if (logger.isDebugEnabled()) {
        logger.debug(" ServerToClientFunctionResultSender65 not sending lastResult {} as the server has shutdown", oneResult);
      }
      return;
    }
    try {
      authorizeResult(oneResult);
      if (!this.fn.hasResult()) {
        throw new IllegalStateException(
            LocalizedStrings.ExecuteFunction_CANNOT_0_RESULTS_HASRESULT_FALSE
                .toLocalizedString("send"));
      }

      if (!headerSent) {
      	 sendHeader();
      }
      if (logger.isDebugEnabled()) {
        logger.debug(" ServerToClientFunctionResultSender65 sending lastResult {}", oneResult);
      }

      List<Object> result = new ArrayList<Object>();
      result.add(oneResult);
      result.add(memberID);
      this.setBuffer();
      this.msg.setServerConnection(this.sc);
      if(oneResult instanceof InternalFunctionException) {
        this.msg.setNumberOfParts(2);
        this.msg.setLastChunkAndNumParts(true, 2);
      } else {
        this.msg.setNumberOfParts(1);
        this.msg.setLastChunkAndNumParts(true, 1);
      }
      this.msg.addObjPart(result);      
      if(oneResult instanceof InternalFunctionException) {
        List<Object> result2 = new ArrayList<Object>();        
        result2.add(BaseCommand.getExceptionTrace((Throwable)oneResult));
        result2.add(memberID);
        this.msg.addObjPart(result2);
      }
      this.msg.sendChunk(this.sc);
      this.sc.setAsTrue(Command.RESPONDED);
      FunctionStats.getFunctionStats(fn.getId(), null).incResultsReturned();
    }
    catch (IOException ex) {
      if (isOkayToSendResult()) {
        throw new FunctionException(
            LocalizedStrings.ExecuteFunction_IOEXCEPTION_WHILE_SENDING_LAST_CHUNK
                .toLocalizedString(), ex);
      }
    }
  }

  @Override
  public synchronized void sendResult(Object oneResult) {
    if (!isOkayToSendResult()) {
      if (logger.isDebugEnabled()) {
        logger.debug(" ServerToClientFunctionResultSender65 not sending result {}  as the server has shutdown",oneResult);
      }
      return;
    }
    try {
      authorizeResult(oneResult);
      if (!this.fn.hasResult()) {
        throw new IllegalStateException(
            LocalizedStrings.ExecuteFunction_CANNOT_0_RESULTS_HASRESULT_FALSE
                .toLocalizedString("send"));
      }
      if (!headerSent) {
        sendHeader();
      }
      if (logger.isDebugEnabled()) {
        logger.debug(" ServerToClientFunctionResultSender65 sending result {}", oneResult);
      }
      DistributedMember memberID = InternalDistributedSystem.getAnyInstance()
          .getDistributionManager().getId();
      List<Object> result = new ArrayList<Object>();
      result.add(oneResult);
      result.add(memberID);
      this.setBuffer();
      this.msg.setNumberOfParts(1);
      this.msg.addObjPart(result);
      this.msg.sendChunk(this.sc);
      FunctionStats.getFunctionStats(fn.getId(), null).incResultsReturned();
    }
    catch (IOException ex) {
      if (isOkayToSendResult()) {
        throw new FunctionException(
            LocalizedStrings.ExecuteFunction_IOEXCEPTION_WHILE_SENDING_RESULT_CHUNK
                .toLocalizedString(), ex);
      }
    }
  }

  @Override
  public synchronized void sendResult(Object oneResult,
      DistributedMember memberID) {
    if (!isOkayToSendResult()) {
      if (logger.isDebugEnabled()) {
        logger.debug(" ServerToClientFunctionResultSender65 not sending result {}  as the server has shutdown", oneResult);
      }
      return;
    }
    try {
      authorizeResult(oneResult);
      if (!this.fn.hasResult()) {
        throw new IllegalStateException(
            LocalizedStrings.ExecuteFunction_CANNOT_0_RESULTS_HASRESULT_FALSE
                .toLocalizedString("send"));
      }
      if (!headerSent) {
        sendHeader();
      }
      if (logger.isDebugEnabled()) {
        logger.debug(" ServerToClientFunctionResultSender65 sending result {}", oneResult);
      }

      List<Object> result = new ArrayList<Object>();
      result.add(oneResult);
      result.add(memberID);
      this.setBuffer();
      this.msg.setNumberOfParts(1);
      this.msg.addObjPart(result);
      this.msg.sendChunk(this.sc);
      FunctionStats.getFunctionStats(fn.getId(), null).incResultsReturned();
    }
    catch (IOException ex) {
      if (isOkayToSendResult()) {
        throw new FunctionException(
            LocalizedStrings.ExecuteFunction_IOEXCEPTION_WHILE_SENDING_RESULT_CHUNK
                .toLocalizedString(), ex);
      }
    }
  }
  
  
  @Override
  protected void writeFunctionExceptionResponse(ChunkedMessage message,
      String errormessage, Throwable e) throws IOException {
    if (logger.isDebugEnabled()) {
      logger.debug(" ServerToClientFunctionResultSender sending Function Error Response : {}",  errormessage);
    }
    int numParts = 0;
    message.clear();
    if (e instanceof FunctionException
        && e.getCause() instanceof InternalFunctionInvocationTargetException) {
      message.setNumberOfParts(3);
      message.addObjPart(e);    
      message.addStringPart(BaseCommand.getExceptionTrace(e));
      InternalFunctionInvocationTargetException fe = (InternalFunctionInvocationTargetException) e.getCause();
      message.addObjPart(fe.getFailedNodeSet());
      numParts = 3;
    }else {
      if(e instanceof FunctionException
          && e.getCause() instanceof QueryInvalidException) {
        // Handle this exception differently since it can contain
        // non-serializable objects.
        // java.io.NotSerializableException: antlr.CommonToken
        // create a new FunctionException on the original one's message (not cause).
        e = new FunctionException(e.getLocalizedMessage());
      } 
      message.setNumberOfParts(2);
      message.addObjPart(e);    
      message.addStringPart(BaseCommand.getExceptionTrace(e));
      numParts = 2;
    }
    message.setServerConnection(this.sc);
    message.setLastChunkAndNumParts(true, numParts);
    //message.setLastChunk(true);    
    message.sendChunk(this.sc);
    this.sc.setAsTrue(Command.RESPONDED);
  }
}
