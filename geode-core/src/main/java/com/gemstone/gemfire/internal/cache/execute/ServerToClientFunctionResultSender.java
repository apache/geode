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
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.cache.operations.ExecuteFunctionOperationContext;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.sockets.ChunkedMessage;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.security.AuthorizeRequestPP;

/**
 */
public class ServerToClientFunctionResultSender implements ResultSender {
  private static final Logger logger = LogService.getLogger();

  protected ChunkedMessage msg = null;

  protected ServerConnection sc = null;

  protected int messageType = -1;

  protected volatile boolean headerSent = false;

  protected Function fn;

  protected ExecuteFunctionOperationContext authContext;

  protected InternalDistributedSystem ids = InternalDistributedSystem
      .getAnyInstance();

  protected AtomicBoolean alreadySendException = new AtomicBoolean(false);
  
  protected boolean lastResultReceived;
  
  protected ByteBuffer commBuffer ; 
  
  protected boolean isSelector;
  
  public boolean isLastResultReceived() {
    return lastResultReceived;
  }

  public ServerToClientFunctionResultSender(ChunkedMessage msg,
      int messageType, ServerConnection sc, Function function,
      ExecuteFunctionOperationContext authzContext) {
    this.msg = msg;
    this.msg.setVersion(sc.getClientVersion());
    this.messageType = messageType;
    this.sc = sc;
    this.fn = function;
    this.authContext = authzContext;
    this.isSelector = sc.getAcceptor().isSelector();
    if(this.isSelector){
      this.commBuffer = msg.getCommBuffer();  
    }     
  }

  public synchronized void lastResult(Object oneResult) {
    this.lastResultReceived = true;
    if (!isOkayToSendResult()) {
      if (logger.isDebugEnabled()) {
        logger.debug("ServerToClientFunctionResultSender not sending lastResult {} as the server has shutdown", oneResult);
      }
      return;
    }
    if(this.lastResultReceived){
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
        logger.debug("ServerToClientFunctionResultSender sending lastResult {}", oneResult);
      }
      this.setBuffer();
      this.msg.setNumberOfParts(1);
      this.msg.addObjPart(oneResult);
      this.msg.setLastChunk(true);
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

  public synchronized void lastResult(Object oneResult,
      DistributedMember memberID) {
    this.lastResultReceived = true;
    if (!isOkayToSendResult()) {
      if (logger.isDebugEnabled()) {
        logger.debug("ServerToClientFunctionResultSender not sending lastResult {} as the server has shutdown", oneResult);
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
        logger.debug("ServerToClientFunctionResultSender sending lastResult {}", oneResult);
      }
      this.setBuffer();
      this.msg.setNumberOfParts(1);
      this.msg.addObjPart(oneResult);
      this.msg.setLastChunk(true);
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

  public synchronized void sendResult(Object oneResult) {
    if (!isOkayToSendResult()) {
      if (logger.isDebugEnabled()) {
        logger.debug("ServerToClientFunctionResultSender not sending result {} as the server has shutdown", oneResult);
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
        logger.debug("ServerToClientFunctionResultSender sending result {}", oneResult);
      }
      this.setBuffer();
      this.msg.setNumberOfParts(1);
      this.msg.addObjPart(oneResult);
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

  public synchronized void sendResult(Object oneResult,
      DistributedMember memberID) {
    if (!isOkayToSendResult()) {
      if (logger.isDebugEnabled()) {
        logger.debug("ServerToClientFunctionResultSender not sending result {} as the server has shutdown", oneResult);
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
        logger.debug("ServerToClientFunctionResultSender sending result {}", oneResult);
      }
      this.setBuffer();
      this.msg.setNumberOfParts(1);
      this.msg.addObjPart(oneResult);
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

  protected void authorizeResult(Object oneResult) throws IOException {
    // check if the caller is authorised to receive these function execution
    // results from server
    AuthorizeRequestPP authzRequestPP = this.sc.getPostAuthzRequest();
    if (authzRequestPP != null) {
      this.authContext.setPostOperation();
      this.authContext = authzRequestPP.executeFunctionAuthorize(oneResult,
          this.authContext);
    }
  }

  protected void writeFunctionExceptionResponse(ChunkedMessage message,
      String errormessage, Throwable e) throws IOException {
    if (logger.isDebugEnabled()) {
      logger.debug("ServerToClientFunctionResultSender sending Function Error Response: {}", errormessage);
    }
    message.clear();
    message.setLastChunk(true);
    message.addObjPart(e);
    message.sendChunk(this.sc);
    this.sc.setAsTrue(Command.RESPONDED);
  }

  protected void sendHeader() throws IOException {
    this.setBuffer();
    this.msg.setMessageType(messageType);
    this.msg.setLastChunk(false);
    this.msg.setNumberOfParts(1);
    this.msg.sendHeader();
    this.headerSent = true;
  }

  public void sendException(Throwable exception) {
    InternalFunctionException iFunxtionException = new InternalFunctionException(
        exception);
    this.lastResult(iFunxtionException);
    this.lastResultReceived = true;
  }
  
  public synchronized void setException(Throwable exception) {
    this.lastResultReceived = true;
    synchronized (this.msg) {
      if (!this.sc.getTransientFlag(Command.RESPONDED)) {
        alreadySendException.set(true);
        try {
          if (!headerSent) {
            sendHeader();
          }
          String exceptionMessage = exception.getMessage() != null ? exception
              .getMessage() : "Exception occured during function execution";
          logger.warn(LocalizedMessage.create(LocalizedStrings.
            ExecuteRegionFunction_EXCEPTION_ON_SERVER_WHILE_EXECUTIONG_FUNCTION_0, 
            this.fn), exception);
          if (logger.isDebugEnabled()) {
            logger.debug("ServerToClientFunctionResultSender sending Function Exception : ");
          }
          writeFunctionExceptionResponse(msg, exceptionMessage, exception);
        }
        catch (IOException ignoreAsSocketIsClosed) {
        }
      }
    }
  }

  protected boolean isOkayToSendResult() {
    return (sc.getAcceptor().isRunning() && !ids.isDisconnecting()
        && !sc.getCachedRegionHelper().getCache().isClosed() && !alreadySendException
        .get());
  }

  protected void setBuffer() {
    if (this.isSelector) {
      Message.setTLCommBuffer(this.commBuffer);
    }
  }

}
