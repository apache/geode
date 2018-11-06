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

package org.apache.geode.internal.cache.execute;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.operations.ExecuteFunctionOperationContext;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.security.AuthorizeRequestPP;

public class ServerToClientFunctionResultSender implements ResultSender {
  private static final Logger logger = LogService.getLogger();

  protected ChunkedMessage msg = null;

  protected ServerConnection sc = null;

  protected int messageType = -1;

  protected volatile boolean headerSent = false;

  protected Function fn;

  protected ExecuteFunctionOperationContext authContext;

  protected InternalDistributedSystem ids = InternalDistributedSystem.getAnyInstance();

  protected AtomicBoolean alreadySendException = new AtomicBoolean(false);

  public synchronized void setLastResultReceived(boolean lastResultReceived) {
    this.lastResultReceived = lastResultReceived;
  }

  protected boolean lastResultReceived;

  protected ByteBuffer commBuffer;

  protected boolean isSelector;

  public boolean isLastResultReceived() {
    return lastResultReceived;
  }

  public ServerToClientFunctionResultSender(ChunkedMessage msg, int messageType,
      ServerConnection sc, Function function, ExecuteFunctionOperationContext authzContext) {
    this.msg = msg;
    this.msg.setVersion(sc.getClientVersion());
    this.messageType = messageType;
    this.sc = sc;
    this.fn = function;
    this.authContext = authzContext;
    this.isSelector = sc.getAcceptor().isSelector();

    if (this.isSelector) {
      this.commBuffer = msg.getCommBuffer();
    }
  }

  public synchronized void lastResult(Object oneResult) {
    if (lastResultReceived) {
      return;
    }
    if (!isOkayToSendResult()) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "ServerToClientFunctionResultSender not sending lastResult {} as the server has shutdown",
            oneResult);
      }
      return;
    }

    if (logger.isDebugEnabled()) {
      logger.debug("ServerToClientFunctionResultSender sending last result1 {} " + oneResult);
    }
    try {
      authorizeResult(oneResult);
      if (!this.fn.hasResult()) {
        throw new IllegalStateException(
            String.format("Cannot %s result as the Function#hasResult() is false",
                "send"));
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
      this.lastResultReceived = true;
      this.sc.setAsTrue(Command.RESPONDED);

      FunctionStats.getFunctionStats(fn.getId()).incResultsReturned();
    } catch (IOException ex) {
      if (isOkayToSendResult()) {
        throw new FunctionException(
            "IOException while sending the last chunk to client",
            ex);
      }
    }
  }

  public synchronized void lastResult(Object oneResult, DistributedMember memberID) {
    if (lastResultReceived) {
      return;
    }
    if (!isOkayToSendResult()) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "ServerToClientFunctionResultSender not sending lastResult {} as the server has shutdown",
            oneResult);
      }
      return;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("ServerToClientFunctionResultSender sending last result2 {} " + oneResult);
    }
    try {
      authorizeResult(oneResult);
      if (!this.fn.hasResult()) {
        throw new IllegalStateException(
            String.format("Cannot %s result as the Function#hasResult() is false",
                "send"));
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
      this.lastResultReceived = true;
      this.sc.setAsTrue(Command.RESPONDED);
      FunctionStats.getFunctionStats(fn.getId()).incResultsReturned();
    } catch (IOException ex) {
      if (isOkayToSendResult()) {
        throw new FunctionException(
            "IOException while sending the last chunk to client",
            ex);
      }
    }
  }

  public synchronized void sendResult(Object oneResult) {
    if (lastResultReceived) {
      return;
    }
    if (!isOkayToSendResult()) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "ServerToClientFunctionResultSender not sending result {} as the server has shutdown",
            oneResult);
      }
      return;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("ServerToClientFunctionResultSender sending result1 {} " + oneResult);
    }
    try {
      authorizeResult(oneResult);
      if (!this.fn.hasResult()) {
        throw new IllegalStateException(
            String.format("Cannot %s result as the Function#hasResult() is false",
                "send"));
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
      FunctionStats.getFunctionStats(fn.getId()).incResultsReturned();
    } catch (IOException ex) {
      if (isOkayToSendResult()) {
        throw new FunctionException(
            "IOException while sending the result chunk to client",
            ex);
      }
    }
  }

  public synchronized void sendResult(Object oneResult, DistributedMember memberID) {
    if (lastResultReceived) {
      return;
    }
    if (!isOkayToSendResult()) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "ServerToClientFunctionResultSender not sending result {} as the server has shutdown",
            oneResult);
      }
      return;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("ServerToClientFunctionResultSender sending result2 {} " + oneResult);
    }
    try {
      authorizeResult(oneResult);
      if (!this.fn.hasResult()) {
        throw new IllegalStateException(
            String.format("Cannot %s result as the Function#hasResult() is false",
                "send"));
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
      FunctionStats.getFunctionStats(fn.getId()).incResultsReturned();
    } catch (IOException ex) {
      if (isOkayToSendResult()) {
        throw new FunctionException(
            "IOException while sending the result chunk to client",
            ex);
      }
    }
  }

  protected void authorizeResult(Object oneResult) throws IOException {
    // check if the caller is authorised to receive these function execution
    // results from server
    AuthorizeRequestPP authzRequestPP = this.sc.getPostAuthzRequest();
    if (authzRequestPP != null) {
      this.authContext.setIsPostOperation(true);
      this.authContext = authzRequestPP.executeFunctionAuthorize(oneResult, this.authContext);
    }
  }

  protected void writeFunctionExceptionResponse(ChunkedMessage message, String errormessage,
      Throwable e) throws IOException {
    if (logger.isDebugEnabled()) {
      logger.debug("ServerToClientFunctionResultSender sending Function Error Response: {}",
          errormessage);
    }
    message.clear();
    message.setLastChunk(true);
    message.addObjPart(e);
    message.sendChunk(this.sc);
    this.sc.setAsTrue(Command.RESPONDED);
  }

  protected void sendHeader() throws IOException {
    if (logger.isDebugEnabled()) {
      logger.debug("ServerToClientFunctionResultSender sending header");
    }
    this.setBuffer();
    this.msg.setMessageType(messageType);
    this.msg.setLastChunk(false);
    this.msg.setNumberOfParts(1);
    this.msg.sendHeader();
    this.headerSent = true;
  }

  public void sendException(Throwable exception) {
    InternalFunctionException iFunxtionException = new InternalFunctionException(exception);
    this.lastResult(iFunxtionException);
    this.lastResultReceived = true;
  }

  public synchronized void setException(Throwable exception) {
    if (lastResultReceived) {
      return;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("ServerToClientFunctionResultSender setting exception {} ", exception);
    }
    synchronized (this.msg) {
      if (!this.sc.getTransientFlag(Command.RESPONDED)) {
        alreadySendException.set(true);
        try {
          if (!headerSent) {
            sendHeader();
          }
          String exceptionMessage = exception.getMessage() != null ? exception.getMessage()
              : "Exception occurred during function execution";
          logger.warn(String.format("Exception on server while executing function : %s",
              this.fn),
              exception);
          if (logger.isDebugEnabled()) {
            logger.debug("ServerToClientFunctionResultSender sending Function Exception : ");
          }
          writeFunctionExceptionResponse(msg, exceptionMessage, exception);
          this.lastResultReceived = true;
        } catch (IOException ignoreAsSocketIsClosed) {
        }
      }
    }
  }

  public boolean isOkayToSendResult() {
    return (sc.getAcceptor().isRunning() && !ids.isDisconnecting()
        && !sc.getCachedRegionHelper().getCache().isClosed() && !alreadySendException.get());
  }

  protected void setBuffer() {
    if (this.isSelector) {
      Message.setTLCommBuffer(this.commBuffer);
    }
  }

}
