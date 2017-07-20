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
package org.apache.geode.cache.client.internal;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.net.SocketTimeoutException;

import org.apache.logging.log4j.Logger;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.PutAllPartialResultException;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;

/**
 * Represents an operation that can be performed in a client by sending a message to a server.
 * 
 * @since GemFire 5.7
 */
public abstract class AbstractOp implements Op {

  private static final Logger logger = LogService.getLogger();

  private final Message msg;

  private boolean allowDuplicateMetadataRefresh;

  protected AbstractOp(int msgType, int msgParts) {
    this.msg = new Message(msgParts, Version.CURRENT);
    getMessage().setMessageType(msgType);
  }

  /**
   * Returns the message that this op will send to the server
   */
  protected Message getMessage() {
    return this.msg;
  }

  protected void initMessagePart() {

  }

  /**
   * Sets the transaction id on the message
   */
  private void setMsgTransactionId() {
    if (participateInTransaction() && getMessage().getTransactionId() == TXManagerImpl.NOTX) {
      getMessage().setTransactionId(TXManagerImpl.getCurrentTXUniqueId());
    }
  }

  /**
   * Attempts to send this operation's message out on the given connection
   * 
   * @param cnx the connection to use when sending
   * @throws Exception if the send fails
   */
  protected void attemptSend(Connection cnx) throws Exception {
    setMsgTransactionId();
    if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_BRIDGE_SERVER)) {
      if (logger.isDebugEnabled()) {
        logger.debug("Sending op={} using {}", getShortClassName(), cnx);
      }
    }
    getMessage().setComms(cnx.getSocket(), cnx.getInputStream(), cnx.getOutputStream(),
        cnx.getCommBuffer(), cnx.getStats());
    try {
      sendMessage(cnx);
    } finally {
      getMessage().unsetComms();
    }
  }

  /** returns the class name w/o package information. useful in logging */
  public String getShortClassName() {
    String cname = getClass().getName();
    return cname.substring(getClass().getPackage().getName().length() + 1);
  }

  /**
   * New implementations of AbstractOp should override this method if the implementation should be
   * excluded from client authentication. e.g. PingOp#sendMessage(Connection cnx)
   * 
   * @see AbstractOp#needsUserId()
   * @see AbstractOp#processSecureBytes(Connection, Message)
   * @see ServerConnection#updateAndGetSecurityPart()
   */
  protected void sendMessage(Connection cnx) throws Exception {
    if (cnx.getServer().getRequiresCredentials()) {
      // Security is enabled on client as well as on server
      getMessage().setMessageHasSecurePartFlag();
      long userId = -1;

      if (UserAttributes.userAttributes.get() == null) { // single user mode
        userId = cnx.getServer().getUserId();
      } else { // multi user mode
        Object id = UserAttributes.userAttributes.get().getServerToId().get(cnx.getServer());
        if (id == null) {
          // This will ensure that this op is retried on another server, unless
          // the retryCount is exhausted. Fix for Bug 41501
          throw new ServerConnectivityException("Connection error while authenticating user");
        }
        userId = (Long) id;
      }
      HeapDataOutputStream hdos = new HeapDataOutputStream(Version.CURRENT);
      try {
        hdos.writeLong(cnx.getConnectionID());
        hdos.writeLong(userId);
        getMessage()
            .setSecurePart(((ConnectionImpl) cnx).getHandShake().encryptBytes(hdos.toByteArray()));
      } finally {
        hdos.close();
      }
    }
    getMessage().send(false);
  }

  /**
   * Attempts to read a response to this operation by reading it from the given connection, and
   * returning it.
   * 
   * @param cnx the connection to read the response from
   * @return the result of the operation or <code>null</code> if the operation has no result.
   * @throws Exception if the execute failed
   */
  protected Object attemptReadResponse(Connection cnx) throws Exception {
    Message msg = createResponseMessage();
    if (msg != null) {
      msg.setComms(cnx.getSocket(), cnx.getInputStream(), cnx.getOutputStream(),
          cnx.getCommBuffer(), cnx.getStats());
      if (msg instanceof ChunkedMessage) {
        try {
          return processResponse(msg, cnx);
        } finally {
          msg.unsetComms();
          processSecureBytes(cnx, msg);
        }
      } else {
        try {
          msg.recv();
        } finally {
          msg.unsetComms();
          processSecureBytes(cnx, msg);
        }
        return processResponse(msg, cnx);
      }
    } else {
      return null;
    }
  }

  /**
   * New implementations of AbstractOp should override this method if the implementation should be
   * excluded from client authentication. e.g. PingOp#processSecureBytes(Connection cnx, Message
   * message)
   * 
   * @see AbstractOp#sendMessage(Connection)
   * @see AbstractOp#needsUserId()
   * @see ServerConnection#updateAndGetSecurityPart()
   */
  protected void processSecureBytes(Connection cnx, Message message) throws Exception {
    if (cnx.getServer().getRequiresCredentials()) {
      if (!message.isSecureMode()) {
        // This can be seen during shutdown
        if (logger.isDebugEnabled()) {
          logger.trace(LogMarker.BRIDGE_SERVER,
              "Response message from {} for {} has no secure part.", cnx, this);
        }
        return;
      }
      byte[] partBytes = message.getSecureBytes();
      if (partBytes == null) {
        if (logger.isDebugEnabled()) {
          logger.debug("Response message for {} has no bytes in secure part.", this);
        }
        return;
      }
      byte[] bytes = ((ConnectionImpl) cnx).getHandShake().decryptBytes(partBytes);
      DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
      cnx.setConnectionID(dis.readLong());
    }
  }

  /**
   * By default just create a normal one part msg. Subclasses can override this.
   */
  protected Message createResponseMessage() {
    return new Message(1, Version.CURRENT);
  }

  protected Object processResponse(Message m, Connection con) throws Exception {
    return processResponse(m);
  }

  /**
   * Processes the given response message returning the result, if any, of the processing.
   * 
   * @return the result of processing the response; null if no result
   * @throws Exception if response could not be processed or we received a response with a server
   *         exception.
   */
  protected abstract Object processResponse(Message msg) throws Exception;

  /**
   * Return true of <code>messageType</code> indicates the operation had an error on the server.
   */
  protected abstract boolean isErrorResponse(int msgType);

  /**
   * Process a response that contains an ack.
   * 
   * @param msg the message containing the response
   * @param opName text describing this op
   * @throws Exception if response could not be processed or we received a response with a server
   *         exception.
   */
  protected void processAck(Message msg, String opName) throws Exception {
    final int msgType = msg.getMessageType();
    if (msgType == MessageType.REPLY) {
      return;
    } else {
      Part part = msg.getPart(0);
      if (msgType == MessageType.EXCEPTION) {
        String s = ": While performing a remote " + opName;
        Throwable t = (Throwable) part.getObject();
        if (t instanceof PutAllPartialResultException) {
          throw (PutAllPartialResultException) t;
        } else {
          throw new ServerOperationException(s, t);
        }
        // Get the exception toString part.
        // This was added for c++ thin client and not used in java
        // Part exceptionToStringPart = msg.getPart(1);
      } else if (isErrorResponse(msgType)) {
        throw new ServerOperationException(part.getString());
      } else {
        throw new InternalGemFireError("Unexpected message type " + MessageType.getString(msgType));
      }
    }
  }

  /**
   * Process a response that contains a single Object result.
   * 
   * @param msg the message containing the response
   * @param opName text describing this op
   * @return the result of the response
   * @throws Exception if response could not be processed or we received a response with a server
   *         exception.
   */
  protected Object processObjResponse(Message msg, String opName) throws Exception {
    Part part = msg.getPart(0);
    final int msgType = msg.getMessageType();
    if (msgType == MessageType.RESPONSE) {
      return part.getObject();
    } else {
      if (msgType == MessageType.EXCEPTION) {
        String s = "While performing a remote " + opName;
        throw new ServerOperationException(s, (Throwable) part.getObject());
        // Get the exception toString part.
        // This was added for c++ thin client and not used in java
        // Part exceptionToStringPart = msg.getPart(1);
      } else if (isErrorResponse(msgType)) {
        throw new ServerOperationException(part.getString());
      } else {
        throw new InternalGemFireError("Unexpected message type " + MessageType.getString(msgType));
      }
    }
  }

  public boolean isAllowDuplicateMetadataRefresh() {
    return allowDuplicateMetadataRefresh;
  }

  public void setAllowDuplicateMetadataRefresh(final boolean allowDuplicateMetadataRefresh) {
    this.allowDuplicateMetadataRefresh = allowDuplicateMetadataRefresh;
  }

  /**
   * Used by subclasses who get chunked responses.
   */
  public interface ChunkHandler {
    /**
     * This method will be called once for every incoming chunk
     * 
     * @param msg the current chunk to handle
     */
    public void handle(ChunkedMessage msg) throws Exception;
  }

  /**
   * Process a chunked response that contains a single Object result.
   * 
   * @param msg the message containing the response
   * @param opName text describing this op
   * @param callback used to handle each chunks data
   * @throws Exception if response could not be processed or we received a response with a server
   *         exception.
   */
  protected void processChunkedResponse(ChunkedMessage msg, String opName, ChunkHandler callback)
      throws Exception {
    msg.readHeader();
    final int msgType = msg.getMessageType();
    if (msgType == MessageType.RESPONSE) {
      do {
        msg.receiveChunk();
        callback.handle(msg);
      } while (!msg.isLastChunk());
    } else {
      if (msgType == MessageType.EXCEPTION) {
        msg.receiveChunk();
        Part part = msg.getPart(0);
        String s = "While performing a remote " + opName;
        throw new ServerOperationException(s, (Throwable) part.getObject());
        // Get the exception toString part.
        // This was added for c++ thin client and not used in java
        // Part exceptionToStringPart = msg.getPart(1);
      } else if (isErrorResponse(msgType)) {
        msg.receiveChunk();
        Part part = msg.getPart(0);
        throw new ServerOperationException(part.getString());
      } else {
        throw new InternalGemFireError("Unexpected message type " + MessageType.getString(msgType));
      }
    }
  }

  /**
   * Set to true if this attempt failed
   */
  protected boolean failed;
  /**
   * Set to true if this attempt timed out
   */
  protected boolean timedOut;

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.geode.cache.client.internal.Op#attempt(org.apache.geode.cache.client.internal.
   * Connection)
   */
  public Object attempt(Connection cnx) throws Exception {
    this.failed = true;
    this.timedOut = false;
    long start = startAttempt(cnx.getStats());
    try {
      try {
        attemptSend(cnx);
        this.failed = false;
      } finally {
        endSendAttempt(cnx.getStats(), start);
      }
      this.failed = true;
      try {
        Object result = attemptReadResponse(cnx);
        this.failed = false;
        return result;
      } catch (SocketTimeoutException ste) {
        this.failed = false;
        this.timedOut = true;
        throw ste;
      }
    } finally {
      endAttempt(cnx.getStats(), start);
    }
  }

  protected boolean hasFailed() {
    return this.failed;
  }

  protected boolean hasTimedOut() {
    return this.timedOut;
  }

  protected abstract long startAttempt(ConnectionStats stats);

  protected abstract void endSendAttempt(ConnectionStats stats, long start);

  protected abstract void endAttempt(ConnectionStats stats, long start);

  /**
   * New implementations of AbstractOp should override this method to return false if the
   * implementation should be excluded from client authentication. e.g. PingOp#needsUserId()
   * <P/>
   * Also, such an operation's <code>MessageType</code> must be added in the 'if' condition in
   * {@link ServerConnection#updateAndGetSecurityPart()}
   * 
   * @return boolean
   * @see AbstractOp#sendMessage(Connection)
   * @see AbstractOp#processSecureBytes(Connection, Message)
   * @see ServerConnection#updateAndGetSecurityPart()
   */
  protected boolean needsUserId() {
    return true;
  }

  /**
   * Subclasses for AbstractOp should override this method to return false in this message should
   * not participate in any existing transaction
   * 
   * @return true if the message should participate in transaction
   */
  protected boolean participateInTransaction() {
    return true;
  }

  @Override
  public boolean useThreadLocalConnection() {
    return true;
  }

  public boolean isGatewaySenderOp() {
    return false;
  }
}
