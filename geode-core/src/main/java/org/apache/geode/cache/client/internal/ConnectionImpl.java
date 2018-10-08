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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ssl.SSLSocket;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.CancelException;
import org.apache.geode.ForcedDisconnectException;
import org.apache.geode.cache.client.internal.ExecuteFunctionOp.ExecuteFunctionOpImpl;
import org.apache.geode.cache.client.internal.ExecuteRegionFunctionOp.ExecuteRegionFunctionOpImpl;
import org.apache.geode.cache.client.internal.ExecuteRegionFunctionSingleHopOp.ExecuteRegionFunctionSingleHopOpImpl;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.tier.ClientSideHandshake;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.cache.tier.sockets.ServerQueueStatus;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.net.SocketCreator;

/**
 * A single client to server connection.
 *
 * The execute method of this class is synchronized to prevent two ops from using the client to
 * server connection at the same time.
 *
 * @since GemFire 5.7
 */
public class ConnectionImpl implements Connection {

  // TODO: DEFAULT_CLIENT_FUNCTION_TIMEOUT should be private
  public static final int DEFAULT_CLIENT_FUNCTION_TIMEOUT = 0;
  private static final String CLIENT_FUNCTION_TIMEOUT_SYSTEM_PROPERTY =
      DistributionConfig.GEMFIRE_PREFIX + "CLIENT_FUNCTION_TIMEOUT";

  private static Logger logger = LogService.getLogger();

  /**
   * Test hook to simulate a client crashing. If true, we will not notify the server when we close
   * the connection.
   */
  private static boolean TEST_DURABLE_CLIENT_CRASH = false;

  private Socket theSocket;
  private ByteBuffer commBuffer;
  private ByteBuffer commBufferForAsyncRead;
  private ServerQueueStatus status;
  private volatile boolean connectFinished;
  private final AtomicBoolean destroyed = new AtomicBoolean();
  private Endpoint endpoint;

  // In Gateway communication version of connected wan site will be stored after successful
  // handshake
  private short wanSiteVersion = -1;

  private final InternalDistributedSystem ds;

  private OutputStream out;
  private InputStream in;

  private long connectionID = Connection.DEFAULT_CONNECTION_ID;

  private ClientSideHandshake handshake;

  public ConnectionImpl(InternalDistributedSystem ds, CancelCriterion cancelCriterion) {
    this.ds = ds;
  }

  public static int getClientFunctionTimeout() {
    int time = Integer.getInteger(CLIENT_FUNCTION_TIMEOUT_SYSTEM_PROPERTY,
        DEFAULT_CLIENT_FUNCTION_TIMEOUT);
    return time >= 0 ? time : DEFAULT_CLIENT_FUNCTION_TIMEOUT;
  }

  public ServerQueueStatus connect(EndpointManager endpointManager, ServerLocation location,
      ClientSideHandshake handshake, int socketBufferSize, int handshakeTimeout, int readTimeout,
      CommunicationMode communicationMode, GatewaySender sender, SocketCreator sc)
      throws IOException {
    theSocket = sc.connectForClient(location.getHostName(), location.getPort(), handshakeTimeout,
        socketBufferSize);
    theSocket.setTcpNoDelay(true);
    theSocket.setSendBufferSize(socketBufferSize);

    // Verify buffer sizes
    verifySocketBufferSize(socketBufferSize, theSocket.getReceiveBufferSize(), "receive");
    verifySocketBufferSize(socketBufferSize, theSocket.getSendBufferSize(), "send");

    theSocket.setSoTimeout(handshakeTimeout);
    out = theSocket.getOutputStream();
    in = theSocket.getInputStream();
    this.status = handshake.handshakeWithServer(this, location, communicationMode);
    commBuffer = ServerConnection.allocateCommBuffer(socketBufferSize, theSocket);
    if (sender != null) {
      commBufferForAsyncRead = ServerConnection.allocateCommBuffer(socketBufferSize, theSocket);
    }
    theSocket.setSoTimeout(readTimeout);
    endpoint = endpointManager.referenceEndpoint(location, this.status.getMemberId());
    this.connectFinished = true;
    this.endpoint.getStats().incConnections(1);
    return status;
  }

  public void close(boolean keepAlive) throws Exception {

    try {
      // if a forced-disconnect has occurred, we can't send messages to anyone
      boolean sendCloseMsg = !TEST_DURABLE_CLIENT_CRASH;
      if (sendCloseMsg) {
        try {
          ((InternalDistributedSystem) ds).getDistributionManager();
        } catch (CancelException e) { // distribution has stopped
          Throwable t = e.getCause();
          if (t instanceof ForcedDisconnectException) {
            // we're crashing - don't attempt to send a message (bug 39317)
            sendCloseMsg = false;
          }
        }
      }

      if (sendCloseMsg) {
        if (logger.isDebugEnabled()) {
          logger.debug("Closing connection {} with keepAlive: {}", this, keepAlive);
        }
        CloseConnectionOp.execute(this, keepAlive);
      }
    } finally {
      destroy();
    }
  }

  public void emergencyClose() {
    commBuffer = null;
    try {
      theSocket.close();
    } catch (IOException | RuntimeException ignore) {
      // ignore
    }
  }

  public boolean isDestroyed() {
    return this.destroyed.get();
  }

  public void destroy() {
    if (!this.destroyed.compareAndSet(false, true)) {
      // was already set to true so someone else did the destroy
      return;
    }

    if (endpoint != null) {
      if (this.connectFinished) {
        endpoint.getStats().incConnections(-1);
      }
      endpoint.removeReference();
    }
    try {
      if (theSocket != null) {
        if (!(theSocket instanceof SSLSocket)) {
          theSocket.getOutputStream().flush();
          theSocket.shutdownOutput();
        }
        theSocket.close();
      }
    } catch (Exception e) {
      if (logger.isDebugEnabled()) {
        logger.debug(e.getMessage(), e);
      }
    }
    releaseCommBuffers();
  }

  private void releaseCommBuffers() {
    ByteBuffer bb = this.commBuffer;
    if (bb != null) {
      this.commBuffer = null;
      ServerConnection.releaseCommBuffer(bb);
    }
    bb = this.commBufferForAsyncRead;
    if (bb != null) {
      this.commBufferForAsyncRead = null;
      ServerConnection.releaseCommBuffer(bb);
    }
  }

  public ByteBuffer getCommBuffer() throws SocketException {
    if (isDestroyed()) {
      // see bug 52193. Since the code used to see this
      // as an attempt to use a close socket just throw
      // a SocketException.
      throw new SocketException("socket was closed");
    }
    return commBuffer;
  }

  public ServerLocation getServer() {
    return endpoint.getLocation();
  }

  public Socket getSocket() {
    return theSocket;
  }

  public OutputStream getOutputStream() {
    return out;
  }

  public InputStream getInputStream() {
    return in;
  }


  public ConnectionStats getStats() {
    return endpoint.getStats();
  }

  @Override
  public String toString() {
    return "Connection[" + endpoint + "]@" + this.hashCode();
  }

  public Endpoint getEndpoint() {
    return endpoint;
  }

  public ServerQueueStatus getQueueStatus() {
    return status;
  }

  public Object execute(Op op) throws Exception {
    Object result;
    // Do not synchronize when used for GatewaySender
    // as the same connection is being used
    if ((op instanceof AbstractOp) && ((AbstractOp) op).isGatewaySenderOp()) {
      result = op.attempt(this);
      endpoint.updateLastExecute();
      return result;
    }
    synchronized (this) {
      if (op instanceof ExecuteFunctionOpImpl || op instanceof ExecuteRegionFunctionOpImpl
          || op instanceof ExecuteRegionFunctionSingleHopOpImpl) {
        int earliertimeout = this.getSocket().getSoTimeout();
        this.getSocket().setSoTimeout(getClientFunctionTimeout());
        try {
          result = op.attempt(this);
        } finally {
          this.getSocket().setSoTimeout(earliertimeout);
        }
      } else {
        result = op.attempt(this);
      }
    }
    endpoint.updateLastExecute();
    return result;

  }


  public static void loadEmergencyClasses() {
    // do nothing
  }

  public short getWanSiteVersion() {
    return wanSiteVersion;
  }

  public void setWanSiteVersion(short wanSiteVersion) {
    this.wanSiteVersion = wanSiteVersion;
  }

  public int getDistributedSystemId() {
    return ((InternalDistributedSystem) this.ds).getDistributionManager().getDistributedSystemId();
  }

  public void setConnectionID(long id) {
    this.connectionID = id;
  }

  public long getConnectionID() {
    return this.connectionID;
  }

  protected byte[] encryptBytes(byte[] messageBytes) throws Exception {
    return handshake.getEncryptor().encryptBytes(messageBytes);
  }

  protected byte[] decryptBytes(byte[] messageBytes) throws Exception {
    return handshake.getEncryptor().decryptBytes(messageBytes);
  }

  protected void setHandshake(ClientSideHandshake handshake) {
    this.handshake = handshake;
  }

  /**
   * test hook
   */
  public static void setTEST_DURABLE_CLIENT_CRASH(boolean v) {
    TEST_DURABLE_CLIENT_CRASH = v;
  }

  public ByteBuffer getCommBufferForAsyncRead() throws SocketException {
    if (isDestroyed()) {
      // see bug 52193. Since the code used to see this
      // as an attempt to use a close socket just throw
      // a SocketException.
      throw new SocketException("socket was closed");
    }
    return commBufferForAsyncRead;
  }

  private void verifySocketBufferSize(int requestedBufferSize, int actualBufferSize, String type) {
    if (actualBufferSize < requestedBufferSize) {
      logger.info("Socket {} buffer size is {} instead of the requested {}.",
          new Object[] {type,
              actualBufferSize,
              requestedBufferSize});
    }
  }
}
