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
package com.gemstone.gemfire.cache.client.internal;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.NoRouteToHostException;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.ForcedDisconnectException;
import com.gemstone.gemfire.cache.client.internal.ExecuteFunctionOp.ExecuteFunctionOpImpl;
import com.gemstone.gemfire.cache.client.internal.ExecuteRegionFunctionOp.ExecuteRegionFunctionOpImpl;
import com.gemstone.gemfire.cache.client.internal.ExecuteRegionFunctionSingleHopOp.ExecuteRegionFunctionSingleHopOpImpl;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.SocketUtils;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.tier.Acceptor;
import com.gemstone.gemfire.internal.cache.tier.sockets.HandShake;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerQueueStatus;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;

/**
 * A single client to server connection.
 * 
 * The execute  method of this class is synchronized to
 * prevent two ops from using the client to server connection
 *  at the same time.
 * @author dsmith
 * @since 5.7
 *
 */
public class ConnectionImpl implements Connection {
  
  private static Logger logger = LogService.getLogger();
  
  /**Test hook to simulate a client crashing. If true, we will
   * not notify the server when we close the connection.
   */
  private static boolean TEST_DURABLE_CLIENT_CRASH = false;
  
  private Socket theSocket;
  private ByteBuffer commBuffer;
  private ByteBuffer commBufferForAsyncRead;
//  private int handShakeTimeout = AcceptorImpl.DEFAULT_HANDSHAKE_TIMEOUT_MS;
  private ServerQueueStatus status;
  private volatile boolean connectFinished;
  private final AtomicBoolean destroyed = new AtomicBoolean();
  private Endpoint endpoint;
  private short wanSiteVersion = -1;//In Gateway communication version of connected wan site
                                   //will be stored after successful handshake
//  private final CancelCriterion cancelCriterion;
  private final InternalDistributedSystem ds;

  private OutputStream out;
  private InputStream in;

  private long connectionID = Connection.DEFAULT_CONNECTION_ID;

  private HandShake handShake;

  public ConnectionImpl(InternalDistributedSystem ds, CancelCriterion cancelCriterion) {
//    this.cancelCriterion = cancelCriterion;
    this.ds = ds;
  }
  
  public ServerQueueStatus connect(EndpointManager endpointManager,
      ServerLocation location, HandShake handShake, int socketBufferSize,
      int handShakeTimeout, int readTimeout, byte communicationMode, GatewaySender sender, SocketCreator sc)
      throws IOException {
    theSocket = sc.connectForClient(
        location.getHostName(), location.getPort(), handShakeTimeout, socketBufferSize);
    theSocket.setTcpNoDelay(true);
    //System.out.println("ConnectionImpl setting buffer sizes: " +
    // socketBufferSize);
    theSocket.setSendBufferSize(socketBufferSize);

    // Verify buffer sizes
    verifySocketBufferSize(socketBufferSize, theSocket.getReceiveBufferSize(), "receive");
    verifySocketBufferSize(socketBufferSize, theSocket.getSendBufferSize(), "send");
    
    theSocket.setSoTimeout(handShakeTimeout);
    out = SocketUtils.getOutputStream(theSocket);//theSocket.getOutputStream();
    in = SocketUtils.getInputStream(theSocket);//theSocket.getInputStream();
    this.status = handShake.greet(this, location, communicationMode);
    commBuffer = ServerConnection.allocateCommBuffer(socketBufferSize, theSocket);
    if (sender != null) {
      commBufferForAsyncRead = ServerConnection
          .allocateCommBuffer(socketBufferSize, theSocket);
    }
    theSocket.setSoTimeout(readTimeout);
    endpoint = endpointManager.referenceEndpoint(location, this.status.getMemberId());
    //logger.warning("ESTABLISHING ENDPOINT:"+location+" MEMBERID:"+endpoint.getMemberId(),new Exception());
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
          ((InternalDistributedSystem)ds).getDistributionManager();
        }
        catch (CancelException e) { // distribution has stopped
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
    }
    finally {
      destroy();
    }
  }
  
  public void emergencyClose() {
    commBuffer = null;
    try {
      theSocket.close();
    } catch (IOException e) {
      //ignore
    } catch (RuntimeException e) {
      //ignore
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
        theSocket.getOutputStream().flush();
        theSocket.shutdownOutput();
        theSocket.close();
      }
    }
    catch (Exception e) {
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
    if ((op instanceof AbstractOp) && ((AbstractOp)op).isGatewaySenderOp()) {
      result = op.attempt(this);
      endpoint.updateLastExecute();
      return result;
    }
    synchronized (this) {
      if (op instanceof ExecuteFunctionOpImpl
          || op instanceof ExecuteRegionFunctionOpImpl
          || op instanceof ExecuteRegionFunctionSingleHopOpImpl) {
        int earliertimeout = this.getSocket().getSoTimeout();
        this.getSocket().setSoTimeout(GemFireCacheImpl.getClientFunctionTimeout());
        result = op.attempt(this);
        this.getSocket().setSoTimeout(earliertimeout);
      } else {
        result = op.attempt(this);
      }
    }
    endpoint.updateLastExecute();
    return result;

  }
  
  
  public static void loadEmergencyClasses() {
    //do nothing
  }
  public short getWanSiteVersion(){
    return wanSiteVersion;
  }
  
  public void setWanSiteVersion(short wanSiteVersion){
    this.wanSiteVersion = wanSiteVersion;
  }
  
  public int getDistributedSystemId() {
    return ((InternalDistributedSystem)this.ds).getDistributionManager().getDistributedSystemId();
  }
  
  public void setConnectionID(long id) {
    this.connectionID = id;
  }

  public long getConnectionID() {
    return this.connectionID;
  }

  protected HandShake getHandShake() {
    return handShake;
  }

  protected void setHandShake(HandShake handShake) {
    this.handShake = handShake;
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
      logger.info(LocalizedMessage.create(LocalizedStrings.Connection_SOCKET_0_IS_1_INSTEAD_OF_THE_REQUESTED_2,
          new Object[] { new StringBuilder(type).append(" buffer size").toString(), actualBufferSize, requestedBufferSize }));
    }
  }
}
