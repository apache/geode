/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.client.internal.ServerBlackList.FailureTracker;
import com.gemstone.gemfire.cache.client.internal.pooling.ConnectionDestroyedException;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerQueueStatus;
import com.gemstone.gemfire.internal.logging.LogService;


/**
 * A wrapper that holds a client to server connection and
 * a server to client connection.
 * 
 *  The clientToServerConnection should not
 *  be used outside of this class.
 * @author dsmith
 *
 */
public class QueueConnectionImpl implements Connection {
  private static final Logger logger = LogService.getLogger();

  private final AtomicReference/*<Connection>*/ clientToServerConn = new AtomicReference();
  private final Endpoint endpoint;
  private volatile ClientUpdater updater;
  private boolean shouldDestroy;
  private QueueManagerImpl manager;
  private final AtomicBoolean sentClientReady = new AtomicBoolean();
  private FailureTracker failureTracker;
  
  public QueueConnectionImpl(QueueManagerImpl manager, Connection clientToServer,
      ClientUpdater updater, FailureTracker failureTracker) {
    this.manager = manager;
    this.clientToServerConn.set(clientToServer);
    this.endpoint = clientToServer.getEndpoint();
    this.updater = updater;
    this.failureTracker = failureTracker;
  }

  public void close(boolean keepAlive) throws Exception {
    throw new UnsupportedOperationException("Subscription connections should only be closed by subscription manager");
  }
  
  public void emergencyClose() {
    Connection conn = (Connection) clientToServerConn.getAndSet(null);
    if(conn != null) {
      conn.emergencyClose();
    }
  }
  
  public void internalClose(boolean keepAlive) throws Exception {
    try {
      getConnection().close(keepAlive);
    } finally {
      updater.close();
    }
  }

  public void destroy() {
    Connection conn = (Connection)this.clientToServerConn.get();
    if (conn != null) {
      manager.connectionCrashed(conn);
    } // else someone else destroyed it
  }
  
  public void internalDestroy() {
    Connection currentConn = (Connection)this.clientToServerConn.get();
    if(currentConn != null) {
      if (!this.clientToServerConn.compareAndSet(currentConn, null)) {
        // someone else did (or is doing) the internalDestroy so return
        return;
      }
      try {
        currentConn.destroy();
      } catch(Exception e) {
        if (logger.isDebugEnabled()) {
          logger.debug("SubscriptionConnectionImpl - error destroying client to server connection", e);
        }
      }
    } 

    ClientUpdater currentUpdater = updater;
    if(currentUpdater != null) {
      try {
        currentUpdater.close();
      } catch(Exception e) {
        if (logger.isDebugEnabled()) {
          logger.debug("SubscriptionConnectionImpl - error destroying client updater", e);
        }
      }
    }
    updater = null;
  }

  /**
   * test hook
   */
  public ClientUpdater getUpdater() {
    return this.updater;
  }
  
  public boolean isDestroyed() {
    return clientToServerConn.get() == null;
  }
  
  public boolean getShouldDestroy() {
    return shouldDestroy;
  }

  public ByteBuffer getCommBuffer() {
    return getConnection().getCommBuffer();
  }

  public Endpoint getEndpoint() {
    return this.endpoint;
  }

  public ServerQueueStatus getQueueStatus() {
    return getConnection().getQueueStatus();
  }

  public ServerLocation getServer() {
    return getEndpoint().getLocation();
  }

  public Socket getSocket() {
    return getConnection().getSocket();
  }
  
  public OutputStream getOutputStream() {
    return getConnection().getOutputStream();
  }
  
  public InputStream getInputStream() {
    return getConnection().getInputStream();
  }

  public ConnectionStats getStats() {
    return getEndpoint().getStats();
  }

  public Object execute(Op op) throws Exception {
    return getConnection().execute(op);
  }
  
  public Connection getConnection() {
    Connection result = (Connection)this.clientToServerConn.get();
    if (result == null) {
      throw new ConnectionDestroyedException();
    }
    return result;
  }
  
  public FailureTracker getFailureTracker() {
    return failureTracker;
  }
  
  /**
   * Indicate that we have, or are about to send
   * the client create message on this connection.
   * 
   * @return true if we have not yet sent client ready.
   */
  public boolean sendClientReady() {
    return sentClientReady.compareAndSet(false, true);
  }
  
  @Override
  public String toString() { 
    Connection result = (Connection)this.clientToServerConn.get();
    if(result != null) {
      return result.toString();
    } else {
      return "SubscriptionConnectionImpl[" + getServer() + ":closed]";
    }
  }

  public static void loadEmergencyClasses() {
    ConnectionImpl.loadEmergencyClasses();
  }
  
  public short getWanSiteVersion(){
    throw new UnsupportedOperationException();
  }
  
  public int getDistributedSystemId() {
    throw new UnsupportedOperationException();
  }
  
  public void setWanSiteVersion(short wanSiteVersion){
    throw new UnsupportedOperationException();
  }

  public void setConnectionID(long id) {
    ((Connection)this.clientToServerConn.get()).setConnectionID(id);
  }

  public long getConnectionID() {
    return ((Connection)this.clientToServerConn.get()).getConnectionID();
  }
}
