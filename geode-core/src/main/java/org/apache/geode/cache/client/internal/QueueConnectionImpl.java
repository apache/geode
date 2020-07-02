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

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.client.internal.ServerDenyList.FailureTracker;
import org.apache.geode.cache.client.internal.pooling.ConnectionDestroyedException;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.tier.sockets.ServerQueueStatus;
import org.apache.geode.logging.internal.log4j.api.LogService;


/**
 * A wrapper that holds a client to server connection and a server to client connection.
 *
 * The clientToServerConnection should not be used outside of this class.
 *
 */
public class QueueConnectionImpl implements ClientCacheConnection {
  private static final Logger logger = LogService.getLogger();

  private final AtomicReference<ClientCacheConnection> clientToServerConn = new AtomicReference<>();
  private final Endpoint endpoint;
  private volatile ClientUpdater updater;
  private QueueManagerImpl manager;
  private final AtomicBoolean sentClientReady = new AtomicBoolean();
  private FailureTracker failureTracker;

  public QueueConnectionImpl(QueueManagerImpl manager, ClientCacheConnection clientToServer,
      ClientUpdater updater, FailureTracker failureTracker) {
    this.manager = manager;
    this.clientToServerConn.set(clientToServer);
    this.endpoint = clientToServer.getEndpoint();
    this.updater = updater;
    this.failureTracker = failureTracker;
  }

  @Override
  public void close(boolean keepAlive) throws Exception {
    throw new UnsupportedOperationException(
        "Subscription connections should only be closed by subscription manager");
  }

  @Override
  public void emergencyClose() {
    ClientCacheConnection conn = clientToServerConn.getAndSet(null);
    if (conn != null) {
      conn.emergencyClose();
    }
  }

  public void internalClose(boolean keepAlive) throws Exception {
    try {
      getConnection().close(keepAlive);
    } finally {
      if (updater != null) {
        updater.close();
      }
    }
  }

  @Override
  public void destroy() {
    ClientCacheConnection conn = this.clientToServerConn.get();
    if (conn != null) {
      manager.connectionCrashed(conn);
    } // else someone else destroyed it
  }

  public void internalDestroy() {
    ClientCacheConnection currentConn = this.clientToServerConn.get();
    if (currentConn != null) {
      if (!this.clientToServerConn.compareAndSet(currentConn, null)) {
        // someone else did (or is doing) the internalDestroy so return
        return;
      }
      try {
        currentConn.destroy();
      } catch (Exception e) {
        if (logger.isDebugEnabled()) {
          logger.debug("SubscriptionConnectionImpl - error destroying client to server connection",
              e);
        }
      }
    }

    ClientUpdater currentUpdater = updater;
    if (currentUpdater != null) {
      try {
        currentUpdater.close();
      } catch (Exception e) {
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

  @Override
  public boolean isDestroyed() {
    return clientToServerConn.get() == null;
  }

  @Override
  public ByteBuffer getCommBuffer() throws SocketException {
    return getConnection().getCommBuffer();
  }

  @Override
  public Endpoint getEndpoint() {
    return this.endpoint;
  }

  @Override
  public ServerQueueStatus getQueueStatus() {
    return getConnection().getQueueStatus();
  }

  @Override
  public ServerLocation getServer() {
    return getEndpoint().getLocation();
  }

  @Override
  public Socket getSocket() {
    return getConnection().getSocket();
  }

  @Override
  public long getBirthDate() {
    return 0;
  }

  @Override
  public void setBirthDate(long ts) {
    // nothing
  }

  @Override
  public OutputStream getOutputStream() {
    return getConnection().getOutputStream();
  }

  @Override
  public InputStream getInputStream() {
    return getConnection().getInputStream();
  }

  @Override
  public ConnectionStats getStats() {
    return getEndpoint().getStats();
  }

  @Override
  public boolean isActive() {
    return false;
  }

  @Override
  public Object execute(Op op) throws Exception {
    return getConnection().execute(op);
  }

  public ClientCacheConnection getConnection() {
    ClientCacheConnection result = this.clientToServerConn.get();
    if (result == null) {
      throw new ConnectionDestroyedException();
    }
    return result;
  }

  @Override
  public ClientCacheConnection getWrappedConnection() {
    return getConnection();
  }

  FailureTracker getFailureTracker() {
    return failureTracker;
  }

  /**
   * Indicate that we have, or are about to send the client create message on this connection.
   *
   * @return true if we have not yet sent client ready.
   */
  boolean sendClientReady() {
    return sentClientReady.compareAndSet(false, true);
  }

  @Override
  public String toString() {
    ClientCacheConnection result = this.clientToServerConn.get();
    if (result != null) {
      return result.toString();
    } else {
      return "SubscriptionConnectionImpl[" + getServer() + ":closed]";
    }
  }

  @Override
  public short getWanSiteVersion() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getDistributedSystemId() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setWanSiteVersion(short wanSiteVersion) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setConnectionID(long id) {
    this.clientToServerConn.get().setConnectionID(id);
  }

  @Override
  public long getConnectionID() {
    return this.clientToServerConn.get().getConnectionID();
  }
}
