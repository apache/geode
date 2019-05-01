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

package org.apache.geode.cache.client.internal.pooling;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.cache.client.internal.Connection;
import org.apache.geode.cache.client.internal.ConnectionImpl;
import org.apache.geode.cache.client.internal.ConnectionStats;
import org.apache.geode.cache.client.internal.Endpoint;
import org.apache.geode.cache.client.internal.Op;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.tier.sockets.ServerQueueStatus;

/**
 * A connection managed by the connection manager. Keeps track of the current state of the
 * connection.
 *
 * @since GemFire 5.7
 *
 */
public class PooledConnection implements Connection {

  /*
   * connection is volatile because we may asynchronously destroy the pooled connection while
   * shutting down.
   */
  private final AtomicReference<Connection> connection;
  private Endpoint endpoint;
  private volatile long lastAccessed;
  private final AtomicBoolean active = new AtomicBoolean(true);
  private final AtomicBoolean shouldDestroy = new AtomicBoolean();

  public PooledConnection(Connection connection) {
    this.connection = new AtomicReference<>(connection);
    endpoint = connection.getEndpoint();
    lastAccessed = System.nanoTime();
  }

  @Override
  public ServerLocation getServer() {
    return getEndpoint().getLocation();
  }

  public boolean isActive() {
    return active.get();
  }

  /**
   * @return true if internal connection was destroyed by this call; false if already destroyed
   */
  boolean internalDestroy() {
    shouldDestroy.set(true);
    active.set(false);
    final Connection connection = this.connection.getAndSet(null);
    if (connection != null) {
      connection.destroy();
      return true;
    }
    return false;
  }

  /**
   * When a pooled connection is destroyed, it's not destroyed right away, but when it is returned
   * to the pool.
   */
  @Override
  public void destroy() {
    shouldDestroy.set(true);
  }

  void internalClose(boolean keepAlive) throws Exception {
    try {
      final Connection connection = this.connection.get();
      if (connection != null) {
        connection.close(keepAlive);
      }
    } finally {
      internalDestroy();
    }
  }

  @Override
  public void close(boolean keepAlive) throws Exception {
    internalClose(keepAlive);
  }

  @Override
  public void emergencyClose() {
    final Connection connection = this.connection.getAndSet(null);
    if (connection != null) {
      connection.emergencyClose();
    }
  }

  final Connection getConnection() {
    final Connection connection = this.connection.get();
    if (null == connection) {
      throw new ConnectionDestroyedException();
    }
    return connection;
  }

  @Override
  public Connection getWrappedConnection() {
    return getConnection();
  }

  /**
   * Set the destroy bit if it is not already set.
   *
   * @return true if we were able to set to bit; false if someone else already did
   */
  private boolean setShouldDestroy() {
    return shouldDestroy.compareAndSet(false, true);
  }

  boolean shouldDestroy() {
    return shouldDestroy.get();
  }

  @Override
  public boolean isDestroyed() {
    return null == connection;
  }

  @Override
  public void passivate(final boolean accessed) {
    if (isDestroyed()) {
      return;
    }
    if (!active.compareAndSet(true, false)) {
      throw new InternalGemFireException("Connection not active");
    }
    if (accessed) {
      lastAccessed = System.nanoTime();
    }
  }


  @Override
  public boolean activate() {
    if (isDestroyed() || shouldDestroy()) {
      return false;
    }
    if (!active.compareAndSet(false, true)) {
      throw new InternalGemFireException("Connection already active");
    }
    return true;
  }

  private long getLastAccessed() {
    return lastAccessed;
  }

  private long remainingIdle(long now, long timeoutNanos) {
    return (getLastAccessed() - now) + timeoutNanos;
  }

  /**
   * If we were able to idle timeout this connection then return -1. If this connection has already
   * been destroyed return 0. Otherwise return the amount of idle time remaining. If the connection
   * is active we can't time it out now and a hint is returned as when we should check again.
   */
  long doIdleTimeout(long now, long timeoutNanos) {
    if (shouldDestroy()) {
      return 0;
    }
    if (isActive()) {
      // this is a reasonable value to return since odds are that
      // when the connection goes inactive it will be resetting its access time.
      return timeoutNanos;
    } else {
      long idleRemaining = remainingIdle(now, timeoutNanos);
      if (idleRemaining <= 0) {
        if (setShouldDestroy()) {
          // we were able to set the destroy bit
          return -1;
        } else {
          // someone else already destroyed it
          return 0;
        }
      } else {
        return idleRemaining;
      }
    }
  }

  /**
   * Return true if the connection has been idle long enough to expire.
   */
  boolean hasIdleExpired(long now, long timeoutNanos) {
    if (isActive()) {
      return false;
    } else {
      return remainingIdle(now, timeoutNanos) <= 0;
    }
  }

  @Override
  public ByteBuffer getCommBuffer() throws SocketException {
    return getConnection().getCommBuffer();
  }

  @Override
  public Socket getSocket() {
    return getConnection().getSocket();
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
  public Endpoint getEndpoint() {
    return endpoint;
  }

  @Override
  public ServerQueueStatus getQueueStatus() {
    return getConnection().getQueueStatus();
  }

  @Override
  public String toString() {
    final Connection connection = this.connection.get();
    if (connection != null) {
      return "Pooled Connection to " + getEndpoint() + ": " + connection.toString();
    } else {
      return "Pooled Connection to " + getEndpoint() + ": Connection[DESTROYED]";
    }
  }

  @Override
  public Object execute(Op op) throws Exception {
    return getConnection().execute(op);
  }

  public static void loadEmergencyClasses() {
    ConnectionImpl.loadEmergencyClasses();
  }

  @Override
  public short getWanSiteVersion() {
    return getConnection().getWanSiteVersion();
  }

  @Override
  public int getDistributedSystemId() {
    return getConnection().getDistributedSystemId();
  }

  @Override
  public void setWanSiteVersion(short wanSiteVersion) {
    getConnection().setWanSiteVersion(wanSiteVersion);
  }

  @Override
  public void setConnectionID(long id) {
    getConnection().setConnectionID(id);
  }

  @Override
  public long getConnectionID() {
    return getConnection().getConnectionID();
  }
}
