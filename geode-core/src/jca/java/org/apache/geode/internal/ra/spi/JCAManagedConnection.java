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

package org.apache.geode.internal.ra.spi;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.resource.NotSupportedException;
import javax.resource.ResourceException;
import javax.resource.spi.ConnectionEvent;
import javax.resource.spi.ConnectionEventListener;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.LocalTransaction;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionMetaData;
import javax.security.auth.Subject;
import javax.transaction.xa.XAResource;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.CopyOnWriteHashSet;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.ra.GFConnectionImpl;

public class JCAManagedConnection implements ManagedConnection {

  private final List<ConnectionEventListener> listeners = new CopyOnWriteArrayList<>();

  private volatile TXManagerImpl transactionManager;

  private volatile InternalCache cache;

  private volatile boolean initialized = false;

  private volatile PrintWriter logWriter;

  private final JCAManagedConnectionFactory connectionFactory;

  private final Set<GFConnectionImpl> connections = new CopyOnWriteHashSet<>();

  private volatile JCALocalTransaction localTransaction = new JCALocalTransaction();

  JCAManagedConnection(JCAManagedConnectionFactory connectionFactory) {
    this.connectionFactory = connectionFactory;
  }

  @Override
  public void addConnectionEventListener(ConnectionEventListener listener) {
    listeners.add(listener);
  }

  @Override
  public void associateConnection(Object connection) throws ResourceException {
    if (!(connection instanceof GFConnectionImpl)) {
      throw new ResourceException("Connection is not of type GFConnection");
    }

    ((GFConnectionImpl) connection).resetManagedConnection(this);
    connections.add((GFConnectionImpl) connection);
  }

  @Override
  public void cleanup() throws ResourceException {
    invalidateAndRemoveConnections();
    if (localTransaction == null || localTransaction.transactionInProgress()) {
      if (initialized && !isCacheClosed()) {
        localTransaction = new JCALocalTransaction(cache, transactionManager);
      } else {
        localTransaction = new JCALocalTransaction();
      }
    }
  }

  /**
   * Invalidate and remove the {@link GFConnectionImpl} from the connections collection.
   * The approach to use removeAll instead of Iterator.remove is purely a performance optimization
   * to avoid creating all the intermediary collections that will be created when using the single
   * remove operation.
   */
  private void invalidateAndRemoveConnections() {
    synchronized (connections) {
      List<GFConnectionImpl> connectionsToRemove = new ArrayList<>();
      for (GFConnectionImpl connection : connections) {
        connection.invalidate();
        connectionsToRemove.add(connection);
      }
      connections.removeAll(connectionsToRemove);
    }
  }

  private boolean isCacheClosed() {
    if (cache != null) {
      return cache.isClosed();
    }
    return true;
  }

  @Override
  public void destroy() throws ResourceException {
    invalidateAndRemoveConnections();
    transactionManager = null;
    cache = null;
    localTransaction = null;
    listeners.clear();
  }

  @Override
  public Object getConnection(Subject arg0, ConnectionRequestInfo arg1) throws ResourceException {
    if (!initialized || isCacheClosed()) {
      init();
    }
    LogWriter logger = cache.getLogger();
    if (logger.fineEnabled()) {
      logger.fine("JCAManagedConnection:getConnection. Returning new Connection");
    }

    GFConnectionImpl connection = new GFConnectionImpl(this);
    connections.add(connection);
    return connection;
  }

  private void init() {
    cache = (InternalCache) CacheFactory.getAnyInstance();
    if (cache == null) {
      throw new RuntimeException("Cache could not be found in JCAManagedConnection");
    }
    LogWriter logger = cache.getLogger();
    if (logger.fineEnabled()) {
      logger.fine("JCAManagedConnection:init. Inside init");
    }
    transactionManager = cache.getTxManager();
    initialized = true;
  }

  @Override
  public LocalTransaction getLocalTransaction() throws ResourceException {
    return localTransaction;
  }

  @Override
  public PrintWriter getLogWriter() throws ResourceException {
    return logWriter;
  }

  @Override
  public ManagedConnectionMetaData getMetaData() throws ResourceException {
    if (initialized && !isCacheClosed()) {
      LogWriter logger = cache.getLogger();
      if (logger.fineEnabled()) {
        logger.fine("JCAManagedConnection:getMetaData");
      }
    }
    return new JCAManagedConnectionMetaData(connectionFactory.getProductName(),
        connectionFactory.getVersion(), connectionFactory.getUserName());
  }

  @Override
  public XAResource getXAResource() throws ResourceException {
    throw new NotSupportedException("XA Transaction not supported");
  }

  @Override
  public void removeConnectionEventListener(ConnectionEventListener arg0) {
    listeners.remove(arg0);

  }

  @Override
  public void setLogWriter(PrintWriter logger) throws ResourceException {
    logWriter = logger;
  }

  private void onError(Exception e) { // TODO: currently unused
    localTransaction = null;

    synchronized (connections) {
      List<GFConnectionImpl> connectionsToRemove = new LinkedList<>(connections);
      for (GFConnectionImpl connection : connections) {
        connection.invalidate();
        connectionsToRemove.add(connection);
        synchronized (listeners) {
          ConnectionEvent event =
              new ConnectionEvent(this, ConnectionEvent.CONNECTION_ERROR_OCCURRED, e);
          event.setConnectionHandle(connection);
          for (ConnectionEventListener listener : listeners) {
            listener.connectionErrorOccurred(event);
          }
        }
      }
      connections.removeAll(connectionsToRemove);
    }
  }

  public void onClose(GFConnectionImpl connection) {
    connection.invalidate();
    connections.remove(connection);

    synchronized (listeners) {
      ConnectionEvent event = new ConnectionEvent(this, ConnectionEvent.CONNECTION_CLOSED);
      event.setConnectionHandle(connection);
      for (ConnectionEventListener listener : listeners) {
        listener.connectionClosed(event);
      }
    }

    if (connections.isEmpty()) {
      // safe to dissociate this managed connection so that it can go to pool
      if (initialized && !isCacheClosed()) {
        localTransaction = new JCALocalTransaction(cache, transactionManager);
      } else {
        localTransaction = new JCALocalTransaction();
      }
    }
  }

}
