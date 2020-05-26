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
import java.util.Iterator;
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

  private final List<ConnectionEventListener> listeners = new CopyOnWriteArrayList<>();;

  private volatile TXManagerImpl transactionManager;

  private volatile InternalCache cache;

  private volatile boolean initialized = false;

  private volatile PrintWriter logWriter;

  private final JCAManagedConnectionFactory connectionFactory;

  private final Set<GFConnectionImpl> connections = new CopyOnWriteHashSet<>();;

  private volatile JCALocalTransaction localTransaction = new JCALocalTransaction();;

  JCAManagedConnection(JCAManagedConnectionFactory connectionFactory) {
    this.connectionFactory = connectionFactory;
  }

  @Override
  public void addConnectionEventListener(ConnectionEventListener listener) {
    this.listeners.add(listener);
  }

  @Override
  public void associateConnection(Object connection) throws ResourceException {
    if (!(connection instanceof GFConnectionImpl)) {
      throw new ResourceException("Connection is not of type GFConnection");
    }

    ((GFConnectionImpl) connection).resetManagedConnection(this);
    this.connections.add((GFConnectionImpl) connection);
  }

  @Override
  public void cleanup() throws ResourceException {
    synchronized (this.connections) {
      Iterator<GFConnectionImpl> iterator = this.connections.iterator();
      while (iterator.hasNext()) {
        GFConnectionImpl connection = iterator.next();
        connection.invalidate();
        iterator.remove();
      }
    }
    if (this.localTransaction == null || this.localTransaction.transactionInProgress()) {
      if (this.initialized && !isCacheClosed()) {
        this.localTransaction = new JCALocalTransaction(this.cache, this.transactionManager);
      } else {
        this.localTransaction = new JCALocalTransaction();
      }
    }
  }

  private boolean isCacheClosed() {
    if (this.cache != null) {
      return this.cache.isClosed();
    }
    return true;
  }

  @Override
  public void destroy() throws ResourceException {
    synchronized (this.connections) {
      Iterator<GFConnectionImpl> iterator = this.connections.iterator();
      while (iterator.hasNext()) {
        GFConnectionImpl connection = iterator.next();
        connection.invalidate();
        iterator.remove();
      }
    }
    this.transactionManager = null;
    this.cache = null;
    this.localTransaction = null;
    this.listeners.clear();
  }

  @Override
  public Object getConnection(Subject arg0, ConnectionRequestInfo arg1) throws ResourceException {
    if (!this.initialized || isCacheClosed()) {
      init();
    }
    LogWriter logger = this.cache.getLogger();
    if (logger.fineEnabled()) {
      logger.fine("JCAManagedConnection:getConnection. Returning new Connection");
    }

    GFConnectionImpl connection = new GFConnectionImpl(this);
    this.connections.add(connection);
    return connection;
  }

  private void init() {
    this.cache = (InternalCache) CacheFactory.getAnyInstance();
    if (this.cache == null) {
      throw new RuntimeException("Cache could not be found in JCAManagedConnection");
    }
    LogWriter logger = this.cache.getLogger();
    if (logger.fineEnabled()) {
      logger.fine("JCAManagedConnection:init. Inside init");
    }
    this.transactionManager = this.cache.getTxManager();
    this.initialized = true;
  }

  @Override
  public LocalTransaction getLocalTransaction() throws ResourceException {
    return this.localTransaction;
  }

  @Override
  public PrintWriter getLogWriter() throws ResourceException {
    return this.logWriter;
  }

  @Override
  public ManagedConnectionMetaData getMetaData() throws ResourceException {
    if (this.initialized && !isCacheClosed()) {
      LogWriter logger = this.cache.getLogger();
      if (logger.fineEnabled()) {
        logger.fine("JCAManagedConnection:getMetaData");
      }
    }
    return new JCAManagedConnectionMetaData(this.connectionFactory.getProductName(),
        this.connectionFactory.getVersion(), this.connectionFactory.getUserName());
  }

  @Override
  public XAResource getXAResource() throws ResourceException {
    throw new NotSupportedException("XA Transaction not supported");
  }

  @Override
  public void removeConnectionEventListener(ConnectionEventListener arg0) {
    this.listeners.remove(arg0);

  }

  @Override
  public void setLogWriter(PrintWriter logger) throws ResourceException {
    this.logWriter = logger;
  }

  private void onError(Exception e) { // TODO: currently unused
    this.localTransaction = null;

    synchronized (this.connections) {
      Iterator<GFConnectionImpl> iterator = this.connections.iterator();
      while (iterator.hasNext()) {
        GFConnectionImpl connection = iterator.next();
        connection.invalidate();

        synchronized (this.listeners) {
          ConnectionEvent event =
              new ConnectionEvent(this, ConnectionEvent.CONNECTION_ERROR_OCCURRED, e);
          event.setConnectionHandle(connection);
          for (ConnectionEventListener listener : this.listeners) {
            listener.connectionErrorOccurred(event);
          }
        }

        iterator.remove();
      }
    }
  }

  public void onClose(GFConnectionImpl connection) {
    connection.invalidate();
    this.connections.remove(connection);

    synchronized (this.listeners) {
      Iterator<ConnectionEventListener> iterator = this.listeners.iterator();
      ConnectionEvent event = new ConnectionEvent(this, ConnectionEvent.CONNECTION_CLOSED);
      event.setConnectionHandle(connection);
      while (iterator.hasNext()) {
        iterator.next().connectionClosed(event);
      }
    }

    if (this.connections.isEmpty()) {
      // safe to dissociate this managed connection so that it can go to pool
      if (this.initialized && !isCacheClosed()) {
        this.localTransaction = new JCALocalTransaction(this.cache, this.transactionManager);
      } else {
        this.localTransaction = new JCALocalTransaction();
      }
    }
  }

}
