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
package com.gemstone.gemfire.internal.ra.spi;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.resource.NotSupportedException;
import javax.resource.ResourceException;
import javax.resource.spi.ConnectionEvent;
import javax.resource.spi.ConnectionEventListener;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.LocalTransaction;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionMetaData;
import javax.security.auth.Subject;
import javax.transaction.SystemException;
import javax.transaction.xa.XAResource;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.TXManagerImpl;
import com.gemstone.gemfire.internal.ra.GFConnectionImpl;

/**
 * 
 *
 */
public class JCAManagedConnection implements ManagedConnection

{
  private final List<ConnectionEventListener> listeners;

  private volatile TXManagerImpl gfTxMgr;

  // private volatile TransactionId currentTxID;
  private volatile GemFireCacheImpl cache;

  private volatile boolean initDone = false;

  private volatile PrintWriter logger;

  private JCAManagedConnectionFactory factory;

  private volatile Set<GFConnectionImpl> connections;

  private volatile JCALocalTransaction localTran;

  private final static boolean DEBUG = false;

  public JCAManagedConnection(JCAManagedConnectionFactory fact) {
    this.factory = fact;
    this.listeners = Collections
        .<ConnectionEventListener> synchronizedList(new ArrayList<ConnectionEventListener>());
    this.localTran = new JCALocalTransaction();
    this.connections = Collections
        .<GFConnectionImpl> synchronizedSet(new HashSet<GFConnectionImpl>());
  }

  public void addConnectionEventListener(ConnectionEventListener listener)
  {
    this.listeners.add(listener);

  }

  public void associateConnection(Object conn) throws ResourceException
  {
    if (!(conn instanceof GFConnectionImpl)) {
      throw new ResourceException("Connection is not of type GFConnection");
    }

    ((GFConnectionImpl)conn).resetManagedConnection(this);
    this.connections.add((GFConnectionImpl)conn);
  }

  public void cleanup() throws ResourceException
  {
    if (DEBUG) {
      try {
        throw new NullPointerException("Asif:JCAManagedConnection:cleanup");
      }
      catch (NullPointerException npe) {
        npe.printStackTrace();
      }
    }
    synchronized (this.connections) {
      Iterator<GFConnectionImpl> connsItr = this.connections.iterator();
      while (connsItr.hasNext()) {
        GFConnectionImpl conn = connsItr.next();
        conn.invalidate();
        connsItr.remove();
      }
    }
    if (this.localTran == null || this.localTran.transactionInProgress()) {
      if (this.initDone && !this.cache.isClosed()) {
        this.localTran = new JCALocalTransaction(cache, gfTxMgr);
      }
      else {
        this.localTran = new JCALocalTransaction();
      }
    }

  }

  public void destroy() throws ResourceException
  {
    if (DEBUG) {
      try {
        throw new NullPointerException("Asif:JCAManagedConnection:destroy");
      }
      catch (NullPointerException npe) {
        npe.printStackTrace();
      }
    }
    synchronized (this.connections) {
      Iterator<GFConnectionImpl> connsItr = this.connections.iterator();
      while (connsItr.hasNext()) {
        GFConnectionImpl conn = connsItr.next();
        conn.invalidate();
        connsItr.remove();
      }
    }
    this.gfTxMgr = null;
    this.cache = null;
    this.localTran = null;
    this.listeners.clear();
  }

  public Object getConnection(Subject arg0, ConnectionRequestInfo arg1)
      throws ResourceException
  {
    if (DEBUG) {
      try {
        throw new NullPointerException(
            "Asif:JCAManagedConnection:getConnection");
      }
      catch (NullPointerException npe) {
        npe.printStackTrace();
      }
    }
    try {
      if (!this.initDone || this.cache.isClosed()) {
        init();
      }
      LogWriter logger = this.cache.getLogger();
      if (logger.fineEnabled()) {
        logger
            .fine("JCAManagedConnection:getConnection. Returning new Connection");
      }

      GFConnectionImpl conn = new GFConnectionImpl(this);
      this.connections.add(conn);
      return conn;
    }
    catch (SystemException e) {
      this.onError(e);
      throw new ResourceException("GemFire Resource unavailable", e);
    }
  }

  private void init() throws SystemException
  {
    this.cache = (GemFireCacheImpl)CacheFactory.getAnyInstance();
    LogWriter logger = this.cache.getLogger();
    if (logger.fineEnabled()) {
      logger.fine("JCAManagedConnection:init. Inside init");
    }
    gfTxMgr = cache.getTxManager();
    this.initDone = true;
  }

  public LocalTransaction getLocalTransaction() throws ResourceException
  {
    if (DEBUG) {
      try {
        throw new NullPointerException(
            "Asif:JCAManagedConnection:getLocalTransaction");
      }
      catch (NullPointerException npe) {
        npe.printStackTrace();
      }
    }

    return this.localTran;
  }

  public PrintWriter getLogWriter() throws ResourceException
  {
    return this.logger;
  }

  public ManagedConnectionMetaData getMetaData() throws ResourceException
  {
    if (DEBUG) {
      try {
        throw new NullPointerException("Asif:JCAManagedConnection:getMetaData");
      }
      catch (NullPointerException npe) {
        npe.printStackTrace();
      }
    }
    if (this.initDone && !this.cache.isClosed()) {
      LogWriter logger = this.cache.getLogger();
      if (logger.fineEnabled()) {
        logger.fine("JCAManagedConnection:getMetaData");
      }
    }
    return new JCAManagedConnectionMetaData(this.factory.getProductName(),
        this.factory.getVersion(), this.factory.getUserName());
  }

  public XAResource getXAResource() throws ResourceException
  {
    throw new NotSupportedException("XA Transaction not supported");
  }

  public void removeConnectionEventListener(ConnectionEventListener arg0)
  {
    this.listeners.remove(arg0);

  }

  public void setLogWriter(PrintWriter logger) throws ResourceException
  {
    this.logger = logger;
  }

  private void onError(Exception e)
  {

    this.localTran = null;

    synchronized (this.connections) {
      Iterator<GFConnectionImpl> connsItr = this.connections.iterator();
      while (connsItr.hasNext()) {
        GFConnectionImpl conn = connsItr.next();
        conn.invalidate();
        synchronized (this.listeners) {
          Iterator<ConnectionEventListener> itr = this.listeners.iterator();
          ConnectionEvent ce = new ConnectionEvent(this,
              ConnectionEvent.CONNECTION_ERROR_OCCURRED, e);
          ce.setConnectionHandle(conn);
          while (itr.hasNext()) {
            itr.next().connectionErrorOccurred(ce);
          }
        }
        connsItr.remove();
      }
    }

  }

  public void onClose(GFConnectionImpl conn) throws ResourceException
  {
    conn.invalidate();
    this.connections.remove(conn);
    synchronized (this.listeners) {
      Iterator<ConnectionEventListener> itr = this.listeners.iterator();
      ConnectionEvent ce = new ConnectionEvent(this,
          ConnectionEvent.CONNECTION_CLOSED);
      ce.setConnectionHandle(conn);
      while (itr.hasNext()) {
        itr.next().connectionClosed(ce);
      }
    }
    if (this.connections.isEmpty()) {
      // safe to dissociate this managedconnection so that it can go to pool
      if (this.initDone && !this.cache.isClosed()) {
        this.localTran = new JCALocalTransaction(this.cache, this.gfTxMgr);
      }
      else {
        this.localTran = new JCALocalTransaction();
      }
    }

  }

}
