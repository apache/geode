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
package org.apache.geode.internal.datasource;

import java.util.Map;

import javax.resource.ResourceException;
import javax.resource.spi.ConnectionEvent;
import javax.resource.spi.ConnectionEventListener;
import javax.resource.spi.ConnectionManager;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionFactory;
import javax.security.auth.Subject;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.jndi.JNDIInvoker;
import org.apache.geode.internal.jta.TransactionManagerImpl;
import org.apache.geode.internal.logging.LogService;

/**
 * This class implements a connection pool manager for managed connections (JCA) for transactional
 * and non-transactional resource connection. Implements ConnectionManager interface. QoS
 * (Transaction, Security etc is taken into account while allocating a connection). Security related
 * features are remaining.
 *
 */
public class JCAConnectionManagerImpl implements ConnectionManager, ConnectionEventListener {

  private static final Logger logger = LogService.getLogger();

  private static final long serialVersionUID = 5281512854051120661L;
  protected transient TransactionManager transManager;
  protected ConnectionPoolCache mannPoolCache;
  protected ConnectionRequestInfo conReqInfo = null;
  protected Subject subject = null;
  protected Map xaResourcesMap = new java.util.HashMap();
  protected boolean isActive = true;

  /*
   * Constructor.
   *
   */
  public JCAConnectionManagerImpl(ManagedConnectionFactory mcf,
      ConfiguredDataSourceProperties configs) {
    // Get the security info and form the Subject
    // Initialize the Pool.
    try {
      isActive = true;
      mannPoolCache = new ManagedPoolCacheImpl(mcf, null, null, this, configs);
    } catch (Exception ex) {
      logger.fatal(String.format(
          "JCAConnectionManagerImpl::Constructor: An exception was caught while initialising due to %s",
          ex.getLocalizedMessage()),
          ex);
    }
  }

  /*
   * allocates a ManagedConnection from the ConnectionPool or creates a new
   * ManagedConnection. @param javax.resource.spi.ManagedConnectionFactory
   *
   * @param javax.resource.spi.ConnectionRequestInfo
   *
   */
  public Object allocateConnection(ManagedConnectionFactory mcf, ConnectionRequestInfo reqInfo)
      throws ResourceException {
    if (!isActive) {
      throw new ResourceException(
          "JCAConnectionManagerImpl::allocateConnection::No valid Connection available");
    }
    ManagedConnection conn = null;
    try {
      conn = (ManagedConnection) mannPoolCache.getPooledConnectionFromPool();
    } catch (PoolException ex) {
      // ex.printStackTrace();
      throw new ResourceException(
          String.format(
              "JCAConnectionManagerImpl:: allocateConnection : in getting connection from pool due to %s",
              ex.getMessage()),
          ex);
    }
    // Check if a connection is having a transactional context
    // if a transactional context is used, get the XA Resource
    // from the ManagedConnection and register it with the
    // Transaction Manager.
    try {
      synchronized (this) {
        if (transManager == null) {
          transManager = JNDIInvoker.getTransactionManager();
        }
      }
      Transaction txn = transManager.getTransaction();
      if (txn != null) {
        // Check if Data Source provides XATransaction
        // if(configs.getTransactionType = "XATransaction")
        XAResource xar = conn.getXAResource();
        txn.enlistResource(xar);
        // Asif :Add in the Map after successful registration of XAResource
        xaResourcesMap.put(conn, xar);
        // else throw a resource exception
      }
    } catch (RollbackException ex) {
      throw new ResourceException(
          String.format("JCAConnectionManagerImpl:: allocateConnection : in transaction due to %s",
              ex.getMessage()),
          ex);
    } catch (SystemException ex) {
      throw new ResourceException(
          String.format(
              "JCAConnectionManagerImpl:: allocateConnection :system exception due to %s",
              ex.getMessage()),
          ex);
    }
    return conn.getConnection(subject, reqInfo);
  }

  /**
   * CallBack for Connection Error.
   *
   * @param event ConnectionEvent
   */
  public void connectionErrorOccurred(ConnectionEvent event) {
    if (isActive) {
      // If its an XAConnection
      ManagedConnection conn = (ManagedConnection) event.getSource();
      XAResource xar = (XAResource) xaResourcesMap.get(conn);
      xaResourcesMap.remove(conn);
      TransactionManagerImpl transManager = TransactionManagerImpl.getTransactionManager();
      try {
        Transaction txn = transManager.getTransaction();
        if (txn != null && xar != null)
          txn.delistResource(xar, XAResource.TMSUCCESS);
      } catch (SystemException se) {
        se.printStackTrace();
      }
      try {
        mannPoolCache.expirePooledConnection(conn);
        // mannPoolCache.destroyPooledConnection(conn);
      } catch (Exception ex) {
        String exception =
            "JCAConnectionManagerImpl::connectionErrorOccurred: Exception occurred due to " + ex;
        if (logger.isDebugEnabled()) {
          logger.debug(exception, ex);
        }
      }
    }
  }

  /**
   * Callback for Connection Closed.
   *
   * @param event ConnectionEvent Object.
   */
  public void connectionClosed(ConnectionEvent event) {
    if (isActive) {
      ManagedConnection conn = (ManagedConnection) event.getSource();
      XAResource xar = null;
      if (xaResourcesMap.get(conn) != null)
        xar = (XAResource) xaResourcesMap.get(conn);
      xaResourcesMap.remove(conn);
      try {
        Transaction txn = transManager.getTransaction();
        if (txn != null && xar != null) {
          txn.delistResource(xar, XAResource.TMSUCCESS);
        }
      } catch (Exception se) {
        String exception =
            "JCAConnectionManagerImpl::connectionClosed: Exception occurred due to " + se;
        if (logger.isDebugEnabled()) {
          logger.debug(exception, se);
        }
      }
      mannPoolCache.returnPooledConnectionToPool(conn);
    }
  }

  /*
   * Local Transactions are not supported by Gemfire cache.
   */
  public void localTransactionCommitted(ConnectionEvent arg0) {
    // do nothing.
  }

  /*
   * Local Transactions are not supported by Gemfire cache.
   */
  public void localTransactionRolledback(ConnectionEvent arg0) {
    // do nothing.
  }

  /*
   * Local Transactions are not supported by Gemfire cache.
   */
  public void localTransactionStarted(ConnectionEvent arg0) {
    // do nothing
  }

  public void clearUp() {
    isActive = false;
    mannPoolCache.clearUp();
  }
}
