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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.resource.ResourceException;
import javax.resource.spi.ConnectionEvent;
import javax.resource.spi.ConnectionEventListener;
import javax.resource.spi.ConnectionManager;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionFactory;
import javax.security.auth.Subject;
import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
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
public class FacetsJCAConnectionManagerImpl
    implements ConnectionManager, ConnectionEventListener, Synchronization {

  private static final Logger logger = LogService.getLogger();

  private static final long serialVersionUID = 2454746064736724758L;

  protected transient TransactionManager transManager;
  protected ConnectionPoolCache mannPoolCache;
  protected ConnectionRequestInfo conReqInfo = null;
  protected Subject subject = null;
  protected boolean isActive = true;
  private transient ThreadLocal xalistThreadLocal = new ThreadLocal() {

    @Override
    protected Object initialValue() {
      return new ArrayList();
    }
  };

  /*
   * Constructor.
   *
   */
  public FacetsJCAConnectionManagerImpl(ManagedConnectionFactory mcf,
      ConfiguredDataSourceProperties configs) {
    // Get the security info and form the Subject
    // Initialize the Pool.
    try {
      isActive = true;
      mannPoolCache = new ManagedPoolCacheImpl(mcf, null, null, this, configs);
    } catch (Exception ex) {
      logger.fatal(String.format(
          "FacetsJCAConnectionManagerImpl::Constructor: An Exception was caught while initializing due to %s",
          ex.getMessage()),
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
          "FacetsJCAConnectionManagerImpl::allocateConnection::No valid Connection available");
    }
    ManagedConnection conn = null;
    try {
      conn = (ManagedConnection) mannPoolCache.getPooledConnectionFromPool();
    } catch (PoolException ex) {
      ex.printStackTrace();
      throw new ResourceException(
          String.format(
              "FacetsJCAConnectionManagerImpl:: allocateConnection : in getting connection from pool due to %s",
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
        java.util.List resList = (List) xalistThreadLocal.get();
        if (resList.size() == 0) {
          // facets specific implementation
          // register syschronisation only once
          txn.registerSynchronization(this);
        }
        resList.add(conn);
        // xalistThreadLocal.set(resList);
        // Asif :Add in the Map after successful registration of XAResource
        // xaResourcesMap.put(conn, xar);
        // else throw a resource exception
      }
    } catch (RollbackException ex) {
      String exception =
          String.format(
              "FacetsJCAConnectionManagerImpl:: An Exception was caught while allocating a connection due to %s",
              ex.getMessage());
      throw new ResourceException(exception, ex);
    } catch (SystemException ex) {
      throw new ResourceException(
          String.format(
              "FacetsJCAConnectionManagerImpl:: allocateConnection :system exception due to %s",
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
      // XAResource xar = (XAResource) xaResourcesMap.get(conn);
      ((List) xalistThreadLocal.get()).remove(conn);
      TransactionManagerImpl transManager = TransactionManagerImpl.getTransactionManager();
      try {
        Transaction txn = transManager.getTransaction();
        if (txn == null) {
          mannPoolCache.returnPooledConnectionToPool(conn);
        } else {
          // do nothing.
        }
      } catch (Exception se) {
        se.printStackTrace();
      }
      try {
        mannPoolCache.expirePooledConnection(conn);
        // mannPoolCache.destroyPooledConnection(conn);
      } catch (Exception ex) {
        String exception =
            "FacetsJCAConnectionManagerImpl::connectionErrorOccurred: Exception occurred due to "
                + ex.getMessage();
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
      TransactionManagerImpl transManager = TransactionManagerImpl.getTransactionManager();
      try {
        Transaction txn = transManager.getTransaction();
        if (txn == null) {
          mannPoolCache.returnPooledConnectionToPool(conn);
        }
      } catch (Exception se) {
        String exception =
            "FacetsJCAConnectionManagerImpl::connectionClosed: Exception occurred due to "
                + se.getMessage();
        if (logger.isDebugEnabled()) {
          logger.debug(exception, se);
        }
      }
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

  /*
   * (non-Javadoc)
   *
   * @see javax.transaction.Synchronization#afterCompletion(int)
   */
  public void afterCompletion(int arg0) {
    // DELIST THE XARESOURCE FROM THE LIST. RETURN ALL THE CONNECTIONS TO THE
    // POOL.
    java.util.List lsConn = (ArrayList) xalistThreadLocal.get();
    Iterator itr = lsConn.iterator();
    while (itr.hasNext()) {
      ManagedConnection conn = (ManagedConnection) itr.next();
      mannPoolCache.returnPooledConnectionToPool(conn);
    }
    lsConn.clear();
    // return all the connections to pool.
  }

  /*
   * (non-Javadoc)
   *
   * @see javax.transaction.Synchronization#beforeCompletion()
   */
  public void beforeCompletion() {
    // TODO Auto-generated method stub
  }
}
