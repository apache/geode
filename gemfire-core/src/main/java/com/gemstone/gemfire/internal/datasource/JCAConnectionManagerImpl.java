/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.datasource;

import java.util.Map;

import javax.resource.ResourceException;
import javax.resource.spi.ConnectionEvent;
import javax.resource.spi.ConnectionEventListener;
import javax.resource.spi.ConnectionManager;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionFactory;
import javax.security.auth.Subject;
//import javax.sql.PooledConnection;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.jndi.JNDIInvoker;
import com.gemstone.gemfire.internal.jta.TransactionManagerImpl;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;

/**
 * This class implements a connection pool manager for managed connections (JCA)
 * for transactional and non-transactional resource connection. Implements
 * ConnectionManager interface. QoS (Transaction, Security etc is taken into
 * account while allocating a connection). Security related features are
 * remaining.
 * 
 * @author rreja
 */
public class JCAConnectionManagerImpl implements ConnectionManager,
    ConnectionEventListener {

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
    }
    catch (Exception ex) { 
      logger.fatal(LocalizedMessage.create(LocalizedStrings.JCAConnectionManagerImpl_EXCEPTION_CAUGHT_WHILE_INITIALIZING, ex.getLocalizedMessage()), ex);
    }
  }

  /*
   * allocates a ManagedConnection from the ConnectionPool or creates a new
   * ManagedConnection. @param javax.resource.spi.ManagedConnectionFactory
   * @param javax.resource.spi.ConnectionRequestInfo
   * 
   * @throws ResourceException
   */
  public Object allocateConnection(ManagedConnectionFactory mcf,
      ConnectionRequestInfo reqInfo) throws ResourceException {
    if (!isActive) { throw new ResourceException(LocalizedStrings.JCAConnectionManagerImpl_JCACONNECTIONMANAGERIMPLALLOCATECONNECTIONNO_VALID_CONNECTION_AVAILABLE.toLocalizedString()); }
    ManagedConnection conn = null;
    try {
      conn = (ManagedConnection) mannPoolCache.getPooledConnectionFromPool();
    }
    catch (PoolException ex) {
      //ex.printStackTrace();
      throw new ResourceException(LocalizedStrings.JCAConnectionManagerImpl_JCACONNECTIONMANAGERIMPL_ALLOCATECONNECTION_IN_GETTING_CONNECTION_FROM_POOL_DUE_TO_0.toLocalizedString(ex.getMessage()), ex);
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
        //Asif :Add in the Map after successful registration of XAResource
        xaResourcesMap.put(conn, xar);
        // else throw a resource exception
      }
    }
    catch (RollbackException ex) {
      throw new ResourceException(LocalizedStrings.JCAConnectionManagerImpl_JCACONNECTIONMANAGERIMPL_ALLOCATECONNECTION_IN_TRANSACTION_DUE_TO_0.toLocalizedString(ex.getMessage()), ex);
    }
    catch (SystemException ex) {
      throw new ResourceException(LocalizedStrings.JCAConnectionManagerImpl_JCACONNECTIONMANAGERIMPL_ALLOCATECONNECTION_SYSTEM_EXCEPTION_DUE_TO_0.toLocalizedString(ex.getMessage()), ex);
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
      TransactionManagerImpl transManager = TransactionManagerImpl
          .getTransactionManager();
      try {
        Transaction txn = transManager.getTransaction();
        if (txn != null && xar != null)
            txn.delistResource(xar, XAResource.TMSUCCESS);
      }
      catch (SystemException se) {
        se.printStackTrace();
      }
      try {
        mannPoolCache.expirePooledConnection(conn);
        //mannPoolCache.destroyPooledConnection(conn);
      }
      catch (Exception ex) {
        String exception = "JCAConnectionManagerImpl::connectionErrorOccured: Exception occured due to "
            + ex;
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
      }
      catch (Exception se) {
        String exception = "JCAConnectionManagerImpl::connectionClosed: Exception occured due to "
            + se;
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
