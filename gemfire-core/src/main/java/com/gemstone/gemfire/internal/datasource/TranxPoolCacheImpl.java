/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.datasource;

/**
 * @author tnegi
 */
import java.sql.SQLException;

import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;
import javax.sql.XADataSource;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * This class models a connection pool for transactional database connection.
 * Extends the AbstractPoolCache to inherit the pool bahavior.
 * 
 * @author tnegi
 */
public class TranxPoolCacheImpl extends AbstractPoolCache {
  private static final long serialVersionUID = 3295652525163658888L;

  private static final Logger logger = LogService.getLogger();
  
  private XADataSource m_xads;

  /**
   * Constructor initializes the ConnectionPoolCacheImpl properties.
   */
  public TranxPoolCacheImpl(XADataSource xads,
      ConnectionEventListener eventListner,
      ConfiguredDataSourceProperties configs) throws PoolException {
    super(eventListner, configs);
    m_xads = xads;
    initializePool();
  }

  /**
   *  
   */
  @Override
  void destroyPooledConnection(Object connectionObject) {
    try {
      ((PooledConnection) connectionObject)
          .removeConnectionEventListener((javax.sql.ConnectionEventListener) connEventListner);
      ((PooledConnection) connectionObject).close();
      connectionObject = null;
    }
    catch (Exception ex) {
      if (logger.isTraceEnabled())
          logger.trace("AbstractPoolcache::destroyPooledConnection:Exception in closing the connection.Ignoring it. The exeption is {}", ex.getMessage(), ex);
    }
  }

  /**
   * Creates a new connection for the pool. This connection can participate in
   * the transactions.
   * 
   * @return the connection from the database as PooledConnection object.
   */
  @Override
  public Object getNewPoolConnection() throws PoolException {
    if (m_xads != null) {
      PooledConnection poolConn = null;
      try {
        poolConn = m_xads.getXAConnection(configProps.getUser(), configProps
            .getPassword());
      }
      catch (SQLException sqx) {
        throw new PoolException(LocalizedStrings.TranxPoolCacheImpl_TRANXPOOLCACHEIMPLGETNEWCONNECTION_EXCEPTION_IN_CREATING_NEW_TRANSACTION_POOLEDCONNECTION.toLocalizedString(), sqx);
      }
      poolConn
          .addConnectionEventListener((javax.sql.ConnectionEventListener) connEventListner);
      return poolConn;
    }
    else {
      if (logger.isDebugEnabled()) {
        logger.debug("TranxPoolCacheImpl::getNewConnection: ConnectionPoolCache not intialized with XADatasource");
      }
      throw new PoolException(LocalizedStrings.TranxPoolCacheImpl_TRANXPOOLCACHEIMPLGETNEWCONNECTION_CONNECTIONPOOLCACHE_NOT_INTIALIZED_WITH_XADATASOURCE.toLocalizedString());
    }
  }
}
