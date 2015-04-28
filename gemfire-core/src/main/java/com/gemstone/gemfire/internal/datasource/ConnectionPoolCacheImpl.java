/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.datasource;

/**
 * This class models a connection pool for non-transactional database
 * connection. Extends the AbstractPoolCache to inherit the pool bahavior.
 * 
 * @author tnegi
 */
import java.sql.SQLException;

import javax.sql.ConnectionEventListener;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.jta.TransactionUtils;
import com.gemstone.gemfire.internal.logging.LogService;

public class ConnectionPoolCacheImpl extends AbstractPoolCache {
  private static final Logger logger = LogService.getLogger();
  
  private static final long serialVersionUID = -3096029291871746431L;

  private ConnectionPoolDataSource m_cpds;

  /**
   * Constructor initializes the ConnectionPoolCacheImpl properties.
   */
  public ConnectionPoolCacheImpl(
      ConnectionPoolDataSource connectionpooldatasource,
      ConnectionEventListener eventListner,
      ConfiguredDataSourceProperties configs) throws PoolException {
    super(eventListner, configs);
    m_cpds = connectionpooldatasource;
    initializePool();
  }

  /**
   * This method destroys the connection.
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
      if (logger.isTraceEnabled()) {
          logger.trace("AbstractPoolcache::destroyPooledConnection:Exception in closing the connection.Ignoring it. The exeption is {}", 
              ex.getMessage(), ex);
      }
    }
  }

  /**
   * Creates a new connection for the pool.
   * 
   * @return the connection from the database as Object.
   * @throws PoolException
   */
  @Override
  public Object getNewPoolConnection() throws PoolException {
    if (m_cpds != null) {
      PooledConnection poolConn = null;
      try {
        poolConn = m_cpds.getPooledConnection(configProps.getUser(),
            configProps.getPassword());
      }
      catch (SQLException sqx) {
        throw new PoolException(LocalizedStrings.ConnectionPoolCacheImpl_CONNECTIONPOOLCACHEIMPLGENEWCONNECTION_EXCEPTION_IN_CREATING_NEW_POOLEDCONNECTION.toLocalizedString(), sqx);
      }
      poolConn
          .addConnectionEventListener((javax.sql.ConnectionEventListener) connEventListner);
      return poolConn;
    }
    else {
      if (logger.isDebugEnabled()) {
        logger.debug("ConnectionPoolCacheImpl::geNewConnection: ConnectionPoolCache not intialized with ConnectionPoolDatasource");
      }
      throw new PoolException(LocalizedStrings.ConnectionPoolCacheImpl_CONNECTIONPOOLCACHEIMPLGENEWCONNECTION_CONNECTIONPOOLCACHE_NOT_INTIALIZED_WITH_CONNECTIONPOOLDATASOURCE.toLocalizedString());
    }
  }
}
