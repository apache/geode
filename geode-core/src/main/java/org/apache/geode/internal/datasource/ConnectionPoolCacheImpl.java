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
package com.gemstone.gemfire.internal.datasource;

/**
 * This class models a connection pool for non-transactional database
 * connection. Extends the AbstractPoolCache to inherit the pool bahavior.
 * 
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
