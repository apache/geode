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

import java.sql.SQLException;

import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;
import javax.sql.XADataSource;

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.logging.LogService;

/**
 * This class models a connection pool for transactional database connection. Extends the
 * AbstractPoolCache to inherit the pool behavior.
 *
 */
public class TranxPoolCacheImpl extends AbstractPoolCache {
  private static final long serialVersionUID = 3295652525163658888L;

  private static final Logger logger = LogService.getLogger();

  private XADataSource m_xads;

  /**
   * Constructor initializes the ConnectionPoolCacheImpl properties.
   */
  public TranxPoolCacheImpl(XADataSource xads, ConnectionEventListener eventListner,
      ConfiguredDataSourceProperties configs) throws PoolException {
    super(eventListner, configs);
    m_xads = xads;
    initializePool();
  }

  @Override
  void destroyPooledConnection(Object connectionObject) {
    try {
      ((PooledConnection) connectionObject)
          .removeConnectionEventListener((javax.sql.ConnectionEventListener) connEventListner);
      ((PooledConnection) connectionObject).close();
      connectionObject = null;
    } catch (Exception ex) {
      if (logger.isTraceEnabled())
        logger.trace(
            "AbstractPoolcache::destroyPooledConnection:Exception in closing the connection.Ignoring it. The exeption is {}",
            ex.getMessage(), ex);
    }
  }

  /**
   * Creates a new connection for the pool. This connection can participate in the transactions.
   *
   * @return the connection from the database as PooledConnection object.
   */
  @Override
  public Object getNewPoolConnection() throws PoolException {
    if (m_xads != null) {
      PooledConnection poolConn = null;
      try {
        poolConn = m_xads.getXAConnection(configProps.getUser(), configProps.getPassword());
      } catch (SQLException sqx) {
        throw new PoolException(
            "TranxPoolCacheImpl::getNewConnection: Exception in creating new transaction PooledConnection",
            sqx);
      }
      poolConn.addConnectionEventListener((javax.sql.ConnectionEventListener) connEventListner);
      return poolConn;
    } else {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "TranxPoolCacheImpl::getNewConnection: ConnectionPoolCache not intialized with XADatasource");
      }
      throw new PoolException(
          "TranxPoolCacheImpl::getNewConnection: ConnectionPoolCache not intialized with XADatasource");
    }
  }
}
