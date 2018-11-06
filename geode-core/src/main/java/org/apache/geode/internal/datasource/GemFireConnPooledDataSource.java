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

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.logging.LogService;

/**
 * GemFireTransactionDataSource extends AbstractDataSource. This is a datasource class which
 * provides connections from the pool. The objects of these class are ConnectionEventListener for
 * connection close and error events.
 *
 * Modified Exception handling & changed name of the function
 */
public class GemFireConnPooledDataSource extends AbstractDataSource
    implements javax.sql.ConnectionEventListener {

  private static final Logger logger = LogService.getLogger();

  private static final long serialVersionUID = 1177231744410855158L;
  protected ConnectionProvider provider;

  /**
   * Creates a new instance of GemFireConnPooledDataSource
   *
   * @param connPoolDS The ConnectionPoolDataSource object for the database driver.
   * @param configs The ConfiguredDataSourceProperties containing the datasource properties.
   */


  /**
   * Place holder for abstract method isWrapperFor(java.lang.Class) in java.sql.Wrapper required by
   * jdk 1.6
   *
   * @param iface - a Class defining an interface.
   */
  public boolean isWrapperFor(Class iface) throws SQLException {
    return true;
  }

  public Object unwrap(Class iface) throws SQLException {
    return iface;
  }



  public GemFireConnPooledDataSource(ConnectionPoolDataSource connPoolDS,
      ConfiguredDataSourceProperties configs) throws SQLException {
    super(configs);
    if ((connPoolDS == null) || (configs == null))
      throw new SQLException(
          "GemFireConnPooledDataSource::ConnectionPoolDataSource class object is null or ConfiguredDataSourceProperties object is null");
    try {
      provider = new GemFireConnectionPoolManager(connPoolDS, configs, this);
    } catch (Exception ex) {
      String exception =
          "An exception was caught while creating a GemFireConnectionPoolManager. %s";
      logger.error(String.format(exception, ex.getLocalizedMessage()), ex);
      throw new SQLException(String.format(exception, ex));
    }
  }

  /**
   * Implementation of datasource interface function. This method is used to get the connection from
   * the pool. Default user name and password will be used.
   *
   * @return ???
   */
  @Override
  public Connection getConnection() throws SQLException {
    if (!isActive) {
      throw new SQLException(
          "GemFireConnPooledDataSource::getConnection::No valid Connection available");
    }
    PooledConnection connPool = null;
    try {
      connPool = (PooledConnection) provider.borrowConnection();
    } catch (PoolException cpe) {
      throw new SQLException(cpe.toString());
    }
    return getSQLConnection(connPool);
  }

  /**
   * Implementation of datasource function. This method is used to get the connection. The specified
   * user name and passowrd will be used.
   *
   * @param clUsername The username for the database connection.
   * @param clPassword The password for the database connection.
   * @return ???
   */
  @Override
  public Connection getConnection(String clUsername, String clPassword) throws SQLException {
    checkCredentials(clUsername, clPassword);
    return getConnection();
  }

  /**
   * Implementation of call back function from ConnectionEventListener interface. This callback will
   * be invoked on connection close event.
   *
   */
  public void connectionClosed(ConnectionEvent event) {
    if (isActive) {
      try {
        PooledConnection conn = (PooledConnection) event.getSource();
        provider.returnConnection(conn);
      } catch (Exception ex) {
        String exception = "GemFireConnPooledDataSource::connectionclosed:Exception =" + ex;
        if (logger.isDebugEnabled()) {
          logger.debug(exception, ex);
        }
      }
    }
  }

  /**
   * Implementation of call back function from ConnectionEventListener interface. This callback will
   * be invoked on connection error event.
   *
   */
  public void connectionErrorOccurred(ConnectionEvent event) {
    if (isActive) {
      try {
        PooledConnection conn = (PooledConnection) event.getSource();
        provider.returnAndExpireConnection(conn);
      } catch (Exception ex) {
        String exception =
            "GemFireConnPooledDataSource::connectionErrorOccurred:error in returning and expiring connection due to "
                + ex;
        if (logger.isDebugEnabled()) {
          logger.debug(exception, ex);
        }
      }
    }
  }

  /**
   * Returns true if the connection is not closed.
   *
   * @param conn Connection object
   * @return boolean True is the connection is alive.
   */
  @Override
  protected boolean validateConnection(Connection conn) {
    try {
      return (!conn.isClosed());
    } catch (SQLException e) {
      return false;
    }
  }

  /**
   * gets tha connection from the pool
   *
   * @return ???
   */
  protected Connection getSQLConnection(PooledConnection poolC) throws SQLException {
    Connection conn = poolC.getConnection();
    boolean val = validateConnection(conn);
    if (val)
      return conn;
    else {
      provider.returnAndExpireConnection(poolC);
      throw new SQLException(
          "GemFireConnPooledDataSource::getConnFromConnPool:java.sql.Connection obtained is invalid");
    }
  }

  /**
   * Returns the connection provider for the datasource.
   *
   * @return ConnectionProvider object for the datasource
   */
  public ConnectionProvider getConnectionProvider() {
    return provider;
  }

  /*
   * Clean up the resources before restart of Cache
   */
  @Override
  public void close() {
    super.close();
    provider.clearUp();
  }
}
