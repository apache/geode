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
package org.apache.geode.internal.datasource;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.i18n.StringId;

/**
 * GemFireTransactionDataSource extends AbstractDataSource. This is a datasource
 * class which provides connections from the pool. The objects of these class
 * are ConnectionEventListener for connection close and error events.
 * 
 * Modified Exception handling & changed name of the function
 */
public class GemFireConnPooledDataSource extends AbstractDataSource implements 
    javax.sql.ConnectionEventListener {

  private static final Logger logger = LogService.getLogger();
  
  private static final long serialVersionUID = 1177231744410855158L;
  protected ConnectionProvider provider;

  /**
   * Creates a new instance of GemFireConnPooledDataSource
   * 
   * @param connPoolDS The ConnectionPoolDataSource object for the database
   *          driver.
   * @param configs The ConfiguredDataSourceProperties containing the datasource
   *          properties.
   * @throws SQLException
   */
  

 /**
   * Place holder for abstract method 
   * isWrapperFor(java.lang.Class) in java.sql.Wrapper
   * required by jdk 1.6
   *
   * @param iface - a Class defining an interface.
   * @throws SQLException 
   */
   public boolean isWrapperFor(Class iface) throws SQLException {
     return true;
   }

   public Object unwrap(Class iface)  throws SQLException {
     return iface;
   }
  



 public GemFireConnPooledDataSource(ConnectionPoolDataSource connPoolDS,
      ConfiguredDataSourceProperties configs) throws SQLException {
    super(configs);
    if ((connPoolDS == null) || (configs == null))
        throw new SQLException(LocalizedStrings.GemFireConnPooledDataSource_GEMFIRECONNPOOLEDDATASOURCECONNECTIONPOOLDATASOURCE_CLASS_OBJECT_IS_NULL_OR_CONFIGUREDDATASOURCEPROPERTIES_OBJECT_IS_NULL.toLocalizedString());
    try {
      provider = new GemFireConnectionPoolManager(connPoolDS, configs, this);
    }
    catch (Exception ex) {
      StringId exception = LocalizedStrings.GemFireConnPooledDataSource_EXCEPTION_CREATING_GEMFIRECONNECTIONPOOLMANAGER; 
      logger.error(LocalizedMessage.create(exception, ex.getLocalizedMessage()), ex);
      throw new SQLException(exception.toLocalizedString(ex));
    }
  }

  /**
   * Implementation of datasource interface function. This method is used to get
   * the connection from the pool. Default user name and password will be used.
   * 
   * @throws SQLException
   * @return ???
   */
 @Override
  public Connection getConnection() throws SQLException {
    if (!isActive) { throw new SQLException(LocalizedStrings.GemFireConnPooledDataSource_GEMFIRECONNPOOLEDDATASOURCEGETCONNECTIONNO_VALID_CONNECTION_AVAILABLE.toLocalizedString()); }
    PooledConnection connPool = null;
    try {
      connPool = (PooledConnection) provider.borrowConnection();
    }
    catch (PoolException cpe) {
      throw new SQLException(cpe.toString());
    }
    return getSQLConnection(connPool);
  }

  /**
   * Implementation of datasource function. This method is used to get the
   * connection. The specified user name and passowrd will be used.
   * 
   * @param clUsername The username for the database connection.
   * @param clPassword The password for the database connection.
   * @throws SQLException
   * @return ???
   */
 @Override
  public Connection getConnection(String clUsername, String clPassword)
      throws SQLException {
    checkCredentials(clUsername, clPassword);
    return getConnection();
  }

  /**
   * Implementation of call back function from ConnectionEventListener
   * interface. This callback will be invoked on connection close event.
   * 
   * @param event
   */
  public void connectionClosed(ConnectionEvent event) {
    if (isActive) {
      try {
        PooledConnection conn = (PooledConnection) event.getSource();
        provider.returnConnection(conn);
      }
      catch (Exception ex) {
        String exception = "GemFireConnPooledDataSource::connectionclosed:Exception ="
            + ex;
        if (logger.isDebugEnabled()) {
          logger.debug(exception, ex);
        }
      }
    }
  }

  /**
   * Implementation of call back function from ConnectionEventListener
   * interface. This callback will be invoked on connection error event.
   * 
   * @param event
   */
  public void connectionErrorOccurred(ConnectionEvent event) {
    if (isActive) {
      try {
        PooledConnection conn = (PooledConnection) event.getSource();
        provider.returnAndExpireConnection(conn);
      }
      catch (Exception ex) {
        String exception = "GemFireConnPooledDataSource::connectionErrorOccured:error in returning and expiring connection due to "
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
    }
    catch (SQLException e) {
      return false;
    }
  }

  /**
   * gets tha connection from the pool
   * 
   * @param poolC
   * @return ???
   */
  protected Connection getSQLConnection(PooledConnection poolC)
      throws SQLException {
    Connection conn = poolC.getConnection();
    boolean val = validateConnection(conn);
    if (val)
      return conn;
    else {
      provider.returnAndExpireConnection(poolC);
      throw new SQLException(LocalizedStrings.GemFireConnPooledDataSource_GEMFIRECONNPOOLEDDATASOURCEGETCONNFROMCONNPOOLJAVASQLCONNECTION_OBTAINED_IS_INVALID.toLocalizedString());
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
  public void clearUp() {
    super.clearUp();
    provider.clearUp();
  }
}
