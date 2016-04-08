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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.jndi.JNDIInvoker;
import com.gemstone.gemfire.internal.jta.TransactionUtils;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * GemFireTransactionDataSource extends AbstractDataSource. This is a datasource
 * class which provides connections from the pool. These connection can
 * participate in the transaction. The objects of these class are
 * ConnectionEventListener for connection close and error events.
 * 
 * Modified the exception handling & changed the name of some
 * functions.
 */
public class GemFireTransactionDataSource extends AbstractDataSource implements 
    ConnectionEventListener {

  private static final Logger logger = LogService.getLogger();
  
  private static final long serialVersionUID = -3095123666092414103L;
  private transient TransactionManager transManager;
  ConnectionProvider provider;
  private Map xaResourcesMap = Collections.synchronizedMap( new HashMap());

  /**
   * Place holder for abstract method 
   * isWrapperFor(java.lang.Class) in java.sql.Wrapper
   * required by jdk 1.6
   *
   * @param iface - a Class defining an interface.
   * @throws SQLException 
   * @return boolean 
   */
   public boolean isWrapperFor(Class iface) throws SQLException {
     return true;
   }

  /**
   * Place holder for abstract method
   * java.lang Object unwrap(java.lang.Class) in java.sql.Wrapper
   * required by jdk 1.6
   * 
   * @param iface - a Class defining an interface.
   * @throws SQLException 
   * @return java.lang.Object 
   */

   public Object unwrap(Class iface)  throws SQLException {
     return iface;
   }

  /**
   * Creates a new instance of GemFireTransactionDataSource
   * 
   * @param xaDS The XADataSource object for the database driver.
   * @param configs - The ConfiguredDataSourceProperties containing the
   *          datasource properties.
   * @throws SQLException
   */
  public GemFireTransactionDataSource(XADataSource xaDS,
      ConfiguredDataSourceProperties configs) throws SQLException {
    super(configs);
    if ((xaDS == null) || (configs == null)) { throw new SQLException(LocalizedStrings.GemFireTransactionDataSource_GEMFIRETRANSACTIONDATASOURCEXADATASOURCE_CLASS_OBJECT_IS_NULL_OR_CONFIGUREDDATASOURCEPROPERTIES_OBJECT_IS_NULL.toLocalizedString()); }
    try {
      provider = new GemFireConnectionPoolManager(xaDS, configs, this);
      transManager = JNDIInvoker.getTransactionManager();
    }
    catch (Exception ex) {
      String exception = "GemFireTransactionDataSource::Exception = " + ex;
      if (logger.isDebugEnabled()) { 
        logger.debug(exception, ex);
      }
      throw new SQLException(exception);
    }
  }

  /**
   * Implementation of datasource function. This method is used to get the
   * connection from the pool. Default user name and password will be used.
   * 
   * @throws SQLException
   * @return ???
   */
  @Override
  public Connection getConnection() throws SQLException {
    if (!isActive) { throw new SQLException(LocalizedStrings.GemFireTransactionDataSource_GEMFIRETRANSACTIONDATASOURCEGETCONNECTIONNO_VALID_CONNECTION_AVAILABLE.toLocalizedString()); }
    XAConnection xaConn = null;
    try {
      xaConn = (XAConnection) provider.borrowConnection();
      Connection conn = getSQLConnection(xaConn);
      registerTranxConnection(xaConn);
      return conn;
    }
    catch (Exception ex) {
      SQLException se = new SQLException(ex.getMessage());
      se.initCause(ex);
      throw se;
    }
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
   * @param event Connection event object
   */
  public void connectionClosed(ConnectionEvent event) {
    if (isActive) {
      try {
        XAConnection conn = (XAConnection) event.getSource();
        XAResource xar = (XAResource) xaResourcesMap.get(conn);
        xaResourcesMap.remove(conn);
        Transaction txn = transManager.getTransaction();
        if (txn != null && xar != null)
            txn.delistResource(xar, XAResource.TMSUCCESS);
        provider.returnConnection(conn);
      }
      catch (Exception e) {
        String exception = "GemFireTransactionDataSource::connectionClosed: Exception occured due to "
            + e;
        if (logger.isDebugEnabled()) { 
          logger.debug(exception, e);
        }
      }
    }
  }

  /**
   * Implementation of call back function from ConnectionEventListener
   * interface. This callback will be invoked on connection error event.
   * 
   * @param event Connection event object
   */
  public void connectionErrorOccurred(ConnectionEvent event) {
    if (isActive) {
      try {
        PooledConnection conn = (PooledConnection) event.getSource();
        provider.returnAndExpireConnection(conn);
      }
      catch (Exception ex) {
        String exception = "GemFireTransactionDataSource::connectionErrorOccured: Exception occured due to "
            + ex;
        if (logger.isDebugEnabled()) { 
          logger.debug(exception, ex);
        }
      }
    }
  }

  /**
   *  
   */
  void registerTranxConnection(XAConnection xaConn) throws Exception
  {
    try {
      synchronized (this) {
        if (transManager == null) {
          transManager = JNDIInvoker.getTransactionManager();
        }
      }
      Transaction txn = transManager.getTransaction();
      if (txn != null) {
        XAResource xar = xaConn.getXAResource();
        txn.enlistResource(xar);
        //Add in the Map after successful registration of XAResource
        this.xaResourcesMap.put(xaConn, xar);        
      }
    }
    catch (Exception ex) {
      Exception e = new Exception(LocalizedStrings.GemFireTransactionDataSource_GEMFIRETRANSACTIONDATASOURCEREGISTERTRANXCONNECTION_EXCEPTION_IN_REGISTERING_THE_XARESOURCE_WITH_THE_TRANSACTIONEXCEPTION_OCCURED_0.toLocalizedString(ex));
      e.initCause(ex);
      throw e;
    }
  }

  /**
   * gets the connection from the pool
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
      throw new SQLException(LocalizedStrings.GemFireTransactionDataSource_GEMFIRECONNPOOLEDDATASOURCEGETCONNFROMCONNPOOLJAVASQLCONNECTION_OBTAINED_IS_INVALID.toLocalizedString());
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
