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
import java.io.Serializable;
import javax.sql.*;

/**
 * GemFireConnectionPoolManager implements ConnectionProvider interface for
 * managing the the conection pools(with and wothout transactions).
 * 
 * @author tnegi
 */
public class GemFireConnectionPoolManager implements ConnectionProvider,
    Serializable {
  private static final long serialVersionUID = 23723212980453813L;
  protected ConnectionPoolCache connPoolCache;

  /**
   * Creates a new instance of GemFireConnectionPoolManager
   * 
   * @param connPool ConnectionPoolDataSource object from the database driver
   * @param configs The ConfiguredDataSourceProperties containing the pool
   *          properties.
   * @param listener Connection event listner for the connections.
   */
  public GemFireConnectionPoolManager(ConnectionPoolDataSource connPool,
      ConfiguredDataSourceProperties configs,
      javax.sql.ConnectionEventListener listener) throws PoolException {
    connPoolCache = new ConnectionPoolCacheImpl(connPool, listener, configs);
  }

  /**
   * Creates a new instance of GemFireConnectionPoolManager. Overloaded function
   * 
   * @param connPool XADataSource object from the database driver
   * @param configs The ConfiguredDataSourceProperties containing the pool
   *          properties.
   * @param listener Connection event listner for the connections.
   */
  public GemFireConnectionPoolManager(XADataSource connPool,
      ConfiguredDataSourceProperties configs,
      javax.sql.ConnectionEventListener listener) throws PoolException {
    connPoolCache = new TranxPoolCacheImpl(connPool, listener, configs);
  }

  /**
   * Returns a PooledConnection object from the pool. Default username and
   * password are used.
   * 
   * @return Connection Object from Pool.
   */
  public Object borrowConnection() throws PoolException {
    return connPoolCache.getPooledConnectionFromPool();
  }

  /**
   * Returns the connection to the pool and the closes it.
   * 
   * @param connectionObject
   *  
   */
  public void returnAndExpireConnection(Object connectionObject) {
    //Asif : The connection is already in the activeCache, but the
    //sql.Connecttion object is not valid , so this PooledConnection
    // needs to be destroyed. We should just change the timestamp
    // associated with the PooledObject such that it will be picked
    // up by the cleaner thread.
    connPoolCache.expirePooledConnection(connectionObject);
  }

  /**
   * Return connection to pool
   * 
   * @param connectionObject
   */
  public void returnConnection(Object connectionObject) {
    connPoolCache.returnPooledConnectionToPool(connectionObject);
  }

  public ConnectionPoolCache getConnectionPoolCache() {
    return connPoolCache;
  }

  /**
   * Clean up the resources before restart of Cache
   */
  public void clearUp() {
    connPoolCache.clearUp();
  }
}
