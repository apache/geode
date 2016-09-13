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

/**
 */
import java.io.Serializable;
import javax.sql.*;

/**
 * GemFireConnectionPoolManager implements ConnectionProvider interface for
 * managing the the conection pools(with and wothout transactions).
 * 
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
