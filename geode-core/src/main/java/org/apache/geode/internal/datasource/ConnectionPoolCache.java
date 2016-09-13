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
 * This interface outlines the behavior of a connection pool.
 * 
 */
public interface ConnectionPoolCache {

  /**
   * This method is used to get the connection from the pool. The default
   * username and password are used for making the conections.
   * 
   * @return Object - connection from the pool.
   * @throws PoolException
   */
  public Object getPooledConnectionFromPool() throws PoolException;

  /**
   * This method will return the Pooled connection object back to the pool.
   * 
   * @param connectionObject - Connection object returned to the pool.
   */
  public void returnPooledConnectionToPool(Object connectionObject);

  /**
   * This method is used to set the time out for active connection so that it
   * can be collected by the cleaner thread Modified by Asif
   */
  public void expirePooledConnection(Object connectionObject);

  /**
   * Clean up the resources before restart of Cache
   */
  public void clearUp();
}
