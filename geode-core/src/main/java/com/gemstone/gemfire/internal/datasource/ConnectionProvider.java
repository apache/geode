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
 * This interface outlines the behavior of a Connection provider.
 * 
 */
public interface ConnectionProvider {

  /**
   * Provides a PooledConnection from the connection pool. Default user and
   * password are used.
   * 
   * @return a PooledConnection object to the user.
   */
  public Object borrowConnection() throws PoolException;

  /**
   * Returns a PooledConnection object to the pool.
   * 
   * @param connectionObject to be returned to the pool
   */
  public void returnConnection(Object connectionObject);

  /**
   * Closes a PooledConnection object .
   */
  public void returnAndExpireConnection(Object connectionObject);

  /**
   * Clean up the resources before restart of Cache
   */
  public void clearUp();
}
