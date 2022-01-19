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
package org.apache.geode.cache.client.internal;


import org.apache.geode.cache.query.SelectResults;

/**
 * Used to send operations from a client to a server.
 *
 * @since GemFire 5.7
 */
public class ServerProxy {
  protected final InternalPool pool;

  /**
   * Creates a server proxy for the given pool.
   *
   * @param pool the pool that this proxy will use to communicate with servers
   */
  public ServerProxy(InternalPool pool) {
    this.pool = pool;
  }

  /**
   * Returns the pool the proxy is using.
   */
  public InternalPool getPool() {
    return pool;
  }

  /**
   * Release use of this pool
   */
  public void detach() {
    pool.detach();
  }

  /**
   * Does a query on a server
   *
   * @param queryPredicate A query language boolean query predicate
   * @return A <code>SelectResults</code> containing the values that match the
   *         <code>queryPredicate</code>.
   */
  public SelectResults query(String queryPredicate, Object[] queryParams) {
    return QueryOp.execute(pool, queryPredicate, queryParams);
  }

}
