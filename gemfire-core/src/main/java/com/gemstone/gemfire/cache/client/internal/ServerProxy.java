/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

import java.util.List;

import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.distributed.internal.ServerLocation;

/**
 * Used to send operations from a client to a server.
 * @author darrel
 * @since 5.7
 */
public class ServerProxy {
  protected final InternalPool pool;
  /**
   * Creates a server proxy for the given pool.
   * @param pool the pool that this proxy will use to communicate with servers
   */
  public ServerProxy(InternalPool pool) {
    this.pool = pool;
  }
  /**
   * Returns the pool the proxy is using.
   */
  public InternalPool getPool() {
    return this.pool;
  }
  /**
   * Release use of this pool
   */
  public void detach() {
    this.pool.detach();
  }

  /**
   * Ping the specified server to see if it is still alive
   * @param server the server to do the execution on
   */
  public void ping(ServerLocation server) {
    PingOp.execute(this.pool, server);
  }
  /**
   * Does a query on a server
   * @param queryPredicate A query language boolean query predicate
   * @return  A <code>SelectResults</code> containing the values
   *            that match the <code>queryPredicate</code>.
   */
  public SelectResults query(String queryPredicate, Object[] queryParams)
  {
    return QueryOp.execute(this.pool, queryPredicate, queryParams);
  }
  
}
