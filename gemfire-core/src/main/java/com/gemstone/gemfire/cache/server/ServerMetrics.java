/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.server;


/**
 * Metrics about the resource usage for a cache server.
 * These metrics are provided to the {@link ServerLoadProbe} for
 * use in calculating the load on the server.
 * @author dsmith
 * @since 5.7
 *
 */
public interface ServerMetrics {
  /**
   * Get the number of open connections
   * for this cache server.
   */
  int getConnectionCount();
  
  /** Get the number of clients connected to this
   * cache server.
   */ 
  int getClientCount();
  
  /**
   * Get the number of client subscription connections hosted on this
   * cache server.
   */
  int getSubscriptionConnectionCount();
  
  /**
   * Get the max connections for this cache server.
   */
  int getMaxConnections();
  
  //TODO grid - Queue sizes, server group counts,
  //CPU Usage, stats, etc.
}
