/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

import java.util.Map;

import java.util.concurrent.ScheduledExecutorService;
import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.internal.cache.PoolStats;

/**
 * The contract between a connection source and a connection pool.
 * Provides methods for the connection source to access the cache
 * and update the list of endpoints on the connection pool.
 * @author dsmith
 * @since 5.7
 *
 */
public interface InternalPool extends Pool, ExecutablePool {
  PoolStats getStats();
  Map getEndpointMap();
  EndpointManager getEndpointManager();
  ScheduledExecutorService getBackgroundProcessor();
  CancelCriterion getCancelCriterion();
  boolean isDurableClient();
  void detach();
  String getPoolOrCacheCancelInProgress();
}
