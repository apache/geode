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

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.geode.CancelCriterion;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.internal.cache.PoolStats;

/**
 * The contract between a connection source and a connection pool. Provides methods for the
 * connection source to access the cache and update the list of endpoints on the connection pool.
 *
 * @since GemFire 5.7
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

  boolean getKeepAlive();

  /**
   * Test hook that returns the port of the primary server. -1 is returned if we have no primary.
   */
  @VisibleForTesting
  int getPrimaryPort();

  /**
   * Test hook to find out current number of connections this pool has.
   */
  @VisibleForTesting
  int getConnectionCount();
}
