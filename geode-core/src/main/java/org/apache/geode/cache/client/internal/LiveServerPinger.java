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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.client.internal.EndpointManager.EndpointListenerAdapter;
import org.apache.geode.cache.client.internal.PoolImpl.PoolTask;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Responsible for pinging live servers to make sure they are still alive.
 *
 */
public class LiveServerPinger extends EndpointListenerAdapter {
  private static final Logger logger = LogService.getLogger();

  private static final long NANOS_PER_MS = 1000000L;

  private final ConcurrentMap<Endpoint, Future> taskFutures = new ConcurrentHashMap<>();
  protected final InternalPool pool;
  protected final long pingIntervalNanos;

  public LiveServerPinger(InternalPool pool) {
    this.pool = pool;
    pingIntervalNanos = ((pool.getPingInterval() + 1) / 2) * NANOS_PER_MS;
  }

  @Override
  public void endpointCrashed(Endpoint endpoint) {
    cancelFuture(endpoint);
  }

  @Override
  public void endpointNoLongerInUse(Endpoint endpoint) {
    cancelFuture(endpoint);
  }

  @Override
  public void endpointNowInUse(Endpoint endpoint) {
    try {
      Future future = pool.getBackgroundProcessor().scheduleWithFixedDelay(new PingTask(endpoint),
          pingIntervalNanos, pingIntervalNanos, TimeUnit.NANOSECONDS);
      taskFutures.put(endpoint, future);
    } catch (RejectedExecutionException e) {
      if (!pool.getCancelCriterion().isCancelInProgress()) {
        throw e;
      }
    }
  }

  private void cancelFuture(Endpoint endpoint) {
    Future future = taskFutures.remove(endpoint);
    if (future != null) {
      future.cancel(false);
    }
  }

  private class PingTask extends PoolTask {
    private final Endpoint endpoint;

    PingTask(Endpoint endpoint) {
      this.endpoint = endpoint;
    }

    @Override
    public void run2() {
      if (endpoint.timeToPing(pingIntervalNanos)) {
        try {
          endpoint.updateLastExecute();
          PingOp.execute(pool, endpoint.getLocation(), endpoint.getMemberId());
        } catch (Exception e) {
          if (logger.isDebugEnabled()) {
            logger.debug("Error occurred while pinging server: {} - {}", endpoint.getLocation(),
                e.getMessage());
          }
          InternalCache cache = GemFireCacheImpl.getInstance();
          if (cache != null) {
            ClientMetadataService cms = cache.getClientMetadataService();
            cms.removeBucketServerLocation(endpoint.getLocation());
          }
          // any failure to ping the server should be considered a crash (eg.
          // socket timeout exception, security exception, failure to connect).
          pool.getEndpointManager().serverCrashed(endpoint);
        }
      }
    }
  }

}
