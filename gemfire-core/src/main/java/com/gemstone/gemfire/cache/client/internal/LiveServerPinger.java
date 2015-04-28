/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.client.internal.EndpointManager.EndpointListenerAdapter;
import com.gemstone.gemfire.cache.client.internal.PoolImpl.PoolTask;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * Responsible for pinging live
 * servers to make sure they
 * are still alive.
 * @author dsmith
 *
 */
public class LiveServerPinger  extends EndpointListenerAdapter {
  private static final Logger logger = LogService.getLogger();
  
  private static final long NANOS_PER_MS = 1000000L;
  
  private final ConcurrentMap/*<Endpoint,Future>*/ taskFutures = new ConcurrentHashMap();
  protected final InternalPool pool;
  protected final long pingIntervalNanos;
  
  public LiveServerPinger(InternalPool pool) {
    this.pool = pool;
    this.pingIntervalNanos = pool.getPingInterval() * NANOS_PER_MS;
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
      Future future = pool.getBackgroundProcessor().scheduleWithFixedDelay(
          new PingTask(endpoint), pingIntervalNanos, pingIntervalNanos,
          TimeUnit.NANOSECONDS);
      taskFutures.put(endpoint, future);
    } catch (RejectedExecutionException e) {
      if (pool.getCancelCriterion().cancelInProgress() == null) {
        throw e;
      }
    }
  }
  
  private void cancelFuture(Endpoint endpoint) {
    Future future = (Future) taskFutures.remove(endpoint);
    if(future != null) {
      future.cancel(false);
    }
  }
  
  private class PingTask extends PoolTask {
    private final Endpoint endpoint;
    
    public PingTask(Endpoint endpoint) {
      this.endpoint = endpoint;
    }

    @Override
    public void run2() {
      if(endpoint.timeToPing(pingIntervalNanos)) {
//      logger.fine("DEBUG pinging " + server);
        try {
          PingOp.execute(pool, endpoint.getLocation());
        } catch(Exception e) {
          if(logger.isDebugEnabled()) {
            logger.debug("Error occured while pinging server: {} - {}", endpoint.getLocation(), e.getMessage());
          }
          GemFireCacheImpl cache = GemFireCacheImpl.getInstance();          
          if (cache != null) {
            ClientMetadataService cms = cache.getClientMetadataService();
            cms.removeBucketServerLocation(endpoint.getLocation());
          }        
          //any failure to ping the server should be considered a crash (eg.
          //socket timeout exception, security exception, failure to connect).
          pool.getEndpointManager().serverCrashed(endpoint);
        }
      } else {
//      logger.fine("DEBUG skipping ping of " + server
//      + " because lastAccessed=" + endpoint.getLastAccessed());
      }
    }
  }
  
}
