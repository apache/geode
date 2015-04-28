/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.internal.cache.control;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.GemFireException;
import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.control.RebalanceOperation;
import com.gemstone.gemfire.cache.control.RebalanceResults;
import com.gemstone.gemfire.cache.partition.PartitionRebalanceInfo;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.partitioned.PartitionedRegionRebalanceOp;
import com.gemstone.gemfire.internal.cache.partitioned.rebalance.CompositeDirector;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * Implements <code>RebalanceOperation</code> for rebalancing Cache resources.
 * 
 * @author Kirk Lund
 */
@SuppressWarnings("synthetic-access")
public class RebalanceOperationImpl implements RebalanceOperation {

  private static final Logger logger = LogService.getLogger();
  
  private final boolean simulation;
  private final GemFireCacheImpl cache;
  private Future<RebalanceResults> future;
  private final AtomicBoolean cancelled = new AtomicBoolean();
  private final Object futureLock = new Object();
  private RegionFilter filter;
  
  RebalanceOperationImpl(GemFireCacheImpl cache, boolean simulation,
      RegionFilter filter) {
    this.simulation = simulation;
    this.cache = cache;
    this.filter = filter;
  }
    
  public void start() {
    final InternalResourceManager manager = this.cache.getResourceManager();
    ScheduledExecutorService ex = manager.getExecutor();
    synchronized (this.futureLock) {
      manager.addInProgressRebalance(this);
      future = ex.submit(new Callable<RebalanceResults>() {
        public RebalanceResults call() {
          SystemFailure.checkFailure();
          cache.getCancelCriterion().checkCancelInProgress(null);
          try {
            return RebalanceOperationImpl.this.call();
          }
          catch (RuntimeException e) {
            logger.debug("Unexpected exception in rebalancing: {}", e.getMessage(), e);
            throw e;
          } finally {
            manager.removeInProgressRebalance(RebalanceOperationImpl.this);
          }
        }
      });
    }
  }
  
  private RebalanceResults call() {
    RebalanceResultsImpl results = new RebalanceResultsImpl();
    ResourceManagerStats stats = cache.getResourceManager().getStats();
    long start = stats.startRebalance();
    try {
    for(PartitionedRegion region: cache.getPartitionedRegions()) {
      if(cancelled.get()) {
        break;
      }
      try {
        //Colocated regions will be rebalanced as part of rebalancing their leader
          if (region.getColocatedWith() == null && filter.include(region)) {
            
            Set<PartitionRebalanceInfo> detailSet = null;
            if (region.isFixedPartitionedRegion()) {
              if (Boolean.getBoolean("gemfire.DISABLE_MOVE_PRIMARIES_ON_STARTUP")) {
                PartitionedRegionRebalanceOp prOp = new PartitionedRegionRebalanceOp(
                    region, simulation, new CompositeDirector(false, false, false, true), true, true, cancelled,
                    stats);
                detailSet = prOp.execute();
              } else {
                continue;
              }
            } else {
              PartitionedRegionRebalanceOp prOp = new PartitionedRegionRebalanceOp(
                  region, simulation, new CompositeDirector(true, true, true, true), true, true, cancelled,
                  stats);
              detailSet = prOp.execute();
            }
            for (PartitionRebalanceInfo details : detailSet) {
              results.addDetails(details);
            }
          }
      } catch(RegionDestroyedException e) {
        //ignore, go on to the next region
      }
    }
    } finally {
      stats.endRebalance(start);
    }
    return results;
  }
  
  private Future<RebalanceResults> getFuture() {
    synchronized (this.futureLock) {
      return this.future;
    }
  }
  
  public boolean cancel() {
    cancelled.set(true);
    if(getFuture().cancel(false)) {
      cache.getResourceManager().removeInProgressRebalance(this);
    }
    return true;
  }

  public RebalanceResults getResults() throws CancellationException, InterruptedException {
      try {
        return getFuture().get();
      } catch (ExecutionException e) {
        if(e.getCause() instanceof GemFireException) {
          throw (GemFireException) e.getCause();
        } else if(e.getCause() instanceof InternalGemFireError) {
          throw (InternalGemFireError) e.getCause();
        } else {
          throw new InternalGemFireError(e.getCause());
        }
      }
  }

  public RebalanceResults getResults(long timeout, TimeUnit unit)
      throws CancellationException, TimeoutException, InterruptedException {
    try {
      return getFuture().get(timeout, unit);
    } catch (ExecutionException e) {
      if(e.getCause() instanceof GemFireException) {
        throw (GemFireException) e.getCause();
      } else if(e.getCause() instanceof InternalGemFireError) {
        throw (InternalGemFireError) e.getCause();
      } else {
        throw new InternalGemFireError(e.getCause());
      }
    }
  }

  public boolean isCancelled() {
    return this.cancelled.get();
  }

  public boolean isDone() {
    return this.cancelled.get() || getFuture().isDone();
  }
  
  /**
   * Returns true if this is a simulation.
   * 
   * @return true if this is a simulation
   */
  boolean isSimulation() {
    return this.simulation;
  }
}
