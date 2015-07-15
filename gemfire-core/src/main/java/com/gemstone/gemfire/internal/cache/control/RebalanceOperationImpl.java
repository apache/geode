/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.internal.cache.control;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
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
  private List<Future<RebalanceResults>> futureList = new ArrayList<Future<RebalanceResults>>();
  private int pendingTasks;
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
    synchronized (this.futureLock) {
      manager.addInProgressRebalance(this);
      this.scheduleRebalance();      
    }
  }
  
  private void scheduleRebalance() {
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
            
            if (region.isFixedPartitionedRegion()) {
              if (Boolean.getBoolean("gemfire.DISABLE_MOVE_PRIMARIES_ON_STARTUP")) {
                PartitionedRegionRebalanceOp prOp = new PartitionedRegionRebalanceOp(
                    region, simulation, new CompositeDirector(false, false, false, true), true, true, cancelled,
                    stats);
                this.futureList.add(submitRebalanceTask(prOp,start));
              } else {
                continue;
              }
            } else {
              PartitionedRegionRebalanceOp prOp = new PartitionedRegionRebalanceOp(
                  region, simulation, new CompositeDirector(true, true, true, true), true, true, cancelled,
                  stats);
              this.futureList.add(submitRebalanceTask(prOp,start));
            }            
          }
      } catch(RegionDestroyedException e) {
        //ignore, go on to the next region
      }
    }
    } finally {
      if(pendingTasks == 0) {
        //if we didn't submit any tasks, end the rebalance now.
        stats.endRebalance(start);
      }
    }
  }
  
  private Future<RebalanceResults> submitRebalanceTask(final PartitionedRegionRebalanceOp rebalanceOp, final long rebalanceStartTime) {
    final InternalResourceManager manager = this.cache.getResourceManager();
    ScheduledExecutorService ex = manager.getExecutor();

    synchronized(futureLock) {
      //this update should happen inside this.futureLock 
      pendingTasks++;

      try {
        Future<RebalanceResults> future = ex.submit(new Callable<RebalanceResults>() {
          public RebalanceResults call() {
            try {
              RebalanceResultsImpl results = new RebalanceResultsImpl();
              SystemFailure.checkFailure();
              cache.getCancelCriterion().checkCancelInProgress(null);

              Set<PartitionRebalanceInfo> detailSet = null;

              detailSet = rebalanceOp.execute();

              for (PartitionRebalanceInfo details : detailSet) {
                results.addDetails(details);
              }
              return results;
            }
            catch (RuntimeException e) {
              logger.debug("Unexpected exception in rebalancing: {}", e.getMessage(), e);
              throw e;
            } finally {
              synchronized (RebalanceOperationImpl.this.futureLock) {
                pendingTasks--;
                if(pendingTasks == 0) {//all threads done
                  manager.removeInProgressRebalance(RebalanceOperationImpl.this);
                  manager.getStats().endRebalance(rebalanceStartTime);
                }
              }
            }
          }
        });

        return future;
      } catch(RejectedExecutionException e) {
        cache.getCancelCriterion().checkCancelInProgress(null);
        throw e;
      }
    }
  }
  
  private List<Future<RebalanceResults>> getFutureList() {
    synchronized(this.futureList) {
      return this.futureList;
    }
  }
  
  public boolean cancel() {
    cancelled.set(true);
    
    synchronized (this.futureLock) {
      for(Future<RebalanceResults> fr : getFutureList()) {
        if(fr.cancel(false)) {
          pendingTasks--;
        }
      }
      if(pendingTasks == 0 ) {
        cache.getResourceManager().removeInProgressRebalance(this);
      }
    }
    
    return true;
  }

  public RebalanceResults getResults() throws CancellationException, InterruptedException {
    RebalanceResultsImpl results = new RebalanceResultsImpl();
    List<Future<RebalanceResults>> frlist =  getFutureList();
    for(Future<RebalanceResults> fr : frlist) {
      try {
        RebalanceResults rr =  fr.get();
        results.addDetails((RebalanceResultsImpl)rr);
        
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
    return results;
  }

  public RebalanceResults getResults(long timeout, TimeUnit unit)
      throws CancellationException, TimeoutException, InterruptedException {
    long endTime = unit.toNanos(timeout) + System.nanoTime();
    
    RebalanceResultsImpl results = new RebalanceResultsImpl();
    List<Future<RebalanceResults>> frlist =  getFutureList();
    for(Future<RebalanceResults> fr : frlist) {
      try {
        long waitTime = endTime - System.nanoTime();
        RebalanceResults rr =  fr.get(waitTime, TimeUnit.NANOSECONDS);                
        results.addDetails((RebalanceResultsImpl)rr);
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
    return results;
  }

  public boolean isCancelled() {
    return this.cancelled.get();
  }

  private boolean isAllDone() {
    for(Future<RebalanceResults> fr : getFutureList()) {
      if(!fr.isDone())
        return false;
    }
    return true;
  }
  
  public boolean isDone() {
    return this.cancelled.get() || isAllDone();
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
