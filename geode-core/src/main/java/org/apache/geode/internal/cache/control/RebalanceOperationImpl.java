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
package org.apache.geode.internal.cache.control;

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

import org.apache.geode.GemFireException;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.control.RebalanceOperation;
import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.cache.partition.PartitionRebalanceInfo;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.partitioned.PartitionedRegionRebalanceOp;
import org.apache.geode.internal.cache.partitioned.rebalance.CompositeDirector;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Implements {@code RebalanceOperation} for rebalancing Cache resources.
 */
@SuppressWarnings("synthetic-access")
public class RebalanceOperationImpl implements RebalanceOperation {
  private static final Logger logger = LogService.getLogger();

  private final boolean simulation;
  private final InternalCache cache;
  private final List<Future<RebalanceResults>> futureList =
      new ArrayList<Future<RebalanceResults>>();
  private int pendingTasks;
  private final AtomicBoolean cancelled = new AtomicBoolean();
  private final Object futureLock = new Object();
  private final RegionFilter filter;

  RebalanceOperationImpl(InternalCache cache, boolean simulation, RegionFilter filter) {
    this.simulation = simulation;
    this.cache = cache;
    this.filter = filter;
  }

  public void start() {
    final InternalResourceManager manager = cache.getInternalResourceManager();
    synchronized (futureLock) {
      manager.addInProgressRebalance(this);
      scheduleRebalance();
    }
  }

  private void scheduleRebalance() {
    ResourceManagerStats stats = cache.getInternalResourceManager().getStats();

    long start = stats.startRebalance();
    try {
      for (PartitionedRegion region : cache.getPartitionedRegions()) {
        if (cancelled.get()) {
          break;
        }
        try {
          // Colocated regions will be rebalanced as part of rebalancing their leader
          if (region.getColocatedWith() == null && filter.include(region)) {

            if (region.isFixedPartitionedRegion()) {
              if (Boolean.getBoolean(
                  GeodeGlossary.GEMFIRE_PREFIX + "DISABLE_MOVE_PRIMARIES_ON_STARTUP")) {
                PartitionedRegionRebalanceOp prOp = new PartitionedRegionRebalanceOp(region,
                    simulation, new CompositeDirector(false, false, false, true), true, true,
                    cancelled, stats);
                futureList.add(submitRebalanceTask(prOp, start));
              } else {
                continue;
              }
            } else {
              PartitionedRegionRebalanceOp prOp =
                  new PartitionedRegionRebalanceOp(region, simulation,
                      new CompositeDirector(true, true, true, true), true, true, cancelled, stats);
              futureList.add(submitRebalanceTask(prOp, start));
            }
          }
        } catch (RegionDestroyedException ignore) {
          // ignore, go on to the next region
        }
      }
    } finally {
      if (pendingTasks == 0) {
        // if we didn't submit any tasks, end the rebalance now.
        stats.endRebalance(start);
      }
    }
  }

  private Future<RebalanceResults> submitRebalanceTask(
      final PartitionedRegionRebalanceOp rebalanceOp, final long rebalanceStartTime) {
    final InternalResourceManager manager = cache.getInternalResourceManager();
    ScheduledExecutorService ex = manager.getExecutor();

    synchronized (futureLock) {
      // this update should happen inside this.futureLock
      pendingTasks++;

      try {
        Future<RebalanceResults> future = ex.submit(new Callable<RebalanceResults>() {
          @Override
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
            } catch (RuntimeException e) {
              logger.debug("Unexpected exception in rebalancing: {}", e.getMessage(), e);
              throw e;
            } finally {
              synchronized (futureLock) {
                pendingTasks--;
                if (pendingTasks == 0) {// all threads done
                  manager.removeInProgressRebalance(RebalanceOperationImpl.this);
                  manager.getStats().endRebalance(rebalanceStartTime);
                }
              }
            }
          }
        });

        return future;
      } catch (RejectedExecutionException e) {
        cache.getCancelCriterion().checkCancelInProgress(null);
        throw e;
      }
    }
  }

  private List<Future<RebalanceResults>> getFutureList() {
    synchronized (futureList) {
      return futureList;
    }
  }

  @Override
  public boolean cancel() {
    cancelled.set(true);

    synchronized (futureLock) {
      for (Future<RebalanceResults> fr : getFutureList()) {
        if (fr.cancel(false)) {
          pendingTasks--;
        }
      }
      if (pendingTasks == 0) {
        cache.getInternalResourceManager().removeInProgressRebalance(this);
      }
    }

    return true;
  }

  @Override
  public RebalanceResults getResults() throws CancellationException, InterruptedException {
    RebalanceResultsImpl results = new RebalanceResultsImpl();
    List<Future<RebalanceResults>> frlist = getFutureList();
    for (Future<RebalanceResults> fr : frlist) {
      try {
        RebalanceResults rr = fr.get();
        results.addDetails((RebalanceResultsImpl) rr);

      } catch (ExecutionException e) {
        if (e.getCause() instanceof GemFireException) {
          throw (GemFireException) e.getCause();
        } else if (e.getCause() instanceof InternalGemFireError) {
          throw (InternalGemFireError) e.getCause();
        } else {
          throw new InternalGemFireError(e.getCause());
        }
      }
    }
    return results;
  }

  @Override
  public RebalanceResults getResults(long timeout, TimeUnit unit)
      throws CancellationException, TimeoutException, InterruptedException {
    long endTime = unit.toNanos(timeout) + System.nanoTime();

    RebalanceResultsImpl results = new RebalanceResultsImpl();
    List<Future<RebalanceResults>> frlist = getFutureList();
    for (Future<RebalanceResults> fr : frlist) {
      try {
        long waitTime = endTime - System.nanoTime();
        RebalanceResults rr = fr.get(waitTime, TimeUnit.NANOSECONDS);
        results.addDetails((RebalanceResultsImpl) rr);
      } catch (ExecutionException e) {
        if (e.getCause() instanceof GemFireException) {
          throw (GemFireException) e.getCause();
        } else if (e.getCause() instanceof InternalGemFireError) {
          throw (InternalGemFireError) e.getCause();
        } else {
          throw new InternalGemFireError(e.getCause());
        }
      }
    }
    return results;
  }

  @Override
  public boolean isCancelled() {
    return cancelled.get();
  }

  private boolean isAllDone() {
    for (Future<RebalanceResults> fr : getFutureList()) {
      if (!fr.isDone()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean isDone() {
    return cancelled.get() || isAllDone();
  }

  /**
   * Returns true if this is a simulation.
   *
   * @return true if this is a simulation
   */
  boolean isSimulation() {
    return simulation;
  }
}
