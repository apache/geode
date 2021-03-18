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
package org.apache.geode.internal.cache.partitioned.colocation;

import static java.lang.System.lineSeparator;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.apache.geode.internal.cache.ColocationHelper.getAllColocationRegions;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.PRHARedundancyProvider;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Provides logging when regions are missing from a colocation hierarchy. This logger runs in it's
 * own thread and waits for child regions to be created before logging them as missing.
 */
public class SingleThreadColocationLogger implements ColocationLogger {
  private static final Logger LOGGER = LogService.getLogger(ColocationLogger.class);

  private final List<String> missingChildren = new ArrayList<>();
  private final AtomicReference<Future<?>> completed = new AtomicReference<>();
  private final Object lock = new Object();

  private final PartitionedRegion region;
  private final long delayMillis;
  private final long intervalMillis;
  private final Consumer<String> logger;
  private final Function<PartitionedRegion, Set<String>> allColocationRegionsProvider;
  private final ExecutorService executorService;

  /**
   * @param region the region that owns this logger instance
   */
  SingleThreadColocationLogger(PartitionedRegion region, long delayMillis, long intervalMillis) {
    this(region, delayMillis, intervalMillis, LOGGER::warn,
        pr -> getAllColocationRegions(pr).keySet(),
        newSingleThreadExecutor(
            runnable -> new LoggingThread("ColocationLogger for " + region.getName(), true,
                runnable)));
  }

  @VisibleForTesting
  public SingleThreadColocationLogger(PartitionedRegion region, long delayMillis,
      long intervalMillis, Consumer<String> logger,
      Function<PartitionedRegion, Set<String>> allColocationRegionsProvider,
      ExecutorService executorService) {
    this.region = region;
    this.delayMillis = delayMillis;
    this.intervalMillis = intervalMillis;
    this.logger = logger;
    this.allColocationRegionsProvider = allColocationRegionsProvider;
    this.executorService = executorService;
  }

  @Override
  public ColocationLogger start() {
    synchronized (lock) {
      if (completed.get() != null) {
        throw new IllegalStateException(this + " is already running");
      }
      completed.set(executorService.submit(checkForMissingColocatedRegionRunnable()));
      return this;
    }
  }

  @Override
  public void stop() {
    synchronized (lock) {
      missingChildren.clear();
      executorService.shutdownNow();
      lock.notifyAll();
    }
  }

  @Override
  public void addMissingChildRegion(String childFullPath) {
    synchronized (lock) {
      if (!missingChildren.contains(childFullPath)) {
        missingChildren.add(childFullPath);
      }
    }
  }

  @Override
  public void addMissingChildRegions(PartitionedRegion childRegion) {
    synchronized (lock) {
      List<String> missingDescendants = childRegion.getMissingColocatedChildren();
      for (String name : missingDescendants) {
        addMissingChildRegion(name);
      }
    }
  }

  /**
   * Updates the missing colocated child region list and returns a copy of the list.
   *
   * <p>
   * The list of missing child regions is normally updated lazily, only when this logger thread
   * periodically wakes up to log warnings about the colocated regions that are still missing. This
   * method performs an on-demand update of the list so if called between logging intervals the
   * returned list is current.
   */
  @Override
  public List<String> updateAndGetMissingChildRegions() {
    synchronized (lock) {
      Set<String> childRegions = allColocationRegionsProvider.apply(region);
      missingChildren.removeAll(childRegions);
    }
    return new ArrayList<>(missingChildren);
  }

  @Override
  public String toString() {
    return "ColocationLogger for " + region.getName();
  }

  @VisibleForTesting
  Future<?> getFuture() {
    return completed.get();
  }

  @VisibleForTesting
  List<String> getMissingChildren() {
    return new ArrayList<>(missingChildren);
  }

  @VisibleForTesting
  ExecutorService getExecutorService() {
    return executorService;
  }

  private Runnable checkForMissingColocatedRegionRunnable() {
    return this::checkForMissingColocatedRegion;
  }

  private void checkForMissingColocatedRegion() {
    DistributedSystem.setThreadsSocketPolicy(true /* conserve sockets */);
    SystemFailure.checkFailure();
    CancelCriterion cancelCriterion = region.getSystem().getCancelCriterion();
    if (cancelCriterion.isCancelInProgress()) {
      return;
    }
    try {
      checkForMissingColocatedRegion(cancelCriterion);
    } catch (VirtualMachineError error) {
      SystemFailure.initiateFailure(error);
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw error;
    } catch (Throwable t) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above). However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Unexpected exception in colocation", t);
      }
    }
  }

  /**
   * Writes a log entry every SLEEP_PERIOD when there are missing colocated child regions for this
   * region.
   */
  private void checkForMissingColocatedRegion(CancelCriterion cancelCriterion)
      throws InterruptedException {
    synchronized (lock) {
      boolean firstLogIteration = true;
      while (true) {
        long waitMillis = firstLogIteration ? delayMillis : intervalMillis;
        // delay for first log message is half of the interval between subsequent log messages
        if (firstLogIteration) {
          firstLogIteration = false;
        }

        lock.wait(waitMillis);

        PRHARedundancyProvider redundancyProvider = region.getRedundancyProvider();
        if (redundancyProvider != null && redundancyProvider.isPersistentRecoveryComplete()) {
          // Terminate the logging thread
          // isPersistentRecoveryComplete is true only when there are no missing colocated regions
          break;
        }
        if (missingChildren.isEmpty()) {
          break;
        }
        if (cancelCriterion.isCancelInProgress()) {
          break;
        }

        logMissingRegions(region.getFullPath());
      }
    }
  }

  /**
   * Write the a logger warning for a PR that has colocated child regions that are missing.
   *
   * @param regionPath the parent region that has missing child regions
   */
  private void logMissingRegions(String regionPath) {
    String namesOfMissingChildren =
        missingChildren.isEmpty() ? "" : String.join(lineSeparator() + '\t', missingChildren);

    logger.accept(String.format(
        "Persistent data recovery for region %s is prevented by offline colocated %s%s%s",
        regionPath,
        missingChildren.size() > 1 ? "regions" : "region",
        lineSeparator() + '\t',
        namesOfMissingChildren));
  }
}
