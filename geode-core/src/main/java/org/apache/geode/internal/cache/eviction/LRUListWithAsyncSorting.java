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
package org.apache.geode.internal.cache.eviction;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.RegionEntryContext;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.lang.SystemPropertyHelper;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LoggingExecutors;
import org.apache.geode.internal.logging.log4j.LogMarker;

/**
 * LRUListWithAsyncSorting holds the eviction list, and the behavior for maintaining the list and
 * determining the next entry to be removed. Each EntriesMap that supports LRU holds one of these.
 * Evicts are always done from the head and assume that it is the least recent entry unless if is
 * being used by a transaction or is already evicted in which case it is removed from the list and
 * the next item is evicted. Adds are always done to the end of the list and should not be marked
 * recently used. An async scanner runs periodically (how often TBD), head to tail, removing entries
 * that have been recently used, marking them as not recently used, and adding them back to the
 * tail. Removes may unlink entries from the list.
 */
public class LRUListWithAsyncSorting extends AbstractEvictionList {

  private static final Logger logger = LogService.getLogger();

  private static final Optional<Integer> EVICTION_SCAN_MAX_THREADS = SystemPropertyHelper
      .getProductIntegerProperty(SystemPropertyHelper.EVICTION_SCAN_MAX_THREADS);

  private static final ExecutorService SINGLETON_EXECUTOR = createExecutor();

  private static final int DEFAULT_EVICTION_SCAN_THRESHOLD_PERCENT = 25;

  private static final int DEFAULT_MAX_EVICTION_ATTEMPTS = 10;

  private final AtomicInteger recentlyUsedCounter = new AtomicInteger();

  private final double scanThreshold;
  private final int maxEvictionAttempts;

  private Future<?> currentScan;

  private final ExecutorService executor;

  private static ExecutorService createExecutor() {
    int threads = EVICTION_SCAN_MAX_THREADS.orElse(0);
    if (threads < 1) {
      threads = Math.max((Runtime.getRuntime().availableProcessors() / 4), 1);
    }
    return LoggingExecutors.newFixedThreadPool("LRUListWithAsyncSortingThread", true, threads);
  }

  LRUListWithAsyncSorting(EvictionController controller) {
    this(controller, SINGLETON_EXECUTOR, DEFAULT_MAX_EVICTION_ATTEMPTS);
  }

  LRUListWithAsyncSorting(EvictionController controller, ExecutorService executor,
      int maxEvictionAttempts) {
    super(controller);
    this.scanThreshold = calculateScanThreshold();
    this.executor = executor;
    this.maxEvictionAttempts = maxEvictionAttempts;
  }

  private double calculateScanThreshold() {
    Optional<Integer> configuredThresholdPercent = SystemPropertyHelper
        .getProductIntegerProperty(SystemPropertyHelper.EVICTION_SCAN_THRESHOLD_PERCENT);

    int thresholdPercent =
        configuredThresholdPercent.orElse(DEFAULT_EVICTION_SCAN_THRESHOLD_PERCENT);
    if (thresholdPercent < 0 || thresholdPercent > 100) {
      thresholdPercent = DEFAULT_EVICTION_SCAN_THRESHOLD_PERCENT;
    }

    return (double) thresholdPercent / 100;
  }

  @Override
  public void clear(RegionVersionVector regionVersionVector, BucketRegion bucketRegion) {
    super.clear(regionVersionVector, bucketRegion);
    recentlyUsedCounter.set(0);
  }

  /**
   * Remove and return the Entry that is considered least recently used.
   */
  @Override
  public EvictableEntry getEvictableEntry() {
    int evictionAttempts = 0;
    for (;;) {
      final EvictionNode evictionNode = unlinkHeadEntry();

      if (evictionNode == null) {
        // hit the end of the list
        return null;
      }

      if (logger.isTraceEnabled(LogMarker.LRU_CLOCK_VERBOSE)) {
        logger.trace(LogMarker.LRU_CLOCK_VERBOSE, "lru considering {}", evictionNode);
      }

      if (!isEvictable(evictionNode)) {
        continue;
      }

      if (evictionNode.isRecentlyUsed() && evictionAttempts < maxEvictionAttempts) {
        evictionAttempts++;
        evictionNode.unsetRecentlyUsed();
        appendEntry(evictionNode);
        continue;
      }

      if (logger.isTraceEnabled(LogMarker.LRU_CLOCK_VERBOSE)) {
        logger.trace(LogMarker.LRU_CLOCK_VERBOSE, "returning unused entry: {}", evictionNode);
      }
      if (evictionNode.isRecentlyUsed()) {
        scanIfNeeded();
        getStatistics().incGreedyReturns(1);
      }
      return (EvictableEntry) evictionNode;
    }
  }

  private synchronized void scanIfNeeded() {
    if (!scanInProgress()) {
      recentlyUsedCounter.set(0);
      currentScan = executor.submit(this::scan);
    }
  }


  /**
   * Determine who/when should invoke scan. Maybe when 10% of the RegionEntries have been dirtied by
   * {@link RegionEntry#setRecentlyUsed(RegionEntryContext)}.
   *
   * Determine when to stop scanning.
   */
  void scan() {
    EvictionNode evictionNode;
    do {
      synchronized (this) {
        evictionNode = head.next();
      }
      int nodesToIterate = size();
      while (evictionNode != null && evictionNode != tail && nodesToIterate > 0) {
        nodesToIterate--;
        // No need to sync on evictionNode here. If the bit is set the only one to clear it is
        // us (i.e. the scan) or evict/remove code. If either of these happen then this will be
        // detected by next and previous being null.
        if (evictionNode.isRecentlyUsed()) {
          evictionNode.unsetRecentlyUsed();
          evictionNode = moveToTailAndGetNext(evictionNode);
        } else {
          synchronized (this) {
            evictionNode = evictionNode.next();
          }
        }
      }
      // null indicates we tried to scan past a node that was concurrently removed.
      // In that case we need to start at the beginning.
    } while (evictionNode == null);
  }

  @Override
  public void incrementRecentlyUsed() {
    int recentlyUsedCount = recentlyUsedCounter.incrementAndGet();
    if (hasThresholdBeenMet(recentlyUsedCount)) {
      scanIfNeeded();
    }
  }

  int getRecentlyUsedCount() {
    return recentlyUsedCounter.get();
  }

  private boolean scanInProgress() {
    return currentScan != null && !currentScan.isDone();
  }

  private boolean hasThresholdBeenMet(int recentlyUsedCount) {
    return size() >= maxEvictionAttempts
        && (double) recentlyUsedCount / size() >= this.scanThreshold;
  }

  private synchronized EvictionNode moveToTailAndGetNext(EvictionNode evictionNode) {
    EvictionNode next = evictionNode.next();
    if (next != null && next != tail) {
      EvictionNode previous = evictionNode.previous();
      next.setPrevious(previous);
      previous.setNext(next);
      evictionNode.setNext(tail);
      tail.previous().setNext(evictionNode);
      evictionNode.setPrevious(tail.previous());
      tail.setPrevious(evictionNode);
    }
    return next;
  }
}
