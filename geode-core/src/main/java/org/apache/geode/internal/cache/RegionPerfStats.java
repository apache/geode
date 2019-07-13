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
package org.apache.geode.internal.cache;

import java.util.function.LongSupplier;

import org.apache.geode.StatisticsFactory;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.internal.NanoTimer;

class RegionPerfStats extends CachePerfStats {

  private final CachePerfStats cachePerfStats;

  RegionPerfStats(StatisticsFactory statisticsFactory, CachePerfStats cachePerfStats,
      String regionName) {
    this(statisticsFactory, cachePerfStats, regionName,
        enableClockStats ? NanoTimer::getTime : () -> 0);
  }

  @VisibleForTesting
  RegionPerfStats(StatisticsFactory statisticsFactory, CachePerfStats cachePerfStats,
      String regionName, LongSupplier clock) {
    super(statisticsFactory, "RegionStats-" + regionName, clock);
    this.cachePerfStats = cachePerfStats;
  }

  @Override
  public void incReliableQueuedOps(int inc) {
    stats.incLong(reliableQueuedOpsId, (long) inc);
    cachePerfStats.incReliableQueuedOps(inc);
  }

  @Override
  public void incReliableQueueSize(int inc) {
    stats.incLong(reliableQueueSizeId, (long) inc);
    cachePerfStats.incReliableQueueSize(inc);
  }

  @Override
  public void incReliableQueueMax(int inc) {
    stats.incLong(reliableQueueMaxId, (long) inc);
    cachePerfStats.incReliableQueueMax(inc);
  }

  @Override
  public void incReliableRegions(int inc) {
    stats.incLong(reliableRegionsId, (long) inc);
    cachePerfStats.incReliableRegions(inc);
  }

  @Override
  public void incReliableRegionsMissing(int inc) {
    stats.incLong(reliableRegionsMissingId, (long) inc);
    cachePerfStats.incReliableRegionsMissing(inc);
  }

  @Override
  public void incReliableRegionsQueuing(int inc) {
    stats.incLong(reliableRegionsQueuingId, (long) inc);
    cachePerfStats.incReliableRegionsQueuing(inc);
  }

  @Override
  public void incReliableRegionsMissingFullAccess(int inc) {
    stats.incLong(reliableRegionsMissingFullAccessId, (long) inc);
    cachePerfStats.incReliableRegionsMissingFullAccess(inc);
  }

  @Override
  public void incReliableRegionsMissingLimitedAccess(int inc) {
    stats.incLong(reliableRegionsMissingLimitedAccessId, (long) inc);
    cachePerfStats.incReliableRegionsMissingLimitedAccess(inc);
  }

  @Override
  public void incReliableRegionsMissingNoAccess(int inc) {
    stats.incLong(reliableRegionsMissingNoAccessId, (long) inc);
    cachePerfStats.incReliableRegionsMissingNoAccess(inc);
  }

  @Override
  public void incQueuedEvents(int inc) {
    stats.incLong(eventsQueuedId, inc);
    cachePerfStats.incQueuedEvents(inc);
  }

  /**
   * @return the timestamp that marks the start of the operation
   */
  @Override
  public long startLoad() {
    stats.incLong(loadsInProgressId, (long) 1);
    return cachePerfStats.startLoad();
  }

  /**
   * @param start the timestamp taken when the operation started
   */
  @Override
  public void endLoad(long start) {
    // note that load times are used in health checks and
    // should not be disabled by enableClockStats==false

    // don't use getStatTime so always enabled
    long ts = getTime();
    stats.incLong(loadTimeId, ts - start);
    stats.incLong(loadsInProgressId, (long) -1);
    stats.incLong(loadsCompletedId, (long) 1);

    // need to think about timings
    cachePerfStats.endLoad(start);
  }

  /**
   * @return the timestamp that marks the start of the operation
   */
  @Override
  public long startNetload() {
    stats.incLong(netloadsInProgressId, (long) 1);
    cachePerfStats.startNetload();
    return getTime();
  }

  /**
   * @param start the timestamp taken when the operation started
   */
  @Override
  public void endNetload(long start) {
    if (enableClockStats) {
      stats.incLong(netloadTimeId, getTime() - start);
    }
    stats.incLong(netloadsInProgressId, (long) -1);
    stats.incLong(netloadsCompletedId, (long) 1);
    cachePerfStats.endNetload(start);
  }

  /**
   * @return the timestamp that marks the start of the operation
   */
  @Override
  public long startNetsearch() {
    stats.incLong(netsearchesInProgressId, (long) 1);
    return cachePerfStats.startNetsearch();
  }

  /**
   * @param start the timestamp taken when the operation started
   */
  @Override
  public void endNetsearch(long start) {
    // note that netsearch is used in health checks and timings should
    // not be disabled by enableClockStats==false

    // don't use getStatTime so always enabled
    long ts = getTime();
    stats.incLong(netsearchTimeId, ts - start);
    stats.incLong(netsearchesInProgressId, (long) -1);
    stats.incLong(netsearchesCompletedId, (long) 1);
    cachePerfStats.endNetsearch(start);
  }

  /**
   * @return the timestamp that marks the start of the operation
   */
  @Override
  public long startCacheWriterCall() {
    stats.incLong(cacheWriterCallsInProgressId, (long) 1);
    cachePerfStats.startCacheWriterCall();
    return getTime();
  }

  /**
   * @param start the timestamp taken when the operation started
   */
  @Override
  public void endCacheWriterCall(long start) {
    if (enableClockStats) {
      stats.incLong(cacheWriterCallTimeId, getTime() - start);
    }
    stats.incLong(cacheWriterCallsInProgressId, (long) -1);
    stats.incLong(cacheWriterCallsCompletedId, (long) 1);
    cachePerfStats.endCacheWriterCall(start);
  }

  /**
   * @return the timestamp that marks the start of the operation
   * @since GemFire 3.5
   */
  @Override
  public long startCacheListenerCall() {
    stats.incLong(cacheListenerCallsInProgressId, (long) 1);
    cachePerfStats.startCacheListenerCall();
    return getTime();
  }

  /**
   * @param start the timestamp taken when the operation started
   * @since GemFire 3.5
   */
  @Override
  public void endCacheListenerCall(long start) {
    if (enableClockStats) {
      stats.incLong(cacheListenerCallTimeId, getTime() - start);
    }
    stats.incLong(cacheListenerCallsInProgressId, (long) -1);
    stats.incLong(cacheListenerCallsCompletedId, (long) 1);
    cachePerfStats.endCacheListenerCall(start);
  }

  /**
   * @return the timestamp that marks the start of the operation
   */
  @Override
  public long startGetInitialImage() {
    stats.incLong(getInitialImagesInProgressId, (long) 1);
    cachePerfStats.startGetInitialImage();
    return getTime();
  }

  /**
   * @param start the timestamp taken when the operation started
   */
  @Override
  public void endGetInitialImage(long start) {
    if (enableClockStats) {
      stats.incLong(getInitialImageTimeId, getTime() - start);
    }
    stats.incLong(getInitialImagesInProgressId, (long) -1);
    stats.incLong(getInitialImagesCompletedId, (long) 1);
    cachePerfStats.endGetInitialImage(start);
  }

  /**
   * @param start the timestamp taken when the operation started
   */
  @Override
  public void endNoGIIDone(long start) {
    if (enableClockStats) {
      stats.incLong(getInitialImageTimeId, getTime() - start);
    }
    stats.incLong(getInitialImagesInProgressId, (long) -1);
    cachePerfStats.endNoGIIDone(start);
  }

  @Override
  public void incGetInitialImageKeysReceived() {
    stats.incLong(getInitialImageKeysReceivedId, (long) 1);
    cachePerfStats.incGetInitialImageKeysReceived();
  }

  @Override
  public long startIndexUpdate() {
    stats.incLong(indexUpdateInProgressId, (long) 1);
    cachePerfStats.startIndexUpdate();
    return getTime();
  }

  @Override
  public void endIndexUpdate(long start) {
    long ts = getTime();
    stats.incLong(indexUpdateTimeId, ts - start);
    stats.incLong(indexUpdateInProgressId, (long) -1);
    stats.incLong(indexUpdateCompletedId, (long) 1);
    cachePerfStats.endIndexUpdate(start);
  }

  @Override
  public void incRegions(int inc) {
    stats.incLong(regionsId, (long) inc);
    cachePerfStats.incRegions(inc);

  }

  @Override
  public void incPartitionedRegions(int inc) {
    stats.incLong(partitionedRegionsId, (long) inc);
    cachePerfStats.incPartitionedRegions(inc);
  }

  @Override
  public void incDestroys() {
    stats.incLong(destroysId, 1L);
    cachePerfStats.incDestroys();
  }

  @Override
  public void incCreates() {
    stats.incLong(createsId, 1L);
    cachePerfStats.incCreates();
  }

  @Override
  public void incInvalidates() {
    stats.incLong(invalidatesId, 1L);
    cachePerfStats.incInvalidates();
  }

  @Override
  public void incTombstoneCount(int amount) {
    stats.incLong(tombstoneCountId, (long) amount);
    cachePerfStats.incTombstoneCount(amount);
  }

  @Override
  public void incTombstoneGCCount() {
    stats.incLong(tombstoneGCCountId, (long) 1);
    cachePerfStats.incTombstoneGCCount();
  }

  @Override
  public void incClearTimeouts() {
    stats.incLong(clearTimeoutsId, (long) 1);
    cachePerfStats.incClearTimeouts();
  }

  @Override
  public void incConflatedEventsCount() {
    stats.incLong(conflatedEventsId, 1);
    cachePerfStats.incConflatedEventsCount();
  }

  /**
   * @param start the timestamp taken when the operation started
   */
  @Override
  public void endGet(long start, boolean miss) {
    if (enableClockStats) {
      long totalNanos = getTime() - start;
      stats.incLong(getTimeId, totalNanos);
    }
    stats.incLong(getsId, 1L);
    if (miss) {
      stats.incLong(missesId, 1L);
    }
    cachePerfStats.endGet(start, miss);
  }

  /**
   * @param start the timestamp taken when the operation started
   * @param isUpdate true if the put was an update (origin remote)
   */
  @Override
  public long endPut(long start, boolean isUpdate) {
    long totalNanos = 0;
    if (isUpdate) {
      stats.incLong(updatesId, (long) 1);
      if (enableClockStats) {
        totalNanos = getTime() - start;
        stats.incLong(updateTimeId, totalNanos);
      }
    } else {
      stats.incLong(putsId, (long) 1);
      if (enableClockStats) {
        totalNanos = getTime() - start;
        stats.incLong(putTimeId, totalNanos);
      }
    }
    cachePerfStats.endPut(start, isUpdate);
    return totalNanos;
  }

  @Override
  public void endPutAll(long start) {
    stats.incLong(putAllsId, (long) 1);
    if (enableClockStats) {
      stats.incLong(putAllTimeId, getTime() - start);
    }
    cachePerfStats.endPutAll(start);
  }

  @Override
  public void endQueryExecution(long executionTime) {
    stats.incLong(queryExecutionsId, (long) 1);
    if (enableClockStats) {
      stats.incLong(queryExecutionTimeId, executionTime);
    }
    cachePerfStats.endQueryExecution(executionTime);
  }

  @Override
  public void endQueryResultsHashCollisionProbe(long start) {
    if (enableClockStats) {
      stats.incLong(queryResultsHashCollisionProbeTimeId, getTime() - start);
    }
    cachePerfStats.endQueryResultsHashCollisionProbe(start);
  }

  @Override
  public void incQueryResultsHashCollisions() {
    stats.incLong(queryResultsHashCollisionsId, (long) 1);
    cachePerfStats.incQueryResultsHashCollisions();
  }

  @Override
  public void incTxConflictCheckTime(long delta) {
    stats.incLong(txConflictCheckTimeId, delta);
    cachePerfStats.incTxConflictCheckTime(delta);
  }

  @Override
  public void txSuccess(long opTime, long txLifeTime, int txChanges) {
    stats.incLong(txCommitsId, (long) 1);
    stats.incLong(txCommitChangesId, (long) txChanges);
    stats.incLong(txCommitTimeId, opTime);
    stats.incLong(txSuccessLifeTimeId, txLifeTime);
    cachePerfStats.txSuccess(opTime, txLifeTime, txChanges);
  }

  @Override
  public void txFailure(long opTime, long txLifeTime, int txChanges) {
    stats.incLong(txFailuresId, (long) 1);
    stats.incLong(txFailureChangesId, (long) txChanges);
    stats.incLong(txFailureTimeId, opTime);
    stats.incLong(txFailedLifeTimeId, txLifeTime);
    cachePerfStats.txFailure(opTime, txLifeTime, txChanges);
  }

  @Override
  public void txRollback(long opTime, long txLifeTime, int txChanges) {
    stats.incLong(txRollbacksId, (long) 1);
    stats.incLong(txRollbackChangesId, (long) txChanges);
    stats.incLong(txRollbackTimeId, opTime);
    stats.incLong(txRollbackLifeTimeId, txLifeTime);
    cachePerfStats.txRollback(opTime, txLifeTime, txChanges);
  }

  @Override
  public void incEventQueueSize(int items) {
    stats.incLong(eventQueueSizeId, (long) items);
    cachePerfStats.incEventQueueSize(items);
  }

  @Override
  public void incEventQueueThrottleCount(int items) {
    stats.incLong(eventQueueThrottleCountId, (long) items);
    cachePerfStats.incEventQueueThrottleCount(items);
  }

  @Override
  protected void incEventQueueThrottleTime(long nanos) {
    stats.incLong(eventQueueThrottleTimeId, nanos);
    cachePerfStats.incEventQueueThrottleTime(nanos);
  }

  @Override
  protected void incEventThreads(int items) {
    stats.incLong(eventThreadsId, (long) items);
    cachePerfStats.incEventThreads(items);
  }

  @Override
  public void incEntryCount(int delta) {
    stats.incLong(entryCountId, delta);
    cachePerfStats.incEntryCount(delta);
  }

  @Override
  public void incRetries() {
    stats.incLong(retriesId, (long) 1);
    cachePerfStats.incRetries();
  }

  @Override
  public void incDiskTasksWaiting() {
    stats.incLong(diskTasksWaitingId, (long) 1);
    cachePerfStats.incDiskTasksWaiting();
  }

  @Override
  public void decDiskTasksWaiting() {
    stats.incLong(diskTasksWaitingId, (long) -1);
    cachePerfStats.decDiskTasksWaiting();
  }

  @Override
  public void decDiskTasksWaiting(int count) {
    stats.incLong(diskTasksWaitingId, (long) -count);
    cachePerfStats.decDiskTasksWaiting(count);
  }

  @Override
  public void incEvictorJobsStarted() {
    stats.incLong(evictorJobsStartedId, (long) 1);
    cachePerfStats.incEvictorJobsStarted();
  }

  @Override
  public void incEvictorJobsCompleted() {
    stats.incLong(evictorJobsCompletedId, (long) 1);
    cachePerfStats.incEvictorJobsCompleted();
  }

  @Override
  public void incEvictorQueueSize(int delta) {
    stats.incLong(evictorQueueSizeId, (long) delta);
    cachePerfStats.incEvictorQueueSize(delta);
  }

  @Override
  public void incEvictWorkTime(long delta) {
    stats.incLong(evictWorkTimeId, delta);
    cachePerfStats.incEvictWorkTime(delta);
  }

  @Override
  public void incClearCount() {
    stats.incLong(clearsId, 1L);
    cachePerfStats.incClearCount();
  }

  @Override
  public void incPRQueryRetries() {
    stats.incLong(partitionedRegionQueryRetriesId, 1);
    cachePerfStats.incPRQueryRetries();
  }

  @Override
  public void incMetaDataRefreshCount() {
    stats.incLong(metaDataRefreshCountId, 1);
    cachePerfStats.incMetaDataRefreshCount();
  }

  @Override
  public void endImport(long entryCount, long start) {
    stats.incLong(importedEntriesCountId, entryCount);
    if (enableClockStats) {
      stats.incLong(importTimeId, getTime() - start);
    }
    cachePerfStats.endImport(entryCount, start);
  }

  @Override
  public void endExport(long entryCount, long start) {
    stats.incLong(exportedEntriesCountId, entryCount);
    if (enableClockStats) {
      stats.incLong(exportTimeId, getTime() - start);
    }
    cachePerfStats.endExport(entryCount, start);
  }

  @Override
  public long startCompression() {
    stats.incLong(compressionCompressionsId, 1);
    cachePerfStats.stats.incLong(compressionCompressionsId, 1);
    return getTime();
  }

  @Override
  public void endCompression(long startTime, long startSize, long endSize) {
    if (enableClockStats) {
      long time = getTime() - startTime;
      stats.incLong(compressionCompressTimeId, time);
      cachePerfStats.stats.incLong(compressionCompressTimeId, time);
    }

    stats.incLong(compressionPreCompressedBytesId, startSize);
    stats.incLong(compressionPostCompressedBytesId, endSize);

    cachePerfStats.stats.incLong(compressionPreCompressedBytesId, startSize);
    cachePerfStats.stats.incLong(compressionPostCompressedBytesId, endSize);
  }

  @Override
  public long startDecompression() {
    stats.incLong(compressionDecompressionsId, 1);
    cachePerfStats.stats.incLong(compressionDecompressionsId, 1);
    return getTime();
  }

  @Override
  public void endDecompression(long startTime) {
    if (enableClockStats) {
      long time = getTime() - startTime;
      stats.incLong(compressionDecompressTimeId, time);
      cachePerfStats.stats.incLong(compressionDecompressTimeId, time);
    }
  }
}
