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

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import org.apache.geode.StatisticsFactory;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.internal.statistics.StatisticsClock;

class RegionPerfStats extends CachePerfStats implements RegionStats {
  private static final String HIT_TAG_VALUE = "hit";
  private static final String MISS_TAG_VALUE = "miss";

  private final CachePerfStats cachePerfStats;
  private final StatisticsClock clock;
  private final MeterRegistry meterRegistry;
  private final Gauge entriesGauge;
  private final Timer cacheGetsHitTimer;
  private final Timer cacheGetsMissTimer;

  RegionPerfStats(StatisticsFactory statisticsFactory, String textId, CachePerfStats cachePerfStats,
      InternalRegion region, MeterRegistry meterRegistry, StatisticsClock clock) {
    this(statisticsFactory, textId, clock, cachePerfStats, region,
        meterRegistry);
  }

  @VisibleForTesting
  RegionPerfStats(StatisticsFactory statisticsFactory, String textId, StatisticsClock clock,
      CachePerfStats cachePerfStats, InternalRegion region, MeterRegistry meterRegistry) {
    this(statisticsFactory, textId, clock, cachePerfStats, region, meterRegistry,
        registerEntriesGauge(region, meterRegistry),
        registerCacheGetsTimer(region, meterRegistry, HIT_TAG_VALUE),
        registerCacheGetsTimer(region, meterRegistry, MISS_TAG_VALUE));
  }

  @VisibleForTesting
  RegionPerfStats(StatisticsFactory statisticsFactory, String textId, StatisticsClock clock,
      CachePerfStats cachePerfStats, InternalRegion region, MeterRegistry meterRegistry,
      Gauge entriesGauge, Timer cacheGetsHitTimer, Timer cacheGetsMissTimer) {
    super(statisticsFactory, textId, clock);

    this.clock = clock;
    this.cachePerfStats = cachePerfStats;
    this.meterRegistry = meterRegistry;
    this.entriesGauge = entriesGauge;
    this.cacheGetsHitTimer = cacheGetsHitTimer;
    this.cacheGetsMissTimer = cacheGetsMissTimer;

    stats.setLongSupplier(entryCountId, region::getLocalSize);
  }

  @Override
  protected void close() {
    meterRegistry.remove(entriesGauge);
    entriesGauge.close();

    meterRegistry.remove(cacheGetsHitTimer);
    cacheGetsHitTimer.close();

    meterRegistry.remove(cacheGetsMissTimer);
    cacheGetsMissTimer.close();

    super.close();
  }

  @Override
  public void incReliableQueuedOps(int inc) {
    stats.incInt(reliableQueuedOpsId, inc);
    cachePerfStats.incReliableQueuedOps(inc);
  }

  @Override
  public void incReliableQueueSize(int inc) {
    stats.incInt(reliableQueueSizeId, inc);
    cachePerfStats.incReliableQueueSize(inc);
  }

  @Override
  public void incReliableQueueMax(int inc) {
    stats.incInt(reliableQueueMaxId, inc);
    cachePerfStats.incReliableQueueMax(inc);
  }

  @Override
  public void incReliableRegions(int inc) {
    stats.incInt(reliableRegionsId, inc);
    cachePerfStats.incReliableRegions(inc);
  }

  @Override
  public void incReliableRegionsMissing(int inc) {
    stats.incInt(reliableRegionsMissingId, inc);
    cachePerfStats.incReliableRegionsMissing(inc);
  }

  @Override
  public void incReliableRegionsQueuing(int inc) {
    stats.incInt(reliableRegionsQueuingId, inc);
    cachePerfStats.incReliableRegionsQueuing(inc);
  }

  @Override
  public void incReliableRegionsMissingFullAccess(int inc) {
    stats.incInt(reliableRegionsMissingFullAccessId, inc);
    cachePerfStats.incReliableRegionsMissingFullAccess(inc);
  }

  @Override
  public void incReliableRegionsMissingLimitedAccess(int inc) {
    stats.incInt(reliableRegionsMissingLimitedAccessId, inc);
    cachePerfStats.incReliableRegionsMissingLimitedAccess(inc);
  }

  @Override
  public void incReliableRegionsMissingNoAccess(int inc) {
    stats.incInt(reliableRegionsMissingNoAccessId, inc);
    cachePerfStats.incReliableRegionsMissingNoAccess(inc);
  }

  @Override
  public void incQueuedEvents(int inc) {
    stats.incLong(eventsQueuedId, inc);
    cachePerfStats.incQueuedEvents(inc);
  }

  @Override
  public long startLoad() {
    stats.incInt(loadsInProgressId, 1);
    return cachePerfStats.startLoad();
  }

  @Override
  public void endLoad(long start) {
    // note that load times are used in health checks and
    // should not be disabled by clock.isEnabled()==false

    // don't use getStatTime so always enabled
    long ts = getTime();
    stats.incLong(loadTimeId, ts - start);
    stats.incInt(loadsInProgressId, -1);
    stats.incInt(loadsCompletedId, 1);

    // need to think about timings
    cachePerfStats.endLoad(start);
  }

  @Override
  public long startNetload() {
    stats.incInt(netloadsInProgressId, 1);
    cachePerfStats.startNetload();
    return getTime();
  }

  @Override
  public void endNetload(long start) {
    if (clock.isEnabled()) {
      stats.incLong(netloadTimeId, getTime() - start);
    }
    stats.incInt(netloadsInProgressId, -1);
    stats.incInt(netloadsCompletedId, 1);
    cachePerfStats.endNetload(start);
  }

  @Override
  public long startNetsearch() {
    stats.incInt(netsearchesInProgressId, 1);
    return cachePerfStats.startNetsearch();
  }

  @Override
  public void endNetsearch(long start) {
    // note that netsearch is used in health checks and timings should
    // not be disabled by clock.isEnabled()==false

    // don't use getStatTime so always enabled
    long ts = getTime();
    stats.incLong(netsearchTimeId, ts - start);
    stats.incInt(netsearchesInProgressId, -1);
    stats.incInt(netsearchesCompletedId, 1);
    cachePerfStats.endNetsearch(start);
  }

  @Override
  public long startCacheWriterCall() {
    stats.incInt(cacheWriterCallsInProgressId, 1);
    cachePerfStats.startCacheWriterCall();
    return getTime();
  }

  @Override
  public void endCacheWriterCall(long start) {
    if (clock.isEnabled()) {
      stats.incLong(cacheWriterCallTimeId, getTime() - start);
    }
    stats.incInt(cacheWriterCallsInProgressId, -1);
    stats.incInt(cacheWriterCallsCompletedId, 1);
    cachePerfStats.endCacheWriterCall(start);
  }

  @Override
  public long startCacheListenerCall() {
    stats.incInt(cacheListenerCallsInProgressId, 1);
    cachePerfStats.startCacheListenerCall();
    return getTime();
  }

  @Override
  public void endCacheListenerCall(long start) {
    if (clock.isEnabled()) {
      stats.incLong(cacheListenerCallTimeId, getTime() - start);
    }
    stats.incInt(cacheListenerCallsInProgressId, -1);
    stats.incInt(cacheListenerCallsCompletedId, 1);
    cachePerfStats.endCacheListenerCall(start);
  }

  @Override
  public long startGetInitialImage() {
    stats.incInt(getInitialImagesInProgressId, 1);
    cachePerfStats.startGetInitialImage();
    return getTime();
  }

  @Override
  public void endGetInitialImage(long start) {
    if (clock.isEnabled()) {
      stats.incLong(getInitialImageTimeId, getTime() - start);
    }
    stats.incInt(getInitialImagesInProgressId, -1);
    stats.incInt(getInitialImagesCompletedId, 1);
    cachePerfStats.endGetInitialImage(start);
  }

  @Override
  public void endNoGIIDone(long start) {
    if (clock.isEnabled()) {
      stats.incLong(getInitialImageTimeId, getTime() - start);
    }
    stats.incInt(getInitialImagesInProgressId, -1);
    cachePerfStats.endNoGIIDone(start);
  }

  @Override
  public void incGetInitialImageKeysReceived() {
    stats.incInt(getInitialImageKeysReceivedId, 1);
    cachePerfStats.incGetInitialImageKeysReceived();
  }

  @Override
  public long startIndexUpdate() {
    stats.incInt(indexUpdateInProgressId, 1);
    cachePerfStats.startIndexUpdate();
    return getTime();
  }

  @Override
  public void endIndexUpdate(long start) {
    long ts = getTime();
    stats.incLong(indexUpdateTimeId, ts - start);
    stats.incInt(indexUpdateInProgressId, -1);
    stats.incInt(indexUpdateCompletedId, 1);
    cachePerfStats.endIndexUpdate(start);
  }

  @Override
  public void incRegions(int inc) {
    stats.incInt(regionsId, inc);
    cachePerfStats.incRegions(inc);

  }

  @Override
  public void incPartitionedRegions(int inc) {
    stats.incInt(partitionedRegionsId, inc);
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
    stats.incInt(tombstoneCountId, amount);
    cachePerfStats.incTombstoneCount(amount);
  }

  @Override
  public void incTombstoneGCCount() {
    stats.incInt(tombstoneGCCountId, 1);
    cachePerfStats.incTombstoneGCCount();
  }

  @Override
  public void incClearTimeouts() {
    stats.incInt(clearTimeoutsId, 1);
    cachePerfStats.incClearTimeouts();
  }

  @Override
  public void incConflatedEventsCount() {
    stats.incLong(conflatedEventsId, 1);
    cachePerfStats.incConflatedEventsCount();
  }

  @Override
  public void endGet(long start, boolean miss) {
    if (clock.isEnabled()) {
      long totalNanos = getTime() - start;
      stats.incLong(getTimeId, totalNanos);
    }
    stats.incLong(getsId, 1L);
    if (miss) {
      stats.incLong(missesId, 1L);
    }
    cachePerfStats.endGet(start, miss);
  }

  @Override
  public void endGetForClient(long start, boolean miss) {
    long totalNanos = clock.isEnabled() ? getTime() - start : 0;
    if (miss) {
      cacheGetsMissTimer.record(totalNanos, NANOSECONDS);
    } else {
      cacheGetsHitTimer.record(totalNanos, NANOSECONDS);
    }
  }

  @Override
  public long endPut(long start, boolean isUpdate) {
    long totalNanos = 0;
    if (isUpdate) {
      stats.incLong(updatesId, 1L);
      if (clock.isEnabled()) {
        totalNanos = getTime() - start;
        stats.incLong(updateTimeId, totalNanos);
      }
    } else {
      stats.incLong(putsId, 1L);
      if (clock.isEnabled()) {
        totalNanos = getTime() - start;
        stats.incLong(putTimeId, totalNanos);
      }
    }
    cachePerfStats.endPut(start, isUpdate);
    return totalNanos;
  }

  @Override
  public void endPutAll(long start) {
    stats.incInt(putAllsId, 1);
    if (clock.isEnabled()) {
      stats.incLong(putAllTimeId, getTime() - start);
    }
    cachePerfStats.endPutAll(start);
  }

  @Override
  public void endQueryExecution(long executionTime) {
    stats.incInt(queryExecutionsId, 1);
    if (clock.isEnabled()) {
      stats.incLong(queryExecutionTimeId, executionTime);
    }
    cachePerfStats.endQueryExecution(executionTime);
  }

  @Override
  public void endQueryResultsHashCollisionProbe(long start) {
    if (clock.isEnabled()) {
      stats.incLong(queryResultsHashCollisionProbeTimeId, getTime() - start);
    }
    cachePerfStats.endQueryResultsHashCollisionProbe(start);
  }

  @Override
  public void incQueryResultsHashCollisions() {
    stats.incInt(queryResultsHashCollisionsId, 1);
    cachePerfStats.incQueryResultsHashCollisions();
  }

  @Override
  public void incTxConflictCheckTime(long delta) {
    stats.incLong(txConflictCheckTimeId, delta);
    cachePerfStats.incTxConflictCheckTime(delta);
  }

  @Override
  public void txSuccess(long opTime, long txLifeTime, int txChanges) {
    stats.incInt(txCommitsId, 1);
    stats.incInt(txCommitChangesId, txChanges);
    stats.incLong(txCommitTimeId, opTime);
    stats.incLong(txSuccessLifeTimeId, txLifeTime);
    cachePerfStats.txSuccess(opTime, txLifeTime, txChanges);
  }

  @Override
  public void txFailure(long opTime, long txLifeTime, int txChanges) {
    stats.incInt(txFailuresId, 1);
    stats.incInt(txFailureChangesId, txChanges);
    stats.incLong(txFailureTimeId, opTime);
    stats.incLong(txFailedLifeTimeId, txLifeTime);
    cachePerfStats.txFailure(opTime, txLifeTime, txChanges);
  }

  @Override
  public void txRollback(long opTime, long txLifeTime, int txChanges) {
    stats.incInt(txRollbacksId, 1);
    stats.incInt(txRollbackChangesId, txChanges);
    stats.incLong(txRollbackTimeId, opTime);
    stats.incLong(txRollbackLifeTimeId, txLifeTime);
    cachePerfStats.txRollback(opTime, txLifeTime, txChanges);
  }

  @Override
  public void incEventQueueSize(int items) {
    stats.incInt(eventQueueSizeId, items);
    cachePerfStats.incEventQueueSize(items);
  }

  @Override
  public void incEventQueueThrottleCount(int items) {
    stats.incInt(eventQueueThrottleCountId, items);
    cachePerfStats.incEventQueueThrottleCount(items);
  }

  @Override
  public void incEventQueueThrottleTime(long nanos) {
    stats.incLong(eventQueueThrottleTimeId, nanos);
    cachePerfStats.incEventQueueThrottleTime(nanos);
  }

  @Override
  public void incEventThreads(int items) {
    stats.incInt(eventThreadsId, items);
    cachePerfStats.incEventThreads(items);
  }

  @Override
  public void incEntryCount(int delta) {
    cachePerfStats.incEntryCount(delta);
  }

  @Override
  public void incRetries() {
    stats.incInt(retriesId, 1);
    cachePerfStats.incRetries();
  }

  @Override
  public void incDiskTasksWaiting() {
    stats.incInt(diskTasksWaitingId, 1);
    cachePerfStats.incDiskTasksWaiting();
  }

  @Override
  public void decDiskTasksWaiting() {
    stats.incInt(diskTasksWaitingId, -1);
    cachePerfStats.decDiskTasksWaiting();
  }

  @Override
  public void decDiskTasksWaiting(int count) {
    stats.incInt(diskTasksWaitingId, -count);
    cachePerfStats.decDiskTasksWaiting(count);
  }

  @Override
  public void incEvictorJobsStarted() {
    stats.incInt(evictorJobsStartedId, 1);
    cachePerfStats.incEvictorJobsStarted();
  }

  @Override
  public void incEvictorJobsCompleted() {
    stats.incInt(evictorJobsCompletedId, 1);
    cachePerfStats.incEvictorJobsCompleted();
  }

  @Override
  public void incEvictorQueueSize(int delta) {
    stats.incInt(evictorQueueSizeId, delta);
    cachePerfStats.incEvictorQueueSize(delta);
  }

  @Override
  public void incEvictWorkTime(long delta) {
    stats.incLong(evictWorkTimeId, delta);
    cachePerfStats.incEvictWorkTime(delta);
  }

  @Override
  public void incRegionClearCount() {
    stats.incLong(regionClearsId, 1L);
    cachePerfStats.incRegionClearCount();
  }

  @Override
  public void incBucketClearCount() {
    stats.incLong(bucketClearsId, 1L);
    cachePerfStats.incBucketClearCount();
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
    if (clock.isEnabled()) {
      stats.incLong(importTimeId, getTime() - start);
    }
    cachePerfStats.endImport(entryCount, start);
  }

  @Override
  public void endExport(long entryCount, long start) {
    stats.incLong(exportedEntriesCountId, entryCount);
    if (clock.isEnabled()) {
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
    if (clock.isEnabled()) {
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
    if (clock.isEnabled()) {
      long time = getTime() - startTime;
      stats.incLong(compressionDecompressTimeId, time);
      cachePerfStats.stats.incLong(compressionDecompressTimeId, time);
    }
  }

  private static Gauge registerEntriesGauge(InternalRegion region, MeterRegistry meterRegistry) {
    return Gauge.builder("geode.cache.entries", region::getLocalSize)
        .description("Current number of entries in the region.")
        .tag("region", region.getName())
        .tag("data.policy", region.getDataPolicy().toString())
        .baseUnit("entries")
        .register(meterRegistry);
  }

  private static Timer registerCacheGetsTimer(InternalRegion region, MeterRegistry meterRegistry,
      String resultTagValue) {
    return Timer.builder("geode.cache.gets")
        .description("Total time and count for GET requests from Java or native clients.")
        .tag("region", region.getName())
        .tag("result", resultTagValue)
        .register(meterRegistry);
  }
}
