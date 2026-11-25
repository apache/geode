/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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

package org.apache.geode.cache.lucene.internal.vector;

import static org.apache.geode.cache.Region.SEPARATOR;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.lucene.internal.repository.IndexRepository;

/**
 * Per-{@link Cache} registry managing per-bucket {@link Tier0VectorDeltaBuffer} instances keyed by
 * {@code (regionPath, indexName, bucketId)}.
 *
 * <p>
 * Provides clean lifecycle hooks for bucket listeners, AEQ flushers, and
 * {@code DistributedHybridKnnFunction} without modifying public Geode APIs.
 */
public class Tier0VectorRegistry {

  /** Default bounded wait when a reader encounters an in-flight primary bucket hydration. */
  public static final long DEFAULT_HYDRATION_AWAIT_MILLIS = 5_000L;
  /** Default capacity of each per-bucket flush-cycle sample ring buffer. */
  public static final int DEFAULT_FLUSH_CYCLE_RING_CAPACITY = 4096;

  private static final Map<Cache, Tier0VectorRegistry> CACHE_REGISTRIES =
      Collections.synchronizedMap(new WeakHashMap<>());

  private final ConcurrentHashMap<IndexKey, ConcurrentHashMap<Integer, Tier0VectorDeltaBuffer<Object>>> buffers =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<IndexKey, ConcurrentHashMap<Integer, CountDownLatch>> hydrationBarriers =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<IndexKey, ConcurrentHashMap<Integer, FlushCycleRing>> flushCycleRings =
      new ConcurrentHashMap<>();
  private volatile boolean flushCycleSamplingEnabled = true;
  private final AtomicLong hydrationWaitCount = new AtomicLong(0L);
  private final AtomicLong hydratedKeysCount = new AtomicLong(0L);
  private final AtomicLong hydratedTombstonesCount = new AtomicLong(0L);
  private final AtomicLong aeqDeltaHydrationCount = new AtomicLong(0L);
  private final AtomicLong fullScanHydrationCount = new AtomicLong(0L);
  private final AtomicLong reconciledStaleIndexKeysCount = new AtomicLong(0L);
  private final AtomicLong lastHydrationEventsInspected = new AtomicLong(0L);
  private final AtomicLong lastHydrationKeysRestored = new AtomicLong(0L);
  private final AtomicLong unsupportedFilterFlushFallbackCount = new AtomicLong(0L);
  private final AtomicLong inlineMaskQueryCount = new AtomicLong(0L);
  private final AtomicLong oversampleFallbackQueryCount = new AtomicLong(0L);

  public Tier0VectorRegistry() {}

  /**
   * Returns the {@link Tier0VectorRegistry} associated with {@code cache}, creating one lazily if
   * absent.
   */
  public static Tier0VectorRegistry forCache(Cache cache) {
    Objects.requireNonNull(cache, "cache must not be null");
    synchronized (CACHE_REGISTRIES) {
      return CACHE_REGISTRIES.computeIfAbsent(cache, c -> new Tier0VectorRegistry());
    }
  }

  /**
   * Removes and clears the registry associated with {@code cache}.
   */
  public static void removeCache(Cache cache) {
    if (cache == null) {
      return;
    }
    Tier0VectorRegistry removed;
    synchronized (CACHE_REGISTRIES) {
      removed = CACHE_REGISTRIES.remove(cache);
    }
    if (removed != null) {
      removed.clearAll();
    }
  }

  public Tier0VectorDeltaBuffer<Object> getOrCreateBuffer(String regionPath, String indexName,
      int bucketId) {
    IndexKey key = new IndexKey(normalizeRegionPath(regionPath), indexName);
    return buffers
        .computeIfAbsent(key, k -> new ConcurrentHashMap<>())
        .computeIfAbsent(bucketId, Tier0VectorDeltaBuffer::new);
  }

  public Tier0VectorDeltaBuffer<Object> getBuffer(String regionPath, String indexName,
      int bucketId) {
    IndexKey key = new IndexKey(normalizeRegionPath(regionPath), indexName);
    ConcurrentHashMap<Integer, Tier0VectorDeltaBuffer<Object>> bucketMap = buffers.get(key);
    return bucketMap == null ? null : bucketMap.get(bucketId);
  }

  public Map<Integer, Tier0VectorDeltaBuffer<Object>> getBuckets(String regionPath,
      String indexName) {
    IndexKey key = new IndexKey(normalizeRegionPath(regionPath), indexName);
    ConcurrentHashMap<Integer, Tier0VectorDeltaBuffer<Object>> bucketMap = buffers.get(key);
    return bucketMap == null ? Collections.emptyMap() : Collections.unmodifiableMap(bucketMap);
  }

  public long recordPut(String regionPath, String indexName, int bucketId, Object key,
      float[] vector, Map<String, Object> metadata) {
    return getOrCreateBuffer(regionPath, indexName, bucketId).put(key, vector, metadata);
  }

  public long recordDestroy(String regionPath, String indexName, int bucketId, Object key) {
    return getOrCreateBuffer(regionPath, indexName, bucketId).destroy(key);
  }

  public void removeBucket(String regionPath, String indexName, int bucketId) {
    IndexKey key = new IndexKey(normalizeRegionPath(regionPath), indexName);
    ConcurrentHashMap<Integer, Tier0VectorDeltaBuffer<Object>> bucketMap = buffers.get(key);
    if (bucketMap != null) {
      Tier0VectorDeltaBuffer<Object> removed = bucketMap.remove(bucketId);
      if (removed != null) {
        removed.clear();
      }
    }
    releaseHydrationLatch(key, bucketId);
    ConcurrentHashMap<Integer, FlushCycleRing> ringMap = flushCycleRings.get(key);
    if (ringMap != null) {
      FlushCycleRing ring = ringMap.remove(bucketId);
      if (ring != null) {
        ring.clear();
      }
    }
  }

  public void removeIndex(String regionPath, String indexName) {
    IndexKey key = new IndexKey(normalizeRegionPath(regionPath), indexName);
    ConcurrentHashMap<Integer, Tier0VectorDeltaBuffer<Object>> removed = buffers.remove(key);
    if (removed != null) {
      removed.values().forEach(Tier0VectorDeltaBuffer::clear);
      removed.clear();
    }
    releaseHydrationLatchesForIndex(key);
    ConcurrentHashMap<Integer, FlushCycleRing> removedRings = flushCycleRings.remove(key);
    if (removedRings != null) {
      removedRings.values().forEach(FlushCycleRing::clear);
      removedRings.clear();
    }
  }

  /**
   * Removes and clears all Tier-0 delta buffers registered for any index on {@code regionPath}.
   */
  public void removeRegion(String regionPath) {
    String normalized = normalizeRegionPath(regionPath);
    buffers.keySet().removeIf(indexKey -> {
      if (indexKey.regionPath.equals(normalized)) {
        ConcurrentHashMap<Integer, Tier0VectorDeltaBuffer<Object>> removed =
            buffers.remove(indexKey);
        if (removed != null) {
          removed.values().forEach(Tier0VectorDeltaBuffer::clear);
          removed.clear();
        }
        return true;
      }
      return false;
    });
    hydrationBarriers.keySet().removeIf(indexKey -> {
      if (indexKey.regionPath.equals(normalized)) {
        releaseHydrationLatchesForIndex(indexKey);
        return true;
      }
      return false;
    });
    flushCycleRings.keySet().removeIf(indexKey -> {
      if (indexKey.regionPath.equals(normalized)) {
        ConcurrentHashMap<Integer, FlushCycleRing> removedRings = flushCycleRings.remove(indexKey);
        if (removedRings != null) {
          removedRings.values().forEach(FlushCycleRing::clear);
          removedRings.clear();
        }
        return true;
      }
      return false;
    });
  }

  /**
   * Opens a per-bucket promotion hydration barrier so concurrent hybrid k-NN readers wait until
   * primary bucket hydration populates {@link Tier0VectorDeltaBuffer}.
   *
   * @return the installed barrier latch token to pass to
   *         {@link #completeHydration(String, String, int, CountDownLatch)}
   */
  public CountDownLatch beginHydration(String regionPath, String indexName, int bucketId) {
    IndexKey key = new IndexKey(normalizeRegionPath(regionPath), indexName);
    CountDownLatch latch = new CountDownLatch(1);
    CountDownLatch previous = hydrationBarriers
        .computeIfAbsent(key, k -> new ConcurrentHashMap<>())
        .put(bucketId, latch);
    if (previous != null) {
      previous.countDown();
    }
    return latch;
  }

  /**
   * Marks the specified hydration barrier complete and unblocks waiting readers.
   */
  public void completeHydration(String regionPath, String indexName, int bucketId,
      CountDownLatch latch) {
    if (latch == null) {
      completeHydration(regionPath, indexName, bucketId);
      return;
    }
    try {
      IndexKey key = new IndexKey(normalizeRegionPath(regionPath), indexName);
      ConcurrentHashMap<Integer, CountDownLatch> bucketMap = hydrationBarriers.get(key);
      if (bucketMap != null) {
        bucketMap.remove(bucketId, latch);
      }
    } finally {
      latch.countDown();
    }
  }

  /**
   * Completes any currently active hydration barrier for {@code (regionPath, indexName, bucketId)}.
   */
  public void completeHydration(String regionPath, String indexName, int bucketId) {
    IndexKey key = new IndexKey(normalizeRegionPath(regionPath), indexName);
    releaseHydrationLatch(key, bucketId);
  }

  /**
   * Waits (up to {@link #DEFAULT_HYDRATION_AWAIT_MILLIS}) for any active promotion hydration on
   * {@code (regionPath, indexName, bucketId)} to finish before snapshotting Tier-0. Returns
   * immediately with zero contention when no promotion is in progress.
   */
  public boolean awaitHydration(String regionPath, String indexName, int bucketId) {
    return awaitHydration(regionPath, indexName, bucketId, DEFAULT_HYDRATION_AWAIT_MILLIS,
        TimeUnit.MILLISECONDS);
  }

  /**
   * Waits up to {@code timeout} for any active promotion hydration on
   * {@code (regionPath, indexName, bucketId)} to complete.
   */
  public boolean awaitHydration(String regionPath, String indexName, int bucketId, long timeout,
      TimeUnit unit) {
    IndexKey key = new IndexKey(normalizeRegionPath(regionPath), indexName);
    ConcurrentHashMap<Integer, CountDownLatch> bucketMap = hydrationBarriers.get(key);
    if (bucketMap == null) {
      return true;
    }
    CountDownLatch latch = bucketMap.get(bucketId);
    if (latch == null || latch.getCount() == 0L) {
      return true;
    }
    hydrationWaitCount.incrementAndGet();
    try {
      return latch.await(timeout, unit);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  public boolean isHydrating(String regionPath, String indexName, int bucketId) {
    IndexKey key = new IndexKey(normalizeRegionPath(regionPath), indexName);
    ConcurrentHashMap<Integer, CountDownLatch> bucketMap = hydrationBarriers.get(key);
    if (bucketMap == null) {
      return false;
    }
    CountDownLatch latch = bucketMap.get(bucketId);
    return latch != null && latch.getCount() > 0L;
  }

  public long getHydrationWaitCount() {
    return hydrationWaitCount.get();
  }

  public long getHydratedKeysCount() {
    return hydratedKeysCount.get();
  }

  public long incrementHydratedKeysCount() {
    return hydratedKeysCount.incrementAndGet();
  }

  public long getHydratedTombstonesCount() {
    return hydratedTombstonesCount.get();
  }

  public long incrementHydratedTombstonesCount() {
    return hydratedTombstonesCount.incrementAndGet();
  }

  public long getAeqDeltaHydrationCount() {
    return aeqDeltaHydrationCount.get();
  }

  public long incrementAeqDeltaHydrationCount() {
    return aeqDeltaHydrationCount.incrementAndGet();
  }

  public long getFullScanHydrationCount() {
    return fullScanHydrationCount.get();
  }

  public long incrementFullScanHydrationCount() {
    return fullScanHydrationCount.incrementAndGet();
  }

  public long getReconciledStaleIndexKeysCount() {
    return reconciledStaleIndexKeysCount.get();
  }

  public long incrementReconciledStaleIndexKeysCount() {
    return reconciledStaleIndexKeysCount.incrementAndGet();
  }

  public boolean isFlushCycleSamplingEnabled() {
    return flushCycleSamplingEnabled;
  }

  public void setFlushCycleSamplingEnabled(boolean flushCycleSamplingEnabled) {
    this.flushCycleSamplingEnabled = flushCycleSamplingEnabled;
  }

  /**
   * Records a per-bucket AEQ flush-cycle sample into a bounded circular ring buffer when flush
   * cycle sampling is enabled.
   */
  public void recordFlushCycle(String regionPath, String indexName, int bucketId, int batchSize,
      int preEvictTier0Size, long flushDurationNanos) {
    if (!flushCycleSamplingEnabled) {
      return;
    }
    IndexKey key = new IndexKey(normalizeRegionPath(regionPath), indexName);
    FlushCycleSample sample = new FlushCycleSample(
        bucketId, batchSize, preEvictTier0Size, flushDurationNanos, System.nanoTime());
    flushCycleRings
        .computeIfAbsent(key, k -> new ConcurrentHashMap<>())
        .computeIfAbsent(bucketId, b -> new FlushCycleRing(DEFAULT_FLUSH_CYCLE_RING_CAPACITY))
        .record(sample);
  }

  /**
   * Drains all recorded flush-cycle samples for {@code (regionPath, indexName, bucketId)} in
   * chronological order and clears the bucket ring buffer.
   */
  public List<FlushCycleSample> drainFlushCycleSamples(String regionPath, String indexName,
      int bucketId) {
    IndexKey key = new IndexKey(normalizeRegionPath(regionPath), indexName);
    ConcurrentHashMap<Integer, FlushCycleRing> bucketMap = flushCycleRings.get(key);
    if (bucketMap == null) {
      return Collections.emptyList();
    }
    FlushCycleRing ring = bucketMap.get(bucketId);
    return ring == null ? Collections.emptyList() : ring.drain();
  }

  /**
   * Drains all recorded flush-cycle samples across all buckets for {@code (regionPath, indexName)}
   * and clears their ring buffers.
   */
  public List<FlushCycleSample> drainFlushCycleSamples(String regionPath, String indexName) {
    IndexKey key = new IndexKey(normalizeRegionPath(regionPath), indexName);
    ConcurrentHashMap<Integer, FlushCycleRing> bucketMap = flushCycleRings.get(key);
    if (bucketMap == null || bucketMap.isEmpty()) {
      return Collections.emptyList();
    }
    List<FlushCycleSample> all = new ArrayList<>();
    for (FlushCycleRing ring : bucketMap.values()) {
      all.addAll(ring.drain());
    }
    return all;
  }

  public long getLastHydrationEventsInspected() {
    return lastHydrationEventsInspected.get();
  }

  public long getLastHydrationKeysRestored() {
    return lastHydrationKeysRestored.get();
  }

  public void recordLastHydrationStats(long eventsInspected, long keysRestored) {
    lastHydrationEventsInspected.set(Math.max(0L, eventsInspected));
    lastHydrationKeysRestored.set(Math.max(0L, keysRestored));
  }

  public long getUnsupportedFilterFlushFallbackCount() {
    return unsupportedFilterFlushFallbackCount.get();
  }

  public long incrementUnsupportedFilterFlushFallbackCount() {
    return unsupportedFilterFlushFallbackCount.incrementAndGet();
  }

  public long getInlineMaskQueryCount() {
    return inlineMaskQueryCount.get();
  }

  public long incrementInlineMaskQueryCount() {
    return inlineMaskQueryCount.incrementAndGet();
  }

  public long getOversampleFallbackQueryCount() {
    return oversampleFallbackQueryCount.get();
  }

  public long incrementOversampleFallbackQueryCount() {
    return oversampleFallbackQueryCount.incrementAndGet();
  }

  private void releaseHydrationLatch(IndexKey key, int bucketId) {
    ConcurrentHashMap<Integer, CountDownLatch> bucketMap = hydrationBarriers.get(key);
    if (bucketMap != null) {
      CountDownLatch latch = bucketMap.remove(bucketId);
      if (latch != null) {
        latch.countDown();
      }
    }
  }

  private void releaseHydrationLatchesForIndex(IndexKey key) {
    ConcurrentHashMap<Integer, CountDownLatch> bucketMap = hydrationBarriers.remove(key);
    if (bucketMap != null) {
      bucketMap.values().forEach(CountDownLatch::countDown);
      bucketMap.clear();
    }
  }

  /**
   * Returns {@code true} if a registry instance is currently tracked for {@code cache}.
   */
  public static boolean hasCache(Cache cache) {
    if (cache == null) {
      return false;
    }
    synchronized (CACHE_REGISTRIES) {
      return CACHE_REGISTRIES.containsKey(cache);
    }
  }

  public void clearAll() {
    buffers.values().forEach(map -> {
      map.values().forEach(Tier0VectorDeltaBuffer::clear);
      map.clear();
    });
    buffers.clear();
    hydrationBarriers.values().forEach(map -> {
      map.values().forEach(CountDownLatch::countDown);
      map.clear();
    });
    hydrationBarriers.clear();
    flushCycleRings.values().forEach(map -> {
      map.values().forEach(FlushCycleRing::clear);
      map.clear();
    });
    flushCycleRings.clear();
  }

  /**
   * Executes a complete Theorem 2 dual-epoch flush cycle for a single bucket:
   * <ol>
   * <li>Atomically swaps {@code (E_active, empty) -> (newActive, E_flushing)}.</li>
   * <li>Applies every entry in the flush batch to {@code repository}.</li>
   * <li>Calls {@link IndexRepository#commitAndRefreshBlocking()} so Lucene {@code IndexWriter}
   * commit and {@code SearcherManager.maybeRefreshBlocking()} both finish before clearing
   * {@code E_flushing}.</li>
   * <li>Calls {@link Tier0VectorDeltaBuffer#completeFlush(long)} (or {@code abortFlush} on
   * failure).</li>
   * </ol>
   *
   * @return {@code true} if a non-empty batch was flushed, {@code false} otherwise
   */
  public boolean flushBucket(String regionPath, String indexName, int bucketId,
      IndexRepository repository,
      Function<Tier0VectorDeltaBuffer.DeltaEntry<Object>, Object> valueResolver)
      throws IOException {
    Objects.requireNonNull(repository, "repository must not be null");
    Objects.requireNonNull(valueResolver, "valueResolver must not be null");
    Tier0VectorDeltaBuffer<Object> buffer = getBuffer(regionPath, indexName, bucketId);
    if (buffer == null) {
      return false;
    }

    Tier0VectorDeltaBuffer.FlushBatch<Object> batch = buffer.beginFlush();
    if (batch == null) {
      return false;
    }

    boolean completed = false;
    try {
      for (Tier0VectorDeltaBuffer.DeltaEntry<Object> entry : batch.getEntries().values()) {
        if (entry.isTombstone()) {
          repository.delete(entry.getKey());
        } else {
          Object value = valueResolver.apply(entry);
          if (value != null) {
            repository.update(entry.getKey(), value);
          } else {
            repository.delete(entry.getKey());
          }
        }
      }
      repository.commitAndRefreshBlocking();
      buffer.completeFlush(batch.getEpoch());
      completed = true;
      return true;
    } finally {
      if (!completed) {
        buffer.abortFlush(batch.getEpoch());
      }
    }
  }

  static String normalizeRegionPath(String regionPath) {
    Objects.requireNonNull(regionPath, "regionPath must not be null");
    return regionPath.startsWith(SEPARATOR) ? regionPath : SEPARATOR + regionPath;
  }

  private static final class IndexKey {
    private final String regionPath;
    private final String indexName;

    IndexKey(String regionPath, String indexName) {
      this.regionPath = regionPath;
      this.indexName = Objects.requireNonNull(indexName, "indexName must not be null");
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof IndexKey)) {
        return false;
      }
      IndexKey that = (IndexKey) o;
      return regionPath.equals(that.regionPath) && indexName.equals(that.indexName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(regionPath, indexName);
    }
  }

  /**
   * Per-bucket true flush-cycle sample measured around AEQ commit/refresh and prior to Tier-0
   * eviction.
   */
  public static final class FlushCycleSample {
    private final int bucketId;
    private final int batchSize;
    private final int preEvictTier0Size;
    private final long flushDurationNanos;
    private final long recordedAtNanos;

    public FlushCycleSample(int bucketId, int batchSize, int preEvictTier0Size,
        long flushDurationNanos, long recordedAtNanos) {
      this.bucketId = bucketId;
      this.batchSize = batchSize;
      this.preEvictTier0Size = preEvictTier0Size;
      this.flushDurationNanos = flushDurationNanos;
      this.recordedAtNanos = recordedAtNanos;
    }

    public int getBucketId() {
      return bucketId;
    }

    public int getBatchSize() {
      return batchSize;
    }

    public int getPreEvictTier0Size() {
      return preEvictTier0Size;
    }

    public long getFlushDurationNanos() {
      return flushDurationNanos;
    }

    public long getRecordedAtNanos() {
      return recordedAtNanos;
    }
  }

  private static final class FlushCycleRing {
    private final FlushCycleSample[] ring;
    private int head;
    private int size;

    FlushCycleRing(int capacity) {
      this.ring = new FlushCycleSample[capacity];
    }

    synchronized void record(FlushCycleSample sample) {
      int tail = (head + size) % ring.length;
      ring[tail] = sample;
      if (size == ring.length) {
        head = (head + 1) % ring.length;
      } else {
        size++;
      }
    }

    synchronized List<FlushCycleSample> drain() {
      if (size == 0) {
        return Collections.emptyList();
      }
      List<FlushCycleSample> out = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        out.add(ring[(head + i) % ring.length]);
      }
      Arrays.fill(ring, null);
      head = 0;
      size = 0;
      return out;
    }

    synchronized void clear() {
      if (size > 0) {
        Arrays.fill(ring, null);
        head = 0;
        size = 0;
      }
    }
  }
}
