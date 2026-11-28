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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.IntConsumer;

import org.apache.lucene.store.AlreadyClosedException;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.cache.lucene.LuceneServiceProvider;
import org.apache.geode.cache.lucene.internal.InternalLuceneIndex;
import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.cache.lucene.internal.PartitionedRepositoryManager;
import org.apache.geode.cache.lucene.internal.repository.IndexRepository;
import org.apache.geode.cache.lucene.internal.repository.RepositoryManager;
import org.apache.geode.cache.partition.PartitionListener;
import org.apache.geode.cache.partition.PartitionListenerAdapter;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.internal.cache.AbstractBucketRegionQueue;
import org.apache.geode.internal.cache.BucketNotFoundException;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.ColocationHelper;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.RegionMap;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.wan.AbstractGatewaySenderEventProcessor;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.cache.wan.InternalGatewaySender;
import org.apache.geode.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderQueue;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderQueue;

/**
 * Synchronous region and bucket listener hooks that capture live vector mutations into per-bucket
 * {@link Tier0VectorDeltaBuffer}s via {@link Tier0VectorRegistry} and maintain Tier-0 consistency
 * across bucket rebalancing without forcing PDX domain-class deserialization.
 *
 * @param <K> region key type
 * @param <V> region value type
 */
public class Tier0VectorBucketListener<K, V> extends CacheListenerAdapter<K, V> {

  private static volatile BiConsumer<Integer, Object> beforeRecordPutHookForTest;
  private static volatile BiConsumer<Integer, Object> beforeRecordDestroyHookForTest;
  private static volatile IntConsumer afterBeginHydrationHookForTest;

  private final String indexName;
  private final Function<V, float[]> vectorExtractor;
  private final Function<V, Map<String, Object>> metadataExtractor;

  /**
   * Test-only hook invoked inside {@code afterPrimary} strictly after
   * {@link Tier0VectorRegistry#beginHydration} advances {@code ownershipGeneration}
   * ({@code t_gen_change}) and installs {@code hydrationLatch}, and strictly before AEQ/bucket
   * reconciliation begins.
   */
  public static void setAfterBeginHydrationHookForTest(IntConsumer hook) {
    afterBeginHydrationHookForTest = hook;
  }

  /**
   * Test-only hook invoked inside {@code afterCreate}/{@code afterUpdate} strictly after the
   * {@code BucketRegion} entry update, synchronous bucket replication, and AEQ enqueue and strictly
   * before {@link Tier0VectorRegistry#recordPut} acquires {@code pathBBarrierLock.readLock()}.
   */
  public static void setBeforeRecordPutHookForTest(BiConsumer<Integer, Object> hook) {
    beforeRecordPutHookForTest = hook;
  }

  /**
   * Test-only hook invoked inside {@code afterDestroy}/{@code afterInvalidate} strictly after the
   * {@code BucketRegion} tombstone update, synchronous bucket replication, and AEQ enqueue and
   * strictly before {@link Tier0VectorRegistry#recordDestroy} acquires
   * {@code pathBBarrierLock.readLock()}.
   */
  public static void setBeforeRecordDestroyHookForTest(BiConsumer<Integer, Object> hook) {
    beforeRecordDestroyHookForTest = hook;
  }

  public Tier0VectorBucketListener(String indexName, Function<V, float[]> vectorExtractor,
      Function<V, Map<String, Object>> metadataExtractor) {
    this.indexName = Objects.requireNonNull(indexName, "indexName must not be null");
    this.vectorExtractor =
        Objects.requireNonNull(vectorExtractor, "vectorExtractor must not be null");
    this.metadataExtractor =
        metadataExtractor != null ? metadataExtractor : v -> Collections.emptyMap();
  }

  @Override
  public void afterCreate(EntryEvent<K, V> event) {
    recordPutEvent(event);
  }

  @Override
  public void afterUpdate(EntryEvent<K, V> event) {
    recordPutEvent(event);
  }

  @Override
  public void afterDestroy(EntryEvent<K, V> event) {
    recordDestroyEvent(event);
  }

  @Override
  public void afterInvalidate(EntryEvent<K, V> event) {
    recordDestroyEvent(event);
  }

  @Override
  public void afterRegionDestroy(RegionEvent<K, V> event) {
    Region<K, V> region = event.getRegion();
    if (region == null) {
      return;
    }
    Cache cache = (Cache) region.getRegionService();
    if (cache != null) {
      Tier0VectorRegistry.forCache(cache).removeIndex(region.getFullPath(), indexName);
    }
  }

  private void recordPutEvent(EntryEvent<K, V> event) {
    Region<K, V> region = event.getRegion();
    if (!(region instanceof PartitionedRegion)) {
      return;
    }
    PartitionedRegion pr = (PartitionedRegion) region;
    int bucketId = pr.getKeyInfo(event.getKey(), event.getCallbackArgument()).getBucketId();
    Cache cache = (Cache) region.getRegionService();
    InternalCache internalCache = (cache instanceof InternalCache) ? (InternalCache) cache : null;
    boolean initialPdxReadSerialized = false;
    if (internalCache != null) {
      initialPdxReadSerialized = internalCache.getPdxReadSerializedOverride();
      internalCache.setPdxReadSerializedOverride(true);
    }
    Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(cache);
    registry.markIndexManaged(region.getFullPath(), indexName);
    Object eventId = extractEventId(event);
    try {
      V value = event.getNewValue();
      if (value == null) {
        if (eventId != null) {
          registry.recordNonVectorEvent(region.getFullPath(), indexName, bucketId, eventId);
        }
        return;
      }
      float[] vector = vectorExtractor.apply(value);
      if (vector == null || vector.length == 0) {
        if (eventId != null) {
          registry.recordNonVectorEvent(region.getFullPath(), indexName, bucketId, eventId);
        }
        return;
      }
      Map<String, Object> metadata = metadataExtractor.apply(value);
      BiConsumer<Integer, Object> hook = beforeRecordPutHookForTest;
      if (hook != null) {
        hook.accept(bucketId, event.getKey());
      }
      registry.recordPut(
          region.getFullPath(), indexName, bucketId, event.getKey(), vector, metadata, value,
          eventId);
    } finally {
      if (internalCache != null) {
        internalCache.setPdxReadSerializedOverride(initialPdxReadSerialized);
      }
    }
  }

  private void recordDestroyEvent(EntryEvent<K, V> event) {
    Region<K, V> region = event.getRegion();
    if (!(region instanceof PartitionedRegion)) {
      return;
    }
    PartitionedRegion pr = (PartitionedRegion) region;
    int bucketId = pr.getKeyInfo(event.getKey(), event.getCallbackArgument()).getBucketId();
    Cache cache = (Cache) region.getRegionService();
    Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(cache);
    registry.markIndexManaged(region.getFullPath(), indexName);
    Object eventId = extractEventId(event);
    BiConsumer<Integer, Object> hook = beforeRecordDestroyHookForTest;
    if (hook != null) {
      hook.accept(bucketId, event.getKey());
    }
    registry.recordDestroy(region.getFullPath(), indexName, bucketId, event.getKey(), eventId);
  }

  private static Object extractEventId(EntryEvent<?, ?> event) {
    if (event instanceof EntryEventImpl) {
      return ((EntryEventImpl) event).getEventId();
    }
    return null;
  }

  /**
   * Creates a {@link PartitionListener} paired with this listener's extractors that cleans up
   * stale Tier-0 buffers when buckets leave or demote and hydrates promoted primary buckets from
   * local bucket state with PDX read-serialized enabled and without overwriting concurrent live
   * tombstones or newer puts.
   */
  public PartitionListener createPartitionListener() {
    return new Tier0PartitionListener<>(indexName, vectorExtractor, metadataExtractor);
  }

  static final class Tier0PartitionListener<K, V> extends PartitionListenerAdapter {
    private final String indexName;
    private final Function<V, float[]> vectorExtractor;
    private final Function<V, Map<String, Object>> metadataExtractor;
    private volatile Region<K, V> region;

    Tier0PartitionListener(String indexName, Function<V, float[]> vectorExtractor,
        Function<V, Map<String, Object>> metadataExtractor) {
      this.indexName = indexName;
      this.vectorExtractor = vectorExtractor;
      this.metadataExtractor = metadataExtractor;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void afterRegionCreate(Region<?, ?> region) {
      this.region = (Region<K, V>) region;
      if (region != null && region.getRegionService() instanceof Cache) {
        Cache cache = (Cache) region.getRegionService();
        if (!cache.isClosed()) {
          Tier0VectorRegistry.forCache(cache).markIndexManaged(region.getFullPath(), indexName);
        }
      }
    }

    @Override
    public void afterPrimary(int bucketId) {
      afterPrimary(bucketId, null);
    }

    void afterPrimary(int bucketId, Runnable afterAeqBeforeFullScanHook) {
      Region<K, V> r = this.region;
      if (!(r instanceof PartitionedRegion)) {
        return;
      }
      PartitionedRegion pr = (PartitionedRegion) r;
      if (pr.getDataStore() == null) {
        return;
      }
      BucketRegion bucket = pr.getDataStore().getLocalBucketById(bucketId);
      Cache cache = (Cache) pr.getRegionService();
      if (cache == null || cache.isClosed()) {
        return;
      }
      Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(cache);
      if (bucket == null) {
        registry.advanceOwnershipGeneration(pr.getFullPath(), indexName, bucketId);
        return;
      }
      CountDownLatch hydrationLatch =
          registry.beginHydration(pr.getFullPath(), indexName, bucketId);
      try {
        IntConsumer beginHook = afterBeginHydrationHookForTest;
        if (beginHook != null) {
          beginHook.accept(bucketId);
        }
        Tier0VectorDeltaBuffer<Object> buffer =
            registry.getOrCreateBuffer(pr.getFullPath(), indexName, bucketId);
        // Live new-term puts/destroys (recordPut/recordDestroy) execute before or concurrently with
        // afterPrimary with sequence >= LIVE_SEQUENCE_START (1L), whereas provisional AEQ delta
        // hydration stamps PROVISIONAL_HYDRATION_SEQUENCE (0L). Using promotionStartSeq = 1L
        // enforces the single unified guard: overwrite iff (existing == null || existing.sequence
        // < 1L) && committedKeySequences < 1L.
        final long promotionStartSeq = Tier0VectorDeltaBuffer.LIVE_SEQUENCE_START;

        InternalCache internalCache =
            (cache instanceof InternalCache) ? (InternalCache) cache : null;
        boolean initialPdxReadSerialized = false;
        if (internalCache != null) {
          initialPdxReadSerialized = internalCache.getPdxReadSerializedOverride();
          internalCache.setPdxReadSerializedOverride(true);
        }
        try {
          AeqHydrationResult aeqResult =
              tryHydrateFromAeqDelta(cache, pr, bucket, bucketId, buffer, registry);
          if (afterAeqBeforeFullScanHook != null) {
            afterAeqBeforeFullScanHook.run();
          }
          // Unconditionally run authoritative bucket + Lucene reconciliation after AEQ delta using
          // sequence-fenced overwrites before releasing hydrationLatch.
          hydrateFromFullBucketScan(
              cache, pr, bucket, bucketId, buffer, registry, promotionStartSeq, aeqResult);
        } finally {
          if (internalCache != null) {
            internalCache.setPdxReadSerializedOverride(initialPdxReadSerialized);
          }
        }
      } finally {
        registry.completeHydration(pr.getFullPath(), indexName, bucketId, hydrationLatch);
      }
    }

    private AeqHydrationResult tryHydrateFromAeqDelta(Cache cache, PartitionedRegion pr,
        BucketRegion bucket, int bucketId, Tier0VectorDeltaBuffer<Object> buffer,
        Tier0VectorRegistry registry) {
      if (cache == null) {
        return null;
      }
      String aeqId = LuceneServiceImpl.getUniqueIndexName(indexName, pr.getFullPath());
      AsyncEventQueue aeq = cache.getAsyncEventQueue(aeqId);
      if (!(aeq instanceof AsyncEventQueueImpl)) {
        return null;
      }
      InternalGatewaySender sender = ((AsyncEventQueueImpl) aeq).getSender();
      AbstractGatewaySenderEventProcessor ep = sender != null ? sender.getEventProcessor() : null;
      RegionQueue regionQueue = ep != null ? ep.getQueue() : null;

      ParallelGatewaySenderQueue parallelQueue = null;
      ConcurrentParallelGatewaySenderQueue concurrentQueue = null;
      if (regionQueue instanceof ConcurrentParallelGatewaySenderQueue) {
        concurrentQueue = (ConcurrentParallelGatewaySenderQueue) regionQueue;
        RegionQueue bucketQueue = concurrentQueue.getQueueByBucket(bucketId);
        if (bucketQueue instanceof ParallelGatewaySenderQueue) {
          parallelQueue = (ParallelGatewaySenderQueue) bucketQueue;
        }
      } else if (regionQueue instanceof ParallelGatewaySenderQueue) {
        parallelQueue = (ParallelGatewaySenderQueue) regionQueue;
      } else {
        return null;
      }

      PartitionedRegion shadowPr = resolveShadowPr(parallelQueue, concurrentQueue, pr);
      if (shadowPr == null || shadowPr.isDestroyed() || shadowPr.getDataStore() == null) {
        return null;
      }
      BucketRegion shadowBucket = shadowPr.getDataStore().getLocalBucketById(bucketId);
      if (shadowBucket == null || shadowBucket.isDestroyed()) {
        return null;
      }
      if (shadowBucket instanceof AbstractBucketRegionQueue
          && shadowBucket.getRegionMap() != null
          && !((AbstractBucketRegionQueue) shadowBucket).isInitialized()) {
        return null;
      }

      BlockingQueue<GatewaySenderEventImpl> tempQueue = parallelQueue != null
          ? parallelQueue.getBucketToTempQueueMap().get(bucketId)
          : concurrentQueue.getBucketTmpQueue(bucketId);

      List<ShadowEventRecord> shadowEvents =
          collectUncommittedShadowEvents(shadowBucket, tempQueue, pr.getFullPath());
      Map<Object, GatewaySenderEventImpl> latestByUserKey = collapseLatestByUserKey(shadowEvents);

      registry.incrementAeqDeltaHydrationCount();
      AeqHydrationResult result = new AeqHydrationResult(shadowEvents.size());
      for (Map.Entry<Object, GatewaySenderEventImpl> entry : latestByUserKey.entrySet()) {
        Object key = entry.getKey();
        if (buffer.containsKey(key)) {
          continue;
        }
        if (isDestroyOrInvalidateEvent(entry.getValue())) {
          if (reconcileTombstone(key, buffer, registry, -1L, result)) {
            result.keysRestored++;
          }
        } else {
          if (hydrateLiveOrMissingEntry(bucket, key, buffer, registry, -1L, result)) {
            result.keysRestored++;
          }
        }
      }
      // Register every uncommitted shadow AEQ event identity against its hydrated sequence (or
      // non-vector marker) so post-promotion Path-A drains recognize pre-crash shadow events as
      // already linearized in Tier-0 while rejecting any stale events after Path-B eviction.
      for (ShadowEventRecord record : shadowEvents) {
        Object eventId = record.event.getEventId();
        Object userKey = record.event.getKey();
        if (eventId == null || userKey == null) {
          continue;
        }
        long keySeq = buffer.getKeySequence(userKey);
        if (keySeq >= Tier0VectorDeltaBuffer.PROVISIONAL_HYDRATION_SEQUENCE) {
          buffer.recordPinnedEvent(eventId, keySeq);
        } else {
          long committedSeq = buffer.getMaxCommittedSequence(userKey);
          if (committedSeq > 0L) {
            buffer.recordPinnedEvent(eventId, committedSeq);
          } else {
            buffer.recordNonVectorEvent(eventId);
          }
        }
      }
      registry.recordLastHydrationStats(result.eventsInspected, result.keysRestored);
      return result;
    }

    private PartitionedRegion resolveShadowPr(ParallelGatewaySenderQueue parallelQueue,
        ConcurrentParallelGatewaySenderQueue concurrentQueue, PartitionedRegion pr) {
      String regionPath = pr.getFullPath();
      PartitionedRegion shadowPr = getShadowPrForPath(parallelQueue, concurrentQueue, regionPath);
      if (shadowPr == null) {
        PartitionedRegion leader = ColocationHelper.getLeaderRegion(pr);
        if (leader != null && !leader.getFullPath().equals(regionPath)) {
          shadowPr = getShadowPrForPath(parallelQueue, concurrentQueue, leader.getFullPath());
        }
      }
      return shadowPr;
    }

    private PartitionedRegion getShadowPrForPath(ParallelGatewaySenderQueue parallelQueue,
        ConcurrentParallelGatewaySenderQueue concurrentQueue, String path) {
      if (parallelQueue != null) {
        PartitionedRegion shadowPr = parallelQueue.getRegion(path);
        if (shadowPr != null) {
          return shadowPr;
        }
      }
      return concurrentQueue != null ? concurrentQueue.getRegion(path) : null;
    }

    private List<ShadowEventRecord> collectUncommittedShadowEvents(BucketRegion shadowBucket,
        BlockingQueue<GatewaySenderEventImpl> tempQueue, String regionPath) {
      List<ShadowEventRecord> events = new ArrayList<>();
      int orderIndex = 0;
      Set<Object> shadowKeys = shadowBucket.keySet();
      if (shadowKeys != null && !shadowKeys.isEmpty()) {
        for (Object shadowKey : shadowKeys) {
          GatewaySenderEventImpl event = readShadowEvent(shadowBucket, shadowKey);
          if (event != null && matchesRegionPath(event, regionPath)) {
            events.add(new ShadowEventRecord(
                resolveEffectiveShadowKey(event, shadowKey),
                resolveEventTimestamp(event),
                0,
                orderIndex++,
                event));
          }
        }
      }
      if (tempQueue != null && !tempQueue.isEmpty()) {
        for (GatewaySenderEventImpl event : tempQueue) {
          if (event != null && matchesRegionPath(event, regionPath)) {
            events.add(new ShadowEventRecord(
                resolveEffectiveShadowKey(event, null),
                resolveEventTimestamp(event),
                1,
                orderIndex++,
                event));
          }
        }
      }
      if (events.size() > 1) {
        Collections.sort(events);
      }
      return events;
    }

    private static long resolveEffectiveShadowKey(GatewaySenderEventImpl event, Object mapKey) {
      Long eventShadowKey = event.getShadowKey();
      if (eventShadowKey != null && eventShadowKey > 0L) {
        return eventShadowKey;
      }
      if (mapKey instanceof Long) {
        return (Long) mapKey;
      }
      if (mapKey instanceof Number) {
        return ((Number) mapKey).longValue();
      }
      return eventShadowKey != null ? eventShadowKey : Long.MIN_VALUE;
    }

    private static long resolveEventTimestamp(GatewaySenderEventImpl event) {
      long versionTimestamp = event.getVersionTimeStamp();
      return versionTimestamp > 0L ? versionTimestamp : event.getCreationTime();
    }

    private GatewaySenderEventImpl readShadowEvent(BucketRegion shadowBucket, Object shadowKey) {
      Object raw = null;
      try {
        raw = shadowBucket.getValueInVMOrDiskWithoutFaultIn(shadowKey);
      } catch (EntryNotFoundException e) {
        return null;
      } catch (RuntimeException ignored) {
        // Mocked BucketRegion instances may leave internal methods unstubbed.
      }
      if (raw == null) {
        Region.Entry<?, ?> entry = shadowBucket.getEntry(shadowKey);
        if (entry != null) {
          try {
            raw = (entry instanceof EntrySnapshot)
                ? ((EntrySnapshot) entry).getRawValue(true)
                : entry.getValue();
          } catch (EntryDestroyedException ignored) {
            return null;
          }
        }
      }
      if (raw instanceof CachedDeserializable) {
        raw = ((CachedDeserializable) raw).getDeserializedValue(shadowBucket,
            shadowBucket.getRegionEntry(shadowKey));
      }
      if (raw == null || raw == Token.TOMBSTONE) {
        return null;
      }
      return (raw instanceof GatewaySenderEventImpl) ? (GatewaySenderEventImpl) raw : null;
    }

    private static boolean matchesRegionPath(GatewaySenderEventImpl event, String regionPath) {
      String eventRegionPath = event.getRegionPath();
      return eventRegionPath == null || regionPath.equals(eventRegionPath);
    }

    private static Map<Object, GatewaySenderEventImpl> collapseLatestByUserKey(
        List<ShadowEventRecord> events) {
      Map<Object, ShadowEventRecord> latestRecords = new LinkedHashMap<>(events.size());
      for (ShadowEventRecord record : events) {
        Object userKey = record.event.getKey();
        if (userKey != null) {
          latestRecords.merge(
              userKey, record, (existing, incoming) -> incoming.compareTo(existing) >= 0
                  ? incoming : existing);
        }
      }
      Map<Object, GatewaySenderEventImpl> latestByUserKey =
          new LinkedHashMap<>(latestRecords.size());
      for (Map.Entry<Object, ShadowEventRecord> entry : latestRecords.entrySet()) {
        latestByUserKey.put(entry.getKey(), entry.getValue().event);
      }
      return latestByUserKey;
    }

    private static final class ShadowEventRecord implements Comparable<ShadowEventRecord> {
      private final long effectiveShadowKey;
      private final long eventTimestamp;
      private final int sourceRank;
      private final int orderIndex;
      private final GatewaySenderEventImpl event;

      ShadowEventRecord(long effectiveShadowKey, long eventTimestamp, int sourceRank,
          int orderIndex, GatewaySenderEventImpl event) {
        this.effectiveShadowKey = effectiveShadowKey;
        this.eventTimestamp = eventTimestamp;
        this.sourceRank = sourceRank;
        this.orderIndex = orderIndex;
        this.event = event;
      }

      @Override
      public int compareTo(ShadowEventRecord other) {
        int cmp = Long.compare(this.effectiveShadowKey, other.effectiveShadowKey);
        if (cmp != 0) {
          return cmp;
        }
        cmp = Long.compare(this.eventTimestamp, other.eventTimestamp);
        if (cmp != 0) {
          return cmp;
        }
        cmp = Integer.compare(this.sourceRank, other.sourceRank);
        if (cmp != 0) {
          return cmp;
        }
        return Integer.compare(this.orderIndex, other.orderIndex);
      }
    }

    private static boolean isDestroyOrInvalidateEvent(GatewaySenderEventImpl event) {
      Operation op = event.getOperation();
      if (op != null) {
        return op.isDestroy() || op.isInvalidate();
      }
      int action = event.getAction();
      return action == 2 || action == 5;
    }

    private void hydrateFromFullBucketScan(Cache cache, PartitionedRegion pr, BucketRegion bucket,
        int bucketId, Tier0VectorDeltaBuffer<Object> buffer, Tier0VectorRegistry registry,
        long promotionStartSeq, AeqHydrationResult aeqResult) {
      if (aeqResult == null) {
        registry.incrementFullScanHydrationCount();
      }
      long eventsInspected = aeqResult != null ? aeqResult.eventsInspected : 0L;
      long keysRestored = aeqResult != null ? aeqResult.keysRestored : 0L;
      // Step 1: Hydrate every live replicated entry using sequence-fenced overwrite
      // (seq < promotionStartSeq or provisional AEQ entry) so authoritative bucket values win
      // over stale AEQ state while concurrent post-promotion writes (seq >= promotionStartSeq) win.
      Set<Object> liveKeys = bucket.keySet();
      Set<Object> liveKeySet = liveKeys == null ? Collections.emptySet() : new HashSet<>(liveKeys);
      for (Object key : liveKeySet) {
        eventsInspected++;
        if (hydrateLiveOrMissingEntry(
            bucket, key, buffer, registry, promotionStartSeq, aeqResult)) {
          keysRestored++;
        }
      }
      RegionMap regionMap = bucket.getRegionMap();
      if (regionMap != null) {
        for (RegionEntry re : regionMap.regionEntries()) {
          if (re != null) {
            boolean isTombstone;
            Object tombKey;
            // Synchronize on the per-key RegionEntry monitor held by RegionMapDestroy across
            // tombstone mutation -> synchronous rf>=1 backup replication -> AEQ enqueue ->
            // recordDestroy so the scan never observes an un-replicated tombstone.
            synchronized (re) {
              isTombstone = re.isTombstone();
              tombKey = isTombstone ? re.getKey() : null;
            }
            if (isTombstone && tombKey != null) {
              eventsInspected++;
              if (reconcileTombstone(
                  tombKey, buffer, registry, promotionStartSeq, aeqResult)) {
                keysRestored++;
              }
            }
          }
        }
      }
      try {
        Set<Object> indexedKeys = collectIndexedKeysForReconciliation(cache, pr, bucketId);
        if (indexedKeys != null && !indexedKeys.isEmpty()) {
          for (Object staleKey : indexedKeys) {
            if (staleKey == null || liveKeySet.contains(staleKey)) {
              continue;
            }
            eventsInspected++;
            boolean[] newlyRestored = new boolean[1];
            if (bucket.getEntry(staleKey) == null
                && applyTombstone(staleKey, buffer, registry, promotionStartSeq, aeqResult,
                    newlyRestored) >= 0L) {
              registry.incrementReconciledStaleIndexKeysCount();
              if (newlyRestored[0]) {
                keysRestored++;
              }
            }
          }
        }
      } catch (IOException | AlreadyClosedException ignored) {
        // Repository closed or transient store issue while moving bucket; ignore safely.
      }
      registry.recordLastHydrationStats(eventsInspected, keysRestored);
    }

    private Set<Object> collectIndexedKeysForReconciliation(Cache cache, PartitionedRegion pr,
        int bucketId) throws IOException {
      if (cache == null) {
        return Collections.emptySet();
      }
      try {
        LuceneService luceneService = LuceneServiceProvider.get(cache);
        if (luceneService == null) {
          return Collections.emptySet();
        }
        LuceneIndex index = luceneService.getIndex(indexName, pr.getFullPath());
        if (!(index instanceof InternalLuceneIndex)) {
          return Collections.emptySet();
        }
        RepositoryManager repoManager = ((InternalLuceneIndex) index).getRepositoryManager();
        if (repoManager instanceof PartitionedRepositoryManager) {
          PartitionedRepositoryManager prRepoManager = (PartitionedRepositoryManager) repoManager;
          Set<Object> indexedKeys = prRepoManager.collectIndexedKeysIfReady(bucketId);
          if (indexedKeys != null && !indexedKeys.isEmpty()) {
            return indexedKeys;
          }
          IndexRepository repo = prRepoManager.getRepositoryIfReady(bucketId);
          if (repo != null && !repo.isClosed()) {
            return repo.collectIndexedKeys();
          }
        }
      } catch (BucketNotFoundException | RuntimeException ignored) {
        // Bucket demoted, index destroyed, or mocked cache without Lucene service.
      }
      return Collections.emptySet();
    }

    private boolean hydrateLiveOrMissingEntry(BucketRegion bucket, Object key,
        Tier0VectorDeltaBuffer<Object> buffer, Tier0VectorRegistry registry,
        long promotionStartSeq, AeqHydrationResult aeqResult) {
      if (promotionStartSeq < 0L && buffer.containsKey(key)) {
        return false;
      }
      V val = readEntryValueWithoutDeserializingPdx(bucket, key);
      if (val == null) {
        return reconcileTombstone(key, buffer, registry, promotionStartSeq, aeqResult);
      }
      float[] vec = vectorExtractor.apply(val);
      if (vec == null || vec.length == 0) {
        return false;
      }
      Map<String, Object> metadata = metadataExtractor.apply(val);
      // Re-verify the entry still exists in the bucket; in authoritative bucket reconciliation
      // (promotionStartSeq >= 0), putIfSequenceBefore overwrites stale pre-promotion/provisional
      // entries while preserving any concurrent live post-promotion destroy or newer put.
      if (bucket.getEntry(key) != null) {
        long seq = promotionStartSeq >= 0L
            ? buffer.putIfSequenceBefore(key, vec, metadata, promotionStartSeq, val)
            : buffer.putProvisionalIfAbsent(key, vec, metadata, val);
        if (seq >= 0L) {
          if (aeqResult == null || aeqResult.hydratedKeys.add(key)) {
            registry.incrementHydratedKeysCount();
            return true;
          }
          return false;
        }
        return false;
      }
      return reconcileTombstone(key, buffer, registry, promotionStartSeq, aeqResult);
    }

    private static boolean reconcileTombstone(Object key, Tier0VectorDeltaBuffer<Object> buffer,
        Tier0VectorRegistry registry, long promotionStartSeq, AeqHydrationResult aeqResult) {
      boolean[] newlyRestored = new boolean[1];
      applyTombstone(key, buffer, registry, promotionStartSeq, aeqResult, newlyRestored);
      return newlyRestored[0];
    }

    private static long applyTombstone(Object key, Tier0VectorDeltaBuffer<Object> buffer,
        Tier0VectorRegistry registry, long promotionStartSeq, AeqHydrationResult aeqResult,
        boolean[] newlyRestoredOut) {
      long seq = promotionStartSeq >= 0L
          ? buffer.deleteIfSequenceBefore(key, promotionStartSeq)
          : buffer.deleteProvisionalIfAbsent(key);
      if (seq >= 0L) {
        if (aeqResult == null || aeqResult.hydratedTombstoneKeys.add(key)) {
          registry.incrementHydratedTombstonesCount();
        }
        if (aeqResult == null || aeqResult.hydratedKeys.add(key)) {
          registry.incrementHydratedKeysCount();
          if (newlyRestoredOut != null) {
            newlyRestoredOut[0] = true;
          }
        }
      }
      return seq;
    }

    private static final class AeqHydrationResult {
      final long eventsInspected;
      long keysRestored;
      final Set<Object> hydratedKeys = new HashSet<>();
      final Set<Object> hydratedTombstoneKeys = new HashSet<>();

      AeqHydrationResult(long eventsInspected) {
        this.eventsInspected = eventsInspected;
      }
    }

    @SuppressWarnings("unchecked")
    private V readEntryValueWithoutDeserializingPdx(BucketRegion bucket, Object key) {
      RegionEntry re = bucket.getRegionEntry(key);
      if (re != null) {
        // Synchronize on the per-key RegionEntry monitor held by AbstractRegionMapPut /
        // RegionMapDestroy across local mutation -> synchronous rf>=1 backup replication
        // (BucketRegion.basicPutPart2 / distributeDestroyOperation) -> AEQ enqueue ->
        // Tier0VectorBucketListener.recordPut / recordDestroy. Guarantees the hydration scan
        // never observes an un-replicated local write before backup replication completes,
        // preserving survival across any subsequent second promotion.
        synchronized (re) {
          return readEntryValueLocked(bucket, key);
        }
      }
      return readEntryValueLocked(bucket, key);
    }

    @SuppressWarnings("unchecked")
    private V readEntryValueLocked(BucketRegion bucket, Object key) {
      Region.Entry<?, ?> entry = bucket.getEntry(key);
      if (entry == null) {
        return null;
      }
      try {
        if (entry instanceof EntrySnapshot) {
          return (V) ((EntrySnapshot) entry).getRawValue(true);
        }
        return (V) entry.getValue();
      } catch (EntryDestroyedException e) {
        return null;
      }
    }

    @Override
    public void afterSecondary(int bucketId) {
      removeBucketBuffer(bucketId);
    }

    @Override
    public void afterBucketRemoved(int bucketId, Iterable<?> keys) {
      removeBucketBuffer(bucketId);
    }

    private void removeBucketBuffer(int bucketId) {
      Region<K, V> r = this.region;
      if (r == null) {
        return;
      }
      Cache cache = (Cache) r.getRegionService();
      if (cache != null && !cache.isClosed()) {
        Tier0VectorRegistry.forCache(cache).removeBucket(r.getFullPath(), indexName, bucketId);
      }
    }
  }
}
