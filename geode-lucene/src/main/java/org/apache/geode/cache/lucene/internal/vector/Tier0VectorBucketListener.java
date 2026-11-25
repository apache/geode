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
import java.util.function.Function;

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

  private final String indexName;
  private final Function<V, float[]> vectorExtractor;
  private final Function<V, Map<String, Object>> metadataExtractor;

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
    try {
      V value = event.getNewValue();
      if (value == null) {
        return;
      }
      float[] vector = vectorExtractor.apply(value);
      if (vector == null || vector.length == 0) {
        return;
      }
      Map<String, Object> metadata = metadataExtractor.apply(value);
      Tier0VectorRegistry.forCache(cache).recordPut(
          region.getFullPath(), indexName, bucketId, event.getKey(), vector, metadata);
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
    Tier0VectorRegistry.forCache(cache).recordDestroy(
        region.getFullPath(), indexName, bucketId, event.getKey());
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

  private static final class Tier0PartitionListener<K, V> extends PartitionListenerAdapter {
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
    }

    @Override
    public void afterPrimary(int bucketId) {
      Region<K, V> r = this.region;
      if (!(r instanceof PartitionedRegion)) {
        return;
      }
      PartitionedRegion pr = (PartitionedRegion) r;
      if (pr.getDataStore() == null) {
        return;
      }
      BucketRegion bucket = pr.getDataStore().getLocalBucketById(bucketId);
      if (bucket == null) {
        return;
      }
      Cache cache = (Cache) pr.getRegionService();
      Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(cache);
      CountDownLatch hydrationLatch =
          registry.beginHydration(pr.getFullPath(), indexName, bucketId);
      try {
        Tier0VectorDeltaBuffer<Object> buffer =
            registry.getOrCreateBuffer(pr.getFullPath(), indexName, bucketId);

        InternalCache internalCache =
            (cache instanceof InternalCache) ? (InternalCache) cache : null;
        boolean initialPdxReadSerialized = false;
        if (internalCache != null) {
          initialPdxReadSerialized = internalCache.getPdxReadSerializedOverride();
          internalCache.setPdxReadSerializedOverride(true);
        }
        try {
          if (!tryHydrateFromAeqDelta(cache, pr, bucket, bucketId, buffer, registry)) {
            hydrateFromFullBucketScan(cache, pr, bucket, bucketId, buffer, registry);
          }
        } finally {
          if (internalCache != null) {
            internalCache.setPdxReadSerializedOverride(initialPdxReadSerialized);
          }
        }
      } finally {
        registry.completeHydration(pr.getFullPath(), indexName, bucketId, hydrationLatch);
      }
    }

    private boolean tryHydrateFromAeqDelta(Cache cache, PartitionedRegion pr, BucketRegion bucket,
        int bucketId, Tier0VectorDeltaBuffer<Object> buffer, Tier0VectorRegistry registry) {
      if (cache == null) {
        return false;
      }
      String aeqId = LuceneServiceImpl.getUniqueIndexName(indexName, pr.getFullPath());
      AsyncEventQueue aeq = cache.getAsyncEventQueue(aeqId);
      if (!(aeq instanceof AsyncEventQueueImpl)) {
        return false;
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
        return false;
      }

      PartitionedRegion shadowPr = resolveShadowPr(parallelQueue, concurrentQueue, pr);
      if (shadowPr == null || shadowPr.isDestroyed() || shadowPr.getDataStore() == null) {
        return false;
      }
      BucketRegion shadowBucket = shadowPr.getDataStore().getLocalBucketById(bucketId);
      if (shadowBucket == null || shadowBucket.isDestroyed()) {
        return false;
      }
      if (shadowBucket instanceof AbstractBucketRegionQueue
          && shadowBucket.getRegionMap() != null
          && !((AbstractBucketRegionQueue) shadowBucket).isInitialized()) {
        return false;
      }

      BlockingQueue<GatewaySenderEventImpl> tempQueue = parallelQueue != null
          ? parallelQueue.getBucketToTempQueueMap().get(bucketId)
          : concurrentQueue.getBucketTmpQueue(bucketId);

      List<ShadowEventRecord> shadowEvents =
          collectUncommittedShadowEvents(shadowBucket, tempQueue, pr.getFullPath());
      Map<Object, GatewaySenderEventImpl> latestByUserKey = collapseLatestByUserKey(shadowEvents);

      registry.incrementAeqDeltaHydrationCount();
      long keysRestored = 0L;
      for (Map.Entry<Object, GatewaySenderEventImpl> entry : latestByUserKey.entrySet()) {
        Object key = entry.getKey();
        if (buffer.containsKey(key)) {
          continue;
        }
        if (isDestroyOrInvalidateEvent(entry.getValue())) {
          if (reconcileTombstone(key, buffer, registry)) {
            keysRestored++;
          }
        } else {
          if (hydrateLiveOrMissingEntry(bucket, key, buffer, registry)) {
            keysRestored++;
          }
        }
      }
      registry.recordLastHydrationStats(shadowEvents.size(), keysRestored);
      return true;
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
        int bucketId, Tier0VectorDeltaBuffer<Object> buffer, Tier0VectorRegistry registry) {
      registry.incrementFullScanHydrationCount();
      long eventsInspected = 0L;
      long keysRestored = 0L;
      Set<Object> liveKeys = bucket.keySet();
      Set<Object> liveKeySet = liveKeys == null ? Collections.emptySet() : new HashSet<>(liveKeys);
      for (Object key : liveKeySet) {
        eventsInspected++;
        if (hydrateLiveOrMissingEntry(bucket, key, buffer, registry)) {
          keysRestored++;
        }
      }
      RegionMap regionMap = bucket.getRegionMap();
      if (regionMap != null) {
        for (RegionEntry re : regionMap.regionEntries()) {
          if (re != null && re.isTombstone()) {
            eventsInspected++;
            Object tombKey = re.getKey();
            if (tombKey != null && !buffer.containsKey(tombKey)
                && reconcileTombstone(tombKey, buffer, registry)) {
              keysRestored++;
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
            if (!buffer.containsKey(staleKey) && bucket.getEntry(staleKey) == null
                && reconcileTombstone(staleKey, buffer, registry)) {
              registry.incrementReconciledStaleIndexKeysCount();
              keysRestored++;
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
        Tier0VectorDeltaBuffer<Object> buffer, Tier0VectorRegistry registry) {
      if (buffer.containsKey(key)) {
        return false;
      }
      V val = readEntryValueWithoutDeserializingPdx(bucket, key);
      if (val == null) {
        return reconcileTombstone(key, buffer, registry);
      }
      float[] vec = vectorExtractor.apply(val);
      if (vec == null || vec.length == 0) {
        return false;
      }
      Map<String, Object> metadata = metadataExtractor.apply(val);
      // Re-verify the entry still exists in the bucket and use putIfAbsent so hydration can
      // NEVER overwrite a concurrent live afterDestroy tombstone or newer afterUpdate put.
      if (bucket.getEntry(key) != null) {
        if (buffer.putIfAbsent(key, vec, metadata) >= 0L) {
          registry.incrementHydratedKeysCount();
          return true;
        }
        return false;
      }
      return reconcileTombstone(key, buffer, registry);
    }

    private static boolean reconcileTombstone(Object key, Tier0VectorDeltaBuffer<Object> buffer,
        Tier0VectorRegistry registry) {
      if (buffer.deleteIfAbsent(key) >= 0L) {
        registry.incrementHydratedKeysCount();
        registry.incrementHydratedTombstonesCount();
        return true;
      }
      return false;
    }

    @SuppressWarnings("unchecked")
    private V readEntryValueWithoutDeserializingPdx(BucketRegion bucket, Object key) {
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
