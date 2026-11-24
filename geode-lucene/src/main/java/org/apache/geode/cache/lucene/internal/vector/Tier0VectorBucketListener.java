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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.cache.partition.PartitionListener;
import org.apache.geode.cache.partition.PartitionListenerAdapter;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.internal.cache.AbstractBucketRegionQueue;
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
            hydrateFromFullBucketScan(bucket, buffer, registry);
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

      List<GatewaySenderEventImpl> shadowEvents =
          collectUncommittedShadowEvents(shadowBucket, tempQueue, pr.getFullPath());
      Map<Object, GatewaySenderEventImpl> latestByUserKey = collapseLatestByUserKey(shadowEvents);

      registry.incrementAeqDeltaHydrationCount();
      for (Map.Entry<Object, GatewaySenderEventImpl> entry : latestByUserKey.entrySet()) {
        Object key = entry.getKey();
        if (buffer.containsKey(key)) {
          continue;
        }
        if (isDestroyOrInvalidateEvent(entry.getValue())) {
          reconcileTombstone(key, buffer, registry);
        } else {
          hydrateLiveOrMissingEntry(bucket, key, buffer, registry);
        }
      }
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

    private List<GatewaySenderEventImpl> collectUncommittedShadowEvents(BucketRegion shadowBucket,
        BlockingQueue<GatewaySenderEventImpl> tempQueue, String regionPath) {
      List<GatewaySenderEventImpl> events = new ArrayList<>();
      Set<Object> shadowKeys = shadowBucket.keySet();
      if (shadowKeys != null && !shadowKeys.isEmpty()) {
        List<Object> orderedKeys = new ArrayList<>(shadowKeys);
        orderedKeys.sort((a, b) -> {
          if (a instanceof Long && b instanceof Long) {
            return Long.compare((Long) a, (Long) b);
          }
          return 0;
        });
        for (Object shadowKey : orderedKeys) {
          GatewaySenderEventImpl event = readShadowEvent(shadowBucket, shadowKey);
          if (event != null && matchesRegionPath(event, regionPath)) {
            events.add(event);
          }
        }
      }
      if (tempQueue != null && !tempQueue.isEmpty()) {
        for (GatewaySenderEventImpl event : tempQueue) {
          if (event != null && matchesRegionPath(event, regionPath)) {
            events.add(event);
          }
        }
      }
      if (events.size() > 1) {
        events.sort((a, b) -> Long.compare(a.getShadowKey(), b.getShadowKey()));
      }
      return events;
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
        List<GatewaySenderEventImpl> events) {
      Map<Object, GatewaySenderEventImpl> latestByUserKey = new LinkedHashMap<>(events.size());
      for (GatewaySenderEventImpl event : events) {
        Object userKey = event.getKey();
        if (userKey != null) {
          latestByUserKey.put(userKey, event);
        }
      }
      return latestByUserKey;
    }

    private static boolean isDestroyOrInvalidateEvent(GatewaySenderEventImpl event) {
      Operation op = event.getOperation();
      if (op != null) {
        return op.isDestroy() || op.isInvalidate();
      }
      int action = event.getAction();
      return action == 2 || action == 5;
    }

    private void hydrateFromFullBucketScan(BucketRegion bucket,
        Tier0VectorDeltaBuffer<Object> buffer, Tier0VectorRegistry registry) {
      registry.incrementFullScanHydrationCount();
      Set<Object> liveKeys = bucket.keySet();
      if (liveKeys != null) {
        for (Object key : liveKeys) {
          hydrateLiveOrMissingEntry(bucket, key, buffer, registry);
        }
      }
      RegionMap regionMap = bucket.getRegionMap();
      if (regionMap != null) {
        for (RegionEntry re : regionMap.regionEntries()) {
          if (re != null && re.isTombstone()) {
            Object tombKey = re.getKey();
            if (tombKey != null && !buffer.containsKey(tombKey)) {
              reconcileTombstone(tombKey, buffer, registry);
            }
          }
        }
      }
    }

    private void hydrateLiveOrMissingEntry(BucketRegion bucket, Object key,
        Tier0VectorDeltaBuffer<Object> buffer, Tier0VectorRegistry registry) {
      if (buffer.containsKey(key)) {
        return;
      }
      V val = readEntryValueWithoutDeserializingPdx(bucket, key);
      if (val == null) {
        reconcileTombstone(key, buffer, registry);
        return;
      }
      float[] vec = vectorExtractor.apply(val);
      if (vec == null || vec.length == 0) {
        return;
      }
      Map<String, Object> metadata = metadataExtractor.apply(val);
      // Re-verify the entry still exists in the bucket and use putIfAbsent so hydration can
      // NEVER overwrite a concurrent live afterDestroy tombstone or newer afterUpdate put.
      if (bucket.getEntry(key) != null) {
        if (buffer.putIfAbsent(key, vec, metadata) >= 0L) {
          registry.incrementHydratedKeysCount();
        }
      } else {
        reconcileTombstone(key, buffer, registry);
      }
    }

    private static void reconcileTombstone(Object key, Tier0VectorDeltaBuffer<Object> buffer,
        Tier0VectorRegistry registry) {
      if (buffer.deleteIfAbsent(key) >= 0L) {
        registry.incrementHydratedKeysCount();
        registry.incrementHydratedTombstonesCount();
      }
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
