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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.partition.PartitionListener;
import org.apache.geode.cache.partition.PartitionListenerAdapter;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;

/**
 * Synchronous region and bucket listener hooks that capture live vector mutations into per-bucket
 * {@link Tier0VectorDeltaBuffer}s via {@link Tier0VectorRegistry} and maintain Tier-0 consistency
 * across bucket rebalancing.
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

  private void recordPutEvent(EntryEvent<K, V> event) {
    Region<K, V> region = event.getRegion();
    if (!(region instanceof PartitionedRegion)) {
      return;
    }
    PartitionedRegion pr = (PartitionedRegion) region;
    int bucketId = pr.getKeyInfo(event.getKey(), event.getCallbackArgument()).getBucketId();
    V value = event.getNewValue();
    if (value == null) {
      return;
    }
    float[] vector = vectorExtractor.apply(value);
    if (vector == null || vector.length == 0) {
      return;
    }
    Map<String, Object> metadata = metadataExtractor.apply(value);
    Cache cache = (Cache) region.getRegionService();
    Tier0VectorRegistry.forCache(cache).recordPut(
        region.getFullPath(), indexName, bucketId, event.getKey(), vector, metadata);
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
   * local bucket state so in-flight entries remain visible across bucket rebalancing even before
   * AEQ flush.
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
      if (bucket == null || bucket.isEmpty()) {
        return;
      }
      Cache cache = (Cache) pr.getRegionService();
      Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(cache);
      Tier0VectorDeltaBuffer<Object> buffer =
          registry.getOrCreateBuffer(pr.getFullPath(), indexName, bucketId);
      for (Object key : bucket.keySet()) {
        @SuppressWarnings("unchecked")
        V val = (V) bucket.get(key);
        if (val == null) {
          continue;
        }
        float[] vec = vectorExtractor.apply(val);
        if (vec != null && vec.length > 0) {
          buffer.put(key, vec, metadataExtractor.apply(val));
        }
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
