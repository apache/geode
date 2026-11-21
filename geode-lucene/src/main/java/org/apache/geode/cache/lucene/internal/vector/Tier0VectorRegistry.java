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
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
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

  private static final Map<Cache, Tier0VectorRegistry> CACHE_REGISTRIES =
      Collections.synchronizedMap(new WeakHashMap<>());

  private final ConcurrentHashMap<IndexKey, ConcurrentHashMap<Integer, Tier0VectorDeltaBuffer<Object>>> buffers =
      new ConcurrentHashMap<>();

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
  }

  public void removeIndex(String regionPath, String indexName) {
    IndexKey key = new IndexKey(normalizeRegionPath(regionPath), indexName);
    ConcurrentHashMap<Integer, Tier0VectorDeltaBuffer<Object>> removed = buffers.remove(key);
    if (removed != null) {
      removed.values().forEach(Tier0VectorDeltaBuffer::clear);
      removed.clear();
    }
  }

  public void clearAll() {
    buffers.values().forEach(map -> {
      map.values().forEach(Tier0VectorDeltaBuffer::clear);
      map.clear();
    });
    buffers.clear();
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
}
