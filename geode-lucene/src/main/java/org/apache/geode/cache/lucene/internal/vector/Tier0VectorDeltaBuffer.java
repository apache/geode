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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import org.apache.lucene.index.VectorSimilarityFunction;

import org.apache.geode.cache.lucene.internal.distributed.EntryScore;

/**
 * Per-bucket lock-free dual-epoch {@code (E_active, E_flushing)} Tier-0 vector delta buffer.
 *
 * <p>
 * Eliminates the read-after-write visibility gap between synchronous partitioned-region mutations
 * and asynchronous Lucene HNSW segment commits:
 * <ol>
 * <li>Live {@link #put(Object, float[], Map)} and {@link #destroy(Object)} mutations are captured
 * synchronously into {@code E_active} under a monotonically increasing stamped epoch and sequence
 * counter.</li>
 * <li>{@link #beginFlush()} atomically swaps {@code (E_active, empty) -> (newActive, E_flushing)}
 * and drains in-flight writers pinned to the old epoch without blocking new writes.</li>
 * <li>{@link #completeFlush(long)} clears {@code E_flushing} ONLY strictly after
 * {@code IndexWriter.commit()} and {@code SearcherManager.maybeRefreshBlocking()} complete.</li>
 * <li>Readers take a wait-free {@link #snapshot()} in reverse epoch order
 * ({@code E_active > E_flushing > Lucene}), applying highest-epoch precedence and masking
 * superseded or tombstoned HNSW docIDs in committed Lucene segments.</li>
 * </ol>
 *
 * @param <K> region key type
 */
public class Tier0VectorDeltaBuffer<K> {

  private static final int SEALED_BIT = 0x80000000;
  private static final int WRITER_MASK = ~SEALED_BIT;

  private final int bucketId;
  private final AtomicLong sequenceGenerator = new AtomicLong(0L);
  private final AtomicLong lastClearedEpoch = new AtomicLong(-1L);
  private final AtomicReference<EpochState<K>> stateRef;

  public Tier0VectorDeltaBuffer(int bucketId) {
    this.bucketId = bucketId;
    EpochBucket<K> initialActive = new EpochBucket<>(0L, 0L);
    this.stateRef = new AtomicReference<>(new EpochState<>(initialActive, null));
  }

  /**
   * Captures a live vector put synchronously in {@code E_active} with empty metadata.
   *
   * @return monotonically increasing mutation sequence number
   */
  public long put(K key, float[] vector) {
    return put(key, vector, Collections.emptyMap());
  }

  /**
   * Captures a live vector put and associated metadata synchronously in {@code E_active}.
   *
   * @param key region key (non-null)
   * @param vector embedding vector (non-null, non-empty; defensively copied)
   * @param metadata optional metadata fields for pre-filter evaluation
   * @return monotonically increasing mutation sequence number
   */
  public long put(K key, float[] vector, Map<String, Object> metadata) {
    Objects.requireNonNull(key, "key must not be null");
    Objects.requireNonNull(vector, "vector must not be null");
    if (vector.length == 0) {
      throw new IllegalArgumentException("vector must not be empty");
    }
    float[] vectorCopy = Arrays.copyOf(vector, vector.length);
    Map<String, Object> metadataCopy = (metadata == null || metadata.isEmpty())
        ? Collections.emptyMap()
        : Collections.unmodifiableMap(new HashMap<>(metadata));

    return recordMutation(key, vectorCopy, metadataCopy, false);
  }

  /**
   * Inserts a live vector entry into {@code E_active} ONLY if {@code key} is currently absent from
   * both {@code E_active} and {@code E_flushing} (including tombstones). Used during primary
   * bucket hydration so hydration never overwrites a concurrent live {@code destroy} tombstone or
   * newer {@code put}.
   *
   * @return monotonically increasing sequence number if inserted, or {@code -1L} if {@code key}
   *         was already present
   */
  public long putIfAbsent(K key, float[] vector, Map<String, Object> metadata) {
    Objects.requireNonNull(key, "key must not be null");
    Objects.requireNonNull(vector, "vector must not be null");
    if (vector.length == 0) {
      throw new IllegalArgumentException("vector must not be empty");
    }
    float[] vectorCopy = Arrays.copyOf(vector, vector.length);
    Map<String, Object> metadataCopy = (metadata == null || metadata.isEmpty())
        ? Collections.emptyMap()
        : Collections.unmodifiableMap(new HashMap<>(metadata));

    while (true) {
      EpochState<K> state = stateRef.get();
      EpochBucket<K> active = state.active;
      int writerCount = active.writerState.get();
      if ((writerCount & SEALED_BIT) != 0) {
        Thread.onSpinWait();
        continue;
      }
      if (!active.writerState.compareAndSet(writerCount, writerCount + 1)) {
        continue;
      }
      try {
        if (state.flushing != null && state.flushing.entries.containsKey(key)) {
          return -1L;
        }
        long[] insertedSeq = new long[] {-1L};
        active.entries.computeIfAbsent(key, k -> {
          long seq = sequenceGenerator.incrementAndGet();
          insertedSeq[0] = seq;
          return new DeltaEntry<>(k, vectorCopy, metadataCopy, false, active.epoch, seq);
        });
        return insertedSeq[0];
      } finally {
        active.writerState.decrementAndGet();
      }
    }
  }

  /**
   * Captures a tombstone deletion synchronously in {@code E_active} to mask any stale vector
   * committed in Lucene HNSW segments or {@code E_flushing}.
   *
   * @param key region key (non-null)
   * @return monotonically increasing mutation sequence number
   */
  public long destroy(K key) {
    Objects.requireNonNull(key, "key must not be null");
    return recordMutation(key, null, Collections.emptyMap(), true);
  }

  private long recordMutation(K key, float[] vector, Map<String, Object> metadata,
      boolean tombstone) {
    while (true) {
      EpochBucket<K> active = stateRef.get().active;
      int writerCount = active.writerState.get();
      if ((writerCount & SEALED_BIT) != 0) {
        Thread.onSpinWait();
        continue;
      }
      if (!active.writerState.compareAndSet(writerCount, writerCount + 1)) {
        continue;
      }
      try {
        long seq = sequenceGenerator.incrementAndGet();
        DeltaEntry<K> entry =
            new DeltaEntry<>(key, vector, metadata, tombstone, active.epoch, seq);
        active.entries.compute(key,
            (k, existing) -> (existing == null || seq > existing.sequence) ? entry : existing);
        return seq;
      } finally {
        active.writerState.decrementAndGet();
      }
    }
  }

  /**
   * Atomically swaps {@code (E_active, empty) -> (newActive, E_flushing)} and waits for any
   * in-flight writers pinned to the previous active epoch to complete before returning the
   * quiescent flush batch.
   *
   * <p>
   * Returns {@code null} if {@code E_active} is empty after draining writers. Throws
   * {@link IllegalStateException} if a previous flush epoch has not been completed or aborted.
   */
  public FlushBatch<K> beginFlush() {
    while (true) {
      EpochState<K> current = stateRef.get();
      if (current.flushing != null) {
        throw new IllegalStateException(
            "Flush already in progress for bucket " + bucketId + " (flushingEpoch="
                + current.flushing.epoch + ")");
      }
      EpochBucket<K> oldActive = current.active;
      EpochBucket<K> newActive = new EpochBucket<>(oldActive.epoch + 1L, oldActive.generation);
      EpochState<K> next = new EpochState<>(newActive, oldActive);
      if (stateRef.compareAndSet(current, next)) {
        // Seal oldActive so no new writers can pin it, then wait for pinned writers to finish.
        oldActive.writerState.getAndUpdate(v -> v | SEALED_BIT);
        while ((oldActive.writerState.get() & WRITER_MASK) != 0) {
          Thread.onSpinWait();
        }
        if (oldActive.entries.isEmpty()) {
          // Nothing to flush; atomically clear flushing while preserving any new active writes.
          clearFlushingEpoch(oldActive.epoch);
          return null;
        }
        return new FlushBatch<>(oldActive.epoch,
            Collections.unmodifiableMap(new HashMap<>(oldActive.entries)));
      }
    }
  }

  /**
   * Clears {@code E_flushing} after {@code IndexWriter.commit()} and
   * {@code SearcherManager.maybeRefreshBlocking()} have both completed for {@code flushEpoch}.
   *
   * @param flushEpoch the epoch returned by {@link #beginFlush()}
   */
  public void completeFlush(long flushEpoch) {
    while (true) {
      EpochState<K> current = stateRef.get();
      if (current.flushing == null || current.flushing.epoch != flushEpoch) {
        if (flushEpoch <= lastClearedEpoch.get()) {
          return;
        }
        throw new IllegalStateException(
            "Cannot complete flush for epoch " + flushEpoch + " on bucket " + bucketId
                + " (current flushingEpoch="
                + (current.flushing == null ? -1L : current.flushing.epoch) + ")");
      }
      EpochState<K> next = new EpochState<>(current.active, null);
      if (stateRef.compareAndSet(current, next)) {
        return;
      }
    }
  }

  /**
   * Restores un-superseded entries from {@code E_flushing} back into {@code E_active} and clears
   * {@code E_flushing} when a Lucene commit/refresh fails, ensuring zero lost visibility.
   *
   * <p>
   * Guarded by the buffer's monotonic {@code clear} generation so a concurrent {@link #clear()}
   * immediately cancels restoration and cleared entries can never be resurrected.
   *
   * @param flushEpoch the epoch returned by {@link #beginFlush()}
   */
  public void abortFlush(long flushEpoch) {
    EpochState<K> current = stateRef.get();
    if (current.flushing == null || current.flushing.epoch != flushEpoch) {
      if (flushEpoch <= lastClearedEpoch.get()) {
        return;
      }
      throw new IllegalStateException(
          "Cannot abort flush for epoch " + flushEpoch + " on bucket " + bucketId);
    }
    EpochBucket<K> flushing = current.flushing;
    long expectedGeneration = flushing.generation;
    for (Map.Entry<K, DeltaEntry<K>> entry : flushing.entries.entrySet()) {
      if (!restoreEntryToActive(entry.getKey(), entry.getValue(), flushing, expectedGeneration)) {
        return;
      }
    }
    clearFlushingEpoch(flushEpoch);
  }

  private boolean restoreEntryToActive(K key, DeltaEntry<K> oldEntry, EpochBucket<K> expectedFlush,
      long expectedGeneration) {
    while (true) {
      EpochState<K> state = stateRef.get();
      if (state.active.generation != expectedGeneration || state.flushing != expectedFlush) {
        // Concurrent clear() (or flush resolution) invalidated this flush epoch — never resurrect.
        return false;
      }
      EpochBucket<K> active = state.active;
      int writerCount = active.writerState.get();
      if ((writerCount & SEALED_BIT) != 0) {
        Thread.onSpinWait();
        continue;
      }
      if (!active.writerState.compareAndSet(writerCount, writerCount + 1)) {
        continue;
      }
      try {
        if (stateRef.get().active.generation != expectedGeneration) {
          return false;
        }
        active.entries.compute(key,
            (k, existing) -> (existing == null || oldEntry.sequence > existing.sequence)
                ? oldEntry : existing);
        return true;
      } finally {
        active.writerState.decrementAndGet();
      }
    }
  }

  private void clearFlushingEpoch(long flushEpoch) {
    while (true) {
      EpochState<K> current = stateRef.get();
      if (current.flushing == null || current.flushing.epoch != flushEpoch) {
        return;
      }
      EpochState<K> next = new EpochState<>(current.active, null);
      if (stateRef.compareAndSet(current, next)) {
        return;
      }
    }
  }

  /**
   * Evicts specific keys from both {@code E_active} and {@code E_flushing} whose stamped sequence
   * number is less than or equal to the sequence committed to Lucene (strictly after
   * {@code commitAndRefreshBlocking()}). Pins {@code E_active} against concurrent
   * {@link #beginFlush()} epoch swaps so committed entries are never stranded in
   * {@code E_flushing}.
   *
   * @param committedKeySequences map from key to the highest sequence committed to Lucene
   */
  public void evictCommitted(Map<K, Long> committedKeySequences) {
    if (committedKeySequences == null || committedKeySequences.isEmpty()) {
      return;
    }
    while (true) {
      EpochState<K> state = stateRef.get();
      EpochBucket<K> active = state.active;
      int writerCount = active.writerState.get();
      if ((writerCount & SEALED_BIT) != 0) {
        Thread.onSpinWait();
        continue;
      }
      if (!active.writerState.compareAndSet(writerCount, writerCount + 1)) {
        continue;
      }
      try {
        EpochBucket<K> flushing = state.flushing;
        for (Map.Entry<K, Long> item : committedKeySequences.entrySet()) {
          K key = item.getKey();
          long committedSeq = item.getValue();
          active.entries.computeIfPresent(key,
              (k, existing) -> existing.sequence <= committedSeq ? null : existing);
          if (flushing != null) {
            flushing.entries.computeIfPresent(key,
                (k, existing) -> existing.sequence <= committedSeq ? null : existing);
          }
        }
        return;
      } finally {
        active.writerState.decrementAndGet();
      }
    }
  }

  /**
   * Returns {@code true} if {@code key} is currently present (live or tombstoned) in either
   * {@code E_active} or {@code E_flushing}.
   */
  public boolean containsKey(K key) {
    EpochState<K> state = stateRef.get();
    if (state.active.entries.containsKey(key)) {
      return true;
    }
    return state.flushing != null && state.flushing.entries.containsKey(key);
  }

  /**
   * Returns the current stamped sequence number for {@code key} across
   * {@code (E_active, E_flushing)}, or {@code -1L} if absent.
   */
  public long getKeySequence(K key) {
    EpochState<K> state = stateRef.get();
    DeltaEntry<K> activeEntry = state.active.entries.get(key);
    if (activeEntry != null) {
      return activeEntry.sequence;
    }
    if (state.flushing != null) {
      DeltaEntry<K> flushingEntry = state.flushing.entries.get(key);
      if (flushingEntry != null) {
        return flushingEntry.sequence;
      }
    }
    return -1L;
  }

  /**
   * Takes a wait-free consistent snapshot of {@code (E_active, E_flushing)} resolved in reverse
   * epoch order ({@code E_active} takes precedence over {@code E_flushing}; highest sequence wins
   * per key).
   *
   * <p>
   * Callers performing hybrid search MUST invoke {@link #snapshot()} BEFORE acquiring/querying the
   * Lucene {@code IndexSearcher} to guarantee zero visibility gap across concurrent flush commits.
   */
  public Snapshot<K> snapshot() {
    EpochState<K> state = stateRef.get();
    Map<K, DeltaEntry<K>> merged = new HashMap<>();

    // First populate from E_flushing (older epoch)
    if (state.flushing != null) {
      for (Map.Entry<K, DeltaEntry<K>> e : state.flushing.entries.entrySet()) {
        merged.put(e.getKey(), e.getValue());
      }
    }

    // Overlay E_active (newer epoch); highest sequence wins per key
    for (Map.Entry<K, DeltaEntry<K>> e : state.active.entries.entrySet()) {
      DeltaEntry<K> activeEntry = e.getValue();
      merged.merge(e.getKey(), activeEntry,
          (existing, incoming) -> incoming.sequence >= existing.sequence ? incoming : existing);
    }

    Map<K, DeltaEntry<K>> liveEntries = new HashMap<>();
    Set<K> tombstonedKeys = new HashSet<>();
    Set<K> maskedKeys = new HashSet<>(merged.size());

    for (Map.Entry<K, DeltaEntry<K>> e : merged.entrySet()) {
      K key = e.getKey();
      DeltaEntry<K> entry = e.getValue();
      maskedKeys.add(key);
      if (entry.tombstone) {
        tombstonedKeys.add(key);
      } else {
        liveEntries.put(key, entry);
      }
    }

    return new Snapshot<>(
        state.active.epoch,
        state.flushing == null ? -1L : state.flushing.epoch,
        Collections.unmodifiableMap(liveEntries),
        Collections.unmodifiableSet(tombstonedKeys),
        Collections.unmodifiableSet(maskedKeys));
  }

  /**
   * Convenience exact top-k vector search directly over the current Tier-0 snapshot.
   */
  public List<EntryScore<K>> search(float[] queryVector, int k,
      VectorSimilarityFunction similarity, Predicate<Map<String, Object>> metadataFilter) {
    return snapshot().search(queryVector, k, similarity, metadataFilter);
  }

  /**
   * Clears both epochs and advances the clear generation so any in-flight {@link #abortFlush(long)}
   * cannot resurrect cleared entries (used when a bucket is removed or destroyed).
   */
  public void clear() {
    while (true) {
      EpochState<K> current = stateRef.get();
      EpochBucket<K> fresh =
          new EpochBucket<>(current.active.epoch + 1L, current.active.generation + 1L);
      if (stateRef.compareAndSet(current, new EpochState<>(fresh, null))) {
        lastClearedEpoch.accumulateAndGet(current.active.epoch, Math::max);
        current.active.writerState.getAndUpdate(v -> v | SEALED_BIT);
        return;
      }
    }
  }

  public int getBucketId() {
    return bucketId;
  }

  public int getActiveSize() {
    return stateRef.get().active.entries.size();
  }

  public int getFlushingSize() {
    EpochBucket<K> flushing = stateRef.get().flushing;
    return flushing == null ? 0 : flushing.entries.size();
  }

  public int getTotalSize() {
    EpochState<K> state = stateRef.get();
    return state.active.entries.size()
        + (state.flushing == null ? 0 : state.flushing.entries.size());
  }

  public long getCurrentEpoch() {
    return stateRef.get().active.epoch;
  }

  public long getFlushingEpoch() {
    EpochBucket<K> flushing = stateRef.get().flushing;
    return flushing == null ? -1L : flushing.epoch;
  }

  public boolean isFlushing() {
    return stateRef.get().flushing != null;
  }

  public long getCurrentSequence() {
    return sequenceGenerator.get();
  }

  /**
   * Immutable stamped mutation record stored in a Tier-0 epoch bucket.
   */
  public static final class DeltaEntry<K> {
    private final K key;
    private final float[] vector;
    private final Map<String, Object> metadata;
    private final boolean tombstone;
    private final long epoch;
    private final long sequence;

    DeltaEntry(K key, float[] vector, Map<String, Object> metadata, boolean tombstone, long epoch,
        long sequence) {
      this.key = key;
      this.vector = vector;
      this.metadata = metadata;
      this.tombstone = tombstone;
      this.epoch = epoch;
      this.sequence = sequence;
    }

    public K getKey() {
      return key;
    }

    public float[] getVector() {
      return vector == null ? null : Arrays.copyOf(vector, vector.length);
    }

    float[] getVectorUnsafe() {
      return vector;
    }

    public Map<String, Object> getMetadata() {
      return metadata;
    }

    public boolean isTombstone() {
      return tombstone;
    }

    public long getEpoch() {
      return epoch;
    }

    public long getSequence() {
      return sequence;
    }
  }

  /**
   * Quiescent batch produced by {@link #beginFlush()} for draining into a Lucene HNSW segment.
   */
  public static final class FlushBatch<K> {
    private final long epoch;
    private final Map<K, DeltaEntry<K>> entries;

    FlushBatch(long epoch, Map<K, DeltaEntry<K>> entries) {
      this.epoch = epoch;
      this.entries = entries;
    }

    public long getEpoch() {
      return epoch;
    }

    public Map<K, DeltaEntry<K>> getEntries() {
      return entries;
    }

    public int size() {
      return entries.size();
    }
  }

  /**
   * Immutable reverse-epoch-resolved snapshot of {@code (E_active, E_flushing)}.
   */
  public static final class Snapshot<K> {
    private final long activeEpoch;
    private final long flushingEpoch;
    private final Map<K, DeltaEntry<K>> liveEntries;
    private final Set<K> tombstonedKeys;
    private final Set<K> maskedKeys;

    Snapshot(long activeEpoch, long flushingEpoch, Map<K, DeltaEntry<K>> liveEntries,
        Set<K> tombstonedKeys, Set<K> maskedKeys) {
      this.activeEpoch = activeEpoch;
      this.flushingEpoch = flushingEpoch;
      this.liveEntries = liveEntries;
      this.tombstonedKeys = tombstonedKeys;
      this.maskedKeys = maskedKeys;
    }

    public long getActiveEpoch() {
      return activeEpoch;
    }

    public long getFlushingEpoch() {
      return flushingEpoch;
    }

    public Map<K, DeltaEntry<K>> getLiveEntries() {
      return liveEntries;
    }

    public Set<K> getTombstonedKeys() {
      return tombstonedKeys;
    }

    /**
     * All keys present in Tier-0 (both live overrides and tombstones) whose stale docIDs in
     * committed Lucene segments MUST be masked.
     */
    public Set<K> getMaskedKeys() {
      return maskedKeys;
    }

    public boolean isMasked(Object key) {
      return maskedKeys.contains(key);
    }

    public boolean isTombstoned(Object key) {
      return tombstonedKeys.contains(key);
    }

    public boolean isEmpty() {
      return maskedKeys.isEmpty();
    }

    /**
     * Executes an exact SIMD-friendly batch vector scan over live Tier-0 entries satisfying the
     * optional {@code metadataFilter}, returning up to {@code k} hits in descending score order.
     */
    public List<EntryScore<K>> search(float[] queryVector, int k,
        VectorSimilarityFunction similarity, Predicate<Map<String, Object>> metadataFilter) {
      Objects.requireNonNull(queryVector, "queryVector must not be null");
      Objects.requireNonNull(similarity, "similarity must not be null");
      if (k <= 0 || liveEntries.isEmpty()) {
        return Collections.emptyList();
      }

      List<K> matchedKeys = new ArrayList<>(liveEntries.size());
      List<float[]> matchedVectors = new ArrayList<>(liveEntries.size());
      for (DeltaEntry<K> entry : liveEntries.values()) {
        float[] vec = entry.getVectorUnsafe();
        if (vec == null || vec.length != queryVector.length) {
          continue;
        }
        if (metadataFilter == null || metadataFilter.test(entry.metadata)) {
          matchedKeys.add(entry.key);
          matchedVectors.add(vec);
        }
      }

      int count = matchedVectors.size();
      if (count == 0) {
        return Collections.emptyList();
      }

      float[][] batchVectors = matchedVectors.toArray(new float[count][]);
      float[] scores = new float[count];
      VectorSimilarityScorer.scoreBatch(queryVector, batchVectors, count, similarity, scores);

      // Min-heap of size k so top-k scoring runs in O(n log k)
      PriorityQueue<EntryScore<K>> minHeap = new PriorityQueue<>(Math.min(k, count),
          Comparator.comparingDouble(EntryScore::getScore));

      for (int i = 0; i < count; i++) {
        float score = scores[i];
        if (Float.isNaN(score)) {
          continue;
        }
        if (minHeap.size() < k) {
          minHeap.offer(new EntryScore<>(matchedKeys.get(i), score));
        } else if (score > minHeap.peek().getScore()) {
          minHeap.poll();
          minHeap.offer(new EntryScore<>(matchedKeys.get(i), score));
        }
      }

      List<EntryScore<K>> results = new ArrayList<>(minHeap.size());
      while (!minHeap.isEmpty()) {
        results.add(minHeap.poll());
      }
      Collections.reverse(results);
      return results;
    }
  }

  private static final class EpochBucket<K> {
    final long epoch;
    final long generation;
    final ConcurrentHashMap<K, DeltaEntry<K>> entries = new ConcurrentHashMap<>();
    final AtomicInteger writerState = new AtomicInteger(0);

    EpochBucket(long epoch, long generation) {
      this.epoch = epoch;
      this.generation = generation;
    }
  }

  private static final class EpochState<K> {
    final EpochBucket<K> active;
    final EpochBucket<K> flushing;

    EpochState(EpochBucket<K> active, EpochBucket<K> flushing) {
      this.active = active;
      this.flushing = flushing;
    }
  }
}
