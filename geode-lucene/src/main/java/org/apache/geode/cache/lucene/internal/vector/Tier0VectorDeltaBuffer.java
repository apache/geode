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
import java.util.Collection;
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
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;

import org.apache.lucene.index.VectorSimilarityFunction;

import org.apache.geode.cache.lucene.internal.distributed.EntryScore;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.ha.ThreadIdentifier;

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

  /**
   * Reserved sequence number stamped on provisional AEQ-hydrated entries during primary promotion
   * before authoritative bucket reconciliation. Live new-term writes and authoritative bucket-scan
   * installs always stamp sequences {@code >= LIVE_SEQUENCE_START (1L)}.
   */
  public static final long PROVISIONAL_HYDRATION_SEQUENCE = 0L;

  /**
   * Minimum sequence number stamped on live new-term mutations ({@link #put}, {@link #destroy})
   * and authoritative bucket-scan installs. Authoritative promotion hydration passes this constant
   * as {@code maxExclusiveSeq} so the single unified predicate
   * {@code (existing == null || existing.sequence < 1L) && committedSeq < 1L} overwrites
   * provisional AEQ entries ({@code seq == 0L}) while rejecting any live or already-committed
   * new-term entry ({@code seq >= 1L}).
   */
  public static final long LIVE_SEQUENCE_START = 1L;

  private static final int SEALED_BIT = 0x80000000;
  private static final int WRITER_MASK = ~SEALED_BIT;
  private static final int MAX_OPTIMISTIC_SNAPSHOT_RETRIES = 64;

  private final int bucketId;
  private final AtomicLong sequenceGenerator = new AtomicLong(0L);
  private final AtomicLong lastClearedEpoch = new AtomicLong(-1L);
  private final AtomicLong evictionVersion = new AtomicLong(0L);
  private final AtomicLong mutationVersion = new AtomicLong(0L);
  private final Object evictionLock = new Object();
  private final ReentrantReadWriteLock pathBBarrierLock = new ReentrantReadWriteLock();
  private final ReentrantLock flushLock;
  private final AtomicReference<EpochState<K>> stateRef;
  private final ConcurrentHashMap<K, Long> committedKeySequences = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Object, Long> pinnedEventSequences = new ConcurrentHashMap<>();
  private final Set<Object> nonVectorEventIds = ConcurrentHashMap.newKeySet();

  public Tier0VectorDeltaBuffer(int bucketId) {
    this(bucketId, new ReentrantLock());
  }

  Tier0VectorDeltaBuffer(int bucketId, ReentrantLock flushLock) {
    this.bucketId = bucketId;
    this.flushLock = Objects.requireNonNull(flushLock, "flushLock must not be null");
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
    return put(key, vector, metadata, null);
  }

  /**
   * Captures a live vector put, metadata, and the immutable region payload atomically inside
   * {@code E_active} so both Path-A and Path-B flushers drain the exact published payload without
   * re-reading live mutable {@code BucketRegion} state.
   *
   * @param key region key (non-null)
   * @param vector embedding vector (non-null, non-empty; defensively copied)
   * @param metadata optional metadata fields for pre-filter evaluation
   * @param regionValue immutable region payload (e.g., {@code PdxInstance} or domain value) bound
   *        to this mutation sequence, or {@code null} when unspecified
   * @return monotonically increasing mutation sequence number
   */
  public long put(K key, float[] vector, Map<String, Object> metadata, Object regionValue) {
    return put(key, vector, metadata, regionValue, null);
  }

  /**
   * Captures a live vector put, metadata, immutable region payload, and originating Geode event
   * identity atomically inside {@code E_active} so Path A can distinguish unpinned pre-pin AEQ
   * events from already-pinned and stale post-eviction AEQ events.
   */
  public long put(K key, float[] vector, Map<String, Object> metadata, Object regionValue,
      Object eventId) {
    Objects.requireNonNull(key, "key must not be null");
    return recordMutation(
        key, copyVector(vector), copyMetadata(metadata), false, regionValue, eventId, null);
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
    return putIfAbsent(key, vector, metadata, null);
  }

  public long putIfAbsent(K key, float[] vector, Map<String, Object> metadata, Object regionValue) {
    Objects.requireNonNull(key, "key must not be null");
    return insertOrOverwrite(
        key, copyVector(vector), copyMetadata(metadata), false, false, -1L, regionValue);
  }

  /**
   * Inserts a provisional AEQ-hydrated live vector entry stamped with
   * {@link #PROVISIONAL_HYDRATION_SEQUENCE} ({@code 0L}) into {@code E_active} ONLY if {@code key}
   * is currently absent from both {@code E_active} and {@code E_flushing} and has no committed
   * new-term sequence ({@code committedKeySequences < 1L}). Subsequent authoritative bucket
   * reconciliation overwrites provisional entries via
   * {@link #putIfSequenceBefore(Object, float[], Map, long)} or
   * {@link #deleteIfSequenceBefore(Object, long)} using {@code maxExclusiveSeq = 1L}.
   */
  long putProvisionalIfAbsent(K key, float[] vector, Map<String, Object> metadata) {
    return putProvisionalIfAbsent(key, vector, metadata, null);
  }

  long putProvisionalIfAbsent(K key, float[] vector, Map<String, Object> metadata,
      Object regionValue) {
    Objects.requireNonNull(key, "key must not be null");
    return insertOrOverwrite(
        key, copyVector(vector), copyMetadata(metadata), false, true, -1L, regionValue);
  }

  /**
   * Inserts or overwrites a live vector entry in {@code E_active} ONLY when {@code key} is absent
   * or has a sequence strictly less than {@code maxExclusiveSeq} across {@code E_active},
   * {@code E_flushing}, AND {@code committedKeySequences}. With
   * {@code maxExclusiveSeq = LIVE_SEQUENCE_START (1L)}, authoritative bucket reconciliation
   * overwrites absent slots and provisional AEQ entries ({@code sequence == 0L}) while preserving
   * any concurrent live post-promotion mutation ({@code sequence >= 1L}) even after it has flushed
   * and evicted from Tier-0.
   *
   * @return newly stamped sequence number ({@code >= 1L}) if applied, or {@code -1L} if preserved
   *         by a newer live or already-committed entry
   */
  public long putIfSequenceBefore(K key, float[] vector, Map<String, Object> metadata,
      long maxExclusiveSeq) {
    return putIfSequenceBefore(key, vector, metadata, maxExclusiveSeq, null);
  }

  public long putIfSequenceBefore(K key, float[] vector, Map<String, Object> metadata,
      long maxExclusiveSeq, Object regionValue) {
    Objects.requireNonNull(key, "key must not be null");
    if (maxExclusiveSeq < 0L) {
      throw new IllegalArgumentException("maxExclusiveSeq must be >= 0");
    }
    return insertOrOverwrite(key, copyVector(vector), copyMetadata(metadata), false, false,
        maxExclusiveSeq, regionValue);
  }

  /**
   * Inserts a tombstone entry into {@code E_active} ONLY if {@code key} is currently absent from
   * both {@code E_active} and {@code E_flushing} (including live entries). Used during primary
   * bucket hydration and pre-crash tombstone reconciliation so hydration never overwrites a
   * concurrent or post-promotion live {@code put}.
   *
   * @return monotonically increasing sequence number if inserted, or {@code -1L} if {@code key}
   *         was already present
   */
  public long deleteIfAbsent(K key) {
    Objects.requireNonNull(key, "key must not be null");
    return insertOrOverwrite(key, null, Collections.emptyMap(), true, false, -1L, null);
  }

  /**
   * Inserts a provisional AEQ-hydrated tombstone stamped with
   * {@link #PROVISIONAL_HYDRATION_SEQUENCE} ({@code 0L}) into {@code E_active} ONLY if {@code key}
   * is currently absent from both {@code E_active} and {@code E_flushing} and has no committed
   * new-term sequence.
   */
  long deleteProvisionalIfAbsent(K key) {
    Objects.requireNonNull(key, "key must not be null");
    return insertOrOverwrite(key, null, Collections.emptyMap(), true, true, -1L, null);
  }

  /**
   * Inserts or overwrites a tombstone in {@code E_active} ONLY when {@code key} is absent or has a
   * sequence strictly less than {@code maxExclusiveSeq} across {@code E_active},
   * {@code E_flushing}, AND {@code committedKeySequences}. With
   * {@code maxExclusiveSeq = LIVE_SEQUENCE_START (1L)}, authoritative bucket reconciliation
   * overwrites absent slots and provisional AEQ entries ({@code sequence == 0L}) while preserving
   * any concurrent live post-promotion mutation ({@code sequence >= 1L}) even after it has flushed
   * and evicted from Tier-0.
   *
   * @return newly stamped sequence number ({@code >= 1L}) if applied, or {@code -1L} if preserved
   *         by a newer live or already-committed entry
   */
  public long deleteIfSequenceBefore(K key, long maxExclusiveSeq) {
    Objects.requireNonNull(key, "key must not be null");
    if (maxExclusiveSeq < 0L) {
      throw new IllegalArgumentException("maxExclusiveSeq must be >= 0");
    }
    return insertOrOverwrite(
        key, null, Collections.emptyMap(), true, false, maxExclusiveSeq, null);
  }

  private static float[] copyVector(float[] vector) {
    Objects.requireNonNull(vector, "vector must not be null");
    if (vector.length == 0) {
      throw new IllegalArgumentException("vector must not be empty");
    }
    return Arrays.copyOf(vector, vector.length);
  }

  private static Map<String, Object> copyMetadata(Map<String, Object> metadata) {
    return (metadata == null || metadata.isEmpty())
        ? Collections.emptyMap()
        : Collections.unmodifiableMap(new HashMap<>(metadata));
  }

  private long insertOrOverwrite(K key, float[] vector, Map<String, Object> metadata,
      boolean tombstone, boolean provisionalHydration, long maxExclusiveSeq, Object regionValue) {
    pathBBarrierLock.readLock().lock();
    try {
      // Synchronize on evictionLock (preserving strict lock hierarchy flushLock ->
      // pathBBarrierLock -> evictionLock -> active.writerState) so the cross-epoch check across
      // E_flushing, committedKeySequences, and E_active is atomic with respect to completeFlush,
      // evictCommitted, evictCommittedEntries, and clear(). A delayed hydration scan resuming in
      // the beginFlush seal-to-commit window deterministically observes sealed E_flushing entries
      // (or, after completeFlush acquires evictionLock first, the advanced committedKeySequences)
      // and rejects stale installs.
      synchronized (evictionLock) {
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
            EpochBucket<K> flushing = stateRef.get().flushing;
            if (flushing != null) {
              if (flushing != active) {
                // Wait for any in-flight writers pinned to oldActive before inspecting flushing
                // so a concurrent live put cannot finish in E_flushing after a newer tombstone
                // is placed in E_active.
                while ((flushing.writerState.get() & WRITER_MASK) != 0) {
                  Thread.onSpinWait();
                }
              }
              DeltaEntry<K> flushingEntry = flushing.entries.get(key);
              if (flushingEntry != null
                  && !shouldOverwriteEntry(flushingEntry, maxExclusiveSeq)) {
                return -1L;
              }
            }
            if (isBlockedByCommittedSequence(key, maxExclusiveSeq)) {
              return -1L;
            }
            boolean[] mutated = new boolean[] {false};
            long[] stampedSeq = new long[] {-1L};
            try {
              active.entries.compute(key, (k, existing) -> {
                EpochBucket<K> currentFlushing = stateRef.get().flushing;
                if (currentFlushing != null) {
                  DeltaEntry<K> currentFlushingEntry = currentFlushing.entries.get(k);
                  if (currentFlushingEntry != null
                      && !shouldOverwriteEntry(currentFlushingEntry, maxExclusiveSeq)) {
                    return existing;
                  }
                }
                if (isBlockedByCommittedSequence(k, maxExclusiveSeq)
                    || !shouldOverwriteEntry(existing, maxExclusiveSeq)) {
                  return existing;
                }
                mutationVersion.incrementAndGet();
                mutated[0] = true;
                long seq = provisionalHydration
                    ? PROVISIONAL_HYDRATION_SEQUENCE
                    : sequenceGenerator.incrementAndGet();
                stampedSeq[0] = seq;
                return new DeltaEntry<>(k, vector, metadata, tombstone, active.epoch, seq,
                    regionValue);
              });
            } finally {
              if (mutated[0]) {
                mutationVersion.incrementAndGet();
              }
            }
            return stampedSeq[0];
          } finally {
            active.writerState.decrementAndGet();
          }
        }
      }
    } finally {
      pathBBarrierLock.readLock().unlock();
    }
  }

  private boolean isBlockedByCommittedSequence(K key, long maxExclusiveSeq) {
    Long committedSeq = committedKeySequences.get(key);
    if (committedSeq == null) {
      return false;
    }
    if (maxExclusiveSeq < 0L) {
      return committedSeq > PROVISIONAL_HYDRATION_SEQUENCE;
    }
    return committedSeq >= maxExclusiveSeq;
  }

  private static <K> boolean shouldOverwriteEntry(DeltaEntry<K> existing, long maxExclusiveSeq) {
    if (existing == null) {
      return true;
    }
    if (maxExclusiveSeq < 0L) {
      return false;
    }
    return existing.sequence < maxExclusiveSeq;
  }

  /**
   * Captures a tombstone deletion synchronously in {@code E_active} to mask any stale vector
   * committed in Lucene HNSW segments or {@code E_flushing}.
   *
   * @param key region key (non-null)
   * @return monotonically increasing mutation sequence number
   */
  public long destroy(K key) {
    return destroy(key, null);
  }

  /**
   * Captures a tombstone deletion synchronously in {@code E_active} bound to its originating Geode
   * event identity so Path A can fence pre-pin and stale post-eviction AEQ events across deletes
   * and recreations.
   */
  public long destroy(K key, Object eventId) {
    Objects.requireNonNull(key, "key must not be null");
    return recordMutation(key, null, Collections.emptyMap(), true, null, eventId, null);
  }

  long putWithPreMutateHookForTest(K key, float[] vector, Map<String, Object> metadata,
      Runnable afterPreIncBeforeMutateHook) {
    return recordMutation(key, copyVector(vector), copyMetadata(metadata), false, null, null,
        afterPreIncBeforeMutateHook);
  }

  private long recordMutation(K key, float[] vector, Map<String, Object> metadata,
      boolean tombstone, Object regionValue, Object eventId,
      Runnable afterPreIncBeforeMutateHook) {
    pathBBarrierLock.readLock().lock();
    try {
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
          mutationVersion.incrementAndGet();
          try {
            if (afterPreIncBeforeMutateHook != null) {
              afterPreIncBeforeMutateHook.run();
            }
            long[] stampedSeq = new long[1];
            active.entries.compute(key, (k, existing) -> {
              long seq = sequenceGenerator.incrementAndGet();
              if (existing == null || active.epoch > existing.epoch
                  || (active.epoch == existing.epoch && seq > existing.sequence)) {
                stampedSeq[0] = seq;
                return new DeltaEntry<>(
                    k, vector, metadata, tombstone, active.epoch, seq, regionValue);
              }
              stampedSeq[0] = existing.sequence;
              return existing;
            });
            // Publish eventId -> seq strictly AFTER DeltaEntry is installed in E_active so Path A
            // never observes an eventId as pinned prior to its linearization point.
            if (eventId != null && stampedSeq[0] > 0L) {
              pinnedEventSequences.merge(normalizeEventId(eventId), stampedSeq[0], Math::max);
            }
            return stampedSeq[0];
          } finally {
            mutationVersion.incrementAndGet();
          }
        } finally {
          active.writerState.decrementAndGet();
        }
      }
    } finally {
      pathBBarrierLock.readLock().unlock();
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
    pathBBarrierLock.readLock().lock();
    try {
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
    } finally {
      pathBBarrierLock.readLock().unlock();
    }
  }

  /**
   * Clears {@code E_flushing} after {@code IndexWriter.commit()} and
   * {@code SearcherManager.maybeRefreshBlocking()} have both completed for {@code flushEpoch}.
   *
   * @param flushEpoch the epoch returned by {@link #beginFlush()}
   */
  public void completeFlush(long flushEpoch) {
    pathBBarrierLock.readLock().lock();
    try {
      synchronized (evictionLock) {
        evictionVersion.incrementAndGet();
        try {
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
            // Record per-key committed sequences (puts and tombstones alike) strictly BEFORE
            // clearing E_flushing so Path A never observes an empty Tier-0 slot without the
            // monotonic committed watermark already visible.
            for (Map.Entry<K, DeltaEntry<K>> entry : current.flushing.entries.entrySet()) {
              long seq = entry.getValue().sequence;
              if (seq > PROVISIONAL_HYDRATION_SEQUENCE) {
                committedKeySequences.merge(entry.getKey(), seq, Math::max);
              }
            }
            EpochState<K> next = new EpochState<>(current.active, null);
            if (stateRef.compareAndSet(current, next)) {
              return;
            }
          }
        } finally {
          evictionVersion.incrementAndGet();
        }
      }
    } finally {
      pathBBarrierLock.readLock().unlock();
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
    pathBBarrierLock.readLock().lock();
    try {
      EpochState<K> current = stateRef.get();
      if (current.flushing == null || current.flushing.epoch != flushEpoch) {
        synchronized (evictionLock) {
          current = stateRef.get();
          if (current.flushing == null || current.flushing.epoch != flushEpoch) {
            if (flushEpoch <= lastClearedEpoch.get()) {
              return;
            }
            throw new IllegalStateException(
                "Cannot abort flush for epoch " + flushEpoch + " on bucket " + bucketId);
          }
        }
      }
      EpochBucket<K> flushing = current.flushing;
      long expectedGeneration = flushing.generation;
      for (Map.Entry<K, DeltaEntry<K>> entry : flushing.entries.entrySet()) {
        if (!restoreEntryToActive(entry.getKey(), entry.getValue(), flushing, expectedGeneration)) {
          return;
        }
      }
      clearFlushingEpoch(flushEpoch);
    } finally {
      pathBBarrierLock.readLock().unlock();
    }
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
        mutationVersion.incrementAndGet();
        try {
          active.entries.compute(key,
              (k, existing) -> (existing == null || oldEntry.isNewerThan(existing))
                  ? oldEntry : existing);
          return true;
        } finally {
          mutationVersion.incrementAndGet();
        }
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
    pathBBarrierLock.readLock().lock();
    try {
      synchronized (evictionLock) {
        evictionVersion.incrementAndGet();
        try {
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
                if (committedSeq > 0L) {
                  this.committedKeySequences.merge(key, committedSeq, Math::max);
                }
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
        } finally {
          evictionVersion.incrementAndGet();
        }
      }
    } finally {
      pathBBarrierLock.readLock().unlock();
    }
  }

  /**
   * Evicts specific keys from both {@code E_active} and {@code E_flushing} ONLY when the currently
   * buffered entry matches the exact immutable {@link DeltaEntry} committed to Lucene (or has
   * {@code sequence <= committed.sequence} with matching tombstone state and payload). Guarantees
   * that a newer mutation pinned while an earlier batch was flushing can never be evicted under the
   * earlier batch's commit.
   */
  public void evictCommittedEntries(Map<K, DeltaEntry<K>> committedEntries) {
    if (committedEntries == null || committedEntries.isEmpty()) {
      return;
    }
    pathBBarrierLock.readLock().lock();
    try {
      synchronized (evictionLock) {
        evictionVersion.incrementAndGet();
        try {
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
              for (Map.Entry<K, DeltaEntry<K>> item : committedEntries.entrySet()) {
                K key = item.getKey();
                DeltaEntry<K> committed = item.getValue();
                if (committed == null) {
                  continue;
                }
                if (committed.sequence > 0L) {
                  this.committedKeySequences.merge(key, committed.sequence, Math::max);
                }
                active.entries.computeIfPresent(key,
                    (k, existing) -> shouldEvictCommittedEntry(existing, committed)
                        ? null : existing);
                if (flushing != null) {
                  flushing.entries.computeIfPresent(key,
                      (k, existing) -> shouldEvictCommittedEntry(existing, committed)
                          ? null : existing);
                }
              }
              return;
            } finally {
              active.writerState.decrementAndGet();
            }
          }
        } finally {
          evictionVersion.incrementAndGet();
        }
      }
    } finally {
      pathBBarrierLock.readLock().unlock();
    }
  }

  private static <K> boolean shouldEvictCommittedEntry(
      DeltaEntry<K> existing, DeltaEntry<K> committed) {
    return existing == committed
        || (existing.sequence <= committed.sequence
            && existing.tombstone == committed.tombstone
            && Objects.equals(existing.regionValue, committed.regionValue));
  }

  /**
   * Returns the highest Tier-0 sequence number durably committed and blocking-refreshed into
   * Lucene for {@code key} (covering puts, overwrites, deletes, and recreations), or {@code 0L}
   * if {@code key} has not yet been committed on this primary term.
   */
  public long getMaxCommittedSequence(K key) {
    if (key == null) {
      return 0L;
    }
    return committedKeySequences.getOrDefault(key, 0L);
  }

  /**
   * Advances the monotonic committed sequence watermark for {@code key} (caller holds
   * {@code flushLock}).
   */
  public void recordCommittedSequence(K key, long sequence) {
    if (key != null && sequence > 0L) {
      committedKeySequences.merge(key, sequence, Math::max);
    }
  }

  /**
   * Normalizes a Geode {@link EventID} by extracting the originating client/member thread ID via
   * {@link ThreadIdentifier#getRealThreadID(long)} (stripping both the upper 32 parallel-WAN
   * routing bits and any {@code putAll}/{@code removeAll} per-bucket fake-thread offset), so an
   * {@code EntryEventImpl}'s {@code EventID} and its corresponding {@code GatewaySenderEventImpl}'s
   * {@code EventID} map to the exact same canonical identity across single-key and bulk operations.
   */
  public static Object normalizeEventId(Object eventId) {
    if (eventId instanceof EventID) {
      EventID id = (EventID) eventId;
      byte[] memberId = id.getMembershipID();
      if (memberId != null) {
        long baseThreadId = ThreadIdentifier.getRealThreadID(id.getThreadID());
        return new CanonicalEventKey(memberId, baseThreadId, id.getSequenceID());
      }
    }
    return eventId;
  }

  /**
   * Associates a Geode event identity with its pinned Tier-0 sequence number (used during primary
   * promotion AEQ hydration so pre-crash shadow events with {@code sequence >= 0L}, including
   * provisional {@link #PROVISIONAL_HYDRATION_SEQUENCE} ({@code 0L}), are recognized as pinned).
   */
  public void recordPinnedEvent(Object eventId, long sequence) {
    if (eventId != null && sequence >= PROVISIONAL_HYDRATION_SEQUENCE) {
      pinnedEventSequences.merge(normalizeEventId(eventId), sequence, Math::max);
    }
  }

  /**
   * Returns the pinned Tier-0 sequence number for {@code eventId}, or {@code -1L} if
   * {@code eventId} has not yet reached its Tier-0 linearization point.
   */
  public long getPinnedEventSequence(Object eventId) {
    if (eventId == null) {
      return -1L;
    }
    return pinnedEventSequences.getOrDefault(normalizeEventId(eventId), -1L);
  }

  /**
   * Marks {@code eventId} as observed by {@code Tier0VectorBucketListener} for a non-vector entry
   * (no vector field extracted) so Path A does not confuse it with an unpinned vector mutation.
   */
  public void recordNonVectorEvent(Object eventId) {
    if (eventId != null) {
      nonVectorEventIds.add(normalizeEventId(eventId));
    }
  }

  /**
   * Returns {@code true} if {@code eventId} was explicitly marked as a non-vector entry.
   */
  public boolean isNonVectorEvent(Object eventId) {
    return eventId != null && nonVectorEventIds.contains(normalizeEventId(eventId));
  }

  /**
   * Releases tracked event identities strictly after an AEQ batch returns {@code true}, keeping
   * per-event metadata bounded by {@code O(|AEQ_b|)}.
   */
  public void acknowledgeEventIds(Collection<?> eventIds) {
    if (eventIds == null || eventIds.isEmpty()) {
      return;
    }
    for (Object eventId : eventIds) {
      if (eventId != null) {
        Object normalized = normalizeEventId(eventId);
        pinnedEventSequences.remove(normalized);
        nonVectorEventIds.remove(normalized);
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
   * Returns the latest immutable {@link DeltaEntry} for {@code key} across
   * {@code (E_active, E_flushing)} in reverse epoch order, or {@code null} if absent. Reading the
   * returned entry atomically binds its stamped sequence number to its immutable payload/tombstone.
   */
  public DeltaEntry<K> getEntry(K key) {
    EpochState<K> state = stateRef.get();
    DeltaEntry<K> activeEntry = state.active.entries.get(key);
    if (activeEntry != null) {
      return activeEntry;
    }
    if (state.flushing != null) {
      return state.flushing.entries.get(key);
    }
    return null;
  }

  /**
   * Returns the current stamped sequence number for {@code key} across
   * {@code (E_active, E_flushing)}, or {@code -1L} if absent.
   */
  public long getKeySequence(K key) {
    DeltaEntry<K> entry = getEntry(key);
    return entry == null ? -1L : entry.sequence;
  }

  /**
   * Takes a wait-free consistent snapshot of {@code (E_active, E_flushing)} resolved in reverse
   * epoch order ({@code E_active} takes precedence over {@code E_flushing}; highest sequence wins
   * per key).
   *
   * <p>
   * Reads {@code E_flushing} strictly BEFORE {@code E_active} and validates
   * {@code evictionVersion},
   * {@code mutationVersion}, {@code stateRef}, and quiescence of {@code active.writerState}
   * afterward so that concurrent writers without flushes, {@link #beginFlush()} rotations, or
   * {@link #abortFlush(long)} restorations can never produce a torn cut, and concurrent
   * {@link #clear()} calls trigger a clean retry.
   *
   * <p>
   * Optimistic attempts linearize at {@code t_1} (after {@code stateRef} and
   * {@code writerState == 0}
   * checks, with zero mutations, rotations, or evictions across the scan). Contended fallback
   * acquires {@code pathBBarrierLock.writeLock()} before {@code evictionLock} to read a quiescent
   * {@code (E_flushing, E_active)} cut.
   *
   * <p>
   * Callers performing hybrid search MUST invoke {@link #snapshot()} BEFORE acquiring/querying the
   * Lucene {@code IndexSearcher} to guarantee zero visibility gap across concurrent flush commits.
   */
  public Snapshot<K> snapshot() {
    return snapshot(null, null, null);
  }

  Snapshot<K> snapshot(Runnable afterFlushingScanHook) {
    return snapshot(null, afterFlushingScanHook, null);
  }

  Snapshot<K> snapshot(Runnable afterFlushingScanHook, Runnable midActiveScanHook) {
    return snapshot(null, afterFlushingScanHook, midActiveScanHook);
  }

  Snapshot<K> snapshot(Runnable afterWriterCheckHook, Runnable afterFlushingScanHook,
      Runnable midActiveScanHook) {
    for (int attempt = 0; attempt < MAX_OPTIMISTIC_SNAPSHOT_RETRIES; attempt++) {
      long v1 = evictionVersion.get();
      if ((v1 & 1L) != 0L) {
        Thread.onSpinWait();
        continue;
      }

      EpochState<K> state = stateRef.get();
      EpochBucket<K> flushing = state.flushing;
      EpochBucket<K> active = state.active;
      if ((active.writerState.get() & WRITER_MASK) != 0) {
        Thread.onSpinWait();
        continue;
      }
      if (attempt == 0 && afterWriterCheckHook != null) {
        afterWriterCheckHook.run();
      }
      long m1 = mutationVersion.get();
      long expectedGeneration = active.generation;

      Map<K, DeltaEntry<K>> merged = scanEpochs(
          flushing,
          active,
          attempt == 0 ? afterFlushingScanHook : null,
          attempt == 0 ? midActiveScanHook : null);

      EpochState<K> after = stateRef.get();
      int activeWriterStateAfter = active.writerState.get();
      long m2 = mutationVersion.get();
      long v2 = evictionVersion.get();
      if (v1 != v2 || (v2 & 1L) != 0L || m1 != m2
          || after.active.generation != expectedGeneration
          || after.active != active || after.flushing != flushing
          || (activeWriterStateAfter & WRITER_MASK) != 0) {
        continue;
      }

      return buildSnapshot(active.epoch, flushing == null ? -1L : flushing.epoch, merged);
    }

    return snapshotUnderBarrierAndEvictionLock();
  }

  private Snapshot<K> snapshotUnderBarrierAndEvictionLock() {
    pathBBarrierLock.writeLock().lock();
    try {
      synchronized (evictionLock) {
        EpochState<K> state = stateRef.get();
        EpochBucket<K> flushing = state.flushing;
        EpochBucket<K> active = state.active;
        Map<K, DeltaEntry<K>> merged = scanEpochs(flushing, active, null, null);
        return buildSnapshot(active.epoch, flushing == null ? -1L : flushing.epoch, merged);
      }
    } finally {
      pathBBarrierLock.writeLock().unlock();
    }
  }

  private Map<K, DeltaEntry<K>> scanEpochs(EpochBucket<K> flushing, EpochBucket<K> active,
      Runnable afterFlushingScanHook, Runnable midActiveScanHook) {
    Map<K, DeltaEntry<K>> merged = new HashMap<>();
    if (flushing != null) {
      for (Map.Entry<K, DeltaEntry<K>> e : flushing.entries.entrySet()) {
        merged.put(e.getKey(), e.getValue());
      }
    }
    if (afterFlushingScanHook != null) {
      afterFlushingScanHook.run();
    }
    overlayBucket(merged, active, midActiveScanHook);
    return merged;
  }

  private Snapshot<K> buildSnapshot(long activeEpoch, long flushingEpoch,
      Map<K, DeltaEntry<K>> merged) {
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
        activeEpoch,
        flushingEpoch,
        Collections.unmodifiableMap(liveEntries),
        Collections.unmodifiableSet(tombstonedKeys),
        Collections.unmodifiableSet(maskedKeys));
  }

  private void overlayBucket(Map<K, DeltaEntry<K>> merged, EpochBucket<K> bucket,
      Runnable afterFirstEntryHook) {
    boolean hookFired = false;
    for (Map.Entry<K, DeltaEntry<K>> e : bucket.entries.entrySet()) {
      DeltaEntry<K> entry = e.getValue();
      merged.merge(e.getKey(), entry,
          (existing, incoming) -> existing.isNewerThan(incoming) ? existing : incoming);
      if (!hookFired && afterFirstEntryHook != null) {
        hookFired = true;
        afterFirstEntryHook.run();
      }
    }
  }

  /**
   * Acquires the per-bucket Path-B read-barrier write lock, blocking new Tier-0 mutations and
   * Path-A evictions until the unsupported-filter flush and Lucene searcher acquisition complete.
   */
  public void lockPathBWrite() {
    pathBBarrierLock.writeLock().lock();
  }

  /**
   * Releases the per-bucket Path-B read-barrier write lock acquired by {@link #lockPathBWrite()}.
   */
  public void unlockPathBWrite() {
    pathBBarrierLock.writeLock().unlock();
  }

  public boolean isPathBWriteHeldByCurrentThread() {
    return pathBBarrierLock.isWriteLockedByCurrentThread();
  }

  /**
   * Returns the per-bucket single-flight flush lock serializing AEQ commits/evictions and Path-B
   * flushes/aborts on this bucket. Lock hierarchy: {@code flushLock -> pathBBarrierLock ->
   * evictionLock}.
   */
  public ReentrantLock getFlushLock() {
    return flushLock;
  }

  /**
   * Returns {@code true} if any thread currently holds the per-bucket single-flight flush lock.
   */
  public boolean isFlushLocked() {
    return flushLock.isLocked();
  }

  long getEvictionVersion() {
    return evictionVersion.get();
  }

  long getMutationVersion() {
    return mutationVersion.get();
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
   * cannot resurrect cleared entries.
   *
   * <p>
   * Theorem 2 lifecycle precondition: invoked exclusively by {@link Tier0VectorRegistry} on bucket
   * demotion/removal ({@code afterSecondary}/{@code afterBucketRemoved}), index/region teardown, or
   * cache close strictly AFTER detaching this buffer from the registry and advancing the monotonic
   * {@code ownershipGeneration} — never while an active primary ownership term remains valid.
   *
   * <p>
   * Synchronizes only on {@code evictionLock} (never blocking on {@code pathBBarrierLock}) so
   * bucket demotion callbacks ({@code afterSecondary}/{@code afterBucketRemoved}) cannot deadlock
   * with an in-flight Path-B write barrier that is simultaneously flushing or committing to the
   * colocated file region.
   */
  public void clear() {
    synchronized (evictionLock) {
      evictionVersion.incrementAndGet();
      try {
        while (true) {
          EpochState<K> current = stateRef.get();
          EpochBucket<K> fresh =
              new EpochBucket<>(current.active.epoch + 1L, current.active.generation + 1L);
          if (stateRef.compareAndSet(current, new EpochState<>(fresh, null))) {
            lastClearedEpoch.accumulateAndGet(current.active.epoch, Math::max);
            current.active.writerState.getAndUpdate(v -> v | SEALED_BIT);
            committedKeySequences.clear();
            pinnedEventSequences.clear();
            nonVectorEventIds.clear();
            return;
          }
        }
      } finally {
        evictionVersion.incrementAndGet();
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
   * Reserves and returns a fresh monotonic sequence fence strictly greater than all previously
   * issued sequences and strictly less than all subsequent mutations. Used at the start of primary
   * bucket hydration so authoritative bucket state overwrites stale pre-promotion entries
   * ({@code sequence < promotionStartSeq}) while preserving concurrent post-promotion writes
   * ({@code sequence >= promotionStartSeq}).
   */
  public long nextSequence() {
    return sequenceGenerator.incrementAndGet();
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
    private final Object regionValue;

    DeltaEntry(K key, float[] vector, Map<String, Object> metadata, boolean tombstone, long epoch,
        long sequence) {
      this(key, vector, metadata, tombstone, epoch, sequence, null);
    }

    DeltaEntry(K key, float[] vector, Map<String, Object> metadata, boolean tombstone, long epoch,
        long sequence, Object regionValue) {
      this.key = key;
      this.vector = vector;
      this.metadata = metadata;
      this.tombstone = tombstone;
      this.epoch = epoch;
      this.sequence = sequence;
      this.regionValue = regionValue;
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

    /**
     * Returns the immutable region payload (e.g., {@code PdxInstance} or domain object) atomically
     * captured when this entry was published into Tier-0, or {@code null} for tombstones or
     * metadata-only unit-test inserts.
     */
    public Object getRegionValue() {
      return regionValue;
    }

    boolean isNewerThan(DeltaEntry<K> other) {
      if (other == null) {
        return true;
      }
      return this.epoch != other.epoch ? this.epoch > other.epoch : this.sequence > other.sequence;
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

  private static final class CanonicalEventKey {
    private final byte[] membershipId;
    private final long baseThreadId;
    private final long sequenceId;
    private final int cachedHash;

    CanonicalEventKey(byte[] membershipId, long baseThreadId, long sequenceId) {
      this.membershipId = membershipId;
      this.baseThreadId = baseThreadId;
      this.sequenceId = sequenceId;
      int h = 1;
      h = 31 * h + EventID.hashCodeMemberId(membershipId);
      h = 31 * h + (int) (sequenceId ^ (sequenceId >>> 32));
      h = 31 * h + (int) (baseThreadId ^ (baseThreadId >>> 32));
      this.cachedHash = h;
    }

    @Override
    public int hashCode() {
      return cachedHash;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof CanonicalEventKey)) {
        return false;
      }
      CanonicalEventKey other = (CanonicalEventKey) obj;
      return sequenceId == other.sequenceId
          && baseThreadId == other.baseThreadId
          && EventID.equalMembershipIds(membershipId, other.membershipId);
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
