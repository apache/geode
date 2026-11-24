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

package org.apache.geode.cache.lucene.internal.vector.continuous;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Server-side Region Version Vector (RVV) monotonic continuous vector dispatcher implementing
 * Theorem 3 (At-Least-Once Completeness &amp; Idempotent Deduplication Under Primary Failover).
 *
 * <p>
 * Key responsibilities:
 * <ul>
 * <li>Stamps outgoing {@link ContinuousVectorEvent}s with {@code (bucketId, memberId,
 * regionVersion)} sourced from Apache Geode's {@link VersionTag}, {@link VersionStamp}, and
 * {@link RegionVersionVector}.</li>
 * <li>Maintains a per-bucket replayable delta tail buffer on primaries and secondaries bounded by
 * checkpoint {@code V_ckpt[b, m] <= V_client[b, m]}.</li>
 * <li>On primary failover ({@link #onPrimaryPromotion}), replays all uncheckpointed tail mutations
 * with {@code v_{b,m} > V_ckpt[b, m]} in strictly ascending {@code regionVersion} order per
 * {@code memberId}, merging authentic replicated entries from Geode's promoted
 * {@link BucketRegion}.</li>
 * </ul>
 */
public class RvvMonotonicVectorDispatcher {

  private static final Logger logger = LogService.getLogger();
  public static final int DEFAULT_MAX_TAIL_PER_STREAM = 100_000;
  /**
   * Default per-{@code (bucketId, memberId)} mutation stride between lightweight RVV watermark
   * heartbeats ({@code O(1/H)} per connection, independent of subscription count {@code M}). Set
   * to {@code 0} to disable heartbeats.
   */
  public static final int DEFAULT_HEARTBEAT_INTERVAL_EVENTS = 64;

  /**
   * Callback invoked whenever a per-{@code (bucketId, memberId)} tail buffer exceeds
   * {@code maxTailPerStream} and must evict an un-checkpointed mutation, signaling that a client
   * behind the evicted version requires a full bucket resync.
   */
  @FunctionalInterface
  public interface TailOverflowListener {
    void onTailOverflow(int bucketId, String memberId, long evictedRegionVersion,
        long checkpointVersion, int maxTailPerStream);
  }

  /**
   * Immutable mutation record stored in a bucket's replayable delta tail buffer.
   */
  public static final class TailMutation {
    private final Object key;
    private final float[] normalizedVector;
    private final int bucketId;
    private final String memberId;
    private final long regionVersion;
    private final int entryVersion;
    private final long timestamp;

    public TailMutation(Object key, float[] normalizedVector, int bucketId, String memberId,
        long regionVersion, int entryVersion, long timestamp) {
      if (memberId == null || memberId.isEmpty()) {
        throw new IllegalArgumentException("memberId must not be null or empty");
      }
      if (regionVersion <= 0L) {
        throw new IllegalArgumentException("regionVersion must be > 0, got: " + regionVersion);
      }
      this.key = key;
      this.normalizedVector =
          Objects.requireNonNull(normalizedVector, "normalizedVector must not be null");
      this.bucketId = bucketId;
      this.memberId = memberId;
      this.regionVersion = regionVersion;
      this.entryVersion = entryVersion;
      this.timestamp = timestamp;
    }

    public Object getKey() {
      return key;
    }

    public float[] getNormalizedVector() {
      return normalizedVector.clone();
    }

    float[] getNormalizedVectorUnsafe() {
      return normalizedVector;
    }

    public int getBucketId() {
      return bucketId;
    }

    public String getMemberId() {
      return memberId;
    }

    public long getRegionVersion() {
      return regionVersion;
    }

    public int getEntryVersion() {
      return entryVersion;
    }

    public long getTimestamp() {
      return timestamp;
    }
  }

  /**
   * Extracted RVV coordinate metadata from a Geode {@link EntryEvent}.
   */
  public static final class ExtractedRvvStamp {
    private final int bucketId;
    private final String memberId;
    private final long regionVersion;
    private final int entryVersion;
    private final boolean primary;

    public ExtractedRvvStamp(int bucketId, String memberId, long regionVersion, int entryVersion,
        boolean primary) {
      this.bucketId = bucketId;
      this.memberId = memberId;
      this.regionVersion = regionVersion;
      this.entryVersion = entryVersion;
      this.primary = primary;
    }

    public int getBucketId() {
      return bucketId;
    }

    public String getMemberId() {
      return memberId;
    }

    public long getRegionVersion() {
      return regionVersion;
    }

    public int getEntryVersion() {
      return entryVersion;
    }

    public boolean isPrimary() {
      return primary;
    }
  }

  private static final class BucketState {
    final Map<String, TreeMap<Long, TailMutation>> tailByMember = new HashMap<>();
    final Map<String, Long> checkpointByMember = new HashMap<>();
    final Map<String, AtomicLong> fallbackVersionByMember = new HashMap<>();
    final Map<String, Long> eventsSinceHeartbeatByMember = new HashMap<>();
    final List<TailMutation> pendingLiveMutations = new ArrayList<>();
    final ConcurrentLinkedQueue<List<ContinuousVectorEvent>> pendingDispatchBatches =
        new ConcurrentLinkedQueue<>();
    final AtomicBoolean dispatchDraining = new AtomicBoolean(false);
    int pendingAsyncPromotions = 0;

    synchronized boolean shouldEmitPeriodicHeartbeat(String memberId, int interval) {
      if (interval <= 0) {
        return false;
      }
      long next = eventsSinceHeartbeatByMember.getOrDefault(memberId, 0L) + 1L;
      if (next >= interval) {
        eventsSinceHeartbeatByMember.put(memberId, 0L);
        return true;
      }
      eventsSinceHeartbeatByMember.put(memberId, next);
      return false;
    }

    synchronized void resetHeartbeatCounter(String memberId) {
      eventsSinceHeartbeatByMember.put(memberId, 0L);
    }

    synchronized List<Long> append(TailMutation mutation, int maxTailPerStream) {
      long ckpt = checkpointByMember.getOrDefault(mutation.memberId, 0L);
      if (mutation.regionVersion <= ckpt) {
        return Collections.emptyList();
      }
      TreeMap<Long, TailMutation> stream =
          tailByMember.computeIfAbsent(mutation.memberId, ignored -> new TreeMap<>());
      stream.putIfAbsent(mutation.regionVersion, mutation);
      List<Long> evictedVersions = null;
      while (stream.size() > maxTailPerStream) {
        Map.Entry<Long, TailMutation> evicted = stream.pollFirstEntry();
        if (evicted != null) {
          if (evictedVersions == null) {
            evictedVersions = new ArrayList<>(2);
          }
          evictedVersions.add(evicted.getKey());
        }
      }
      return evictedVersions == null ? Collections.emptyList() : evictedVersions;
    }

    synchronized long nextFallbackVersion(String memberId) {
      return fallbackVersionByMember
          .computeIfAbsent(memberId, ignored -> new AtomicLong(0L))
          .incrementAndGet();
    }

    synchronized void observeVersion(String memberId, long version) {
      fallbackVersionByMember
          .computeIfAbsent(memberId, ignored -> new AtomicLong(0L))
          .accumulateAndGet(version, Math::max);
    }

    synchronized List<TailMutation> collectReplayableTail(Collection<TailMutation> recoveredEntries,
        int maxTailPerStream, List<TailOverflowRecord> overflowCollector) {
      if (recoveredEntries != null) {
        for (TailMutation entry : recoveredEntries) {
          List<Long> evicted = append(entry, maxTailPerStream);
          if (!evicted.isEmpty() && overflowCollector != null) {
            long ckpt = checkpointByMember.getOrDefault(entry.memberId, 0L);
            for (long evictedVersion : evicted) {
              overflowCollector.add(
                  new TailOverflowRecord(entry.bucketId, entry.memberId, evictedVersion, ckpt));
            }
          }
        }
      }
      List<TailMutation> replay = new ArrayList<>();
      List<String> sortedMembers = new ArrayList<>(tailByMember.keySet());
      Collections.sort(sortedMembers);
      for (String memberId : sortedMembers) {
        long ckpt = checkpointByMember.getOrDefault(memberId, 0L);
        TreeMap<Long, TailMutation> stream = tailByMember.get(memberId);
        if (stream == null || stream.isEmpty()) {
          continue;
        }
        NavigableMap<Long, TailMutation> tail = stream.tailMap(ckpt, false);
        replay.addAll(tail.values());
      }
      return replay;
    }

    synchronized List<TailMutation> collectReplayableTailForReconnect(
        Map<String, Long> clientHighWaterByMember, Map<String, Long> maxVersionByMemberOut) {
      List<TailMutation> replay = new ArrayList<>();
      List<String> sortedMembers = new ArrayList<>(tailByMember.keySet());
      Collections.sort(sortedMembers);
      for (String memberId : sortedMembers) {
        TreeMap<Long, TailMutation> stream = tailByMember.get(memberId);
        if (stream == null || stream.isEmpty()) {
          continue;
        }
        long ckpt = checkpointByMember.getOrDefault(memberId, 0L);
        long clientHighWater = 0L;
        if (clientHighWaterByMember != null) {
          Long hw = clientHighWaterByMember.get(memberId);
          if (hw != null && hw > 0L) {
            clientHighWater = hw;
          }
        }
        long lowerExclusive = Math.max(ckpt, clientHighWater);
        long streamHighWater = stream.lastKey();
        if (lowerExclusive < streamHighWater) {
          maxVersionByMemberOut.put(memberId, streamHighWater);
          NavigableMap<Long, TailMutation> tail = stream.tailMap(lowerExclusive, false);
          replay.addAll(tail.values());
        }
      }
      return replay;
    }

    synchronized void advanceCheckpoint(String memberId, long ackedVersion) {
      long prev = checkpointByMember.getOrDefault(memberId, 0L);
      if (ackedVersion <= prev) {
        return;
      }
      checkpointByMember.put(memberId, ackedVersion);
      TreeMap<Long, TailMutation> stream = tailByMember.get(memberId);
      if (stream != null) {
        stream.headMap(ackedVersion, true).clear();
      }
    }

    synchronized long getCheckpoint(String memberId) {
      return checkpointByMember.getOrDefault(memberId, 0L);
    }

    synchronized int getTailSize() {
      int total = 0;
      for (TreeMap<Long, TailMutation> stream : tailByMember.values()) {
        total += stream.size();
      }
      return total;
    }
  }

  private static final class TailOverflowRecord {
    final int bucketId;
    final String memberId;
    final long evictedVersion;
    final long checkpointVersion;

    TailOverflowRecord(int bucketId, String memberId, long evictedVersion, long checkpointVersion) {
      this.bucketId = bucketId;
      this.memberId = memberId;
      this.evictedVersion = evictedVersion;
      this.checkpointVersion = checkpointVersion;
    }
  }

  private final ChordalVoronoiRouter router;
  private final int maxTailPerStream;
  private final ConcurrentHashMap<Integer, BucketState> bucketStates = new ConcurrentHashMap<>();
  private final CopyOnWriteArrayList<ContinuousVectorEventSink> sinks =
      new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<TailOverflowListener> overflowListeners =
      new CopyOnWriteArrayList<>();
  private final Set<RvvStreamKey> resyncRequiredStreams = ConcurrentHashMap.newKeySet();

  private final AtomicLong totalEventsDispatched = new AtomicLong();
  private final AtomicLong totalBatchesDispatched = new AtomicLong();
  private final AtomicLong totalHeartbeatsDispatched = new AtomicLong();
  private final AtomicLong totalReplayedEventsDispatched = new AtomicLong();
  private final AtomicLong totalPromotionsHandled = new AtomicLong();
  private final AtomicLong totalReconnectReplaysHandled = new AtomicLong();
  private final AtomicLong tailOverflowCount = new AtomicLong();
  private volatile Executor dispatchExecutor;
  private volatile int heartbeatIntervalEvents;

  public RvvMonotonicVectorDispatcher(ChordalVoronoiRouter router) {
    this(router, DEFAULT_MAX_TAIL_PER_STREAM, null, DEFAULT_HEARTBEAT_INTERVAL_EVENTS);
  }

  public RvvMonotonicVectorDispatcher(ChordalVoronoiRouter router, int maxTailPerStream) {
    this(router, maxTailPerStream, null, DEFAULT_HEARTBEAT_INTERVAL_EVENTS);
  }

  public RvvMonotonicVectorDispatcher(ChordalVoronoiRouter router, int maxTailPerStream,
      Executor dispatchExecutor) {
    this(router, maxTailPerStream, dispatchExecutor, DEFAULT_HEARTBEAT_INTERVAL_EVENTS);
  }

  public RvvMonotonicVectorDispatcher(ChordalVoronoiRouter router, int maxTailPerStream,
      Executor dispatchExecutor, int heartbeatIntervalEvents) {
    this.router = Objects.requireNonNull(router, "router must not be null");
    if (maxTailPerStream <= 0) {
      throw new IllegalArgumentException("maxTailPerStream must be > 0, got: " + maxTailPerStream);
    }
    if (heartbeatIntervalEvents < 0) {
      throw new IllegalArgumentException(
          "heartbeatIntervalEvents must be >= 0, got: " + heartbeatIntervalEvents);
    }
    this.maxTailPerStream = maxTailPerStream;
    this.dispatchExecutor = dispatchExecutor;
    this.heartbeatIntervalEvents = heartbeatIntervalEvents;
  }

  /**
   * Configures the per-{@code (bucketId, memberId)} mutation stride between lightweight RVV
   * watermark heartbeats. Pass {@code 0} to disable periodic heartbeats.
   */
  public void setHeartbeatIntervalEvents(int heartbeatIntervalEvents) {
    if (heartbeatIntervalEvents < 0) {
      throw new IllegalArgumentException(
          "heartbeatIntervalEvents must be >= 0, got: " + heartbeatIntervalEvents);
    }
    this.heartbeatIntervalEvents = heartbeatIntervalEvents;
  }

  public int getHeartbeatIntervalEvents() {
    return heartbeatIntervalEvents;
  }

  public long getTotalHeartbeatsDispatched() {
    return totalHeartbeatsDispatched.get();
  }

  /**
   * Sets an optional asynchronous dispatch executor. When non-null, coalesced event batches are
   * enqueued onto a strictly ordered per-bucket FIFO queue and drained serially per bucket so
   * region mutation threads never block on network I/O while preserving Theorem 3's per-stream
   * RVV monotonicity. Pass {@code null} to revert to synchronous coalesced batch dispatch.
   */
  public void setDispatchExecutor(Executor dispatchExecutor) {
    this.dispatchExecutor = dispatchExecutor;
  }

  public Executor getDispatchExecutor() {
    return dispatchExecutor;
  }

  /**
   * Waits up to {@code timeoutMillis} for all queued asynchronous dispatch batches across all
   * buckets to finish draining to sinks.
   */
  public boolean awaitPendingDispatches(long timeoutMillis) throws InterruptedException {
    long deadline = System.currentTimeMillis() + Math.max(0L, timeoutMillis);
    while (true) {
      boolean anyPending = false;
      for (BucketState state : bucketStates.values()) {
        if (!state.pendingDispatchBatches.isEmpty() || state.dispatchDraining.get()) {
          anyPending = true;
          break;
        }
      }
      if (!anyPending) {
        return true;
      }
      if (System.currentTimeMillis() >= deadline) {
        return false;
      }
      Thread.sleep(2L);
    }
  }

  public void addEventSink(ContinuousVectorEventSink sink) {
    sinks.add(Objects.requireNonNull(sink, "sink must not be null"));
  }

  public boolean removeEventSink(ContinuousVectorEventSink sink) {
    return sinks.remove(sink);
  }

  public void addTailOverflowListener(TailOverflowListener listener) {
    overflowListeners.add(Objects.requireNonNull(listener, "listener must not be null"));
  }

  public boolean removeTailOverflowListener(TailOverflowListener listener) {
    return overflowListeners.remove(listener);
  }

  /**
   * Handles a bucket mutation on either a primary or secondary replica.
   *
   * <p>
   * Both primaries and secondaries record the normalized mutation into the bucket's replayable
   * delta tail buffer. Only primaries evaluate subscriptions and emit {@link ContinuousVectorEvent}
   * notifications immediately. When an asynchronous primary-promotion replay is pending for
   * {@code bucketId}, live primary mutations are buffered until replay completes so they cannot
   * overtake earlier tail versions in {@link RvvClientGate}.
   */
  public List<ContinuousVectorEvent> onBucketMutation(int bucketId, String memberId,
      long regionVersion, int entryVersion, Object key, float[] embedding, boolean isPrimary) {
    if (memberId == null || memberId.isEmpty()) {
      throw new IllegalArgumentException("memberId must not be null or empty");
    }
    float[] normalized = VectorSubscription.normalize(embedding);
    BucketState state = bucketStates.computeIfAbsent(bucketId, ignored -> new BucketState());

    long effectiveVersion;
    if (regionVersion > 0L) {
      effectiveVersion = regionVersion;
      state.observeVersion(memberId, effectiveVersion);
    } else {
      effectiveVersion = state.nextFallbackVersion(memberId);
    }

    TailMutation mutation = new TailMutation(key, normalized, bucketId, memberId, effectiveVersion,
        entryVersion, System.currentTimeMillis());

    List<TailOverflowRecord> overflows = null;
    List<ContinuousVectorEvent> emitted;
    // Serialize append + dispatch per bucket to preserve per-(b, m) version ordering
    synchronized (state) {
      if (isPrimary && state.pendingAsyncPromotions > 0) {
        // Gate live primary writes behind the pending async promotion replay barrier
        state.pendingLiveMutations.add(mutation);
        return Collections.emptyList();
      }
      List<Long> evicted = state.append(mutation, maxTailPerStream);
      if (!evicted.isEmpty()) {
        long ckpt = state.getCheckpoint(memberId);
        overflows = new ArrayList<>(evicted.size());
        for (long evictedVersion : evicted) {
          overflows.add(new TailOverflowRecord(bucketId, memberId, evictedVersion, ckpt));
        }
      }
      if (!isPrimary) {
        emitted = Collections.emptyList();
      } else {
        emitted = evaluateAndDispatch(state, mutation, false);
      }
    }
    if (overflows != null) {
      notifyOverflows(overflows);
    }
    return emitted;
  }

  /**
   * Marks the start of an asynchronous primary-promotion replay on {@code bucketId}. Live primary
   * mutations arriving before {@link #completeAsyncPromotion(int, Collection)} are buffered and
   * drained strictly after tail replay finishes.
   */
  public void beginAsyncPromotion(int bucketId) {
    BucketState state = bucketStates.computeIfAbsent(bucketId, ignored -> new BucketState());
    synchronized (state) {
      state.pendingAsyncPromotions++;
    }
  }

  /**
   * Completes an asynchronous primary-promotion replay previously opened with
   * {@link #beginAsyncPromotion(int)}, replaying uncheckpointed tail entries first and then
   * draining any buffered live primary mutations in ascending version order.
   */
  public List<ContinuousVectorEvent> completeAsyncPromotion(int bucketId,
      Collection<TailMutation> recoveredBucketEntries) {
    totalPromotionsHandled.incrementAndGet();
    BucketState state = bucketStates.computeIfAbsent(bucketId, ignored -> new BucketState());
    List<TailOverflowRecord> overflows = new ArrayList<>();
    List<ContinuousVectorEvent> emitted = new ArrayList<>();
    synchronized (state) {
      List<TailMutation> toReplay = state.collectReplayableTail(recoveredBucketEntries,
          maxTailPerStream, overflows);
      for (TailMutation mutation : toReplay) {
        emitted.addAll(evaluateAndDispatch(state, mutation, true));
      }
      emitPromotionWatermarksLocked(state, bucketId, toReplay);
      if (state.pendingAsyncPromotions > 0) {
        state.pendingAsyncPromotions--;
      }
      if (state.pendingAsyncPromotions == 0 && !state.pendingLiveMutations.isEmpty()) {
        drainPendingLiveMutationsLocked(state, emitted, overflows);
      }
    }
    if (!overflows.isEmpty()) {
      notifyOverflows(overflows);
    }
    return emitted;
  }

  /**
   * Cancels a pending asynchronous promotion barrier (e.g., if the replay executor rejects the
   * task during teardown) and drains any buffered live mutations if no other promotions remain.
   */
  public void cancelAsyncPromotion(int bucketId) {
    BucketState state = bucketStates.get(bucketId);
    if (state == null) {
      return;
    }
    List<TailOverflowRecord> overflows = new ArrayList<>();
    List<ContinuousVectorEvent> emitted = new ArrayList<>();
    synchronized (state) {
      if (state.pendingAsyncPromotions > 0) {
        state.pendingAsyncPromotions--;
      }
      if (state.pendingAsyncPromotions == 0 && !state.pendingLiveMutations.isEmpty()) {
        drainPendingLiveMutationsLocked(state, emitted, overflows);
      }
    }
    if (!overflows.isEmpty()) {
      notifyOverflows(overflows);
    }
  }

  /**
   * Replays all uncheckpointed tail mutations ({@code v > V_ckpt[b, m]}) for {@code bucketId} when
   * a secondary bucket promotes to primary.
   */
  public List<ContinuousVectorEvent> onPrimaryPromotion(int bucketId) {
    return onPrimaryPromotion(bucketId, Collections.emptyList());
  }

  /**
   * Replays all uncheckpointed tail mutations ({@code v > V_ckpt[b, m]}) for {@code bucketId},
   * merging any authentic replicated entries recovered directly from Geode's promoted
   * {@link BucketRegion}.
   */
  public List<ContinuousVectorEvent> onPrimaryPromotion(int bucketId,
      Collection<TailMutation> recoveredBucketEntries) {
    totalPromotionsHandled.incrementAndGet();
    BucketState state = bucketStates.computeIfAbsent(bucketId, ignored -> new BucketState());
    List<TailOverflowRecord> overflows = new ArrayList<>();
    List<ContinuousVectorEvent> emitted = new ArrayList<>();
    synchronized (state) {
      List<TailMutation> toReplay = state.collectReplayableTail(recoveredBucketEntries,
          maxTailPerStream, overflows);
      for (TailMutation mutation : toReplay) {
        emitted.addAll(evaluateAndDispatch(state, mutation, true));
      }
      emitPromotionWatermarksLocked(state, bucketId, toReplay);
    }
    if (!overflows.isEmpty()) {
      notifyOverflows(overflows);
    }
    return emitted;
  }

  /**
   * Replays all uncheckpointed tail mutations in
   * {@code (max(V_ckpt[b, m], clientHighWaterByMember[m]), V_S[b, m]]} across every retained
   * stream {@code m} (covering both pre-crash {@code m_P} and post-promotion {@code m_S}) under
   * {@code synchronized (state)}, followed by trailing watermark heartbeats per {@code m}.
   */
  public List<ContinuousVectorEvent> replayUncheckpointedForReconnect(int bucketId,
      Map<String, Long> clientHighWaterByMember) {
    return replayUncheckpointedForReconnect(bucketId, clientHighWaterByMember, null);
  }

  /**
   * Replays uncheckpointed tail mutations for client reconnect under {@code synchronized (state)},
   * optionally invoking {@code onReplayedUnderLock} before releasing the bucket lock so callers
   * can atomically transition from reconnect replay to live {@code m_S} event forwarding without
   * overtaking replayed versions.
   */
  public List<ContinuousVectorEvent> replayUncheckpointedForReconnect(int bucketId,
      Map<String, Long> clientHighWaterByMember, Runnable onReplayedUnderLock) {
    totalReconnectReplaysHandled.incrementAndGet();
    BucketState state = (onReplayedUnderLock != null)
        ? bucketStates.computeIfAbsent(bucketId, ignored -> new BucketState())
        : bucketStates.get(bucketId);
    if (state == null) {
      return Collections.emptyList();
    }
    List<ContinuousVectorEvent> emitted = new ArrayList<>();
    synchronized (state) {
      TreeMap<String, Long> maxVersionByMember = new TreeMap<>();
      List<TailMutation> toReplay =
          state.collectReplayableTailForReconnect(clientHighWaterByMember, maxVersionByMember);
      for (TailMutation mutation : toReplay) {
        emitted.addAll(evaluateAndDispatch(state, mutation, true));
      }
      if (onReplayedUnderLock != null) {
        onReplayedUnderLock.run();
      }
      emitStreamWatermarksLocked(state, bucketId, maxVersionByMember);
    }
    return emitted;
  }

  /**
   * Advances checkpoint {@code V_ckpt[bucketId, memberId]} monotonically up to
   * {@code ackedVersion} (maintaining {@code V_ckpt[b, m] <= V_client[b, m]}) and compacts tail
   * entries with {@code regionVersion <= V_ckpt[b, m]}.
   */
  public void acknowledgeCheckpoint(int bucketId, String memberId, long ackedVersion) {
    if (memberId == null || memberId.isEmpty() || ackedVersion <= 0L) {
      return;
    }
    BucketState state = bucketStates.computeIfAbsent(bucketId, ignored -> new BucketState());
    state.advanceCheckpoint(memberId, ackedVersion);
  }

  /**
   * Alias for {@link #acknowledgeCheckpoint(int, String, long)} that advances {@code V_ckpt} and
   * compacts acknowledged tail entries up to {@code ackedVersion}.
   */
  public void compactUpTo(int bucketId, String memberId, long ackedVersion) {
    acknowledgeCheckpoint(bucketId, memberId, ackedVersion);
  }

  public long getCheckpointVersion(int bucketId, String memberId) {
    BucketState state = bucketStates.get(bucketId);
    return state == null ? 0L : state.getCheckpoint(memberId);
  }

  public int getTailSize(int bucketId) {
    BucketState state = bucketStates.get(bucketId);
    return state == null ? 0 : state.getTailSize();
  }

  public long getTailOverflowCount() {
    return tailOverflowCount.get();
  }

  public boolean isResyncRequired(int bucketId, String memberId) {
    if (memberId == null || memberId.isEmpty()) {
      return false;
    }
    return resyncRequiredStreams.contains(new RvvStreamKey(bucketId, memberId));
  }

  public Set<RvvStreamKey> getResyncRequiredStreams() {
    return Collections.unmodifiableSet(resyncRequiredStreams);
  }

  public void clearResyncRequired(int bucketId, String memberId) {
    if (memberId == null || memberId.isEmpty()) {
      return;
    }
    resyncRequiredStreams.remove(new RvvStreamKey(bucketId, memberId));
  }

  public void onBucketRemoved(int bucketId) {
    bucketStates.remove(bucketId);
    resyncRequiredStreams.removeIf(key -> key.getBucketId() == bucketId);
  }

  private void drainPendingLiveMutationsLocked(BucketState state,
      List<ContinuousVectorEvent> emitted, List<TailOverflowRecord> overflows) {
    List<TailMutation> buffered = new ArrayList<>(state.pendingLiveMutations);
    state.pendingLiveMutations.clear();
    buffered.sort(Comparator.comparing(TailMutation::getMemberId)
        .thenComparingLong(TailMutation::getRegionVersion));
    for (TailMutation live : buffered) {
      List<Long> evicted = state.append(live, maxTailPerStream);
      if (!evicted.isEmpty()) {
        long ckpt = state.getCheckpoint(live.memberId);
        for (long evictedVersion : evicted) {
          overflows.add(new TailOverflowRecord(live.bucketId, live.memberId, evictedVersion, ckpt));
        }
      }
      emitted.addAll(evaluateAndDispatch(state, live, false));
    }
  }

  private void notifyOverflows(List<TailOverflowRecord> overflows) {
    for (TailOverflowRecord rec : overflows) {
      tailOverflowCount.incrementAndGet();
      resyncRequiredStreams.add(new RvvStreamKey(rec.bucketId, rec.memberId));
      logger.warn(
          "RVV delta tail overflow on bucket={}, member={}: evicted un-checkpointed regionVersion={} "
              + "(V_ckpt={}, maxTailPerStream={}); stream marked for full resync",
          rec.bucketId, rec.memberId, rec.evictedVersion, rec.checkpointVersion, maxTailPerStream);
      for (TailOverflowListener listener : overflowListeners) {
        listener.onTailOverflow(rec.bucketId, rec.memberId, rec.evictedVersion,
            rec.checkpointVersion, maxTailPerStream);
      }
    }
  }

  public long getTotalEventsDispatched() {
    return totalEventsDispatched.get();
  }

  public long getTotalBatchesDispatched() {
    return totalBatchesDispatched.get();
  }

  public long getTotalReplayedEventsDispatched() {
    return totalReplayedEventsDispatched.get();
  }

  public long getTotalPromotionsHandled() {
    return totalPromotionsHandled.get();
  }

  public long getTotalReconnectReplaysHandled() {
    return totalReconnectReplaysHandled.get();
  }

  /**
   * Extracts authentic Geode RVV coordinates {@code (bucketId, memberId, regionVersion,
   * entryVersion, isPrimary)} from an {@link EntryEvent}.
   */
  public static ExtractedRvvStamp extractFromEntryEvent(EntryEvent<?, ?> event) {
    Objects.requireNonNull(event, "event must not be null");
    int bucketId = 0;
    boolean isPrimary = true;
    String memberId = null;
    long regionVersion = 0L;
    int entryVersion = 0;

    Region<?, ?> region = event.getRegion();
    BucketRegion bucketRegion = null;

    if (event instanceof EntryEventImpl) {
      EntryEventImpl impl = (EntryEventImpl) event;
      if (impl.getKeyInfo() != null && impl.getKeyInfo().getBucketId() >= 0) {
        bucketId = impl.getKeyInfo().getBucketId();
      }
      if (region instanceof PartitionedRegion) {
        PartitionedRegion pr = (PartitionedRegion) region;
        if (pr.getDataStore() != null) {
          bucketRegion = pr.getDataStore().getLocalBucketById(bucketId);
          if (bucketRegion != null) {
            isPrimary = bucketRegion.getBucketAdvisor().isPrimary();
          }
        }
      } else if (region instanceof BucketRegion) {
        bucketRegion = (BucketRegion) region;
        bucketId = bucketRegion.getId();
        isPrimary = bucketRegion.getBucketAdvisor().isPrimary();
      }

      VersionTag<?> tag = impl.getVersionTag();
      if (tag != null) {
        regionVersion = tag.getRegionVersion();
        entryVersion = tag.getEntryVersion();
        VersionSource<?> tagMember = tag.getMemberID();
        if (tagMember != null) {
          memberId = tagMember.toString();
        }
      }
    }

    if (memberId == null) {
      if (bucketRegion != null && bucketRegion.getVersionMember() != null) {
        // In Geode, a null VersionTag.getMemberID() on a local BucketRegion always identifies
        // bucketRegion.getVersionMember() (the primary that generated regionVersion from its RVV),
        // even when event.isOriginRemote() is true (put routed from another cluster member).
        memberId = bucketRegion.getVersionMember().toString();
      } else if (event.getDistributedMember() != null) {
        memberId = event.getDistributedMember().toString();
      } else {
        memberId = "local";
      }
    }

    // Fallback to bucket's RegionVersionVector if VersionTag lacked a regionVersion
    if (regionVersion <= 0L && bucketRegion != null) {
      @SuppressWarnings("rawtypes")
      RegionVersionVector rvv = bucketRegion.getVersionVector();
      if (rvv != null) {
        @SuppressWarnings("unchecked")
        long localVer = rvv.getVersionForMember(bucketRegion.getVersionMember());
        if (localVer > 0L) {
          regionVersion = localVer;
        }
      }
    }

    return new ExtractedRvvStamp(bucketId, memberId, regionVersion, entryVersion, isPrimary);
  }

  /**
   * Snapshots replicated entries and their authentic Geode {@link VersionStamp}s from a promoted
   * {@link BucketRegion}, sorted in strictly ascending {@code (memberId, regionVersion)} order.
   */
  public static List<TailMutation> extractBucketSnapshot(PartitionedRegion partitionedRegion,
      int bucketId, Function<Object, float[]> vectorExtractor) {
    Objects.requireNonNull(partitionedRegion, "partitionedRegion must not be null");
    Objects.requireNonNull(vectorExtractor, "vectorExtractor must not be null");
    if (partitionedRegion.getDataStore() == null) {
      return Collections.emptyList();
    }
    BucketRegion bucketRegion = partitionedRegion.getDataStore().getLocalBucketById(bucketId);
    if (bucketRegion == null || bucketRegion.isDestroyed()) {
      return Collections.emptyList();
    }

    VersionSource<?> localVersionMember = bucketRegion.getVersionMember();
    String localMemberId = localVersionMember != null ? localVersionMember.toString() : "local";

    List<TailMutation> snapshot = new ArrayList<>();
    @SuppressWarnings("unchecked")
    Iterator<RegionEntry> iterator = bucketRegion.getRegionMap().regionEntries().iterator();
    while (iterator.hasNext()) {
      RegionEntry re = iterator.next();
      if (re == null || re.isRemoved() || re.isTombstone()) {
        continue;
      }
      Object key = re.getKey();
      Object rawVal = re.getValue(bucketRegion);
      if (rawVal == null || Token.isInvalidOrRemoved(rawVal)) {
        continue;
      }
      Object value = rawVal;
      if (rawVal instanceof CachedDeserializable) {
        value = ((CachedDeserializable) rawVal).getDeserializedValue(bucketRegion, re);
      }
      if (value == null || Token.isInvalidOrRemoved(value)) {
        continue;
      }
      float[] rawVec = vectorExtractor.apply(value);
      if (rawVec == null) {
        continue;
      }
      float[] normalized = VectorSubscription.normalize(rawVec);

      VersionStamp<?> stamp = re.getVersionStamp();
      String memberId = localMemberId;
      long regionVersion = 0L;
      int entryVersion = 0;
      long timestamp = re.getLastModified();
      if (stamp != null && stamp.hasValidVersion()) {
        VersionSource<?> stampMember = stamp.getMemberID();
        if (stampMember != null) {
          memberId = stampMember.toString();
        }
        regionVersion = stamp.getRegionVersion();
        entryVersion = stamp.getEntryVersion();
        if (stamp.getVersionTimeStamp() > 0L) {
          timestamp = stamp.getVersionTimeStamp();
        }
      }
      if (regionVersion > 0L) {
        snapshot.add(new TailMutation(key, normalized, bucketId, memberId, regionVersion,
            entryVersion, timestamp));
      }
    }

    snapshot.sort(Comparator.comparing(TailMutation::getMemberId)
        .thenComparingLong(TailMutation::getRegionVersion));
    return snapshot;
  }

  private List<ContinuousVectorEvent> evaluateAndDispatch(BucketState state, TailMutation mutation,
      boolean replayed) {
    List<ChordalVoronoiRouter.MatchResult> matches =
        router.routeNormalized(mutation.getNormalizedVectorUnsafe());
    boolean emitHeartbeat =
        !replayed && state.shouldEmitPeriodicHeartbeat(mutation.memberId, heartbeatIntervalEvents);
    if (matches.isEmpty() && !emitHeartbeat) {
      return Collections.emptyList();
    }

    List<ContinuousVectorEvent> matchEvents;
    if (matches.isEmpty()) {
      matchEvents = Collections.emptyList();
    } else {
      List<ContinuousVectorEvent> events = new ArrayList<>(matches.size());
      for (ChordalVoronoiRouter.MatchResult match : matches) {
        events.add(new ContinuousVectorEvent(match.getSubscriptionId(), mutation.key,
            match.getSimilarityScore(), mutation.bucketId, mutation.memberId,
            mutation.regionVersion, mutation.entryVersion, mutation.timestamp, replayed));
      }
      int count = events.size();
      totalEventsDispatched.addAndGet(count);
      if (replayed) {
        totalReplayedEventsDispatched.addAndGet(count);
      }
      matchEvents = Collections.unmodifiableList(events);
    }

    List<ContinuousVectorEvent> sinkBatch;
    if (!emitHeartbeat) {
      sinkBatch = matchEvents;
    } else {
      ContinuousVectorEvent heartbeat = ContinuousVectorEvent.watermarkHeartbeat(mutation.bucketId,
          mutation.memberId, mutation.regionVersion, mutation.timestamp);
      totalHeartbeatsDispatched.incrementAndGet();
      if (matchEvents.isEmpty()) {
        sinkBatch = Collections.singletonList(heartbeat);
      } else {
        List<ContinuousVectorEvent> combined = new ArrayList<>(matchEvents.size() + 1);
        combined.addAll(matchEvents);
        combined.add(heartbeat);
        sinkBatch = Collections.unmodifiableList(combined);
      }
    }

    enqueueOrDispatchBatchLocked(state, sinkBatch);
    return matchEvents;
  }

  private void emitPromotionWatermarksLocked(BucketState state, int bucketId,
      List<TailMutation> replayedMutations) {
    if (heartbeatIntervalEvents <= 0 || replayedMutations == null || replayedMutations.isEmpty()) {
      return;
    }
    TreeMap<String, Long> maxVersionByMember = new TreeMap<>();
    for (TailMutation m : replayedMutations) {
      maxVersionByMember.merge(m.memberId, m.regionVersion, Math::max);
    }
    emitStreamWatermarksLocked(state, bucketId, maxVersionByMember);
  }

  private void emitStreamWatermarksLocked(BucketState state, int bucketId,
      Map<String, Long> maxVersionByMember) {
    if (heartbeatIntervalEvents <= 0 || maxVersionByMember == null
        || maxVersionByMember.isEmpty()) {
      return;
    }
    long now = System.currentTimeMillis();
    for (Map.Entry<String, Long> entry : maxVersionByMember.entrySet()) {
      String memberId = entry.getKey();
      long maxVersion = entry.getValue();
      state.resetHeartbeatCounter(memberId);
      ContinuousVectorEvent hb =
          ContinuousVectorEvent.watermarkHeartbeat(bucketId, memberId, maxVersion, now);
      totalHeartbeatsDispatched.incrementAndGet();
      enqueueOrDispatchBatchLocked(state, Collections.singletonList(hb));
    }
  }

  private void enqueueOrDispatchBatchLocked(BucketState state,
      List<ContinuousVectorEvent> sinkBatch) {
    totalBatchesDispatched.incrementAndGet();
    Executor exec = this.dispatchExecutor;
    if (exec != null) {
      // Enqueued inside synchronized(state) -> preserves exact ascending per-(b, m) RVV order
      state.pendingDispatchBatches.offer(sinkBatch);
      scheduleBucketDrain(state, exec);
    } else {
      if (!state.pendingDispatchBatches.isEmpty()) {
        drainBucketDispatchQueue(state, null);
      }
      dispatchBatchToSinks(sinkBatch);
    }
  }

  private void scheduleBucketDrain(BucketState state, Executor exec) {
    if (state.dispatchDraining.compareAndSet(false, true)) {
      try {
        exec.execute(() -> drainBucketDispatchQueue(state, exec));
      } catch (RejectedExecutionException rejected) {
        state.dispatchDraining.set(false);
        drainBucketDispatchQueue(state, null);
      }
    }
  }

  private void drainBucketDispatchQueue(BucketState state, Executor exec) {
    try {
      List<ContinuousVectorEvent> next;
      while ((next = state.pendingDispatchBatches.poll()) != null) {
        dispatchBatchToSinks(next);
      }
    } finally {
      state.dispatchDraining.set(false);
      if (!state.pendingDispatchBatches.isEmpty() && exec != null) {
        scheduleBucketDrain(state, exec);
      }
    }
  }

  private void dispatchBatchToSinks(List<ContinuousVectorEvent> batch) {
    for (ContinuousVectorEventSink sink : sinks) {
      sink.onEventBatch(batch);
    }
  }
}
