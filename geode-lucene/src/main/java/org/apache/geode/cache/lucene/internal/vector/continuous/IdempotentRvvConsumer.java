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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * End-to-end crash-idempotent wrapper around {@link RvvClientGate} closing both client-crash
 * persistence windows for Theorem 3:
 *
 * <ol>
 * <li><b>Crash after watermark/intent persist, before effect execution</b>: incoming match events
 * are durably staged into a pending-intent log keyed by {@link EventDeliveryId}
 * {@code (subscriptionId, bucketId, memberId, regionVersion)} before advancing received
 * watermarks; on restart, {@link #recover()} replays any pending intents whose effects have not yet
 * executed.</li>
 * <li><b>Crash after effect execution, before committed-watermark persistence</b>: committed
 * reconnect watermarks advance strictly after {@link EffectHandler} and durable deduplication
 * recording complete; replayed events matching an already-recorded {@link EventDeliveryId} are
 * deterministically suppressed with zero duplicate side effects.</li>
 * </ol>
 */
public class IdempotentRvvConsumer implements ContinuousVectorEventSink {

  /** Immutable idempotency key {@code (subscriptionId, bucketId, memberId, regionVersion)}. */
  public static final class EventDeliveryId implements Comparable<EventDeliveryId> {
    private final String subscriptionId;
    private final int bucketId;
    private final String memberId;
    private final long regionVersion;

    public EventDeliveryId(String subscriptionId, int bucketId, String memberId,
        long regionVersion) {
      if (subscriptionId == null || subscriptionId.isEmpty()) {
        throw new IllegalArgumentException("subscriptionId must not be null or empty");
      }
      if (memberId == null || memberId.isEmpty()) {
        throw new IllegalArgumentException("memberId must not be null or empty");
      }
      if (regionVersion <= 0L) {
        throw new IllegalArgumentException("regionVersion must be > 0, got: " + regionVersion);
      }
      this.subscriptionId = subscriptionId;
      this.bucketId = bucketId;
      this.memberId = memberId;
      this.regionVersion = regionVersion;
    }

    public static EventDeliveryId fromEvent(ContinuousVectorEvent event) {
      Objects.requireNonNull(event, "event must not be null");
      return new EventDeliveryId(event.getSubscriptionId(), event.getBucketId(),
          event.getMemberId(), event.getRegionVersion());
    }

    public String getSubscriptionId() {
      return subscriptionId;
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

    @Override
    public int compareTo(EventDeliveryId other) {
      int cmp = subscriptionId.compareTo(other.subscriptionId);
      if (cmp != 0) {
        return cmp;
      }
      cmp = Integer.compare(bucketId, other.bucketId);
      if (cmp != 0) {
        return cmp;
      }
      cmp = memberId.compareTo(other.memberId);
      if (cmp != 0) {
        return cmp;
      }
      return Long.compare(regionVersion, other.regionVersion);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof EventDeliveryId)) {
        return false;
      }
      EventDeliveryId that = (EventDeliveryId) o;
      return bucketId == that.bucketId
          && regionVersion == that.regionVersion
          && Objects.equals(subscriptionId, that.subscriptionId)
          && Objects.equals(memberId, that.memberId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(subscriptionId, bucketId, memberId, regionVersion);
    }

    @Override
    public String toString() {
      return subscriptionId + "|" + bucketId + "|" + memberId + "|" + regionVersion;
    }
  }

  /** Consumer side-effect callback invoked strictly at most once per {@link EventDeliveryId}. */
  @FunctionalInterface
  public interface EffectHandler {
    void apply(ContinuousVectorEvent event, EventDeliveryId deliveryId);
  }

  /** Crash-boundary phases used for deterministic fault-injection testing. */
  public enum CrashPhase {
    /** Fires after gate admission check, strictly BEFORE atomic durable PENDING intent staging. */
    BEFORE_DURABLE_PENDING_STAGE,
    /** Fires after staging durable intent & received watermark, BEFORE executing effect. */
    AFTER_WATERMARK_PERSIST_BEFORE_EFFECT,
    /** Fires after executing effect & ledger deduplication record, BEFORE committed watermark. */
    AFTER_EFFECT_BEFORE_WATERMARK_COMMIT
  }

  /** Optional test hook invoked at deterministic crash persistence boundaries. */
  @FunctionalInterface
  public interface CrashBoundaryHook {
    void onBoundary(CrashPhase phase, ContinuousVectorEvent event, EventDeliveryId deliveryId);
  }

  /** Durable client-side store holding pending WAL intents, applied ledger IDs, and watermarks. */
  public interface DurableDeliveryStore {
    void stagePendingIntent(ContinuousVectorEvent event, EventDeliveryId id);

    default boolean stagePendingIntentIfAbsent(ContinuousVectorEvent event, EventDeliveryId id) {
      stagePendingIntent(event, id);
      return true;
    }

    default boolean isPendingIntent(EventDeliveryId id) {
      return false;
    }

    void markPendingCompleted(EventDeliveryId id);

    List<ContinuousVectorEvent> getPendingIntentsOrdered();

    boolean isEffectApplied(EventDeliveryId id);

    boolean markEffectAppliedIfAbsent(EventDeliveryId id);

    void persistReceivedWatermark(String subscriptionId, int bucketId, String memberId,
        long regionVersion);

    long getReceivedWatermark(String subscriptionId, int bucketId, String memberId);

    void persistCommittedWatermark(String subscriptionId, int bucketId, String memberId,
        long regionVersion);

    long getCommittedWatermark(String subscriptionId, int bucketId, String memberId);

    Set<String> getTrackedSubscriptions(int bucketId, String memberId);

    Map<String, Long> getCommittedHighWaterByMember(int bucketId,
        Collection<String> activeSubscriptionIds);

    default long getLowestPendingVersion(String subscriptionId, int bucketId, String memberId) {
      return Long.MAX_VALUE;
    }

    /** Returns every committed {@code (subscriptionId, bucketId, memberId) -> version} entry. */
    default Map<EventDeliveryId, Long> getCommittedWatermarkSnapshot() {
      return Collections.emptyMap();
    }
  }

  /** Thread-safe in-memory durable-store implementation surviving simulated client restarts. */
  public static class InMemoryDurableDeliveryStore implements DurableDeliveryStore {
    private static final class StreamCoords {
      final String subscriptionId;
      final int bucketId;
      final String memberId;

      StreamCoords(String subscriptionId, int bucketId, String memberId) {
        this.subscriptionId = subscriptionId;
        this.bucketId = bucketId;
        this.memberId = memberId;
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) {
          return true;
        }
        if (!(o instanceof StreamCoords)) {
          return false;
        }
        StreamCoords that = (StreamCoords) o;
        return bucketId == that.bucketId
            && Objects.equals(subscriptionId, that.subscriptionId)
            && Objects.equals(memberId, that.memberId);
      }

      @Override
      public int hashCode() {
        return Objects.hash(subscriptionId, bucketId, memberId);
      }
    }

    private final ConcurrentHashMap<EventDeliveryId, ContinuousVectorEvent> pendingIntents =
        new ConcurrentHashMap<>();
    private final Set<EventDeliveryId> appliedLedger = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<StreamCoords, Long> receivedWatermarks =
        new ConcurrentHashMap<>();
    private final ConcurrentHashMap<StreamCoords, Long> committedWatermarks =
        new ConcurrentHashMap<>();

    @Override
    public void stagePendingIntent(ContinuousVectorEvent event, EventDeliveryId id) {
      pendingIntents.putIfAbsent(id, event);
    }

    @Override
    public boolean stagePendingIntentIfAbsent(ContinuousVectorEvent event, EventDeliveryId id) {
      return pendingIntents.putIfAbsent(id, event) == null;
    }

    @Override
    public boolean isPendingIntent(EventDeliveryId id) {
      return pendingIntents.containsKey(id);
    }

    @Override
    public void markPendingCompleted(EventDeliveryId id) {
      pendingIntents.remove(id);
    }

    @Override
    public List<ContinuousVectorEvent> getPendingIntentsOrdered() {
      List<Map.Entry<EventDeliveryId, ContinuousVectorEvent>> entries =
          new ArrayList<>(pendingIntents.entrySet());
      entries.sort(Map.Entry.comparingByKey());
      List<ContinuousVectorEvent> result = new ArrayList<>(entries.size());
      for (Map.Entry<EventDeliveryId, ContinuousVectorEvent> entry : entries) {
        result.add(entry.getValue());
      }
      return result;
    }

    @Override
    public boolean isEffectApplied(EventDeliveryId id) {
      return appliedLedger.contains(id);
    }

    @Override
    public boolean markEffectAppliedIfAbsent(EventDeliveryId id) {
      return appliedLedger.add(id);
    }

    @Override
    public void persistReceivedWatermark(String subscriptionId, int bucketId, String memberId,
        long regionVersion) {
      receivedWatermarks.merge(new StreamCoords(subscriptionId, bucketId, memberId), regionVersion,
          Math::max);
    }

    @Override
    public long getReceivedWatermark(String subscriptionId, int bucketId, String memberId) {
      return receivedWatermarks.getOrDefault(new StreamCoords(subscriptionId, bucketId, memberId),
          0L);
    }

    @Override
    public void persistCommittedWatermark(String subscriptionId, int bucketId, String memberId,
        long regionVersion) {
      committedWatermarks.merge(new StreamCoords(subscriptionId, bucketId, memberId), regionVersion,
          Math::max);
    }

    @Override
    public long getCommittedWatermark(String subscriptionId, int bucketId, String memberId) {
      return committedWatermarks.getOrDefault(new StreamCoords(subscriptionId, bucketId, memberId),
          0L);
    }

    @Override
    public Set<String> getTrackedSubscriptions(int bucketId, String memberId) {
      Set<String> subs = new HashSet<>();
      for (StreamCoords coords : committedWatermarks.keySet()) {
        if (coords.bucketId == bucketId && Objects.equals(memberId, coords.memberId)) {
          subs.add(coords.subscriptionId);
        }
      }
      for (StreamCoords coords : receivedWatermarks.keySet()) {
        if (coords.bucketId == bucketId && Objects.equals(memberId, coords.memberId)) {
          subs.add(coords.subscriptionId);
        }
      }
      return subs;
    }

    @Override
    public Map<String, Long> getCommittedHighWaterByMember(int bucketId,
        Collection<String> activeSubscriptionIds) {
      Set<String> members = new HashSet<>();
      for (StreamCoords coords : committedWatermarks.keySet()) {
        if (coords.bucketId == bucketId) {
          members.add(coords.memberId);
        }
      }
      Map<String, Long> result = new HashMap<>();
      for (String memberId : members) {
        long minWatermark = Long.MAX_VALUE;
        boolean found = false;
        if (activeSubscriptionIds != null && !activeSubscriptionIds.isEmpty()) {
          for (String subId : activeSubscriptionIds) {
            long wm = getCommittedWatermark(subId, bucketId, memberId);
            if (wm <= 0L) {
              minWatermark = 0L;
              found = true;
              break;
            }
            found = true;
            if (wm < minWatermark) {
              minWatermark = wm;
            }
          }
        } else {
          for (Map.Entry<StreamCoords, Long> entry : committedWatermarks.entrySet()) {
            StreamCoords k = entry.getKey();
            if (k.bucketId == bucketId && Objects.equals(memberId, k.memberId)
                && entry.getValue() > 0L) {
              found = true;
              minWatermark = Math.min(minWatermark, entry.getValue());
            }
          }
        }
        if (found && minWatermark > 0L && minWatermark != Long.MAX_VALUE) {
          result.put(memberId, minWatermark);
        }
      }
      return Collections.unmodifiableMap(result);
    }

    @Override
    public long getLowestPendingVersion(String subscriptionId, int bucketId, String memberId) {
      long lowest = Long.MAX_VALUE;
      for (EventDeliveryId id : pendingIntents.keySet()) {
        if (id.getBucketId() == bucketId
            && Objects.equals(subscriptionId, id.getSubscriptionId())
            && Objects.equals(memberId, id.getMemberId())) {
          if (id.getRegionVersion() < lowest) {
            lowest = id.getRegionVersion();
          }
        }
      }
      return lowest;
    }

    @Override
    public Map<EventDeliveryId, Long> getCommittedWatermarkSnapshot() {
      Map<EventDeliveryId, Long> snapshot = new HashMap<>(committedWatermarks.size() * 2);
      for (Map.Entry<StreamCoords, Long> entry : committedWatermarks.entrySet()) {
        if (entry.getValue() != null && entry.getValue() > 0L) {
          StreamCoords coords = entry.getKey();
          snapshot.put(new EventDeliveryId(coords.subscriptionId, coords.bucketId, coords.memberId,
              entry.getValue()), entry.getValue());
        }
      }
      return Collections.unmodifiableMap(snapshot);
    }
  }

  private final RvvClientGate gate;
  private final DurableDeliveryStore store;
  private final EffectHandler effectHandler;
  private final CrashBoundaryHook crashHook;
  private final ConcurrentHashMap<String, Object> streamStageLocks = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, ReentrantLock> streamExecutionFences =
      new ConcurrentHashMap<>();
  private final Set<EventDeliveryId> inFlightExecutions = ConcurrentHashMap.newKeySet();
  private final AtomicLong effectsExecutedCount = new AtomicLong();
  private final AtomicLong deduplicatedEffectsCount = new AtomicLong();
  private final AtomicLong recoveredPendingCount = new AtomicLong();
  private final AtomicLong durableAdmissionsCount = new AtomicLong();

  public IdempotentRvvConsumer(RvvClientGate gate, DurableDeliveryStore store,
      EffectHandler effectHandler) {
    this(gate, store, effectHandler, null);
  }

  public IdempotentRvvConsumer(RvvClientGate gate, DurableDeliveryStore store,
      EffectHandler effectHandler, CrashBoundaryHook crashHook) {
    this.gate = Objects.requireNonNull(gate, "gate must not be null");
    this.store = Objects.requireNonNull(store, "store must not be null");
    this.effectHandler = Objects.requireNonNull(effectHandler, "effectHandler must not be null");
    this.crashHook = crashHook;
  }

  private Object getStreamStageLock(int bucketId, String memberId) {
    return streamStageLocks.computeIfAbsent(bucketId + "|" + memberId, ignored -> new Object());
  }

  private static String streamFenceKey(String subscriptionId, int bucketId, String memberId) {
    return subscriptionId + "|" + bucketId + "|" + memberId;
  }

  private ReentrantLock getStreamExecutionFence(String subscriptionId, int bucketId,
      String memberId) {
    return streamExecutionFences.computeIfAbsent(streamFenceKey(subscriptionId, bucketId, memberId),
        ignored -> new ReentrantLock());
  }

  private List<ReentrantLock> acquireSortedExecutionFences(Set<String> keys) {
    if (keys.isEmpty()) {
      return Collections.emptyList();
    }
    TreeSet<String> sorted = new TreeSet<>(keys);
    List<ReentrantLock> acquired = new ArrayList<>(sorted.size());
    for (String key : sorted) {
      ReentrantLock lock =
          streamExecutionFences.computeIfAbsent(key, ignored -> new ReentrantLock());
      lock.lock();
      acquired.add(lock);
    }
    return acquired;
  }

  private static void releaseExecutionFences(List<ReentrantLock> acquired) {
    for (int i = acquired.size() - 1; i >= 0; i--) {
      acquired.get(i).unlock();
    }
  }

  @Override
  public void onEvent(ContinuousVectorEvent event) {
    processEvent(event);
  }

  @Override
  public void onEventBatch(List<ContinuousVectorEvent> events) {
    processEventBatch(events);
  }

  /**
   * Sequentially stages an incoming match event into the durable pending-intent log after
   * atomically verifying it has not already been committed, applied, or staged under the stream
   * admission lock.
   */
  public EventDeliveryId stagePendingIntent(ContinuousVectorEvent event) {
    Objects.requireNonNull(event, "event must not be null");
    if (event.isHeartbeat()) {
      return null;
    }
    EventDeliveryId id = EventDeliveryId.fromEvent(event);
    String subId = event.getSubscriptionId();
    int bucketId = event.getBucketId();
    String memberId = event.getMemberId();
    long ver = event.getRegionVersion();
    synchronized (getStreamStageLock(bucketId, memberId)) {
      long committed = store.getCommittedWatermark(subId, bucketId, memberId);
      if (ver <= committed || store.isEffectApplied(id) || store.isPendingIntent(id)) {
        return id;
      }
      if (crashHook != null) {
        crashHook.onBoundary(CrashPhase.BEFORE_DURABLE_PENDING_STAGE, event, id);
      }
      if (store.stagePendingIntentIfAbsent(event, id)) {
        durableAdmissionsCount.incrementAndGet();
      }
      store.persistReceivedWatermark(subId, bucketId, memberId, ver);
    }
    return id;
  }

  /**
   * Processes a FIFO batch of received {@link ContinuousVectorEvent}s by sequentially staging every
   * non-heartbeat match intent into durable {@code PENDING} state before executing side effects or
   * advancing committed watermarks for any later match or trailing heartbeat in the batch.
   *
   * @return number of first-time match effects / heartbeats processed.
   */
  public int processEventBatch(List<ContinuousVectorEvent> events) {
    Objects.requireNonNull(events, "events must not be null");
    for (ContinuousVectorEvent event : events) {
      if (event != null && !event.isHeartbeat()) {
        stagePendingIntent(event);
      }
    }
    int processed = 0;
    for (ContinuousVectorEvent event : events) {
      if (event != null && processEvent(event)) {
        processed++;
      }
    }
    return processed;
  }

  /**
   * Processes an incoming {@link ContinuousVectorEvent} through sequential per-stream durable
   * pending-intent staging, idempotent {@link EventDeliveryId} ledger checking, effect execution,
   * and contiguous prefix watermark commitment below any unfinished {@code PENDING} intent.
   *
   * @return {@code true} if the event effect was executed (or heartbeat processed) for the first
   *         time, or {@code false} if deterministically suppressed as an already-applied duplicate.
   */
  public boolean processEvent(ContinuousVectorEvent event) {
    Objects.requireNonNull(event, "event must not be null");
    int bucketId = event.getBucketId();
    String memberId = event.getMemberId();
    long ver = event.getRegionVersion();
    Object streamLock = getStreamStageLock(bucketId, memberId);

    if (event.isHeartbeat()) {
      synchronized (streamLock) {
        Collection<String> subs = gate.getActiveSubscriptionIds();
        if (subs.isEmpty()) {
          subs = store.getTrackedSubscriptions(bucketId, memberId);
        }
        boolean blockedByPending = false;
        Map<String, Long> safeGateAdvances = null;
        for (String subId : subs) {
          store.persistReceivedWatermark(subId, bucketId, memberId, ver);
          long lowestPending = store.getLowestPendingVersion(subId, bucketId, memberId);
          long safeCommit =
              (lowestPending == Long.MAX_VALUE) ? ver : Math.min(ver, lowestPending - 1L);
          if (safeCommit > store.getCommittedWatermark(subId, bucketId, memberId)) {
            store.persistCommittedWatermark(subId, bucketId, memberId, safeCommit);
          }
          if (lowestPending <= ver) {
            blockedByPending = true;
          }
          if (safeCommit > 0L) {
            if (safeGateAdvances == null) {
              safeGateAdvances = new HashMap<>();
            }
            safeGateAdvances.put(subId, safeCommit);
          }
        }
        if (blockedByPending) {
          if (safeGateAdvances != null) {
            for (Map.Entry<String, Long> e : safeGateAdvances.entrySet()) {
              gate.restoreHighWaterVersion(e.getKey(), bucketId, memberId, e.getValue());
            }
          }
          return true;
        }
        return gate.tryAccept(event);
      }
    }

    EventDeliveryId id = EventDeliveryId.fromEvent(event);
    String subId = event.getSubscriptionId();
    // Phase 1: Atomic durable admission check over committed + applied + pending state AND WAL
    // staging inside ONE streamStageLock (no unlocked RvvClientGate pre-check).
    synchronized (streamLock) {
      long committed = store.getCommittedWatermark(subId, bucketId, memberId);
      if (ver <= committed || store.isEffectApplied(id)) {
        deduplicatedEffectsCount.incrementAndGet();
        store.markPendingCompleted(id);
        long lowestPending = store.getLowestPendingVersion(subId, bucketId, memberId);
        if (lowestPending > ver) {
          long updated = advanceContiguousCommittedWatermarkLocked(subId, bucketId, memberId, ver);
          gate.restoreHighWaterVersion(subId, bucketId, memberId, updated);
        }
        return false;
      }

      if (store.isPendingIntent(id)) {
        if (inFlightExecutions.contains(id)) {
          deduplicatedEffectsCount.incrementAndGet();
          return false;
        }
      } else {
        if (crashHook != null) {
          crashHook.onBoundary(CrashPhase.BEFORE_DURABLE_PENDING_STAGE, event, id);
        }
        if (store.stagePendingIntentIfAbsent(event, id)) {
          durableAdmissionsCount.incrementAndGet();
        }
      }
      store.persistReceivedWatermark(subId, bucketId, memberId, ver);
    }

    // Phase 2: Per-stream execution/recovery fence serializes handler execution against both
    // out-of-order later versions on the same stream and reconnect/restart recovery.
    ReentrantLock execFence = getStreamExecutionFence(subId, bucketId, memberId);
    execFence.lock();
    try {
      synchronized (streamLock) {
        long committed = store.getCommittedWatermark(subId, bucketId, memberId);
        if (ver <= committed || store.isEffectApplied(id)) {
          deduplicatedEffectsCount.incrementAndGet();
          store.markPendingCompleted(id);
          long lowestPending = store.getLowestPendingVersion(subId, bucketId, memberId);
          if (lowestPending > ver) {
            long updated =
                advanceContiguousCommittedWatermarkLocked(subId, bucketId, memberId, ver);
            gate.restoreHighWaterVersion(subId, bucketId, memberId, updated);
          }
          return false;
        }
        inFlightExecutions.add(id);
      }

      boolean appliedNow;
      try {
        if (crashHook != null) {
          crashHook.onBoundary(CrashPhase.AFTER_WATERMARK_PERSIST_BEFORE_EFFECT, event, id);
        }

        appliedNow = executeIdempotentlyLocked(event, id);

        // Window B: crash-hook point right after effect+ledger execution, BEFORE committed
        // watermark
        if (crashHook != null) {
          crashHook.onBoundary(CrashPhase.AFTER_EFFECT_BEFORE_WATERMARK_COMMIT, event, id);
        }
      } finally {
        inFlightExecutions.remove(id);
      }

      synchronized (streamLock) {
        store.markPendingCompleted(id);
        long lowestPending = store.getLowestPendingVersion(subId, bucketId, memberId);
        if (lowestPending > ver) {
          long prevCommitted = store.getCommittedWatermark(subId, bucketId, memberId);
          long committed =
              advanceContiguousCommittedWatermarkLocked(subId, bucketId, memberId, ver);
          gate.tryAccept(event);
          if (committed > prevCommitted && committed > ver) {
            gate.restoreHighWaterVersion(subId, bucketId, memberId, committed);
          }
        }
        return appliedNow;
      }
    } finally {
      execFence.unlock();
    }
  }

  /**
   * Executes client crash recovery on restart or ordinary reconnect: acquires per-stream
   * execution/recovery fences so no in-flight handler can overlap recovery, drains every
   * uncompleted durable pending intent in ascending {@link EventDeliveryId} order, advances
   * committed watermarks, and restores {@link RvvClientGate}'s in-memory state.
   *
   * @return number of durable pending intents recovered and closed.
   */
  public int recover() {
    Set<String> fenceKeys = new HashSet<>(streamExecutionFences.keySet());
    for (ContinuousVectorEvent ev : store.getPendingIntentsOrdered()) {
      fenceKeys.add(streamFenceKey(ev.getSubscriptionId(), ev.getBucketId(), ev.getMemberId()));
    }
    for (EventDeliveryId key : store.getCommittedWatermarkSnapshot().keySet()) {
      fenceKeys.add(streamFenceKey(key.getSubscriptionId(), key.getBucketId(), key.getMemberId()));
    }
    List<ReentrantLock> acquired = acquireSortedExecutionFences(fenceKeys);
    try {
      return recoverUnderFencesLocked(null);
    } finally {
      releaseExecutionFences(acquired);
    }
  }

  private int recoverUnderFencesLocked(Integer bucketFilter) {
    List<ContinuousVectorEvent> pending = store.getPendingIntentsOrdered();
    int recovered = 0;
    for (ContinuousVectorEvent event : pending) {
      if (bucketFilter != null && event.getBucketId() != bucketFilter) {
        continue;
      }
      EventDeliveryId id = EventDeliveryId.fromEvent(event);
      ReentrantLock fence = getStreamExecutionFence(event.getSubscriptionId(), event.getBucketId(),
          event.getMemberId());
      fence.lock();
      try {
        if (!store.isPendingIntent(id)) {
          continue;
        }
        executeIdempotentlyLocked(event, id);
        synchronized (getStreamStageLock(event.getBucketId(), event.getMemberId())) {
          store.markPendingCompleted(id);
          long committed = advanceContiguousCommittedWatermarkLocked(event.getSubscriptionId(),
              event.getBucketId(), event.getMemberId(), event.getRegionVersion());
          gate.restoreHighWaterVersion(event.getSubscriptionId(), event.getBucketId(),
              event.getMemberId(), committed);
          recovered++;
        }
      } finally {
        fence.unlock();
      }
    }
    if (recovered > 0) {
      recoveredPendingCount.addAndGet(recovered);
    }

    // Also restore committed watermarks for streams with no remaining pending intents (including
    // 0%-match subscribers updated via heartbeats) so gate.getSafeHighWaterByMember stays exact.
    for (Map.Entry<EventDeliveryId, Long> entry : store.getCommittedWatermarkSnapshot()
        .entrySet()) {
      EventDeliveryId key = entry.getKey();
      if (bucketFilter != null && key.getBucketId() != bucketFilter) {
        continue;
      }
      synchronized (getStreamStageLock(key.getBucketId(), key.getMemberId())) {
        gate.restoreHighWaterVersion(key.getSubscriptionId(), key.getBucketId(),
            key.getMemberId(), entry.getValue());
      }
    }
    return recovered;
  }

  private long advanceContiguousCommittedWatermarkLocked(String subId, int bucketId,
      String memberId, long completedVersion) {
    long prevCommitted = store.getCommittedWatermark(subId, bucketId, memberId);
    long lowestPending = store.getLowestPendingVersion(subId, bucketId, memberId);
    if (lowestPending <= completedVersion) {
      return prevCommitted;
    }
    long safe = Math.max(prevCommitted, completedVersion);
    while (safe + 1L < lowestPending
        && store.isEffectApplied(new EventDeliveryId(subId, bucketId, memberId, safe + 1L))) {
      safe++;
    }
    if (safe > prevCommitted) {
      store.persistCommittedWatermark(subId, bucketId, memberId, safe);
    }
    return safe;
  }

  private boolean executeIdempotentlyLocked(ContinuousVectorEvent event, EventDeliveryId id) {
    if (!store.isEffectApplied(id)) {
      effectHandler.apply(event, id);
      if (store.markEffectAppliedIfAbsent(id)) {
        effectsExecutedCount.incrementAndGet();
        return true;
      }
    }
    deduplicatedEffectsCount.incrementAndGet();
    return false;
  }

  /**
   * Prepares an ordinary client reconnect (without process restart) by fencing every in-flight
   * handler on {@code bucketId}, draining every unfinished durable {@code PENDING} intent whose
   * effect has not yet retired, committing watermarks, and returning the safe {@code V_client}
   * high-water map by member for {@code bucketId} while fenced.
   */
  public Map<String, Long> prepareForReconnect(int bucketId) {
    Set<String> fenceKeys = new HashSet<>();
    String bucketToken = "|" + bucketId + "|";
    for (String existingKey : streamExecutionFences.keySet()) {
      if (existingKey.contains(bucketToken)) {
        fenceKeys.add(existingKey);
      }
    }
    for (ContinuousVectorEvent ev : store.getPendingIntentsOrdered()) {
      if (ev.getBucketId() == bucketId) {
        fenceKeys.add(streamFenceKey(ev.getSubscriptionId(), ev.getBucketId(), ev.getMemberId()));
      }
    }
    for (EventDeliveryId key : store.getCommittedWatermarkSnapshot().keySet()) {
      if (key.getBucketId() == bucketId) {
        fenceKeys.add(streamFenceKey(key.getSubscriptionId(), key.getBucketId(),
            key.getMemberId()));
      }
    }
    List<ReentrantLock> acquired = acquireSortedExecutionFences(fenceKeys);
    try {
      recoverUnderFencesLocked(bucketId);
      return getCommittedHighWaterByMember(bucketId);
    } finally {
      releaseExecutionFences(acquired);
    }
  }

  /**
   * Executes client-side pending intent retirement via {@link #prepareForReconnect(int)} before
   * issuing server-side reconnect replay for {@code bucketId}, ensuring notifications already
   * staged in durable {@code PENDING} WAL prior to connection loss are never re-delivered twice.
   */
  public List<ContinuousVectorEvent> reconnectAndReplay(ContinuousVectorQueryService service,
      int bucketId) {
    return reconnectAndReplay(service, bucketId, null, null);
  }

  public List<ContinuousVectorEvent> reconnectAndReplay(ContinuousVectorQueryService service,
      int bucketId, ContinuousVectorEventSink targetSink, Runnable onReplayedUnderLock) {
    Objects.requireNonNull(service, "service must not be null");
    Map<String, Long> clientHw = prepareForReconnect(bucketId);
    return service.replayUncheckpointedForReconnect(bucketId, clientHw, targetSink,
        onReplayedUnderLock);
  }

  /**
   * Executes client-side pending intent retirement via {@link #prepareForReconnect(int)}, then
   * atomically validates {@code V_client >= V_floor}, enrolls {@code subscriberId} in
   * {@code S_healthy}, and replays {@code (V_client, V_S]} under server locks.
   */
  public List<ContinuousVectorEvent> admitAndReconnectReplay(ContinuousVectorQueryService service,
      String subscriberId, int bucketId, ContinuousVectorEventSink targetSink,
      Runnable onReplayedUnderLock) {
    Objects.requireNonNull(service, "service must not be null");
    Map<String, Long> clientHw = prepareForReconnect(bucketId);
    return service.admitAndReplayForReconnect(subscriberId, bucketId, clientHw, targetSink,
        onReplayedUnderLock);
  }

  /**
   * Returns safe committed high-water marks keyed by {@code memberId} for passing to
   * {@link RvvMonotonicVectorDispatcher#replayUncheckpointedForReconnect} after recovery.
   */
  public Map<String, Long> getCommittedHighWaterByMember(int bucketId) {
    return store.getCommittedHighWaterByMember(bucketId, gate.getActiveSubscriptionIds());
  }

  public RvvClientGate getGate() {
    return gate;
  }

  public DurableDeliveryStore getStore() {
    return store;
  }

  public long getEffectsExecutedCount() {
    return effectsExecutedCount.get();
  }

  public long getDeduplicatedEffectsCount() {
    return deduplicatedEffectsCount.get();
  }

  public long getRecoveredPendingCount() {
    return recoveredPendingCount.get();
  }

  public long getDurableAdmissionsCount() {
    return durableAdmissionsCount.get();
  }
}
