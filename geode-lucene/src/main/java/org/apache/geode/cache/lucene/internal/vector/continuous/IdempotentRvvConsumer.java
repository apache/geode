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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

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
  private final ConcurrentHashMap<String, Object> streamLocks = new ConcurrentHashMap<>();
  private final AtomicLong effectsExecutedCount = new AtomicLong();
  private final AtomicLong deduplicatedEffectsCount = new AtomicLong();
  private final AtomicLong recoveredPendingCount = new AtomicLong();

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

  @Override
  public void onEvent(ContinuousVectorEvent event) {
    processEvent(event);
  }

  /**
   * Processes an incoming {@link ContinuousVectorEvent} through the durable pending-intent log,
   * idempotent {@link EventDeliveryId} ledger, and committed-watermark persistence.
   *
   * @return {@code true} if the event effect was executed (or heartbeat processed) for the first
   *         time, or {@code false} if deterministically suppressed as an already-applied duplicate.
   */
  public boolean processEvent(ContinuousVectorEvent event) {
    Objects.requireNonNull(event, "event must not be null");
    if (event.isHeartbeat()) {
      int bucketId = event.getBucketId();
      String memberId = event.getMemberId();
      long ver = event.getRegionVersion();
      Collection<String> subs = gate.getActiveSubscriptionIds();
      if (subs.isEmpty()) {
        subs = store.getTrackedSubscriptions(bucketId, memberId);
      }
      boolean blockedByPending = false;
      Map<String, Long> safeGateAdvances = null;
      for (String subId : subs) {
        Object lock = streamLocks.computeIfAbsent(subId + "|" + bucketId + "|" + memberId,
            ignored -> new Object());
        synchronized (lock) {
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

    EventDeliveryId id = EventDeliveryId.fromEvent(event);
    String lockKey = event.getSubscriptionId() + "|" + event.getBucketId() + "|"
        + event.getMemberId();
    Object lock = streamLocks.computeIfAbsent(lockKey, ignored -> new Object());
    synchronized (lock) {
      long gateHighWater = gate.getHighWaterVersion(event.getSubscriptionId(), event.getBucketId(),
          event.getMemberId());
      if (event.getRegionVersion() <= gateHighWater && store.isEffectApplied(id)) {
        return false;
      }

      // Window A: durably stage pending intent + received watermark BEFORE executing side effect
      store.stagePendingIntent(event, id);
      store.persistReceivedWatermark(event.getSubscriptionId(), event.getBucketId(),
          event.getMemberId(), event.getRegionVersion());
      if (crashHook != null) {
        crashHook.onBoundary(CrashPhase.AFTER_WATERMARK_PERSIST_BEFORE_EFFECT, event, id);
      }

      boolean appliedNow = executeIdempotentlyLocked(event, id);

      // Window B: crash-hook point right after effect+ledger execution, BEFORE committed watermark
      if (crashHook != null) {
        crashHook.onBoundary(CrashPhase.AFTER_EFFECT_BEFORE_WATERMARK_COMMIT, event, id);
      }

      store.markPendingCompleted(id);
      String subId = event.getSubscriptionId();
      int bucketId = event.getBucketId();
      String memberId = event.getMemberId();
      long ver = event.getRegionVersion();
      long lowestPending = store.getLowestPendingVersion(subId, bucketId, memberId);
      if (lowestPending > ver) {
        long prevCommitted = store.getCommittedWatermark(subId, bucketId, memberId);
        long committed = advanceContiguousCommittedWatermarkLocked(subId, bucketId, memberId, ver);
        gate.tryAccept(event);
        if (committed > prevCommitted && committed > ver) {
          gate.restoreHighWaterVersion(subId, bucketId, memberId, committed);
        }
      }
      return appliedNow;
    }
  }

  /**
   * Executes client crash recovery on restart: drains every uncompleted durable pending intent in
   * ascending {@link EventDeliveryId} order (executing effects only if absent from the applied
   * ledger), advances committed watermarks, and restores {@link RvvClientGate}'s in-memory state.
   *
   * @return number of durable pending intents recovered and closed.
   */
  public int recover() {
    List<ContinuousVectorEvent> pending = store.getPendingIntentsOrdered();
    int recovered = 0;
    for (ContinuousVectorEvent event : pending) {
      EventDeliveryId id = EventDeliveryId.fromEvent(event);
      String lockKey = event.getSubscriptionId() + "|" + event.getBucketId() + "|"
          + event.getMemberId();
      Object lock = streamLocks.computeIfAbsent(lockKey, ignored -> new Object());
      synchronized (lock) {
        executeIdempotentlyLocked(event, id);
        store.markPendingCompleted(id);
        long committed = advanceContiguousCommittedWatermarkLocked(event.getSubscriptionId(),
            event.getBucketId(), event.getMemberId(), event.getRegionVersion());
        gate.restoreHighWaterVersion(event.getSubscriptionId(), event.getBucketId(),
            event.getMemberId(), committed);
        recovered++;
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
      gate.restoreHighWaterVersion(key.getSubscriptionId(), key.getBucketId(), key.getMemberId(),
          entry.getValue());
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
}
