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
import java.util.HashSet;
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
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
   * Default bounded per-{@code (bucketId, memberId)} out-of-order version holdback window before a
   * missing hole is skipped to prevent indefinite stalling under aborted writes.
   */
  public static final int DEFAULT_MAX_HOLDBACK_GAP = 128;
  /**
   * Default bounded per-{@code (bucketId, memberId)} maximum holdback age (milliseconds) before a
   * persistent missing predecessor version gap is closed and marked resync-required so contiguous
   * dispatch and watermark checkpoints cannot stall indefinitely ({@code 0} disables age expiry).
   */
  public static final long DEFAULT_MAX_HOLDBACK_AGE_MILLIS = 5_000L;

  /**
   * Callback invoked whenever a per-{@code (bucketId, memberId)} holdback gap is force-closed due
   * to exceeding {@code maxHoldbackGap} or {@code maxHoldbackAgeMillis}, notifying listeners that
   * the skipped version hole requires subscriber resynchronization.
   */
  @FunctionalInterface
  public interface HoldbackGapListener {
    void onHoldbackGapClosed(int bucketId, String memberId, long expectedVersion,
        long resumedVersion, String reason);
  }

  /**
   * Callback invoked whenever a rejoining or reconnecting subscriber presents a client high-water
   * mark strictly below the stream's compacted replay-retention floor {@code V_floor[b, m]},
   * requiring a full snapshot resynchronization before continuity admission.
   */
  @FunctionalInterface
  public interface ReplayFloorResyncListener {
    void onReplayFloorResyncRequired(int bucketId, String memberId, long clientHighWater,
        long replayFloor);
  }

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
    private static final float[] NO_VECTOR_MARKER = new float[0];

    private final Object key;
    private final float[] normalizedVector;
    private final int bucketId;
    private final String memberId;
    private final long regionVersion;
    private final int entryVersion;
    private final long timestamp;
    private final boolean nonMatchingMarker;

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
      this.nonMatchingMarker = false;
    }

    private TailMutation(int bucketId, String memberId, long regionVersion, long timestamp) {
      if (memberId == null || memberId.isEmpty()) {
        throw new IllegalArgumentException("memberId must not be null or empty");
      }
      if (regionVersion <= 0L) {
        throw new IllegalArgumentException("regionVersion must be > 0, got: " + regionVersion);
      }
      this.key = null;
      this.normalizedVector = NO_VECTOR_MARKER;
      this.bucketId = bucketId;
      this.memberId = memberId;
      this.regionVersion = regionVersion;
      this.entryVersion = 0;
      this.timestamp = timestamp;
      this.nonMatchingMarker = true;
    }

    static TailMutation nonMatchingMarker(int bucketId, String memberId, long regionVersion,
        long timestamp) {
      return new TailMutation(bucketId, memberId, regionVersion, timestamp);
    }

    boolean isNonMatchingMarker() {
      return nonMatchingMarker;
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

  private static final class PendingDispatchBatch {
    final List<ContinuousVectorEvent> events;
    final ContinuousVectorEventSink targetSink;

    PendingDispatchBatch(List<ContinuousVectorEvent> events, ContinuousVectorEventSink targetSink) {
      this.events = events;
      this.targetSink = targetSink;
    }
  }

  private static final class BucketState {
    final Map<String, TreeMap<Long, TailMutation>> tailByMember = new HashMap<>();
    final Map<String, Long> checkpointByMember = new HashMap<>();
    final Map<String, Long> replayFloorByMember = new HashMap<>();
    final Map<String, Map<String, Long>> subscriberAcksByMember = new HashMap<>();
    final Map<String, AtomicLong> fallbackVersionByMember = new HashMap<>();
    final Map<String, Long> eventsSinceHeartbeatByMember = new HashMap<>();
    final Map<String, Long> oldestUncoveredMutationTimeMsByMember = new HashMap<>();
    final Map<String, Long> nextExpectedVersionByMember = new HashMap<>();
    final Map<String, TreeMap<Long, TailMutation>> holdbackByMember = new HashMap<>();
    final Map<String, Long> holdbackGapStartTimeMsByMember = new HashMap<>();
    final Map<String, Long> lastDispatchedVersionByMember = new HashMap<>();
    final List<TailMutation> pendingLiveMutations = new ArrayList<>();
    final ConcurrentLinkedQueue<PendingDispatchBatch> pendingDispatchBatches =
        new ConcurrentLinkedQueue<>();
    final AtomicBoolean dispatchDraining = new AtomicBoolean(false);
    volatile Thread drainOwnerThread = null;
    int pendingAsyncPromotions = 0;

    synchronized boolean shouldEmitPeriodicHeartbeat(String memberId, int interval) {
      return shouldEmitPeriodicHeartbeat(memberId, interval, System.currentTimeMillis());
    }

    synchronized boolean shouldEmitPeriodicHeartbeat(String memberId, int interval,
        long mutationTimestampMs) {
      if (interval <= 0) {
        return false;
      }
      long prev = eventsSinceHeartbeatByMember.getOrDefault(memberId, 0L);
      long next = prev + 1L;
      if (next >= interval) {
        eventsSinceHeartbeatByMember.put(memberId, 0L);
        oldestUncoveredMutationTimeMsByMember.put(memberId, 0L);
        return true;
      }
      if (prev == 0L) {
        long firstTs = mutationTimestampMs > 0L ? mutationTimestampMs : System.currentTimeMillis();
        oldestUncoveredMutationTimeMsByMember.put(memberId, firstTs);
      }
      eventsSinceHeartbeatByMember.put(memberId, next);
      return false;
    }

    synchronized void resetHeartbeatCounter(String memberId) {
      eventsSinceHeartbeatByMember.put(memberId, 0L);
      oldestUncoveredMutationTimeMsByMember.put(memberId, 0L);
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
          replayFloorByMember.merge(mutation.memberId, evicted.getKey(), Math::max);
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

    synchronized List<TailMutation> collectReplayableTailForReconnect(int bucketId,
        Map<String, Long> clientHighWaterByMember, boolean respectHoldbackCeiling,
        Map<String, Long> maxVersionByMemberOut,
        List<ReplayFloorViolationRecord> floorViolationsOut) {
      List<TailMutation> replay = new ArrayList<>();
      Set<String> allMembers = new HashSet<>(tailByMember.keySet());
      allMembers.addAll(replayFloorByMember.keySet());
      List<String> sortedMembers = new ArrayList<>(allMembers);
      Collections.sort(sortedMembers);
      for (String memberId : sortedMembers) {
        long floor = replayFloorByMember.getOrDefault(memberId, 0L);
        long ckpt = checkpointByMember.getOrDefault(memberId, 0L);
        long clientHighWater = 0L;
        if (clientHighWaterByMember != null) {
          Long hw = clientHighWaterByMember.get(memberId);
          if (hw != null && hw > 0L) {
            clientHighWater = hw;
          }
          if (floor > 0L && clientHighWater < floor) {
            if (floorViolationsOut != null) {
              floorViolationsOut.add(
                  new ReplayFloorViolationRecord(bucketId, memberId, clientHighWater, floor));
            }
            continue;
          }
        }
        TreeMap<Long, TailMutation> stream = tailByMember.get(memberId);
        if (stream == null || stream.isEmpty()) {
          continue;
        }
        long lowerExclusive = Math.max(Math.max(ckpt, floor), clientHighWater);
        long streamHighWater = stream.lastKey();
        TreeMap<Long, TailMutation> held = holdbackByMember.get(memberId);
        if (respectHoldbackCeiling && held != null && !held.isEmpty()) {
          streamHighWater = Math.min(streamHighWater, held.firstKey() - 1L);
        }
        if (lowerExclusive < streamHighWater) {
          maxVersionByMemberOut.put(memberId, streamHighWater);
          NavigableMap<Long, TailMutation> tail =
              stream.subMap(lowerExclusive, false, streamHighWater, true);
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
      replayFloorByMember.merge(memberId, ackedVersion, Math::max);
      TreeMap<Long, TailMutation> stream = tailByMember.get(memberId);
      if (stream != null) {
        stream.headMap(ackedVersion, true).clear();
      }
    }

    synchronized long getCheckpoint(String memberId) {
      return checkpointByMember.getOrDefault(memberId, 0L);
    }

    synchronized long getReplayFloor(String memberId) {
      return replayFloorByMember.getOrDefault(memberId, 0L);
    }

    synchronized long recordSubscriberAck(String subscriberId, String memberId, long ackedVersion,
        Set<String> healthySubscribers) {
      Map<String, Long> acks =
          subscriberAcksByMember.computeIfAbsent(memberId, ignored -> new HashMap<>());
      acks.merge(subscriberId, ackedVersion, Math::max);
      return recomputeMinHealthyCheckpoint(memberId, healthySubscribers);
    }

    synchronized void removeSubscriberFromAcks(String subscriberId,
        Set<String> remainingHealthySubscribers) {
      if (subscriberAcksByMember.isEmpty()) {
        return;
      }
      List<String> members = new ArrayList<>(subscriberAcksByMember.keySet());
      for (String memberId : members) {
        Map<String, Long> acks = subscriberAcksByMember.get(memberId);
        if (acks != null) {
          acks.remove(subscriberId);
        }
        recomputeMinHealthyCheckpoint(memberId, remainingHealthySubscribers);
      }
    }

    synchronized long recomputeMinHealthyCheckpoint(String memberId,
        Set<String> healthySubscribers) {
      if (healthySubscribers == null || healthySubscribers.isEmpty()) {
        return checkpointByMember.getOrDefault(memberId, 0L);
      }
      Map<String, Long> acks = subscriberAcksByMember.get(memberId);
      if (acks == null || acks.isEmpty()) {
        return checkpointByMember.getOrDefault(memberId, 0L);
      }
      long minAck = Long.MAX_VALUE;
      for (String subscriberId : healthySubscribers) {
        long ack = acks.getOrDefault(subscriberId, 0L);
        if (ack <= 0L) {
          minAck = 0L;
          break;
        }
        if (ack < minAck) {
          minAck = ack;
        }
      }
      if (minAck > 0L && minAck != Long.MAX_VALUE) {
        advanceCheckpoint(memberId, minAck);
      }
      return checkpointByMember.getOrDefault(memberId, 0L);
    }

    synchronized long getSubscriberAck(String subscriberId, String memberId) {
      Map<String, Long> acks = subscriberAcksByMember.get(memberId);
      return acks == null ? 0L : acks.getOrDefault(subscriberId, 0L);
    }

    synchronized long getLastDispatchedVersion(String memberId) {
      return lastDispatchedVersionByMember.getOrDefault(memberId, 0L);
    }

    synchronized long getNextExpectedVersion(String memberId) {
      return nextExpectedVersionByMember.getOrDefault(memberId, 1L);
    }

    synchronized int getTailSize() {
      int total = 0;
      for (TreeMap<Long, TailMutation> stream : tailByMember.values()) {
        total += stream.size();
      }
      return total;
    }

    synchronized int getHoldbackSize() {
      int total = 0;
      for (TreeMap<Long, TailMutation> holdback : holdbackByMember.values()) {
        total += holdback.size();
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

  private static final class HoldbackGapRecord {
    final int bucketId;
    final String memberId;
    final long expectedVersion;
    final long resumedVersion;
    final String reason;

    HoldbackGapRecord(int bucketId, String memberId, long expectedVersion, long resumedVersion,
        String reason) {
      this.bucketId = bucketId;
      this.memberId = memberId;
      this.expectedVersion = expectedVersion;
      this.resumedVersion = resumedVersion;
      this.reason = reason;
    }
  }

  private static final class ReplayFloorViolationRecord {
    final int bucketId;
    final String memberId;
    final long clientHighWater;
    final long replayFloor;

    ReplayFloorViolationRecord(int bucketId, String memberId, long clientHighWater,
        long replayFloor) {
      this.bucketId = bucketId;
      this.memberId = memberId;
      this.clientHighWater = clientHighWater;
      this.replayFloor = replayFloor;
    }
  }

  private final ChordalVoronoiRouter router;
  private final int maxTailPerStream;
  private final ConcurrentHashMap<Integer, BucketState> bucketStates = new ConcurrentHashMap<>();
  private final CopyOnWriteArrayList<ContinuousVectorEventSink> sinks =
      new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<TailOverflowListener> overflowListeners =
      new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<HoldbackGapListener> holdbackGapListeners =
      new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<ReplayFloorResyncListener> replayFloorListeners =
      new CopyOnWriteArrayList<>();
  private final Set<RvvStreamKey> resyncRequiredStreams = ConcurrentHashMap.newKeySet();
  private final Set<String> registeredCheckpointSubscribers = ConcurrentHashMap.newKeySet();
  private final Object subscriberRegistryLock = new Object();

  private final AtomicLong totalEventsDispatched = new AtomicLong();
  private final AtomicLong totalBatchesDispatched = new AtomicLong();
  private final AtomicLong totalHeartbeatsDispatched = new AtomicLong();
  private final AtomicLong totalReplayedEventsDispatched = new AtomicLong();
  private final AtomicLong totalPromotionsHandled = new AtomicLong();
  private final AtomicLong totalReconnectReplaysHandled = new AtomicLong();
  private final AtomicLong tailOverflowCount = new AtomicLong();
  private final AtomicLong holdbackGapClosedCount = new AtomicLong();
  private final AtomicLong replayFloorResyncCount = new AtomicLong();
  private final AtomicLong totalIdleHeartbeatsFlushed = new AtomicLong();
  private final AtomicLong totalHoldbackBufferedCount = new AtomicLong();
  private final AtomicLong totalNonMatchingVersionsObserved = new AtomicLong();
  private final Object idleSchedulerLock = new Object();
  private volatile Executor dispatchExecutor;
  private volatile int heartbeatIntervalEvents;
  private volatile boolean enforceContiguousVersions = true;
  private volatile int maxHoldbackGap = DEFAULT_MAX_HOLDBACK_GAP;
  private volatile long maxHoldbackAgeMillis = DEFAULT_MAX_HOLDBACK_AGE_MILLIS;
  private ScheduledExecutorService idleHeartbeatScheduler;

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
   * Controls whether live primary events must strictly follow contiguous ascending
   * {@code regionVersion} ordering per {@code (bucketId, memberId)} via the bounded holdback
   * buffer (default {@code true}).
   */
  public void setEnforceContiguousVersions(boolean enforceContiguousVersions) {
    this.enforceContiguousVersions = enforceContiguousVersions;
  }

  public boolean isEnforceContiguousVersions() {
    return enforceContiguousVersions;
  }

  /**
   * Configures the maximum out-of-order version gap held in the per-{@code (bucketId, memberId)}
   * holdback buffer before advancing past a missing hole (default {@code 128}).
   */
  public void setMaxHoldbackGap(int maxHoldbackGap) {
    if (maxHoldbackGap <= 0) {
      throw new IllegalArgumentException("maxHoldbackGap must be > 0, got: " + maxHoldbackGap);
    }
    this.maxHoldbackGap = maxHoldbackGap;
  }

  public int getMaxHoldbackGap() {
    return maxHoldbackGap;
  }

  /**
   * Configures the bounded maximum holdback age (milliseconds) before a delayed predecessor gap in
   * the per-{@code (bucketId, memberId)} holdback buffer is closed and flagged as resync-required
   * (default {@code 5000 ms}; pass {@code 0} to disable age-based gap closure).
   */
  public void setMaxHoldbackAgeMillis(long maxHoldbackAgeMillis) {
    if (maxHoldbackAgeMillis < 0L) {
      throw new IllegalArgumentException(
          "maxHoldbackAgeMillis must be >= 0, got: " + maxHoldbackAgeMillis);
    }
    this.maxHoldbackAgeMillis = maxHoldbackAgeMillis;
  }

  public long getMaxHoldbackAgeMillis() {
    return maxHoldbackAgeMillis;
  }

  /**
   * Closes any held out-of-order version gaps whose oldest holdback entry has waited at least
   * {@link #getMaxHoldbackAgeMillis()}, marking the affected {@code (bucketId, memberId)} stream
   * as resync-required and draining buffered successors contiguously.
   *
   * @return number of holdback version gaps closed.
   */
  public int flushExpiredHoldbackGaps() {
    return flushExpiredHoldbackGaps(System.currentTimeMillis());
  }

  /**
   * Closes any held out-of-order version gaps whose oldest holdback entry reached
   * {@link #getMaxHoldbackAgeMillis()} as of {@code nowMillis}, marking affected streams
   * resync-required and draining ready successors contiguously.
   *
   * @return number of holdback version gaps closed.
   */
  public int flushExpiredHoldbackGaps(long nowMillis) {
    if (!enforceContiguousVersions || maxHoldbackAgeMillis <= 0L) {
      return 0;
    }
    List<HoldbackGapRecord> gapRecords = new ArrayList<>();
    for (Map.Entry<Integer, BucketState> bucketEntry : bucketStates.entrySet()) {
      int bucketId = bucketEntry.getKey();
      BucketState state = bucketEntry.getValue();
      synchronized (state) {
        if (state.holdbackByMember.isEmpty()) {
          continue;
        }
        List<String> members = new ArrayList<>(state.holdbackByMember.keySet());
        Collections.sort(members);
        for (String memberId : members) {
          TreeMap<Long, TailMutation> holdback = state.holdbackByMember.get(memberId);
          if (holdback == null || holdback.isEmpty()) {
            continue;
          }
          long gapStartMs = resolveOldestHoldbackTimestampLocked(state, memberId, holdback,
              nowMillis);
          if (gapStartMs <= 0L || (nowMillis - gapStartMs) < maxHoldbackAgeMillis) {
            continue;
          }
          long nextExpected = state.nextExpectedVersionByMember.getOrDefault(memberId, 1L);
          long firstHeld = holdback.firstKey();
          if (firstHeld > nextExpected) {
            gapRecords.add(new HoldbackGapRecord(bucketId, memberId, nextExpected, firstHeld,
                "MAX_HOLDBACK_AGE_EXCEEDED"));
            state.nextExpectedVersionByMember.put(memberId, firstHeld);
          }
          drainContiguousHoldbackLocked(state, bucketId, memberId, holdback, nowMillis, gapRecords);
        }
      }
    }
    if (!gapRecords.isEmpty()) {
      notifyHoldbackGaps(gapRecords);
    }
    return gapRecords.size();
  }

  /**
   * Starts a background daemon scheduler that periodically flushes idle-tail watermark heartbeats
   * via {@link #flushIdleHeartbeats(long, long)}.
   */
  public void startIdleHeartbeatScheduler(long maxIdleMillis, long checkPeriodMillis) {
    if (maxIdleMillis < 0L) {
      throw new IllegalArgumentException("maxIdleMillis must be >= 0, got: " + maxIdleMillis);
    }
    if (checkPeriodMillis <= 0L) {
      throw new IllegalArgumentException(
          "checkPeriodMillis must be > 0, got: " + checkPeriodMillis);
    }
    synchronized (idleSchedulerLock) {
      stopIdleHeartbeatScheduler();
      ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "rvv-idle-heartbeat-flusher");
        t.setDaemon(true);
        return t;
      });
      scheduler.scheduleWithFixedDelay(
          () -> flushIdleHeartbeats(System.currentTimeMillis(), maxIdleMillis), checkPeriodMillis,
          checkPeriodMillis, TimeUnit.MILLISECONDS);
      this.idleHeartbeatScheduler = scheduler;
    }
  }

  /** Stops the background idle-tail watermark heartbeat scheduler, if running. */
  public void stopIdleHeartbeatScheduler() {
    synchronized (idleSchedulerLock) {
      if (idleHeartbeatScheduler != null) {
        idleHeartbeatScheduler.shutdownNow();
        idleHeartbeatScheduler = null;
      }
    }
  }

  /**
   * Flushes trailing watermark heartbeats for streams whose oldest uncovered mutation reached
   * {@code maxAgeMillis}.
   */
  public int flushIdleHeartbeats(long maxAgeMillis) {
    return flushIdleHeartbeats(System.currentTimeMillis(), maxAgeMillis);
  }

  /**
   * Checks every {@code (bucketId, memberId)} stream and emits a trailing
   * {@link ContinuousVectorEvent#watermarkHeartbeat} carrying the last contiguously dispatched
   * version whenever {@code eventsSinceHeartbeat > 0} and the age since the oldest uncovered
   * mutation reaches {@code maxAgeMillis}
   * ({@code nowMillis - oldestUncoveredMutationTimeMs >= maxAgeMillis}), resetting both
   * {@code eventsSinceHeartbeat} and {@code oldestUncoveredMutationTimeMs} to {@code 0}. This
   * prevents starvation under both idle tails and steady sub-{@code H} trickles.
   *
   * @return number of age-bounded watermark heartbeats emitted.
   */
  public int flushIdleHeartbeats(long nowMillis, long maxAgeMillis) {
    if (maxAgeMillis < 0L) {
      throw new IllegalArgumentException("maxAgeMillis must be >= 0, got: " + maxAgeMillis);
    }
    if (maxHoldbackAgeMillis > 0L) {
      flushExpiredHoldbackGaps(nowMillis);
    }
    if (heartbeatIntervalEvents <= 0) {
      return 0;
    }
    int flushed = 0;
    for (Map.Entry<Integer, BucketState> bucketEntry : bucketStates.entrySet()) {
      int bucketId = bucketEntry.getKey();
      BucketState state = bucketEntry.getValue();
      synchronized (state) {
        if (state.eventsSinceHeartbeatByMember.isEmpty()) {
          continue;
        }
        List<String> members = new ArrayList<>(state.eventsSinceHeartbeatByMember.keySet());
        Collections.sort(members);
        for (String memberId : members) {
          long pending = state.eventsSinceHeartbeatByMember.getOrDefault(memberId, 0L);
          if (pending <= 0L) {
            continue;
          }
          long oldestUncoveredMs =
              state.oldestUncoveredMutationTimeMsByMember.getOrDefault(memberId, 0L);
          if (oldestUncoveredMs <= 0L || (nowMillis - oldestUncoveredMs) < maxAgeMillis) {
            continue;
          }
          long lastVersion = state.lastDispatchedVersionByMember.getOrDefault(memberId, 0L);
          if (lastVersion <= 0L) {
            continue;
          }
          state.resetHeartbeatCounter(memberId);
          ContinuousVectorEvent hb = ContinuousVectorEvent.watermarkHeartbeat(bucketId, memberId,
              lastVersion, nowMillis);
          totalHeartbeatsDispatched.incrementAndGet();
          totalIdleHeartbeatsFlushed.incrementAndGet();
          enqueueOrDispatchBatchLocked(state, Collections.singletonList(hb), null);
          flushed++;
        }
      }
    }
    return flushed;
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

  public void addHoldbackGapListener(HoldbackGapListener listener) {
    holdbackGapListeners.add(Objects.requireNonNull(listener, "listener must not be null"));
  }

  public boolean removeHoldbackGapListener(HoldbackGapListener listener) {
    return holdbackGapListeners.remove(listener);
  }

  public void addReplayFloorResyncListener(ReplayFloorResyncListener listener) {
    replayFloorListeners.add(Objects.requireNonNull(listener, "listener must not be null"));
  }

  public boolean removeReplayFloorResyncListener(ReplayFloorResyncListener listener) {
    return replayFloorListeners.remove(listener);
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
    List<HoldbackGapRecord> gapRecords = null;
    List<ContinuousVectorEvent> emitted;
    // Serialize append + contiguous ordering + dispatch per bucket to preserve per-(b, m) order
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
      } else if (enforceContiguousVersions) {
        gapRecords = new ArrayList<>(1);
        emitted = dispatchContiguousOrHoldLocked(state, bucketId, mutation, gapRecords);
      } else {
        emitted = dispatchSingleVersionLocked(state, mutation, false);
      }
    }
    if (overflows != null) {
      notifyOverflows(overflows);
    }
    if (gapRecords != null && !gapRecords.isEmpty()) {
      notifyHoldbackGaps(gapRecords);
    }
    return emitted;
  }

  /**
   * Records a non-matching bucket RVV version (e.g., {@code destroy}, {@code invalidate}, or a
   * non-vector entry mutation) so contiguous {@code nextExpectedVersion} sequencing advances
   * cleanly without leaving a permanent gap in the holdback buffer.
   */
  public void onNonMatchingBucketVersion(int bucketId, String memberId, long regionVersion) {
    if (memberId == null || memberId.isEmpty()) {
      throw new IllegalArgumentException("memberId must not be null or empty");
    }
    if (regionVersion <= 0L) {
      return;
    }
    totalNonMatchingVersionsObserved.incrementAndGet();
    BucketState state = bucketStates.computeIfAbsent(bucketId, ignored -> new BucketState());
    state.observeVersion(memberId, regionVersion);
    long nowMs = System.currentTimeMillis();
    TailMutation marker = TailMutation.nonMatchingMarker(bucketId, memberId, regionVersion, nowMs);
    List<HoldbackGapRecord> gapRecords = null;
    synchronized (state) {
      if (state.pendingAsyncPromotions > 0) {
        state.pendingLiveMutations.add(marker);
        return;
      }
      if (enforceContiguousVersions) {
        gapRecords = new ArrayList<>(1);
        dispatchContiguousOrHoldLocked(state, bucketId, marker, gapRecords);
      } else {
        dispatchSingleVersionLocked(state, marker, false);
      }
    }
    if (gapRecords != null && !gapRecords.isEmpty()) {
      notifyHoldbackGaps(gapRecords);
    }
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
    List<HoldbackGapRecord> gapRecords = new ArrayList<>();
    List<ContinuousVectorEvent> emitted = new ArrayList<>();
    synchronized (state) {
      List<TailMutation> toReplay = state.collectReplayableTail(recoveredBucketEntries,
          maxTailPerStream, overflows);
      for (TailMutation mutation : toReplay) {
        emitted.addAll(dispatchSingleVersionLocked(state, mutation, true));
        state.nextExpectedVersionByMember.merge(mutation.memberId, mutation.regionVersion + 1L,
            Math::max);
      }
      List<TailMutation> allReplayed = new ArrayList<>(toReplay);
      drainAllHoldbackOnPromotionLocked(state, emitted, allReplayed);
      emitPromotionWatermarksLocked(state, bucketId, allReplayed);
      if (state.pendingAsyncPromotions > 0) {
        state.pendingAsyncPromotions--;
      }
      if (state.pendingAsyncPromotions == 0 && !state.pendingLiveMutations.isEmpty()) {
        drainPendingLiveMutationsLocked(state, emitted, overflows, gapRecords);
      }
    }
    if (!overflows.isEmpty()) {
      notifyOverflows(overflows);
    }
    if (!gapRecords.isEmpty()) {
      notifyHoldbackGaps(gapRecords);
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
    List<HoldbackGapRecord> gapRecords = new ArrayList<>();
    List<ContinuousVectorEvent> emitted = new ArrayList<>();
    synchronized (state) {
      if (state.pendingAsyncPromotions > 0) {
        state.pendingAsyncPromotions--;
      }
      if (state.pendingAsyncPromotions == 0 && !state.pendingLiveMutations.isEmpty()) {
        drainPendingLiveMutationsLocked(state, emitted, overflows, gapRecords);
      }
    }
    if (!overflows.isEmpty()) {
      notifyOverflows(overflows);
    }
    if (!gapRecords.isEmpty()) {
      notifyHoldbackGaps(gapRecords);
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
        emitted.addAll(dispatchSingleVersionLocked(state, mutation, true));
        state.nextExpectedVersionByMember.merge(mutation.memberId, mutation.regionVersion + 1L,
            Math::max);
      }
      List<TailMutation> allReplayed = new ArrayList<>(toReplay);
      drainAllHoldbackOnPromotionLocked(state, emitted, allReplayed);
      emitPromotionWatermarksLocked(state, bucketId, allReplayed);
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
    return replayUncheckpointedForReconnect(bucketId, clientHighWaterByMember, null, null);
  }

  /**
   * Replays uncheckpointed tail mutations for client reconnect under {@code synchronized (state)},
   * pushing replayed batches and trailing watermarks onto the per-bucket serial FIFO queue before
   * invoking {@code onReplayedUnderLock} and releasing the bucket lock so callers can atomically
   * transition from reconnect replay to live {@code m_S} event forwarding without overtaking
   * replayed versions.
   */
  public List<ContinuousVectorEvent> replayUncheckpointedForReconnect(int bucketId,
      Map<String, Long> clientHighWaterByMember, Runnable onReplayedUnderLock) {
    return replayUncheckpointedForReconnect(bucketId, clientHighWaterByMember, null,
        onReplayedUnderLock);
  }

  /**
   * Replays uncheckpointed tail mutations for a specific reconnecting client sink
   * ({@code targetSink}; {@code null} broadcasts to all registered sinks) under
   * {@code synchronized (state)}, pushing replayed batches and trailing watermarks onto the same
   * per-bucket serial FIFO queue before invoking {@code onReplayedUnderLock} and releasing the
   * bucket lock.
   */
  public List<ContinuousVectorEvent> replayUncheckpointedForReconnect(int bucketId,
      Map<String, Long> clientHighWaterByMember, ContinuousVectorEventSink targetSink,
      Runnable onReplayedUnderLock) {
    totalReconnectReplaysHandled.incrementAndGet();
    BucketState state = (onReplayedUnderLock != null)
        ? bucketStates.computeIfAbsent(bucketId, ignored -> new BucketState())
        : bucketStates.get(bucketId);
    if (state == null) {
      return Collections.emptyList();
    }
    List<ContinuousVectorEvent> emitted = new ArrayList<>();
    List<ReplayFloorViolationRecord> floorViolations = new ArrayList<>();
    synchronized (state) {
      TreeMap<String, Long> maxVersionByMember = new TreeMap<>();
      List<TailMutation> toReplay = state.collectReplayableTailForReconnect(bucketId,
          clientHighWaterByMember, enforceContiguousVersions, maxVersionByMember, floorViolations);
      for (TailMutation mutation : toReplay) {
        emitted.addAll(evaluateAndDispatch(state, mutation, true, targetSink));
      }
      emitStreamWatermarksLocked(state, bucketId, maxVersionByMember, targetSink);
      if (onReplayedUnderLock != null) {
        onReplayedUnderLock.run();
      }
    }
    if (!floorViolations.isEmpty()) {
      notifyReplayFloorViolations(floorViolations);
    }
    return emitted;
  }

  /**
   * Validates a reconnecting or newly joining subscriber's high-water mark against the stream's
   * compacted replay-retention floor {@code V_floor[bucketId, memberId]} before admitting it to
   * uninterrupted continuity. If {@code clientHighWater < V_floor}, immediately flags
   * {@code resyncRequired} and returns {@code false} without enrolling {@code subscriberId}.
   */
  public boolean admitSubscriberForContinuity(String subscriberId, int bucketId, String memberId,
      long clientHighWater) {
    if (memberId == null || memberId.isEmpty()) {
      throw new IllegalArgumentException("memberId must not be null or empty");
    }
    ReplayFloorViolationRecord violation = null;
    synchronized (subscriberRegistryLock) {
      BucketState state = bucketStates.computeIfAbsent(bucketId, ignored -> new BucketState());
      long floor = state.getReplayFloor(memberId);
      if (floor > 0L && clientHighWater < floor) {
        violation = new ReplayFloorViolationRecord(bucketId, memberId, clientHighWater, floor);
      } else if (subscriberId != null && !subscriberId.isEmpty()) {
        registeredCheckpointSubscribers.add(subscriberId);
        if (clientHighWater > 0L) {
          Set<String> healthy =
              Collections.unmodifiableSet(new HashSet<>(registeredCheckpointSubscribers));
          state.recordSubscriberAck(subscriberId, memberId, clientHighWater, healthy);
        }
      }
    }
    if (violation != null) {
      notifyReplayFloorViolations(Collections.singletonList(violation));
      return false;
    }
    return true;
  }

  /**
   * Validates every tracked member stream on {@code bucketId} against
   * {@code clientHighWaterByMember}; flags {@code resyncRequired} for any stream whose compacted
   * replay floor {@code V_floor[bucketId, m]} exceeds {@code clientHighWaterByMember[m]}.
   *
   * @return {@code true} if all streams satisfy {@code V_client >= V_floor}, or {@code false} if
   *         one or more streams require full snapshot resynchronization.
   */
  public boolean admitSubscriberForContinuity(int bucketId,
      Map<String, Long> clientHighWaterByMember) {
    BucketState state = bucketStates.get(bucketId);
    if (state == null) {
      return true;
    }
    List<ReplayFloorViolationRecord> violations = new ArrayList<>();
    synchronized (state) {
      List<String> members = new ArrayList<>(state.replayFloorByMember.keySet());
      Collections.sort(members);
      for (String memberId : members) {
        long floor = state.replayFloorByMember.getOrDefault(memberId, 0L);
        long clientHw = 0L;
        if (clientHighWaterByMember != null) {
          Long hw = clientHighWaterByMember.get(memberId);
          if (hw != null && hw > 0L) {
            clientHw = hw;
          }
        }
        if (floor > 0L && clientHw < floor) {
          violations.add(new ReplayFloorViolationRecord(bucketId, memberId, clientHw, floor));
        }
      }
    }
    if (!violations.isEmpty()) {
      notifyReplayFloorViolations(violations);
      return false;
    }
    return true;
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
   * Registers a healthy subscriber whose ACK stream participates in the bounded
   * {@code min_{i in healthy} ack[i]} checkpoint calculation.
   */
  public void registerSubscriberForCheckpoint(String subscriberId) {
    if (subscriberId == null || subscriberId.isEmpty()) {
      throw new IllegalArgumentException("subscriberId must not be null or empty");
    }
    synchronized (subscriberRegistryLock) {
      registeredCheckpointSubscribers.add(subscriberId);
    }
  }

  /**
   * Removes a subscriber from healthy checkpoint tracking and immediately advances {@code V_ckpt}
   * unconditionally to {@code min_{i in remainingHealthy} ack[i]} across all active streams.
   */
  public boolean unregisterSubscriberForCheckpoint(String subscriberId) {
    if (subscriberId == null || subscriberId.isEmpty()) {
      return false;
    }
    synchronized (subscriberRegistryLock) {
      boolean removed = registeredCheckpointSubscribers.remove(subscriberId);
      if (!removed) {
        return false;
      }
      Set<String> healthy =
          Collections.unmodifiableSet(new HashSet<>(registeredCheckpointSubscribers));
      for (BucketState state : bucketStates.values()) {
        state.removeSubscriberFromAcks(subscriberId, healthy);
      }
      return true;
    }
  }

  public Set<String> getRegisteredCheckpointSubscribers() {
    synchronized (subscriberRegistryLock) {
      return Collections.unmodifiableSet(new HashSet<>(registeredCheckpointSubscribers));
    }
  }

  /**
   * Records a healthy subscriber ACK {@code ack[subscriberId, bucketId, memberId]} monotonically
   * and advances server checkpoint {@code V_ckpt[bucketId, memberId]} unconditionally to
   * {@code min_{i in healthy} ack[i]} without lagging behind on idle tails or staggered arrival
   * order.
   *
   * @return resulting {@code V_ckpt[bucketId, memberId]}.
   */
  public long acknowledgeSubscriberCheckpoint(String subscriberId, int bucketId, String memberId,
      long ackedVersion) {
    if (subscriberId == null || subscriberId.isEmpty()) {
      throw new IllegalArgumentException("subscriberId must not be null or empty");
    }
    if (memberId == null || memberId.isEmpty() || ackedVersion <= 0L) {
      return getCheckpointVersion(bucketId, memberId);
    }
    synchronized (subscriberRegistryLock) {
      registeredCheckpointSubscribers.add(subscriberId);
      Set<String> healthy =
          Collections.unmodifiableSet(new HashSet<>(registeredCheckpointSubscribers));
      BucketState state = bucketStates.computeIfAbsent(bucketId, ignored -> new BucketState());
      return state.recordSubscriberAck(subscriberId, memberId, ackedVersion, healthy);
    }
  }

  public long getSubscriberCheckpointAck(String subscriberId, int bucketId, String memberId) {
    BucketState state = bucketStates.get(bucketId);
    return state == null ? 0L : state.getSubscriberAck(subscriberId, memberId);
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

  public long getReplayFloorVersion(int bucketId, String memberId) {
    BucketState state = bucketStates.get(bucketId);
    return state == null ? 0L : state.getReplayFloor(memberId);
  }

  public int getMaxTailPerStream() {
    return maxTailPerStream;
  }

  public long getLastDispatchedVersion(int bucketId, String memberId) {
    BucketState state = bucketStates.get(bucketId);
    return state == null ? 0L : state.getLastDispatchedVersion(memberId);
  }

  public long getNextExpectedVersion(int bucketId, String memberId) {
    BucketState state = bucketStates.get(bucketId);
    return state == null ? 1L : state.getNextExpectedVersion(memberId);
  }

  public int getTailSize(int bucketId) {
    BucketState state = bucketStates.get(bucketId);
    return state == null ? 0 : state.getTailSize();
  }

  public int getHoldbackSize(int bucketId) {
    BucketState state = bucketStates.get(bucketId);
    return state == null ? 0 : state.getHoldbackSize();
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
      List<ContinuousVectorEvent> emitted, List<TailOverflowRecord> overflows,
      List<HoldbackGapRecord> gapRecords) {
    List<TailMutation> buffered = new ArrayList<>(state.pendingLiveMutations);
    state.pendingLiveMutations.clear();
    buffered.sort(Comparator.comparing(TailMutation::getMemberId)
        .thenComparingLong(TailMutation::getRegionVersion));
    for (TailMutation live : buffered) {
      if (!live.isNonMatchingMarker()) {
        List<Long> evicted = state.append(live, maxTailPerStream);
        if (!evicted.isEmpty()) {
          long ckpt = state.getCheckpoint(live.memberId);
          for (long evictedVersion : evicted) {
            overflows
                .add(new TailOverflowRecord(live.bucketId, live.memberId, evictedVersion, ckpt));
          }
        }
      }
      if (enforceContiguousVersions) {
        emitted.addAll(dispatchContiguousOrHoldLocked(state, live.bucketId, live, gapRecords));
      } else {
        emitted.addAll(dispatchSingleVersionLocked(state, live, false));
      }
    }
  }

  private List<ContinuousVectorEvent> dispatchContiguousOrHoldLocked(BucketState state,
      int bucketId, TailMutation mutation, List<HoldbackGapRecord> gapRecords) {
    long v = mutation.regionVersion;
    long nowMs = mutation.timestamp > 0L ? mutation.timestamp : System.currentTimeMillis();
    long nextExpected = state.nextExpectedVersionByMember.getOrDefault(mutation.memberId, 1L);
    if (v < nextExpected) {
      return Collections.emptyList();
    }
    if (v > nextExpected) {
      TreeMap<Long, TailMutation> holdback =
          state.holdbackByMember.computeIfAbsent(mutation.memberId, ignored -> new TreeMap<>());
      boolean wasEmpty = holdback.isEmpty();
      if (holdback.putIfAbsent(v, mutation) == null) {
        totalHoldbackBufferedCount.incrementAndGet();
      }
      if (wasEmpty) {
        state.holdbackGapStartTimeMsByMember.put(mutation.memberId, nowMs);
      } else {
        state.holdbackGapStartTimeMsByMember.putIfAbsent(mutation.memberId, nowMs);
      }
      long highestHeld = holdback.lastKey();
      boolean gapExceeded =
          (highestHeld - nextExpected) > maxHoldbackGap || holdback.size() > maxHoldbackGap;
      long gapStartMs =
          resolveOldestHoldbackTimestampLocked(state, mutation.memberId, holdback, nowMs);
      boolean ageExceeded =
          maxHoldbackAgeMillis > 0L && gapStartMs > 0L
              && (nowMs - gapStartMs) >= maxHoldbackAgeMillis;
      if (!gapExceeded && !ageExceeded) {
        return Collections.emptyList();
      }
      String reason = gapExceeded ? "MAX_HOLDBACK_GAP_EXCEEDED" : "MAX_HOLDBACK_AGE_EXCEEDED";
      long resumed = holdback.firstKey();
      if (gapRecords != null) {
        gapRecords.add(
            new HoldbackGapRecord(bucketId, mutation.memberId, nextExpected, resumed, reason));
      }
      state.nextExpectedVersionByMember.put(mutation.memberId, resumed);
      state.holdbackGapStartTimeMsByMember.put(mutation.memberId, nowMs);
      return drainContiguousHoldbackLocked(state, bucketId, mutation.memberId, holdback, nowMs,
          gapRecords);
    }

    List<ContinuousVectorEvent> emitted = new ArrayList<>();
    emitted.addAll(dispatchSingleVersionLocked(state, mutation, false));
    TreeMap<Long, TailMutation> holdback = state.holdbackByMember.get(mutation.memberId);
    if (holdback != null && !holdback.isEmpty()) {
      emitted.addAll(drainContiguousHoldbackLocked(state, bucketId, mutation.memberId, holdback,
          nowMs, gapRecords));
    }
    return emitted;
  }

  private List<ContinuousVectorEvent> drainContiguousHoldbackLocked(BucketState state,
      int bucketId, String memberId, TreeMap<Long, TailMutation> holdback, long nowMillis,
      List<HoldbackGapRecord> gapRecords) {
    List<ContinuousVectorEvent> emitted = new ArrayList<>();
    long initialExpected = state.nextExpectedVersionByMember.getOrDefault(memberId, 1L);
    try {
      while (!holdback.isEmpty()) {
        long nextExpected = state.nextExpectedVersionByMember.getOrDefault(memberId, 1L);
        long first = holdback.firstKey();
        if (first < nextExpected) {
          holdback.pollFirstEntry();
          continue;
        }
        if (first > nextExpected) {
          boolean gapExceeded =
              (holdback.lastKey() - nextExpected) > maxHoldbackGap
                  || holdback.size() > maxHoldbackGap;
          long gapStartMs =
              resolveOldestHoldbackTimestampLocked(state, memberId, holdback, nowMillis);
          boolean ageExceeded = maxHoldbackAgeMillis > 0L && gapStartMs > 0L
              && (nowMillis - gapStartMs) >= maxHoldbackAgeMillis;
          if (!gapExceeded && !ageExceeded) {
            break;
          }
          String reason = gapExceeded ? "MAX_HOLDBACK_GAP_EXCEEDED" : "MAX_HOLDBACK_AGE_EXCEEDED";
          if (gapRecords != null) {
            gapRecords.add(new HoldbackGapRecord(bucketId, memberId, nextExpected, first, reason));
          }
          state.nextExpectedVersionByMember.put(memberId, first);
          state.holdbackGapStartTimeMsByMember.put(memberId, nowMillis);
        }
        TailMutation ready = holdback.pollFirstEntry().getValue();
        emitted.addAll(dispatchSingleVersionLocked(state, ready, false));
      }
    } finally {
      if (holdback.isEmpty()) {
        state.holdbackByMember.remove(memberId);
        state.holdbackGapStartTimeMsByMember.remove(memberId);
      } else {
        long afterExpected = state.nextExpectedVersionByMember.getOrDefault(memberId, 1L);
        if (afterExpected > initialExpected) {
          long oldestRemainingTs = holdback.firstEntry().getValue().timestamp;
          state.holdbackGapStartTimeMsByMember.put(memberId,
              oldestRemainingTs > 0L ? oldestRemainingTs : nowMillis);
        } else {
          state.holdbackGapStartTimeMsByMember.putIfAbsent(memberId, nowMillis);
        }
      }
    }
    return emitted;
  }

  private long resolveOldestHoldbackTimestampLocked(BucketState state, String memberId,
      TreeMap<Long, TailMutation> holdback, long fallbackNowMs) {
    Long recorded = state.holdbackGapStartTimeMsByMember.get(memberId);
    if (recorded != null && recorded > 0L) {
      return recorded;
    }
    if (!holdback.isEmpty()) {
      long ts = holdback.firstEntry().getValue().timestamp;
      if (ts > 0L) {
        state.holdbackGapStartTimeMsByMember.put(memberId, ts);
        return ts;
      }
    }
    state.holdbackGapStartTimeMsByMember.put(memberId, fallbackNowMs);
    return fallbackNowMs;
  }

  private void drainAllHoldbackOnPromotionLocked(BucketState state,
      List<ContinuousVectorEvent> emitted, List<TailMutation> replayedOut) {
    if (state.holdbackByMember.isEmpty()) {
      return;
    }
    List<String> members = new ArrayList<>(state.holdbackByMember.keySet());
    Collections.sort(members);
    for (String memberId : members) {
      TreeMap<Long, TailMutation> holdback = state.holdbackByMember.get(memberId);
      if (holdback == null) {
        continue;
      }
      while (!holdback.isEmpty()) {
        long nextExpected = state.nextExpectedVersionByMember.getOrDefault(memberId, 1L);
        TailMutation ready = holdback.pollFirstEntry().getValue();
        if (ready.regionVersion < nextExpected) {
          continue;
        }
        emitted.addAll(dispatchSingleVersionLocked(state, ready, true));
        if (!ready.isNonMatchingMarker()) {
          replayedOut.add(ready);
        }
      }
    }
    state.holdbackByMember.clear();
    state.holdbackGapStartTimeMsByMember.clear();
  }

  private List<ContinuousVectorEvent> dispatchSingleVersionLocked(BucketState state,
      TailMutation mutation, boolean replayed) {
    state.lastDispatchedVersionByMember.merge(mutation.memberId, mutation.regionVersion, Math::max);
    state.nextExpectedVersionByMember.merge(mutation.memberId, mutation.regionVersion + 1L,
        Math::max);
    if (!replayed && router.getSubscriptionCount() == 0
        && registeredCheckpointSubscribers.isEmpty()) {
      // Across empty |S| = 0 intervals with no registered checkpoint subscribers, maintain strict
      // version continuity while reclaiming pre-subscription tail entries so zero-subscriber load
      // never triggers spurious tail overflow.
      state.advanceCheckpoint(mutation.memberId, mutation.regionVersion);
    }
    if (mutation.isNonMatchingMarker()) {
      boolean emitHeartbeat =
          !replayed
              && state.shouldEmitPeriodicHeartbeat(mutation.memberId, heartbeatIntervalEvents,
                  mutation.timestamp);
      if (emitHeartbeat) {
        ContinuousVectorEvent hb = ContinuousVectorEvent.watermarkHeartbeat(mutation.bucketId,
            mutation.memberId, mutation.regionVersion, mutation.timestamp);
        totalHeartbeatsDispatched.incrementAndGet();
        enqueueOrDispatchBatchLocked(state, Collections.singletonList(hb), null);
      }
      return Collections.emptyList();
    }
    return evaluateAndDispatch(state, mutation, replayed);
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

  private void notifyHoldbackGaps(List<HoldbackGapRecord> gapRecords) {
    for (HoldbackGapRecord rec : gapRecords) {
      holdbackGapClosedCount.incrementAndGet();
      resyncRequiredStreams.add(new RvvStreamKey(rec.bucketId, rec.memberId));
      logger.warn(
          "RVV holdback gap closed on bucket={}, member={}: missing expectedVersion={}..{} "
              + "(reason={}); stream marked for resync",
          rec.bucketId, rec.memberId, rec.expectedVersion, rec.resumedVersion - 1L, rec.reason);
      for (HoldbackGapListener listener : holdbackGapListeners) {
        listener.onHoldbackGapClosed(rec.bucketId, rec.memberId, rec.expectedVersion,
            rec.resumedVersion, rec.reason);
      }
    }
  }

  private void notifyReplayFloorViolations(List<ReplayFloorViolationRecord> violations) {
    for (ReplayFloorViolationRecord rec : violations) {
      replayFloorResyncCount.incrementAndGet();
      resyncRequiredStreams.add(new RvvStreamKey(rec.bucketId, rec.memberId));
      logger.warn(
          "RVV subscriber rejoin below compacted replay floor on bucket={}, member={}: "
              + "V_client={} < V_floor={}; stream marked for full resync",
          rec.bucketId, rec.memberId, rec.clientHighWater, rec.replayFloor);
      for (ReplayFloorResyncListener listener : replayFloorListeners) {
        listener.onReplayFloorResyncRequired(rec.bucketId, rec.memberId, rec.clientHighWater,
            rec.replayFloor);
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

  public long getTotalIdleHeartbeatsFlushed() {
    return totalIdleHeartbeatsFlushed.get();
  }

  public long getTotalHoldbackBufferedCount() {
    return totalHoldbackBufferedCount.get();
  }

  public long getTotalNonMatchingVersionsObserved() {
    return totalNonMatchingVersionsObserved.get();
  }

  public long getHoldbackGapClosedCount() {
    return holdbackGapClosedCount.get();
  }

  public long getReplayFloorResyncCount() {
    return replayFloorResyncCount.get();
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
      if (isPrimary && bucketRegion != null && bucketRegion.getVersionMember() != null) {
        // In Geode, a null VersionTag.getMemberID() on a local primary BucketRegion identifies
        // bucketRegion.getVersionMember() (the primary that generated regionVersion from its RVV),
        // even when event.isOriginRemote() is true (put routed from another cluster member).
        memberId = bucketRegion.getVersionMember().toString();
      } else if (!isPrimary && bucketRegion != null
          && bucketRegion.getBucketAdvisor().basicGetPrimaryMember() != null) {
        // On a secondary BucketRegion, never misattribute replicated mutations to the secondary's
        // own getVersionMember(); attribute to the bucket's primary member instead.
        memberId = bucketRegion.getBucketAdvisor().basicGetPrimaryMember().toString();
      } else if (event.getDistributedMember() != null) {
        memberId = event.getDistributedMember().toString();
      } else if (bucketRegion != null && bucketRegion.getVersionMember() != null) {
        memberId = bucketRegion.getVersionMember().toString();
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
    return evaluateAndDispatch(state, mutation, replayed, null);
  }

  private List<ContinuousVectorEvent> evaluateAndDispatch(BucketState state, TailMutation mutation,
      boolean replayed, ContinuousVectorEventSink targetSink) {
    List<ChordalVoronoiRouter.MatchResult> matches =
        router.routeNormalized(mutation.getNormalizedVectorUnsafe());
    boolean emitHeartbeat =
        !replayed && state.shouldEmitPeriodicHeartbeat(mutation.memberId, heartbeatIntervalEvents,
            mutation.timestamp);
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

    // Data-before-watermark invariant at equal version v_h: place all match alerts strictly before
    // the v_h watermark heartbeat in the same atomic batch pushed onto the per-bucket FIFO queue.
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

    enqueueOrDispatchBatchLocked(state, sinkBatch, targetSink);
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
    emitStreamWatermarksLocked(state, bucketId, maxVersionByMember, null);
  }

  private void emitStreamWatermarksLocked(BucketState state, int bucketId,
      Map<String, Long> maxVersionByMember, ContinuousVectorEventSink targetSink) {
    if (heartbeatIntervalEvents <= 0 || maxVersionByMember == null
        || maxVersionByMember.isEmpty()) {
      return;
    }
    long now = System.currentTimeMillis();
    for (Map.Entry<String, Long> entry : maxVersionByMember.entrySet()) {
      String memberId = entry.getKey();
      long maxVersion = entry.getValue();
      if (targetSink == null) {
        state.resetHeartbeatCounter(memberId);
      }
      ContinuousVectorEvent hb =
          ContinuousVectorEvent.watermarkHeartbeat(bucketId, memberId, maxVersion, now, true);
      totalHeartbeatsDispatched.incrementAndGet();
      enqueueOrDispatchBatchLocked(state, Collections.singletonList(hb), targetSink);
    }
  }

  private void enqueueOrDispatchBatchLocked(BucketState state,
      List<ContinuousVectorEvent> sinkBatch, ContinuousVectorEventSink targetSink) {
    totalBatchesDispatched.incrementAndGet();
    PendingDispatchBatch batch = new PendingDispatchBatch(sinkBatch, targetSink);
    state.pendingDispatchBatches.offer(batch);
    Executor exec = this.dispatchExecutor;
    if (exec != null) {
      // Enqueued inside synchronized(state) -> preserves exact ascending per-(b, m) RVV order
      scheduleBucketDrain(state, exec);
    } else {
      drainSynchronously(state);
    }
  }

  private void drainSynchronously(BucketState state) {
    Thread current = Thread.currentThread();
    while (!state.pendingDispatchBatches.isEmpty()) {
      if (state.dispatchDraining.compareAndSet(false, true)) {
        drainBucketDispatchQueue(state, null);
        return;
      }
      if (state.drainOwnerThread == current) {
        return;
      }
      Thread.yield();
    }
  }

  private void scheduleBucketDrain(BucketState state, Executor exec) {
    if (state.dispatchDraining.compareAndSet(false, true)) {
      try {
        exec.execute(() -> drainBucketDispatchQueue(state, exec));
      } catch (RejectedExecutionException rejected) {
        drainBucketDispatchQueue(state, null);
      }
    }
  }

  private void drainBucketDispatchQueue(BucketState state, Executor exec) {
    state.drainOwnerThread = Thread.currentThread();
    try {
      while (true) {
        PendingDispatchBatch next;
        while ((next = state.pendingDispatchBatches.poll()) != null) {
          dispatchBatchToSinks(next);
        }
        state.drainOwnerThread = null;
        state.dispatchDraining.set(false);
        if (state.pendingDispatchBatches.isEmpty()) {
          break;
        }
        if (exec == null) {
          if (!state.dispatchDraining.compareAndSet(false, true)) {
            break;
          }
          state.drainOwnerThread = Thread.currentThread();
        } else {
          scheduleBucketDrain(state, exec);
          break;
        }
      }
    } catch (RuntimeException | Error t) {
      state.drainOwnerThread = null;
      state.dispatchDraining.set(false);
      if (!state.pendingDispatchBatches.isEmpty() && exec != null) {
        scheduleBucketDrain(state, exec);
      }
      throw t;
    }
  }

  private void dispatchBatchToSinks(PendingDispatchBatch batch) {
    if (batch.targetSink != null) {
      batch.targetSink.onEventBatch(batch.events);
      return;
    }
    for (ContinuousVectorEventSink sink : sinks) {
      sink.onEventBatch(batch.events);
    }
  }
}
