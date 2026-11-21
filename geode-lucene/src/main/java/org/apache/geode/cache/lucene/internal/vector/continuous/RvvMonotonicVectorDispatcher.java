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
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

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

  public static final int DEFAULT_MAX_TAIL_PER_STREAM = 100_000;

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

    synchronized void append(TailMutation mutation, int maxTailPerStream) {
      long ckpt = checkpointByMember.getOrDefault(mutation.memberId, 0L);
      if (mutation.regionVersion <= ckpt) {
        return;
      }
      TreeMap<Long, TailMutation> stream =
          tailByMember.computeIfAbsent(mutation.memberId, ignored -> new TreeMap<>());
      stream.putIfAbsent(mutation.regionVersion, mutation);
      while (stream.size() > maxTailPerStream) {
        stream.pollFirstEntry();
      }
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
        int maxTailPerStream) {
      if (recoveredEntries != null) {
        for (TailMutation entry : recoveredEntries) {
          append(entry, maxTailPerStream);
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

  private final ChordalVoronoiRouter router;
  private final int maxTailPerStream;
  private final ConcurrentHashMap<Integer, BucketState> bucketStates = new ConcurrentHashMap<>();
  private final CopyOnWriteArrayList<ContinuousVectorEventSink> sinks =
      new CopyOnWriteArrayList<>();

  private final AtomicLong totalEventsDispatched = new AtomicLong();
  private final AtomicLong totalReplayedEventsDispatched = new AtomicLong();
  private final AtomicLong totalPromotionsHandled = new AtomicLong();

  public RvvMonotonicVectorDispatcher(ChordalVoronoiRouter router) {
    this(router, DEFAULT_MAX_TAIL_PER_STREAM);
  }

  public RvvMonotonicVectorDispatcher(ChordalVoronoiRouter router, int maxTailPerStream) {
    this.router = Objects.requireNonNull(router, "router must not be null");
    if (maxTailPerStream <= 0) {
      throw new IllegalArgumentException("maxTailPerStream must be > 0, got: " + maxTailPerStream);
    }
    this.maxTailPerStream = maxTailPerStream;
  }

  public void addEventSink(ContinuousVectorEventSink sink) {
    sinks.add(Objects.requireNonNull(sink, "sink must not be null"));
  }

  public boolean removeEventSink(ContinuousVectorEventSink sink) {
    return sinks.remove(sink);
  }

  /**
   * Handles a bucket mutation on either a primary or secondary replica.
   *
   * <p>
   * Both primaries and secondaries record the normalized mutation into the bucket's replayable
   * delta tail buffer. Only primaries evaluate subscriptions and emit {@link ContinuousVectorEvent}
   * notifications immediately.
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

    // Serialize append + dispatch per bucket to preserve per-(b, m) version ordering
    synchronized (state) {
      state.append(mutation, maxTailPerStream);
      if (!isPrimary) {
        return Collections.emptyList();
      }
      return evaluateAndDispatch(mutation, false);
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
    synchronized (state) {
      List<TailMutation> toReplay = state.collectReplayableTail(recoveredBucketEntries,
          maxTailPerStream);
      if (toReplay.isEmpty()) {
        return Collections.emptyList();
      }
      List<ContinuousVectorEvent> emitted = new ArrayList<>();
      for (TailMutation mutation : toReplay) {
        emitted.addAll(evaluateAndDispatch(mutation, true));
      }
      return emitted;
    }
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

  public long getCheckpointVersion(int bucketId, String memberId) {
    BucketState state = bucketStates.get(bucketId);
    return state == null ? 0L : state.getCheckpoint(memberId);
  }

  public int getTailSize(int bucketId) {
    BucketState state = bucketStates.get(bucketId);
    return state == null ? 0 : state.getTailSize();
  }

  public void onBucketRemoved(int bucketId) {
    bucketStates.remove(bucketId);
  }

  public long getTotalEventsDispatched() {
    return totalEventsDispatched.get();
  }

  public long getTotalReplayedEventsDispatched() {
    return totalReplayedEventsDispatched.get();
  }

  public long getTotalPromotionsHandled() {
    return totalPromotionsHandled.get();
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

  private List<ContinuousVectorEvent> evaluateAndDispatch(TailMutation mutation, boolean replayed) {
    List<ChordalVoronoiRouter.MatchResult> matches =
        router.routeNormalized(mutation.getNormalizedVectorUnsafe());
    if (matches.isEmpty()) {
      return Collections.emptyList();
    }
    List<ContinuousVectorEvent> events = new ArrayList<>(matches.size());
    for (ChordalVoronoiRouter.MatchResult match : matches) {
      ContinuousVectorEvent event = new ContinuousVectorEvent(match.getSubscriptionId(),
          mutation.key, match.getSimilarityScore(), mutation.bucketId, mutation.memberId,
          mutation.regionVersion, mutation.entryVersion, mutation.timestamp, replayed);
      events.add(event);
      totalEventsDispatched.incrementAndGet();
      if (replayed) {
        totalReplayedEventsDispatched.incrementAndGet();
      }
      for (ContinuousVectorEventSink sink : sinks) {
        sink.onEvent(event);
      }
    }
    return events;
  }
}
