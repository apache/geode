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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Client-side Region Version Vector (RVV) high-water gate implementing Theorem 3's idempotent
 * deduplication and strict per-{@code (subscriptionId, bucketId, memberId)} monotonicity.
 *
 * <p>
 * Tracks high-water version {@code V_client[subscriptionId, b, m]} and deterministically discards
 * any incoming event carrying {@code v_{b,m} <= V_client[subscriptionId, b, m]}, accepting only
 * strictly increasing {@code v_{b,m} > V_client[subscriptionId, b, m]}. Check, high-water advance,
 * and downstream listener delivery are serialized per {@code (subscriptionId, bucketId, memberId)}
 * stream so concurrent callers can never observe out-of-order callbacks.
 */
public class RvvClientGate {

  private static final class SubscriptionStreamKey {
    private final String subscriptionId;
    private final int bucketId;
    private final String memberId;

    SubscriptionStreamKey(String subscriptionId, int bucketId, String memberId) {
      this.subscriptionId = subscriptionId;
      this.bucketId = bucketId;
      this.memberId = memberId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof SubscriptionStreamKey)) {
        return false;
      }
      SubscriptionStreamKey that = (SubscriptionStreamKey) o;
      return bucketId == that.bucketId
          && Objects.equals(subscriptionId, that.subscriptionId)
          && Objects.equals(memberId, that.memberId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(subscriptionId, bucketId, memberId);
    }
  }

  private static final class StreamState {
    volatile long highWaterVersion;
  }

  private final ConcurrentHashMap<SubscriptionStreamKey, StreamState> streamStates =
      new ConcurrentHashMap<>();
  private final CopyOnWriteArrayList<Consumer<ContinuousVectorEvent>> listeners =
      new CopyOnWriteArrayList<>();
  private final AtomicLong acceptedCount = new AtomicLong();
  private final AtomicLong discardedCount = new AtomicLong();

  /**
   * Registers a downstream listener invoked strictly once per accepted, monotonically ordered
   * {@link ContinuousVectorEvent}.
   */
  public void addListener(Consumer<ContinuousVectorEvent> listener) {
    listeners.add(Objects.requireNonNull(listener, "listener must not be null"));
  }

  public boolean removeListener(Consumer<ContinuousVectorEvent> listener) {
    return listeners.remove(listener);
  }

  /**
   * Removes all tracked high-water state for an unregistered subscription so it cannot pin
   * {@link #getSafeCheckpointVersion(int, String)} or block server-side tail compaction.
   *
   * @return number of {@code (subscriptionId, bucketId, memberId)} stream entries removed.
   */
  public int removeSubscription(String subscriptionId) {
    if (subscriptionId == null) {
      return 0;
    }
    int removed = 0;
    for (SubscriptionStreamKey key : streamStates.keySet()) {
      if (subscriptionId.equals(key.subscriptionId) && streamStates.remove(key) != null) {
        removed++;
      }
    }
    return removed;
  }

  /**
   * Evaluates the RVV high-water gate for {@code event}, serializing high-water advancement and
   * listener delivery per {@code (subscriptionId, bucketId, memberId)} stream.
   *
   * @return {@code true} if {@code event.getRegionVersion() > V_client[sub, b, m]} (accepted and
   *         forwarded to downstream listeners), or {@code false} if deterministically discarded as
   *         duplicate/stale.
   */
  public boolean tryAccept(ContinuousVectorEvent event) {
    Objects.requireNonNull(event, "event must not be null");
    SubscriptionStreamKey key = new SubscriptionStreamKey(event.getSubscriptionId(),
        event.getBucketId(), event.getMemberId());
    long incomingVersion = event.getRegionVersion();
    StreamState state = streamStates.computeIfAbsent(key, ignored -> new StreamState());

    synchronized (state) {
      if (incomingVersion <= state.highWaterVersion) {
        discardedCount.incrementAndGet();
        return false;
      }
      state.highWaterVersion = incomingVersion;
      acceptedCount.incrementAndGet();
      for (Consumer<ContinuousVectorEvent> listener : listeners) {
        listener.accept(event);
      }
      return true;
    }
  }

  /**
   * Evaluates the RVV high-water gate across a coalesced batch of {@link ContinuousVectorEvent}s
   * in order, returning the number of accepted events.
   */
  public int tryAcceptBatch(List<ContinuousVectorEvent> events) {
    Objects.requireNonNull(events, "events must not be null");
    int accepted = 0;
    for (ContinuousVectorEvent event : events) {
      if (tryAccept(event)) {
        accepted++;
      }
    }
    return accepted;
  }

  /**
   * Returns {@code V_client[subscriptionId, bucketId, memberId]}, or {@code 0} if no event has been
   * accepted yet for that stream.
   */
  public long getHighWaterVersion(String subscriptionId, int bucketId, String memberId) {
    StreamState state =
        streamStates.get(new SubscriptionStreamKey(subscriptionId, bucketId, memberId));
    return state == null ? 0L : state.highWaterVersion;
  }

  /**
   * Returns a safe checkpoint bound {@code V_ckpt[b, m] <= min_q V_client[q, b, m]} across all
   * currently active subscriptions that have received events for {@code (bucketId, memberId)}.
   */
  public long getSafeCheckpointVersion(int bucketId, String memberId) {
    long min = Long.MAX_VALUE;
    boolean found = false;
    for (Map.Entry<SubscriptionStreamKey, StreamState> entry : streamStates.entrySet()) {
      SubscriptionStreamKey k = entry.getKey();
      if (k.bucketId == bucketId && k.memberId.equals(memberId)) {
        long version = entry.getValue().highWaterVersion;
        if (version > 0L) {
          found = true;
          if (version < min) {
            min = version;
          }
        }
      }
    }
    return found ? min : 0L;
  }

  /**
   * Returns a snapshot of safe checkpoint versions per {@link RvvStreamKey}.
   */
  public Map<RvvStreamKey, Long> getSafeCheckpointSnapshot() {
    Map<RvvStreamKey, Long> snapshot = new HashMap<>();
    for (Map.Entry<SubscriptionStreamKey, StreamState> entry : streamStates.entrySet()) {
      long version = entry.getValue().highWaterVersion;
      if (version <= 0L) {
        continue;
      }
      SubscriptionStreamKey k = entry.getKey();
      RvvStreamKey streamKey = new RvvStreamKey(k.bucketId, k.memberId);
      snapshot.merge(streamKey, version, Math::min);
    }
    return Collections.unmodifiableMap(snapshot);
  }

  /**
   * Returns a snapshot of all accepted stream high-water marks grouped by subscription ID.
   */
  public Map<String, Map<RvvStreamKey, Long>> getHighWaterSnapshotBySubscription() {
    Map<String, Map<RvvStreamKey, Long>> result = new HashMap<>();
    for (Map.Entry<SubscriptionStreamKey, StreamState> entry : streamStates.entrySet()) {
      long version = entry.getValue().highWaterVersion;
      if (version <= 0L) {
        continue;
      }
      SubscriptionStreamKey k = entry.getKey();
      result.computeIfAbsent(k.subscriptionId, ignored -> new HashMap<>())
          .put(new RvvStreamKey(k.bucketId, k.memberId), version);
    }
    return result;
  }

  public long getAcceptedCount() {
    return acceptedCount.get();
  }

  public long getDiscardedCount() {
    return discardedCount.get();
  }

  /**
   * Utility verifying that a list of accepted events is strictly monotonic in {@code regionVersion}
   * for every {@code (subscriptionId, bucketId, memberId)} stream.
   */
  public static List<String> verifyStrictMonotonicity(List<ContinuousVectorEvent> acceptedEvents) {
    Map<SubscriptionStreamKey, Long> lastSeen = new HashMap<>();
    List<String> violations = new ArrayList<>();
    for (ContinuousVectorEvent event : acceptedEvents) {
      SubscriptionStreamKey key = new SubscriptionStreamKey(event.getSubscriptionId(),
          event.getBucketId(), event.getMemberId());
      long prev = lastSeen.getOrDefault(key, 0L);
      if (event.getRegionVersion() <= prev) {
        violations.add("Non-monotonic event for sub=" + event.getSubscriptionId()
            + ", bucket=" + event.getBucketId()
            + ", member=" + event.getMemberId()
            + ": prev=" + prev + ", curr=" + event.getRegionVersion());
      } else {
        lastSeen.put(key, event.getRegionVersion());
      }
    }
    return violations;
  }
}
