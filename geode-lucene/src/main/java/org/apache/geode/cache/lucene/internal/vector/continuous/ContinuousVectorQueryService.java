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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Function;

import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.partition.PartitionListener;
import org.apache.geode.cache.partition.PartitionListenerAdapter;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.internal.cache.PartitionedRegion;

/**
 * Service facade tying {@link ChordalVoronoiRouter} and {@link RvvMonotonicVectorDispatcher} to
 * Apache Geode partitioned region bucket events, primary promotions, and client subscriptions.
 */
public class ContinuousVectorQueryService {

  private final ChordalVoronoiRouter router;
  private final RvvMonotonicVectorDispatcher dispatcher;
  private final CopyOnWriteArrayList<RvvClientGate> clientGates = new CopyOnWriteArrayList<>();

  public ContinuousVectorQueryService(int dimension) {
    this(ChordalVoronoiRouter.RouterConfig.builder(dimension).build());
  }

  public ContinuousVectorQueryService(ChordalVoronoiRouter.RouterConfig routerConfig) {
    this.router = new ChordalVoronoiRouter(routerConfig);
    this.dispatcher = new RvvMonotonicVectorDispatcher(this.router);
  }

  public ContinuousVectorQueryService(ChordalVoronoiRouter router,
      RvvMonotonicVectorDispatcher dispatcher) {
    this.router = Objects.requireNonNull(router, "router must not be null");
    this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher must not be null");
  }

  /**
   * Attaches a client-side {@link RvvClientGate} so subscription registrations and
   * {@link #unregisterSubscription(String)} automatically synchronize active subscription IDs
   * (enabling watermark heartbeats for 0%-match subscribers and avoiding stale {@code V_ckpt}
   * pinning).
   */
  public void attachClientGate(RvvClientGate gate) {
    Objects.requireNonNull(gate, "gate must not be null");
    gate.registerSubscriptions(router.getSubscriptionIds());
    clientGates.add(gate);
  }

  public boolean detachClientGate(RvvClientGate gate) {
    return clientGates.remove(gate);
  }

  public void registerSubscription(VectorSubscription subscription) {
    router.addSubscription(subscription);
    for (RvvClientGate gate : clientGates) {
      gate.registerSubscription(subscription.getSubscriptionId());
    }
  }

  public void registerSubscription(String subscriptionId, float[] queryVector,
      float similarityThreshold) {
    registerSubscription(new VectorSubscription(subscriptionId, queryVector, similarityThreshold));
  }

  public void registerSubscriptions(Collection<VectorSubscription> subscriptions) {
    router.addSubscriptions(subscriptions);
    for (RvvClientGate gate : clientGates) {
      for (VectorSubscription sub : subscriptions) {
        gate.registerSubscription(sub.getSubscriptionId());
      }
    }
  }

  public boolean unregisterSubscription(String subscriptionId) {
    boolean removed = router.removeSubscription(subscriptionId);
    if (subscriptionId != null) {
      for (RvvClientGate gate : clientGates) {
        gate.removeSubscription(subscriptionId);
      }
    }
    return removed;
  }

  public void setHeartbeatIntervalEvents(int heartbeatIntervalEvents) {
    dispatcher.setHeartbeatIntervalEvents(heartbeatIntervalEvents);
  }

  public int getHeartbeatIntervalEvents() {
    return dispatcher.getHeartbeatIntervalEvents();
  }

  public void setEnforceContiguousVersions(boolean enforceContiguousVersions) {
    dispatcher.setEnforceContiguousVersions(enforceContiguousVersions);
  }

  public boolean isEnforceContiguousVersions() {
    return dispatcher.isEnforceContiguousVersions();
  }

  public void setMaxHoldbackGap(int maxHoldbackGap) {
    dispatcher.setMaxHoldbackGap(maxHoldbackGap);
  }

  public int getMaxHoldbackGap() {
    return dispatcher.getMaxHoldbackGap();
  }

  public int flushIdleHeartbeats(long maxIdleMillis) {
    return dispatcher.flushIdleHeartbeats(maxIdleMillis);
  }

  public int flushIdleHeartbeats(long nowMillis, long maxIdleMillis) {
    return dispatcher.flushIdleHeartbeats(nowMillis, maxIdleMillis);
  }

  public void startIdleHeartbeatScheduler(long maxIdleMillis, long checkPeriodMillis) {
    dispatcher.startIdleHeartbeatScheduler(maxIdleMillis, checkPeriodMillis);
  }

  public void stopIdleHeartbeatScheduler() {
    dispatcher.stopIdleHeartbeatScheduler();
  }

  public void addEventSink(ContinuousVectorEventSink sink) {
    dispatcher.addEventSink(sink);
  }

  public boolean removeEventSink(ContinuousVectorEventSink sink) {
    return dispatcher.removeEventSink(sink);
  }

  /**
   * Configures an optional asynchronous dispatch executor on the underlying
   * {@link RvvMonotonicVectorDispatcher}. When non-null, matched event batches are enqueued onto a
   * strictly ordered per-bucket FIFO queue so region mutation threads never block on network I/O
   * while preserving Theorem 3's per-{@code (bucketId, memberId)} monotonicity.
   */
  public void setAsyncDispatchExecutor(Executor dispatchExecutor) {
    dispatcher.setDispatchExecutor(dispatchExecutor);
  }

  /**
   * Waits up to {@code timeoutMillis} for all asynchronously queued event batches across all
   * buckets to finish draining to registered sinks.
   */
  public boolean awaitPendingDispatches(long timeoutMillis) throws InterruptedException {
    return dispatcher.awaitPendingDispatches(timeoutMillis);
  }

  /**
   * Processes a bucket vector mutation with explicit RVV coordinates.
   */
  public List<ContinuousVectorEvent> onBucketMutation(int bucketId, String memberId,
      long regionVersion, Object key, float[] embedding, boolean isPrimary) {
    return onBucketMutation(bucketId, memberId, regionVersion, 0, key, embedding, isPrimary);
  }

  /**
   * Processes a bucket vector mutation with explicit RVV coordinates and entry version.
   */
  public List<ContinuousVectorEvent> onBucketMutation(int bucketId, String memberId,
      long regionVersion, int entryVersion, Object key, float[] embedding, boolean isPrimary) {
    return dispatcher.onBucketMutation(bucketId, memberId, regionVersion, entryVersion, key,
        embedding, isPrimary);
  }

  /**
   * Advances contiguous RVV sequencing past a non-vector bucket operation (such as {@code destroy},
   * {@code invalidate}, or a non-vector entry update) without emitting vector matches.
   */
  public void onNonMatchingBucketVersion(int bucketId, String memberId, long regionVersion) {
    dispatcher.onNonMatchingBucketVersion(bucketId, memberId, regionVersion);
  }

  /**
   * Processes a live Geode {@link EntryEvent} by extracting authentic {@code (bucketId, memberId,
   * regionVersion, entryVersion)} RVV metadata from Geode's {@code VersionTag} and bucket state.
   */
  public List<ContinuousVectorEvent> onBucketMutation(EntryEvent<?, ?> event, float[] embedding) {
    if (embedding == null) {
      return Collections.emptyList();
    }
    RvvMonotonicVectorDispatcher.ExtractedRvvStamp stamp =
        RvvMonotonicVectorDispatcher.extractFromEntryEvent(event);
    return dispatcher.onBucketMutation(stamp.getBucketId(), stamp.getMemberId(),
        stamp.getRegionVersion(), stamp.getEntryVersion(), event.getKey(), embedding,
        stamp.isPrimary());
  }

  /**
   * Replays uncheckpointed tail mutations for {@code bucketId} on primary promotion.
   */
  public List<ContinuousVectorEvent> onPrimaryPromotion(int bucketId) {
    return dispatcher.onPrimaryPromotion(bucketId);
  }

  /**
   * Replays uncheckpointed tail mutations for {@code bucketId} on primary promotion, merging
   * replicated entries and authentic Geode {@code VersionStamp}s from the promoted
   * {@link PartitionedRegion} bucket.
   */
  public List<ContinuousVectorEvent> onPrimaryPromotion(PartitionedRegion partitionedRegion,
      int bucketId, Function<Object, float[]> vectorExtractor) {
    List<RvvMonotonicVectorDispatcher.TailMutation> recovered =
        RvvMonotonicVectorDispatcher.extractBucketSnapshot(partitionedRegion, bucketId,
            vectorExtractor);
    return dispatcher.onPrimaryPromotion(bucketId, recovered);
  }

  /**
   * Replays uncheckpointed tail mutations in
   * {@code (max(V_ckpt[b, m], clientHighWaterByMember[m]), V_S[b, m]]} across all retained
   * streams {@code m} (both pre-crash {@code m_P} and post-promotion {@code m_S}) on reconnect.
   */
  public List<ContinuousVectorEvent> replayUncheckpointedForReconnect(int bucketId,
      Map<String, Long> clientHighWaterByMember) {
    return dispatcher.replayUncheckpointedForReconnect(bucketId, clientHighWaterByMember);
  }

  /**
   * Replays uncheckpointed tail mutations for client reconnect under {@code synchronized (state)},
   * pushing replayed batches and trailing watermarks onto the per-bucket serial FIFO queue before
   * invoking {@code onReplayedUnderLock} and releasing the bucket lock.
   */
  public List<ContinuousVectorEvent> replayUncheckpointedForReconnect(int bucketId,
      Map<String, Long> clientHighWaterByMember, Runnable onReplayedUnderLock) {
    return dispatcher.replayUncheckpointedForReconnect(bucketId, clientHighWaterByMember,
        onReplayedUnderLock);
  }

  /**
   * Replays uncheckpointed tail mutations for a specific reconnecting client sink
   * ({@code targetSink}) under {@code synchronized (state)}, pushing replayed batches and trailing
   * watermarks onto the per-bucket serial FIFO queue before invoking {@code onReplayedUnderLock}
   * and releasing the bucket lock.
   */
  public List<ContinuousVectorEvent> replayUncheckpointedForReconnect(int bucketId,
      Map<String, Long> clientHighWaterByMember, ContinuousVectorEventSink targetSink,
      Runnable onReplayedUnderLock) {
    return dispatcher.replayUncheckpointedForReconnect(bucketId, clientHighWaterByMember,
        targetSink, onReplayedUnderLock);
  }

  /**
   * Advances server-side checkpoint {@code V_ckpt[bucketId, memberId]} after client acknowledgment.
   */
  public void acknowledgeCheckpoint(int bucketId, String memberId, long ackedVersion) {
    dispatcher.acknowledgeCheckpoint(bucketId, memberId, ackedVersion);
  }

  /**
   * Compacts acknowledged tail entries up to {@code ackedVersion}.
   */
  public void compactUpTo(int bucketId, String memberId, long ackedVersion) {
    dispatcher.compactUpTo(bucketId, memberId, ackedVersion);
  }

  /**
   * Creates a Geode {@link CacheListener} that extracts vector embeddings from entry values and
   * dispatches RVV-stamped notifications through this service.
   */
  public <K, V> CacheListener<K, V> createCacheListener(Function<Object, float[]> vectorExtractor) {
    Objects.requireNonNull(vectorExtractor, "vectorExtractor must not be null");
    return new CacheListenerAdapter<K, V>() {
      @Override
      public void afterCreate(EntryEvent<K, V> event) {
        handleEvent(event);
      }

      @Override
      public void afterUpdate(EntryEvent<K, V> event) {
        handleEvent(event);
      }

      @Override
      public void afterDestroy(EntryEvent<K, V> event) {
        handleNonVectorEvent(event);
      }

      @Override
      public void afterInvalidate(EntryEvent<K, V> event) {
        handleNonVectorEvent(event);
      }

      private void handleEvent(EntryEvent<K, V> event) {
        V newValue = event.getNewValue();
        if (newValue == null) {
          handleNonVectorEvent(event);
          return;
        }
        float[] embedding = vectorExtractor.apply(newValue);
        if (embedding != null) {
          onBucketMutation(event, embedding);
        } else {
          handleNonVectorEvent(event);
        }
      }

      private void handleNonVectorEvent(EntryEvent<K, V> event) {
        RvvMonotonicVectorDispatcher.ExtractedRvvStamp stamp =
            RvvMonotonicVectorDispatcher.extractFromEntryEvent(event);
        if (stamp.isPrimary() && stamp.getRegionVersion() > 0L) {
          onNonMatchingBucketVersion(stamp.getBucketId(), stamp.getMemberId(),
              stamp.getRegionVersion());
        }
      }
    };
  }

  /**
   * Creates a Geode {@link PartitionListener} that automatically replays uncertain tail mutations
   * from {@code V_ckpt} whenever a secondary bucket promotes to primary.
   */
  public PartitionListener createPartitionListener(Function<Object, float[]> vectorExtractor) {
    return createPartitionListener(vectorExtractor, null);
  }

  /**
   * Creates a Geode {@link PartitionListener} that snapshots the promoted bucket on
   * {@code afterPrimary(bucketId)} and dispatches replay either synchronously (when
   * {@code replayExecutor == null}) or via the supplied {@code replayExecutor} to avoid blocking
   * Geode's bucket promotion lock during network I/O.
   */
  public PartitionListener createPartitionListener(Function<Object, float[]> vectorExtractor,
      Executor replayExecutor) {
    Objects.requireNonNull(vectorExtractor, "vectorExtractor must not be null");
    return new PartitionListenerAdapter() {
      private volatile PartitionedRegion partitionedRegion;

      @Override
      public void afterRegionCreate(Region<?, ?> region) {
        if (region instanceof PartitionedRegion) {
          this.partitionedRegion = (PartitionedRegion) region;
        }
      }

      @Override
      public void afterPrimary(int bucketId) {
        PartitionedRegion pr = this.partitionedRegion;
        List<RvvMonotonicVectorDispatcher.TailMutation> recovered = pr != null
            ? RvvMonotonicVectorDispatcher.extractBucketSnapshot(pr, bucketId, vectorExtractor)
            : Collections.emptyList();
        if (replayExecutor != null) {
          dispatcher.beginAsyncPromotion(bucketId);
          try {
            replayExecutor.execute(() -> dispatcher.completeAsyncPromotion(bucketId, recovered));
          } catch (RejectedExecutionException ignored) {
            // Replay executor already shut down during cache/node teardown
            dispatcher.cancelAsyncPromotion(bucketId);
          }
        } else {
          dispatcher.onPrimaryPromotion(bucketId, recovered);
        }
      }

      @Override
      public void afterBucketRemoved(int bucketId, Iterable<?> keys) {
        dispatcher.onBucketRemoved(bucketId);
      }
    };
  }

  public ChordalVoronoiRouter getRouter() {
    return router;
  }

  public RvvMonotonicVectorDispatcher getDispatcher() {
    return dispatcher;
  }

  public ChordalVoronoiRouter.RouterTelemetry getTelemetry() {
    return router.getTelemetry();
  }

  public int getActiveSubscriptionCount() {
    return router.getSubscriptionCount();
  }
}
