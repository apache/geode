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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.partition.PartitionListener;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class RvvMonotonicVectorDispatcherTest {

  @Test
  public void theorem3FailoverReplayDeliversExactlyOnceInStrictMonotonicOrder() {
    float[] subVec = new float[] {1.0f, 0.0f, 0.0f, 0.0f};
    float[] matchingVec = new float[] {0.98f, 0.20f, 0.0f, 0.0f};
    float[] nonMatchingVec = new float[] {0.0f, 1.0f, 0.0f, 0.0f};

    ContinuousVectorQueryService primary = new ContinuousVectorQueryService(4);
    ContinuousVectorQueryService secondary = new ContinuousVectorQueryService(4);
    primary.registerSubscription("sub-alpha", subVec, 0.90f);
    secondary.registerSubscription("sub-alpha", subVec, 0.90f);

    RvvClientGate clientGate = new RvvClientGate();
    List<ContinuousVectorEvent> accepted = new ArrayList<>();
    clientGate.addListener(accepted::add);

    // Primary delivers events for v <= 12; v = 13..20 are dropped mid-flight due to crash
    primary.addEventSink(event -> {
      if (event.getRegionVersion() <= 12L) {
        clientGate.tryAccept(event);
      }
    });
    secondary.addEventSink(clientGate::tryAccept);

    int bucketId = 5;
    String primaryMember = "server1(id:1)";
    Set<String> expectedMatchingKeys = new HashSet<>();

    for (long v = 1L; v <= 20L; v++) {
      boolean shouldMatch = (v % 2L == 1L); // odd versions match: 1,3,5,7,9,11,13,15,17,19
      float[] vec = shouldMatch ? matchingVec : nonMatchingVec;
      String key = "doc-" + v;
      if (shouldMatch) {
        expectedMatchingKeys.add(key);
      }
      primary.onBucketMutation(bucketId, primaryMember, v, key, vec, true);
      secondary.onBucketMutation(bucketId, primaryMember, v, key, vec, false);
    }

    // Client received v = 1, 3, 5, 7, 9, 11 (V_client = 11); checkpoint lags at V_ckpt = 6 <= 11
    assertThat(clientGate.getHighWaterVersion("sub-alpha", bucketId, primaryMember)).isEqualTo(11L);
    assertThat(accepted).hasSize(6);
    secondary.acknowledgeCheckpoint(bucketId, primaryMember, 6L);

    // Primary crashes! Secondary promotes to primary and replays tail > V_ckpt (v = 7..20)
    List<ContinuousVectorEvent> replayed = secondary.onPrimaryPromotion(bucketId);
    assertThat(replayed).isNotEmpty();
    assertThat(replayed).allMatch(ContinuousVectorEvent::isReplayed);

    // Post-failover writes arrive on newly promoted primary (distinct memberId = server2)
    String promotedMember = "server2(id:2)";
    for (long v = 1L; v <= 6L; v++) {
      String key = "post-failover-" + v;
      expectedMatchingKeys.add(key);
      secondary.onBucketMutation(bucketId, promotedMember, v, key, matchingVec, true);
    }

    // Verify Theorem 3 guarantees:
    // 1. Zero missed notifications
    Set<Object> deliveredKeys = new HashSet<>();
    for (ContinuousVectorEvent e : accepted) {
      deliveredKeys.add(e.getKey());
    }
    assertThat(deliveredKeys).containsExactlyInAnyOrderElementsOf(expectedMatchingKeys);

    // 2. Zero duplicate accepted deliveries (replayed v=7,9,11 were deterministically dropped)
    assertThat(accepted).hasSize(expectedMatchingKeys.size());
    assertThat(clientGate.getDiscardedCount()).isEqualTo(3L);

    // 3. Strict per-(subscriptionId, bucketId, memberId) monotonicity
    assertThat(RvvClientGate.verifyStrictMonotonicity(accepted)).isEmpty();
  }

  @Test
  public void clientGateEnforcesStrictMonotonicityUnderOutOfOrderAndMultiSubscriptionStreams() {
    RvvClientGate gate = new RvvClientGate();
    List<ContinuousVectorEvent> accepted = new ArrayList<>();
    gate.addListener(accepted::add);

    String[] subs = new String[] {"sub-1", "sub-2", "sub-3"};
    String[] members = new String[] {"m1", "m2"};

    // Create monotonic baseline events plus duplicates and out-of-order replays
    List<ContinuousVectorEvent> rawStream = new ArrayList<>();
    for (int bucket = 0; bucket < 3; bucket++) {
      for (String member : members) {
        for (long v = 1L; v <= 10L; v++) {
          for (String sub : subs) {
            rawStream.add(new ContinuousVectorEvent(sub, "k-" + bucket + "-" + member + "-" + v,
                0.95f, bucket, member, v, 1, 1000L + v, false));
          }
        }
      }
    }

    // Inject duplicate replayed copies and out-of-order stale versions
    List<ContinuousVectorEvent> noisyStream = new ArrayList<>(rawStream);
    Random rng = new Random(1234L);
    for (int i = 0; i < 60; i++) {
      ContinuousVectorEvent sample = rawStream.get(rng.nextInt(rawStream.size()));
      noisyStream.add(new ContinuousVectorEvent(sample.getSubscriptionId(), sample.getKey(),
          sample.getSimilarityScore(), sample.getBucketId(), sample.getMemberId(),
          sample.getRegionVersion(), sample.getEntryVersion(), sample.getTimestamp(), true));
    }

    // Feed ascending base interleaved with duplicates, then shuffled replay wave
    for (ContinuousVectorEvent event : rawStream) {
      assertThat(gate.tryAccept(event)).isTrue();
    }
    Collections.shuffle(noisyStream, rng);
    for (ContinuousVectorEvent replay : noisyStream) {
      // Every replayed or stale event must be deterministically rejected
      assertThat(gate.tryAccept(replay)).isFalse();
    }

    assertThat(accepted).hasSize(rawStream.size());
    assertThat(gate.getAcceptedCount()).isEqualTo(rawStream.size());
    assertThat(gate.getDiscardedCount()).isEqualTo(noisyStream.size());
    assertThat(RvvClientGate.verifyStrictMonotonicity(accepted)).isEmpty();

    // Verify safe checkpoint reflects min across all subscriptions for each (bucket, member)
    for (int bucket = 0; bucket < 3; bucket++) {
      for (String member : members) {
        assertThat(gate.getSafeCheckpointVersion(bucket, member)).isEqualTo(10L);
      }
    }
  }

  @Test
  public void checkpointCompactionEvictsAcknowledgedTailAndIgnoresRegressiveAcks() {
    ContinuousVectorQueryService service = new ContinuousVectorQueryService(4);
    service.registerSubscription("sub-1", new float[] {1, 0, 0, 0}, 0.5f);

    int bucketId = 2;
    String member = "member-A";
    for (long v = 1L; v <= 10L; v++) {
      service.onBucketMutation(bucketId, member, v, "k-" + v, new float[] {1, 0, 0, 0}, false);
    }
    assertThat(service.getDispatcher().getTailSize(bucketId)).isEqualTo(10);

    // Advance checkpoint to 6 -> evicts v = 1..6, retains v = 7..10
    service.acknowledgeCheckpoint(bucketId, member, 6L);
    assertThat(service.getDispatcher().getCheckpointVersion(bucketId, member)).isEqualTo(6L);
    assertThat(service.getDispatcher().getTailSize(bucketId)).isEqualTo(4);

    // Regressive checkpoint ack (4 < 6) must be ignored
    service.acknowledgeCheckpoint(bucketId, member, 4L);
    assertThat(service.getDispatcher().getCheckpointVersion(bucketId, member)).isEqualTo(6L);

    // Promotion replays strictly v = 7, 8, 9, 10
    List<ContinuousVectorEvent> replayed = service.onPrimaryPromotion(bucketId);
    assertThat(replayed).extracting(ContinuousVectorEvent::getRegionVersion)
        .containsExactly(7L, 8L, 9L, 10L);
  }

  @Test
  public void continuousVectorQueryServiceFacadeTracksLifecycleAndTelemetry() {
    ContinuousVectorQueryService service = new ContinuousVectorQueryService(8);
    List<ContinuousVectorEvent> sinkEvents = new ArrayList<>();
    service.addEventSink(sinkEvents::add);

    float[] q = new float[] {1, 0, 0, 0, 0, 0, 0, 0};
    service.registerSubscription("sub-facade", q, 0.9f);
    assertThat(service.getActiveSubscriptionCount()).isEqualTo(1);

    List<ContinuousVectorEvent> emitted =
        service.onBucketMutation(0, "srv1", 1L, "k1", q, true);
    assertThat(emitted).hasSize(1);
    assertThat(sinkEvents).hasSize(1);
    assertThat(service.getTelemetry().getTotalRoutedVectors()).isEqualTo(1L);
    assertThat(service.getDispatcher().getTotalEventsDispatched()).isEqualTo(1L);

    assertThat(service.unregisterSubscription("sub-facade")).isTrue();
    assertThat(service.getActiveSubscriptionCount()).isZero();
    assertThat(service.onBucketMutation(0, "srv1", 2L, "k2", q, true)).isEmpty();
  }

  @Test
  public void asyncReplayExecutorPromotionGatesLiveWritesUntilTailReplayCompletes() {
    float[] vec = new float[] {1, 0, 0, 0};
    ContinuousVectorQueryService service = new ContinuousVectorQueryService(4);
    service.registerSubscription("sub-async", vec, 0.9f);

    RvvClientGate clientGate = new RvvClientGate();
    List<ContinuousVectorEvent> accepted = new ArrayList<>();
    clientGate.addListener(accepted::add);
    service.addEventSink(clientGate::tryAccept);

    int bucketId = 7;
    String memberId = "member-promote";

    // Pre-failover: v=1,2 delivered to client; v=3,4,5 replicated to secondary tail only
    for (long v = 1L; v <= 5L; v++) {
      boolean alreadyDelivered = (v <= 2L);
      service.onBucketMutation(bucketId, memberId, v, "doc-" + v, vec, false);
      if (alreadyDelivered) {
        clientGate.tryAccept(new ContinuousVectorEvent("sub-async", "doc-" + v, 1.0f, bucketId,
            memberId, v, 1, 100L + v, false));
      }
    }
    service.acknowledgeCheckpoint(bucketId, memberId, 2L);
    assertThat(accepted).extracting(ContinuousVectorEvent::getRegionVersion)
        .containsExactly(1L, 2L);

    // Deterministic manual queued Executor simulating delayed async promotion replay
    Queue<Runnable> queuedReplayTasks = new ArrayDeque<>();
    Executor manualExecutor = queuedReplayTasks::add;
    PartitionListener partitionListener =
        service.createPartitionListener(val -> (float[]) val, manualExecutor);

    // Trigger afterPrimary(bucketId) -> opens promotion barrier and queues async replay
    partitionListener.afterPrimary(bucketId);
    assertThat(queuedReplayTasks).hasSize(1);

    // Live writes v=6, v=7 arrive on the newly promoted primary BEFORE async replay runs!
    List<ContinuousVectorEvent> live6 =
        service.onBucketMutation(bucketId, memberId, 6L, "doc-6", vec, true);
    List<ContinuousVectorEvent> live7 =
        service.onBucketMutation(bucketId, memberId, 7L, "doc-7", vec, true);
    assertThat(live6).isEmpty();
    assertThat(live7).isEmpty();
    assertThat(accepted).extracting(ContinuousVectorEvent::getRegionVersion)
        .containsExactly(1L, 2L);

    // Drain the queued replay task -> replays v=3,4,5 first, then drains buffered live v=6,7
    queuedReplayTasks.poll().run();

    assertThat(accepted).extracting(ContinuousVectorEvent::getRegionVersion)
        .containsExactly(1L, 2L, 3L, 4L, 5L, 6L, 7L);
    assertThat(RvvClientGate.verifyStrictMonotonicity(accepted)).isEmpty();
    assertThat(clientGate.getDiscardedCount()).isZero();

    // Verify RejectedExecutionException cleanly releases the barrier instead of wedging bucketId
    PartitionListener rejectingListener = service.createPartitionListener(val -> (float[]) val,
        task -> {
          throw new RejectedExecutionException("shutting down");
        });
    rejectingListener.afterPrimary(bucketId);
    List<ContinuousVectorEvent> afterReject =
        service.onBucketMutation(bucketId, memberId, 8L, "doc-8", vec, true);
    assertThat(afterReject).hasSize(1);
    assertThat(accepted).extracting(ContinuousVectorEvent::getRegionVersion)
        .containsExactly(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L);
  }

  @Test
  public void unregisteringSubscriptionRemovesClientGateStateAndAllowsTailCompaction() {
    float[] vec = new float[] {1, 0, 0, 0};
    ContinuousVectorQueryService service = new ContinuousVectorQueryService(4);
    RvvClientGate clientGate = new RvvClientGate();
    service.attachClientGate(clientGate);
    service.addEventSink(clientGate::tryAccept);

    service.registerSubscription("sub-1", vec, 0.9f);
    service.registerSubscription("sub-2", vec, 0.9f);

    int bucketId = 3;
    String memberId = "member-compact";

    for (long v = 1L; v <= 5L; v++) {
      service.onBucketMutation(bucketId, memberId, v, "doc-" + v, vec, true);
    }
    assertThat(clientGate.getHighWaterVersion("sub-1", bucketId, memberId)).isEqualTo(5L);
    assertThat(clientGate.getHighWaterVersion("sub-2", bucketId, memberId)).isEqualTo(5L);
    assertThat(clientGate.getSafeCheckpointVersion(bucketId, memberId)).isEqualTo(5L);

    // Unregister sub-1 -> must purge sub-1 from clientGate so it does not pin V_ckpt at 5
    assertThat(service.unregisterSubscription("sub-1")).isTrue();
    assertThat(clientGate.getHighWaterVersion("sub-1", bucketId, memberId)).isZero();

    for (long v = 6L; v <= 10L; v++) {
      service.onBucketMutation(bucketId, memberId, v, "doc-" + v, vec, true);
    }
    assertThat(clientGate.getHighWaterVersion("sub-2", bucketId, memberId)).isEqualTo(10L);
    assertThat(clientGate.getSafeCheckpointVersion(bucketId, memberId)).isEqualTo(10L);

    // Compact dispatcher tail using safe checkpoint -> reclaims all sub-1 and sub-2 tail entries
    service.compactUpTo(bucketId, memberId,
        clientGate.getSafeCheckpointVersion(bucketId, memberId));
    assertThat(service.getDispatcher().getCheckpointVersion(bucketId, memberId)).isEqualTo(10L);
    assertThat(service.getDispatcher().getTailSize(bucketId)).isZero();
  }

  @Test
  public void concurrentTryAcceptOnSameStreamSerializesListenerDeliveryStrictlyMonotonically()
      throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      for (int round = 0; round < 200; round++) {
        RvvClientGate gate = new RvvClientGate();
        List<ContinuousVectorEvent> received = new CopyOnWriteArrayList<>();
        gate.addListener(event -> {
          if (event.getRegionVersion() == 5L) {
            // Widen concurrency window to expose unsynchronized listener races
            Thread.yield();
          }
          received.add(event);
        });

        ContinuousVectorEvent v5 = new ContinuousVectorEvent("sub-race", "k5", 0.99f, 1, "m1", 5L,
            1, 1005L, false);
        ContinuousVectorEvent v6 = new ContinuousVectorEvent("sub-race", "k6", 0.99f, 1, "m1", 6L,
            1, 1006L, false);

        CyclicBarrier barrier = new CyclicBarrier(2);
        Future<Boolean> f1 = pool.submit(() -> {
          barrier.await(5, TimeUnit.SECONDS);
          return gate.tryAccept(v5);
        });
        Future<Boolean> f2 = pool.submit(() -> {
          barrier.await(5, TimeUnit.SECONDS);
          return gate.tryAccept(v6);
        });

        boolean a5 = f1.get(5, TimeUnit.SECONDS);
        boolean a6 = f2.get(5, TimeUnit.SECONDS);

        assertThat(a6).isTrue();
        assertThat(RvvClientGate.verifyStrictMonotonicity(received))
            .as("Round %d produced out-of-order concurrent callback delivery", round)
            .isEmpty();
        if (a5) {
          assertThat(received).extracting(ContinuousVectorEvent::getRegionVersion)
              .containsExactly(5L, 6L);
        } else {
          assertThat(received).extracting(ContinuousVectorEvent::getRegionVersion)
              .containsExactly(6L);
        }
      }
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  public void tailBufferOverflowEmitsCounterResyncSignalAndOverflowCallback() {
    ChordalVoronoiRouter router = new ChordalVoronoiRouter(4);
    router.addSubscription(new VectorSubscription("sub-ovf", new float[] {1, 0, 0, 0}, 0.8f));
    RvvMonotonicVectorDispatcher dispatcher = new RvvMonotonicVectorDispatcher(router, 4);

    List<Long> evictedVersions = new ArrayList<>();
    dispatcher.addTailOverflowListener((bucketId, memberId, evictedVersion, ckpt, maxTail) -> {
      assertThat(bucketId).isEqualTo(3);
      assertThat(memberId).isEqualTo("srv-overflow");
      assertThat(ckpt).isGreaterThanOrEqualTo(0L);
      assertThat(maxTail).isEqualTo(4);
      evictedVersions.add(evictedVersion);
    });

    for (long v = 1L; v <= 7L; v++) {
      dispatcher.onBucketMutation(3, "srv-overflow", v, 1, "k-" + v, new float[] {1, 0, 0, 0},
          false);
      assertThat(dispatcher.getReplayFloorVersion(3, "srv-overflow"))
          .isLessThanOrEqualTo(dispatcher.getCheckpointVersion(3, "srv-overflow"));
    }

    assertThat(dispatcher.getTailSize(3)).isEqualTo(4);
    assertThat(dispatcher.getTailOverflowCount()).isEqualTo(3L);
    assertThat(evictedVersions).containsExactly(1L, 2L, 3L);
    assertThat(dispatcher.getReplayFloorVersion(3, "srv-overflow")).isEqualTo(3L);
    assertThat(dispatcher.getCheckpointVersion(3, "srv-overflow")).isEqualTo(7L);
    assertThat(dispatcher.isResyncRequired(3, "srv-overflow")).isTrue();
    assertThat(dispatcher.getResyncRequiredStreams())
        .containsExactly(new RvvStreamKey(3, "srv-overflow"));

    dispatcher.clearResyncRequired(3, "srv-overflow");
    assertThat(dispatcher.isResyncRequired(3, "srv-overflow")).isFalse();
  }

  @Test
  public void coalescedBatchAndAsyncPerBucketDispatchPreservesStrictRvvMonotonicityAcrossFailover()
      throws Exception {
    int dim = 4;
    float[] vec = new float[] {1, 0, 0, 0};
    ContinuousVectorQueryService service = new ContinuousVectorQueryService(dim);

    // Register 64 matching subscriptions so each mutation produces a 64-event coalesced batch
    int numSubs = 64;
    for (int m = 0; m < numSubs; m++) {
      service.registerSubscription("batch-sub-" + m, vec, 0.85f);
    }

    ExecutorService asyncDispatchPool = Executors.newFixedThreadPool(4);
    try {
      service.setAsyncDispatchExecutor(asyncDispatchPool);

      RvvClientGate clientGate = new RvvClientGate();
      List<ContinuousVectorEvent> accepted = new CopyOnWriteArrayList<>();
      clientGate.addListener(accepted::add);

      AtomicInteger batchCalls = new AtomicInteger(0);
      AtomicInteger heartbeatBatches = new AtomicInteger(0);
      AtomicInteger singleEventFallbackCalls = new AtomicInteger(0);
      service.addEventSink(new ContinuousVectorEventSink() {
        @Override
        public void onEvent(ContinuousVectorEvent event) {
          singleEventFallbackCalls.incrementAndGet();
          clientGate.tryAccept(event);
        }

        @Override
        public void onEventBatch(List<ContinuousVectorEvent> events) {
          if (events.size() == 1 && events.get(0).isHeartbeat()) {
            heartbeatBatches.incrementAndGet();
            clientGate.tryAcceptBatch(events);
            return;
          }
          batchCalls.incrementAndGet();
          assertThat(events).hasSize(numSubs);
          clientGate.tryAcceptBatch(events);
        }
      });

      int bucketId = 5;
      String memberId = "member-async";

      // 1. Live primary mutations v=1..10 -> each mutation emits 1 coalesced batch of 64 events
      for (long v = 1L; v <= 10L; v++) {
        service.onBucketMutation(bucketId, memberId, v, "doc-" + v, vec, true);
      }
      assertThat(service.awaitPendingDispatches(5_000L)).isTrue();
      assertThat(batchCalls.get()).isEqualTo(10);
      assertThat(singleEventFallbackCalls.get()).isZero();
      assertThat(accepted).hasSize(10 * numSubs);
      assertThat(service.getDispatcher().getTotalBatchesDispatched()).isEqualTo(10L);

      // Advance checkpoint to v=6, simulate secondary recording v=7..15 (uncertain tail)
      service.acknowledgeCheckpoint(bucketId, memberId, 6L);
      for (long v = 11L; v <= 15L; v++) {
        service.onBucketMutation(bucketId, memberId, v, "doc-" + v, vec, false);
      }

      // 2. Primary failover promotion + concurrent live mutations v=16..20 under async dispatch
      service.getDispatcher().beginAsyncPromotion(bucketId);
      for (long v = 16L; v <= 20L; v++) {
        service.onBucketMutation(bucketId, memberId, v, "doc-" + v, vec, true);
      }
      service.getDispatcher().completeAsyncPromotion(bucketId, Collections.emptyList());

      assertThat(service.awaitPendingDispatches(5_000L)).isTrue();

      // Strictly 20 unique versions * 64 subs = 1,280 accepted events; replayed v=7..10 deduped!
      assertThat(accepted).hasSize(20 * numSubs);
      assertThat(heartbeatBatches.get()).isEqualTo(1);
      assertThat(clientGate.getDiscardedCount()).isEqualTo(4L * numSubs);
      assertThat(RvvClientGate.verifyStrictMonotonicity(accepted))
          .as("Async per-bucket coalesced dispatch must preserve strict Theorem 3 RVV monotonicity")
          .isEmpty();
      for (int m = 0; m < numSubs; m++) {
        assertThat(clientGate.getHighWaterVersion("batch-sub-" + m, bucketId, memberId))
            .isEqualTo(20L);
      }
    } finally {
      asyncDispatchPool.shutdownNow();
    }
  }

  @Test
  public void zeroMatchSubscriberAdvancesCheckpointViaHeartbeatsWithoutTailOverflowOrFailoverLoss() {
    float[] matchVec = new float[] {1.0f, 0.0f, 0.0f, 0.0f};
    float[] zeroMatchQuery = new float[] {0.0f, 0.0f, 0.0f, 1.0f};
    int maxTail = 32;
    int heartbeatInterval = 8;

    ChordalVoronoiRouter primaryRouter = new ChordalVoronoiRouter(4);
    RvvMonotonicVectorDispatcher primaryDispatcher =
        new RvvMonotonicVectorDispatcher(primaryRouter, maxTail, null, heartbeatInterval);
    ContinuousVectorQueryService primary =
        new ContinuousVectorQueryService(primaryRouter, primaryDispatcher);

    ChordalVoronoiRouter secondaryRouter = new ChordalVoronoiRouter(4);
    RvvMonotonicVectorDispatcher secondaryDispatcher =
        new RvvMonotonicVectorDispatcher(secondaryRouter, maxTail, null, heartbeatInterval);
    ContinuousVectorQueryService secondary =
        new ContinuousVectorQueryService(secondaryRouter, secondaryDispatcher);

    RvvClientGate clientGate = new RvvClientGate();
    AtomicInteger matchingCallbacks = new AtomicInteger();
    AtomicInteger zeroMatchCallbacks = new AtomicInteger();
    List<ContinuousVectorEvent> accepted = new ArrayList<>();
    clientGate.addListener(ev -> {
      accepted.add(ev);
      if ("sub-matching".equals(ev.getSubscriptionId())) {
        matchingCallbacks.incrementAndGet();
      } else if ("sub-zero-match".equals(ev.getSubscriptionId())) {
        zeroMatchCallbacks.incrementAndGet();
      }
    });

    primary.attachClientGate(clientGate);
    primary.registerSubscription("sub-matching", matchVec, 0.85f);
    primary.registerSubscription("sub-zero-match", zeroMatchQuery, 0.99f);
    secondary.registerSubscription("sub-matching", matchVec, 0.85f);
    secondary.registerSubscription("sub-zero-match", zeroMatchQuery, 0.99f);

    primary.addEventSink(clientGate::tryAccept);
    secondary.addEventSink(clientGate::tryAccept);

    int bucketId = 2;
    String memberId = "srv-heartbeat";
    int totalLive = 5 * maxTail; // 160 mutations >> maxTail (32)

    for (long v = 1L; v <= totalLive; v++) {
      List<ContinuousVectorEvent> emitted =
          primary.onBucketMutation(bucketId, memberId, v, "k-" + v, matchVec, true);
      secondary.onBucketMutation(bucketId, memberId, v, "k-" + v, matchVec, false);
      // Returned list contains strictly match events (no heartbeats leaking into caller result)
      assertThat(emitted).hasSize(1);
      assertThat(emitted.get(0).isHeartbeat()).isFalse();

      // Advance server checkpoints strictly from safe client checkpoint across ALL active subs
      long safeCkpt = clientGate.getSafeCheckpointVersion(bucketId, memberId);
      if (v < heartbeatInterval) {
        // Before first heartbeat, 0%-match subscriber is at 0 -> safe checkpoint must be 0
        assertThat(safeCkpt).isZero();
      }
      primary.acknowledgeCheckpoint(bucketId, memberId, safeCkpt);
      secondary.acknowledgeCheckpoint(bucketId, memberId, safeCkpt);
    }

    assertThat(primaryDispatcher.getTailOverflowCount()).isZero();
    assertThat(primaryDispatcher.isResyncRequired(bucketId, memberId)).isFalse();
    assertThat(secondaryDispatcher.getTailOverflowCount()).isZero();
    assertThat(primaryDispatcher.getTotalHeartbeatsDispatched())
        .isEqualTo(totalLive / heartbeatInterval);
    assertThat(clientGate.getHeartbeatsAcceptedCount())
        .isEqualTo(totalLive / heartbeatInterval);
    assertThat(clientGate.getHighWaterVersion("sub-zero-match", bucketId, memberId))
        .isEqualTo(totalLive);
    assertThat(clientGate.getSafeCheckpointVersion(bucketId, memberId)).isEqualTo(totalLive);
    assertThat(primaryDispatcher.getTailSize(bucketId)).isZero();
    assertThat(zeroMatchCallbacks.get()).isZero();
    assertThat(matchingCallbacks.get()).isEqualTo(totalLive);

    // Failover phase: primary crashes after replicating v=161..175 to secondary without delivering
    for (long v = totalLive + 1L; v <= totalLive + 15L; v++) {
      secondary.onBucketMutation(bucketId, memberId, v, "k-" + v, matchVec, false);
    }
    List<ContinuousVectorEvent> replayed = secondary.onPrimaryPromotion(bucketId);
    assertThat(replayed).hasSize(15);
    assertThat(replayed).allMatch(ContinuousVectorEvent::isReplayed);

    // Trailing promotion watermark heartbeat advances even the 0%-match subscriber to v=175!
    assertThat(clientGate.getHighWaterVersion("sub-zero-match", bucketId, memberId))
        .isEqualTo(totalLive + 15L);
    assertThat(clientGate.getSafeCheckpointVersion(bucketId, memberId))
        .isEqualTo(totalLive + 15L);
    assertThat(zeroMatchCallbacks.get()).isZero();
    assertThat(matchingCallbacks.get()).isEqualTo(totalLive + 15);
    assertThat(RvvClientGate.verifyStrictMonotonicity(accepted)).isEmpty();
  }

  @Test
  public void slowReconnectReplaysBothPreCrashAndPostPromotionStreamsWithWatermarksAndConcurrency()
      throws Exception {
    float[] matchVec = new float[] {1.0f, 0.0f, 0.0f, 0.0f};
    float[] nonMatchVec = new float[] {0.0f, 1.0f, 0.0f, 0.0f};
    float[] zeroMatchQuery = new float[] {0.0f, 0.0f, 0.0f, 1.0f};

    ContinuousVectorQueryService primary = new ContinuousVectorQueryService(4);
    ContinuousVectorQueryService secondary = new ContinuousVectorQueryService(4);
    primary.setHeartbeatIntervalEvents(4);
    secondary.setHeartbeatIntervalEvents(4);

    RvvClientGate clientGate = new RvvClientGate();
    List<ContinuousVectorEvent> accepted = new CopyOnWriteArrayList<>();
    AtomicInteger zeroMatchCallbacks = new AtomicInteger();
    clientGate.addListener(ev -> {
      accepted.add(ev);
      if ("sub-zero".equals(ev.getSubscriptionId())) {
        zeroMatchCallbacks.incrementAndGet();
      }
    });
    primary.attachClientGate(clientGate);
    primary.registerSubscription("sub-match", matchVec, 0.90f);
    primary.registerSubscription("sub-zero", zeroMatchQuery, 0.99f);
    secondary.registerSubscription("sub-match", matchVec, 0.90f);
    secondary.registerSubscription("sub-zero", zeroMatchQuery, 0.99f);

    // Phase 1: Pre-crash primary delivers v = 1..12; v = 13..20 are dropped before crash
    ContinuousVectorEventSink primarySink = event -> {
      if (event.getRegionVersion() <= 12L) {
        clientGate.tryAccept(event);
      }
    };
    primary.addEventSink(primarySink);

    int bucketId = 9;
    String memberP = "server1(id:1)";
    String memberS = "server2(id:2)";
    Set<String> expectedKeys = new HashSet<>();

    for (long v = 1L; v <= 20L; v++) {
      boolean isMatch = (v % 2L == 1L); // odd versions match
      float[] vec = isMatch ? matchVec : nonMatchVec;
      String key = "pre-" + v;
      if (isMatch) {
        expectedKeys.add(key);
      }
      primary.onBucketMutation(bucketId, memberP, v, key, vec, true);
      secondary.onBucketMutation(bucketId, memberP, v, key, vec, false);
    }
    // Client saw up to v=12 (periodic heartbeat at 12, match at 11 -> safe high-water is 12)
    assertThat(clientGate.getSafeCheckpointVersion(bucketId, memberP)).isEqualTo(12L);
    secondary.acknowledgeCheckpoint(bucketId, memberP, 4L);

    // Phase 2: Primary crashes & secondary promotes while client is DISCONNECTED (sink detached)
    List<ContinuousVectorEvent> promotionEmitted = secondary.onPrimaryPromotion(bucketId);
    assertThat(promotionEmitted).isNotEmpty();

    // During slow disconnect window, post-promotion m_S writes v = 1..15 arrive while client is
    // still disconnected (events dropped on dead pre-reconnect socket)
    for (long v = 1L; v <= 15L; v++) {
      boolean isMatch = (v % 3L != 0L); // v = 1,2,4,5,7,8,10,11,13,14 match
      float[] vec = isMatch ? matchVec : nonMatchVec;
      String key = "post-disconn-" + v;
      if (isMatch) {
        expectedKeys.add(key);
      }
      secondary.onBucketMutation(bucketId, memberS, v, key, vec, true);
    }

    // Phase 3: Client reconnects while concurrent live m_S writes (16..40) run on another thread
    Set<Integer> liveEnabledBuckets = Collections.synchronizedSet(new HashSet<>());
    ContinuousVectorEventSink reconnectSink = new ContinuousVectorEventSink() {
      @Override
      public void onEvent(ContinuousVectorEvent event) {
        if (event.isReplayed() || liveEnabledBuckets.contains(event.getBucketId())) {
          clientGate.tryAccept(event);
        }
      }
    };
    secondary.addEventSink(reconnectSink);

    Map<String, Long> clientHighWater = clientGate.getSafeHighWaterByMember(bucketId);
    assertThat(clientHighWater).containsEntry(memberP, 12L).doesNotContainKey(memberS);

    for (long v = 16L; v <= 40L; v++) {
      expectedKeys.add("post-conc-" + v);
    }

    ExecutorService writerPool = Executors.newSingleThreadExecutor();
    try {
      CountDownLatch writerStarted = new CountDownLatch(1);
      Future<?> writerFuture = writerPool.submit(() -> {
        writerStarted.countDown();
        for (long v = 16L; v <= 40L; v++) {
          secondary.onBucketMutation(bucketId, memberS, v, "post-conc-" + v, matchVec, true);
          if (v == 20L || v == 28L) {
            Thread.yield();
          }
        }
      });
      writerStarted.await(5, TimeUnit.SECONDS);

      List<ContinuousVectorEvent> replayed = secondary.replayUncheckpointedForReconnect(bucketId,
          clientHighWater, () -> liveEnabledBuckets.add(bucketId));
      // Must skip pre-1..12 (already delivered to client) and replay strictly > 12 for m_P plus
      // uncheckpointed m_S tail!
      assertThat(replayed).isNotEmpty();
      assertThat(replayed).allMatch(ContinuousVectorEvent::isReplayed);
      assertThat(replayed)
          .filteredOn(e -> memberP.equals(e.getMemberId()))
          .extracting(ContinuousVectorEvent::getRegionVersion)
          .containsExactly(13L, 15L, 17L, 19L);

      writerFuture.get(10, TimeUnit.SECONDS);
    } finally {
      writerPool.shutdownNow();
    }

    // Verify 0 missed events, 0 duplicates, strict monotonicity, and trailing watermarks for 0%-sub
    Set<Object> deliveredKeys = new HashSet<>();
    for (ContinuousVectorEvent ev : accepted) {
      deliveredKeys.add(ev.getKey());
    }
    assertThat(deliveredKeys).containsExactlyInAnyOrderElementsOf(expectedKeys);
    assertThat(accepted).hasSize(expectedKeys.size());
    assertThat(clientGate.getDiscardedCount()).isZero();
    assertThat(zeroMatchCallbacks.get()).isZero();
    assertThat(RvvClientGate.verifyStrictMonotonicity(accepted)).isEmpty();
    assertThat(clientGate.getSafeCheckpointVersion(bucketId, memberP)).isEqualTo(20L);
    assertThat(clientGate.getHighWaterVersion("sub-match", bucketId, memberS)).isEqualTo(40L);
    long safeMemberS = clientGate.getSafeCheckpointVersion(bucketId, memberS);
    assertThat(safeMemberS).isBetween(36L, 40L);

    // Null map / unpopulated bucket and post-checkpoint compaction edge cases must remain safe
    assertThat(secondary.replayUncheckpointedForReconnect(9999, null)).isEmpty();
    secondary.acknowledgeCheckpoint(bucketId, memberP,
        clientGate.getSafeCheckpointVersion(bucketId, memberP));
    if (safeMemberS < 40L) {
      secondary.acknowledgeCheckpoint(bucketId, memberS, safeMemberS);
      secondary.replayUncheckpointedForReconnect(bucketId, null);
      assertThat(clientGate.getSafeCheckpointVersion(bucketId, memberS)).isEqualTo(40L);
      assertThat(accepted).hasSize(expectedKeys.size());
      assertThat(RvvClientGate.verifyStrictMonotonicity(accepted)).isEmpty();
    }
    secondary.acknowledgeCheckpoint(bucketId, memberS, 40L);
    assertThat(secondary.getDispatcher().getTailSize(bucketId)).isZero();
    assertThat(secondary.replayUncheckpointedForReconnect(bucketId, null)).isEmpty();
    assertThat(secondary.getDispatcher().getTotalReconnectReplaysHandled())
        .isGreaterThanOrEqualTo(3L);
  }

  @Test
  public void matchExactlyOnHeartbeatMutation64DeliversMatchStrictlyBeforeWatermarkAndAdvancesZeroMatchClient()
      throws Exception {
    float[] matchVec = new float[] {1.0f, 0.0f, 0.0f, 0.0f};
    float[] nonMatchVec = new float[] {0.0f, 1.0f, 0.0f, 0.0f};
    float[] zeroMatchQuery = new float[] {0.0f, 0.0f, 0.0f, 1.0f};

    ExecutorService exec4T = Executors.newFixedThreadPool(4);
    try {
      ContinuousVectorQueryService service = new ContinuousVectorQueryService(4);
      // Verify default stride is H = 64
      assertThat(service.getHeartbeatIntervalEvents())
          .isEqualTo(RvvMonotonicVectorDispatcher.DEFAULT_HEARTBEAT_INTERVAL_EVENTS)
          .isEqualTo(64);
      service.setAsyncDispatchExecutor(exec4T);
      service.registerSubscription("sub-match", matchVec, 0.90f);
      service.registerSubscription("sub-zero", zeroMatchQuery, 0.99f);

      RvvClientGate clientGate = new RvvClientGate();
      List<ContinuousVectorEvent> acceptedMatches = new CopyOnWriteArrayList<>();
      AtomicInteger zeroCallbacks = new AtomicInteger();
      clientGate.addListener(ev -> {
        acceptedMatches.add(ev);
        if ("sub-zero".equals(ev.getSubscriptionId())) {
          zeroCallbacks.incrementAndGet();
        }
      });
      service.attachClientGate(clientGate);

      List<List<ContinuousVectorEvent>> observedBatches = new CopyOnWriteArrayList<>();
      List<ContinuousVectorEvent> flattenedDelivered = new CopyOnWriteArrayList<>();
      service.addEventSink(new ContinuousVectorEventSink() {
        @Override
        public void onEvent(ContinuousVectorEvent event) {
          flattenedDelivered.add(event);
          clientGate.tryAccept(event);
        }

        @Override
        public void onEventBatch(List<ContinuousVectorEvent> events) {
          observedBatches.add(events);
          for (ContinuousVectorEvent ev : events) {
            onEvent(ev);
          }
        }
      });

      int bucketId = 5;
      String memberId = "server1(id:1)";
      for (long v = 1L; v <= 63L; v++) {
        service.onBucketMutation(bucketId, memberId, v, "nomatch-" + v, nonMatchVec, true);
      }
      assertThat(service.awaitPendingDispatches(5_000L)).isTrue();
      assertThat(observedBatches).isEmpty();
      assertThat(acceptedMatches).isEmpty();
      assertThat(clientGate.getSafeCheckpointVersion(bucketId, memberId)).isZero();

      // Mutation #64 (v_h = 64) triggers BOTH a match and the periodic #64 watermark heartbeat!
      service.onBucketMutation(bucketId, memberId, 64L, "match-64", matchVec, true);
      assertThat(service.awaitPendingDispatches(5_000L)).isTrue();

      assertThat(observedBatches).hasSize(1);
      List<ContinuousVectorEvent> boundaryBatch = observedBatches.get(0);
      assertThat(boundaryBatch).hasSize(2);
      // Equal-version v_h ordering: match alert MUST precede watermarkHeartbeat(v_h)
      assertThat(boundaryBatch.get(0).isHeartbeat()).isFalse();
      assertThat(boundaryBatch.get(0).getRegionVersion()).isEqualTo(64L);
      assertThat(boundaryBatch.get(0).getKey()).isEqualTo("match-64");
      assertThat(boundaryBatch.get(1).isHeartbeat()).isTrue();
      assertThat(boundaryBatch.get(1).getRegionVersion()).isEqualTo(64L);

      assertThat(flattenedDelivered).hasSize(2);
      assertThat(flattenedDelivered.get(0).isHeartbeat()).isFalse();
      assertThat(flattenedDelivered.get(1).isHeartbeat()).isTrue();

      assertThat(acceptedMatches).hasSize(1);
      assertThat(acceptedMatches.get(0).getKey()).isEqualTo("match-64");
      assertThat(clientGate.getDiscardedCount()).isZero();
      assertThat(clientGate.getHeartbeatsAcceptedCount()).isEqualTo(1L);
      assertThat(clientGate.getHighWaterVersion("sub-zero", bucketId, memberId)).isEqualTo(64L);
      assertThat(clientGate.getSafeCheckpointVersion(bucketId, memberId)).isEqualTo(64L);
      assertThat(zeroCallbacks.get()).isZero();
      assertThat(RvvClientGate.verifyStrictMonotonicity(acceptedMatches)).isEmpty();
    } finally {
      exec4T.shutdownNow();
    }
  }

  @Test
  public void slowReconnectWithFourThreadExecutorConcurrentWritesAndTwoClientsAtDifferentWatermarks()
      throws Exception {
    float[] matchVec = new float[] {1.0f, 0.0f, 0.0f, 0.0f};
    float[] nonMatchVec = new float[] {0.0f, 1.0f, 0.0f, 0.0f};
    float[] zeroMatchQuery = new float[] {0.0f, 0.0f, 0.0f, 1.0f};

    ExecutorService exec4TPrimary = Executors.newFixedThreadPool(4);
    ExecutorService exec4TSecondary = Executors.newFixedThreadPool(4);
    ExecutorService writerPool = Executors.newSingleThreadExecutor();
    try {
      ContinuousVectorQueryService primary = new ContinuousVectorQueryService(4);
      ContinuousVectorQueryService secondary = new ContinuousVectorQueryService(4);
      primary.setHeartbeatIntervalEvents(4);
      secondary.setHeartbeatIntervalEvents(4);
      primary.setAsyncDispatchExecutor(exec4TPrimary);
      secondary.setAsyncDispatchExecutor(exec4TSecondary);

      RvvClientGate gateA = new RvvClientGate();
      RvvClientGate gateB = new RvvClientGate();
      List<ContinuousVectorEvent> acceptedA = new CopyOnWriteArrayList<>();
      List<ContinuousVectorEvent> acceptedB = new CopyOnWriteArrayList<>();
      gateA.addListener(acceptedA::add);
      gateB.addListener(acceptedB::add);
      primary.attachClientGate(gateA);
      primary.attachClientGate(gateB);
      primary.registerSubscription("sub-match", matchVec, 0.90f);
      primary.registerSubscription("sub-zero", zeroMatchQuery, 0.99f);
      secondary.registerSubscription("sub-match", matchVec, 0.90f);
      secondary.registerSubscription("sub-zero", zeroMatchQuery, 0.99f);

      // Client B disconnects after v=4; Client A disconnects after v=12 (V_client_A != V_client_B)
      primary.addEventSink(event -> {
        if (event.getRegionVersion() <= 12L) {
          gateA.tryAccept(event);
        }
        if (event.getRegionVersion() <= 4L) {
          gateB.tryAccept(event);
        }
      });

      int bucketId = 11;
      String memberP = "server1(id:1)";
      String memberS = "server2(id:2)";
      Set<String> expectedKeys = new HashSet<>();

      for (long v = 1L; v <= 20L; v++) {
        boolean isMatch = (v % 2L == 1L);
        float[] vec = isMatch ? matchVec : nonMatchVec;
        String key = "pre-" + v;
        if (isMatch) {
          expectedKeys.add(key);
        }
        primary.onBucketMutation(bucketId, memberP, v, key, vec, true);
        secondary.onBucketMutation(bucketId, memberP, v, key, vec, false);
      }
      assertThat(primary.awaitPendingDispatches(5_000L)).isTrue();
      assertThat(gateA.getSafeCheckpointVersion(bucketId, memberP)).isEqualTo(12L);
      assertThat(gateB.getSafeCheckpointVersion(bucketId, memberP)).isEqualTo(4L);

      // Safe checkpoint cannot exceed min(V_client_A, V_client_B) = 4
      secondary.acknowledgeCheckpoint(bucketId, memberP, 4L);

      // Primary crashes; secondary promotes & ingests post-promotion m_S 1..20 while BOTH clients
      // remain disconnected
      secondary.onPrimaryPromotion(bucketId);
      assertThat(secondary.awaitPendingDispatches(5_000L)).isTrue();
      for (long v = 1L; v <= 20L; v++) {
        boolean isMatch = (v % 2L == 0L); // includes heartbeat boundaries 4, 8, 12, 16, 20
        float[] vec = isMatch ? matchVec : nonMatchVec;
        String key = "post-disconn-" + v;
        if (isMatch) {
          expectedKeys.add(key);
        }
        secondary.onBucketMutation(bucketId, memberS, v, key, vec, true);
      }
      assertThat(secondary.awaitPendingDispatches(5_000L)).isTrue();

      // Reconnect both clients with per-client sinks while concurrent m_S writes 21..80 blast
      // through the 4-thread async executor
      Set<Integer> liveA = Collections.synchronizedSet(new HashSet<>());
      Set<Integer> liveB = Collections.synchronizedSet(new HashSet<>());
      List<ContinuousVectorEvent> wireA = new CopyOnWriteArrayList<>();
      List<ContinuousVectorEvent> wireB = new CopyOnWriteArrayList<>();

      ContinuousVectorEventSink sinkA = new ContinuousVectorEventSink() {
        @Override
        public void onEvent(ContinuousVectorEvent event) {
          if (event.isReplayed() || liveA.contains(event.getBucketId())) {
            wireA.add(event);
            gateA.tryAccept(event);
          }
        }
      };
      ContinuousVectorEventSink sinkB = new ContinuousVectorEventSink() {
        @Override
        public void onEvent(ContinuousVectorEvent event) {
          if (event.isReplayed() || liveB.contains(event.getBucketId())) {
            wireB.add(event);
            gateB.tryAccept(event);
          }
        }
      };
      secondary.addEventSink(sinkA);
      secondary.addEventSink(sinkB);

      for (long v = 21L; v <= 80L; v++) {
        expectedKeys.add("post-conc-" + v);
      }

      Map<String, Long> hwA = gateA.getSafeHighWaterByMember(bucketId);
      Map<String, Long> hwB = gateB.getSafeHighWaterByMember(bucketId);
      assertThat(hwA).containsEntry(memberP, 12L);
      assertThat(hwB).containsEntry(memberP, 4L);

      CountDownLatch writerStarted = new CountDownLatch(1);
      Future<?> writerFuture = writerPool.submit(() -> {
        writerStarted.countDown();
        for (long v = 21L; v <= 80L; v++) {
          secondary.onBucketMutation(bucketId, memberS, v, "post-conc-" + v, matchVec, true);
          if (v % 10L == 0L) {
            Thread.yield();
          }
        }
      });
      writerStarted.await(5, TimeUnit.SECONDS);

      List<ContinuousVectorEvent> replayA =
          secondary.replayUncheckpointedForReconnect(bucketId, hwA, sinkA,
              () -> liveA.add(bucketId));
      List<ContinuousVectorEvent> replayB =
          secondary.replayUncheckpointedForReconnect(bucketId, hwB, sinkB,
              () -> liveB.add(bucketId));

      writerFuture.get(10, TimeUnit.SECONDS);
      assertThat(secondary.awaitPendingDispatches(10_000L)).isTrue();

      // Client A only needed m_P > 12 (versions 13, 15, 17, 19); Client B needed m_P > 4 (5..19)
      assertThat(replayA)
          .filteredOn(e -> memberP.equals(e.getMemberId()))
          .extracting(ContinuousVectorEvent::getRegionVersion)
          .containsExactly(13L, 15L, 17L, 19L);
      assertThat(replayB)
          .filteredOn(e -> memberP.equals(e.getMemberId()))
          .extracting(ContinuousVectorEvent::getRegionVersion)
          .containsExactly(5L, 7L, 9L, 11L, 13L, 15L, 17L, 19L);

      for (List<ContinuousVectorEvent> accepted : java.util.Arrays.asList(acceptedA, acceptedB)) {
        Set<Object> deliveredKeys = new HashSet<>();
        for (ContinuousVectorEvent ev : accepted) {
          deliveredKeys.add(ev.getKey());
        }
        assertThat(deliveredKeys).containsExactlyInAnyOrderElementsOf(expectedKeys);
        assertThat(accepted).hasSize(expectedKeys.size());
        assertThat(RvvClientGate.verifyStrictMonotonicity(accepted)).isEmpty();
      }
      assertThat(gateA.getDiscardedCount()).isZero();
      assertThat(gateB.getDiscardedCount()).isZero();
      assertThat(gateA.getSafeCheckpointVersion(bucketId, memberP)).isEqualTo(20L);
      assertThat(gateB.getSafeCheckpointVersion(bucketId, memberP)).isEqualTo(20L);
      assertThat(gateA.getSafeCheckpointVersion(bucketId, memberS)).isEqualTo(80L);
      assertThat(gateB.getSafeCheckpointVersion(bucketId, memberS)).isEqualTo(80L);
      assertDataStrictlyBeforeEqualVersionHeartbeat(wireA);
      assertDataStrictlyBeforeEqualVersionHeartbeat(wireB);
    } finally {
      writerPool.shutdownNow();
      exec4TPrimary.shutdownNow();
      exec4TSecondary.shutdownNow();
    }
  }

  @Test
  public void outOfOrderHoldbackOrders65And66StrictlyBehindDelayed64PlusNonVectorGapWithoutDrops() {
    float[] matchVec = new float[] {1.0f, 0.0f, 0.0f, 0.0f};
    float[] nonMatchVec = new float[] {0.0f, 1.0f, 0.0f, 0.0f};
    float[] zeroMatchQuery = new float[] {0.0f, 0.0f, 0.0f, 1.0f};

    ContinuousVectorQueryService service = new ContinuousVectorQueryService(4);
    assertThat(service.isEnforceContiguousVersions()).isTrue();
    service.registerSubscription("sub-match", matchVec, 0.90f);
    service.registerSubscription("sub-zero", zeroMatchQuery, 0.99f);

    RvvClientGate clientGate = new RvvClientGate();
    List<ContinuousVectorEvent> accepted = new ArrayList<>();
    clientGate.addListener(accepted::add);
    service.attachClientGate(clientGate);
    service.addEventSink(clientGate::tryAccept);

    int bucketId = 4;
    String memberId = "server1(id:1)";

    // Feed contiguous prefix 1..63 (non-matching so no callbacks, 63 events toward H=64)
    for (long v = 1L; v <= 63L; v++) {
      service.onBucketMutation(bucketId, memberId, v, "k-" + v, nonMatchVec, true);
    }
    assertThat(service.getDispatcher().getHoldbackSize(bucketId)).isZero();
    assertThat(accepted).isEmpty();

    // Concurrent thread reordering: v=65 and v=66 arrive BEFORE v=64
    assertThat(service.onBucketMutation(bucketId, memberId, 65L, "k-65", matchVec, true)).isEmpty();
    assertThat(service.onBucketMutation(bucketId, memberId, 66L, "k-66", matchVec, true)).isEmpty();
    assertThat(service.getDispatcher().getHoldbackSize(bucketId)).isEqualTo(2);
    assertThat(accepted).isEmpty();
    assertThat(clientGate.getDiscardedCount()).isZero();

    // Delayed v=64 arrives (right on H=64 boundary!): drains 64 -> 65 -> 66 strictly in order
    service.onBucketMutation(bucketId, memberId, 64L, "k-64", matchVec, true);
    assertThat(service.getDispatcher().getHoldbackSize(bucketId)).isZero();
    assertThat(accepted).extracting(ContinuousVectorEvent::getRegionVersion)
        .containsExactly(64L, 65L, 66L);
    assertThat(clientGate.getDiscardedCount()).isZero();
    assertThat(clientGate.getHighWaterVersion("sub-zero", bucketId, memberId)).isEqualTo(64L);
    assertThat(RvvClientGate.verifyStrictMonotonicity(accepted)).isEmpty();

    // Destroy/invalidate/non-vector gap at v=67 while v=68 arrives first
    assertThat(service.onBucketMutation(bucketId, memberId, 68L, "k-68", matchVec, true)).isEmpty();
    assertThat(service.getDispatcher().getHoldbackSize(bucketId)).isEqualTo(1);
    service.onNonMatchingBucketVersion(bucketId, memberId, 67L);
    assertThat(service.getDispatcher().getHoldbackSize(bucketId)).isZero();
    assertThat(accepted).extracting(ContinuousVectorEvent::getRegionVersion)
        .containsExactly(64L, 65L, 66L, 68L);
    assertThat(clientGate.getDiscardedCount()).isZero();

    // Bounded maxHoldbackGap escape prevents permanent deadlock if an operation aborts past gap
    service.setMaxHoldbackGap(3);
    service.onBucketMutation(bucketId, memberId, 70L, "k-70", matchVec, true);
    service.onBucketMutation(bucketId, memberId, 71L, "k-71", matchVec, true);
    assertThat(service.getDispatcher().getHoldbackSize(bucketId)).isEqualTo(2);
    // Skipping 69, arriving 73 gives span 73 - 69 = 4 > 3 -> drains 70, 71 cleanly (73 held)
    service.onBucketMutation(bucketId, memberId, 73L, "k-73", matchVec, true);
    assertThat(accepted).extracting(ContinuousVectorEvent::getRegionVersion)
        .containsExactly(64L, 65L, 66L, 68L, 70L, 71L);
    assertThat(clientGate.getDiscardedCount()).isZero();
    assertThat(RvvClientGate.verifyStrictMonotonicity(accepted)).isEmpty();
  }

  @Test
  public void timedIdleTailHeartbeatFlushAdvancesCheckpointForSubIntervalZeroMatchStream()
      throws Exception {
    float[] matchVec = new float[] {1.0f, 0.0f, 0.0f, 0.0f};
    float[] nonMatchVec = new float[] {0.0f, 1.0f, 0.0f, 0.0f};
    float[] zeroMatchQuery = new float[] {0.0f, 0.0f, 0.0f, 1.0f};

    ContinuousVectorQueryService service = new ContinuousVectorQueryService(4);
    assertThat(service.getHeartbeatIntervalEvents()).isEqualTo(64);
    service.registerSubscription("sub-match", matchVec, 0.90f);
    service.registerSubscription("sub-zero", zeroMatchQuery, 0.99f);

    RvvClientGate clientGate = new RvvClientGate();
    List<ContinuousVectorEvent> accepted = new ArrayList<>();
    AtomicInteger matchCallbacks = new AtomicInteger();
    AtomicInteger zeroCallbacks = new AtomicInteger();
    clientGate.addListener(ev -> {
      accepted.add(ev);
      if ("sub-match".equals(ev.getSubscriptionId())) {
        matchCallbacks.incrementAndGet();
      } else {
        zeroCallbacks.incrementAndGet();
      }
    });
    service.attachClientGate(clientGate);
    service.addEventSink(clientGate::tryAccept);

    int bucketId = 3;
    String memberId = "srv-idle";

    // Send 63 zero-match mutations (1..63 < H=64) and then go permanently idle
    for (long v = 1L; v <= 63L; v++) {
      service.onBucketMutation(bucketId, memberId, v, "idle-doc-" + v, nonMatchVec, true);
    }
    assertThat(matchCallbacks.get()).isZero();
    assertThat(zeroCallbacks.get()).isZero();
    assertThat(clientGate.getSafeCheckpointVersion(bucketId, memberId)).isZero();

    long baseNow = System.currentTimeMillis();
    // Before idle window expires -> no flush
    assertThat(service.flushIdleHeartbeats(baseNow, 60_000L)).isZero();
    assertThat(clientGate.getSafeCheckpointVersion(bucketId, memberId)).isZero();

    // After idle window expires -> emits trailing watermarkHeartbeat(63) & resets pending counter
    assertThat(service.flushIdleHeartbeats(baseNow + 65_000L, 60_000L)).isEqualTo(1);
    assertThat(clientGate.getHighWaterVersion("sub-zero", bucketId, memberId)).isEqualTo(63L);
    assertThat(clientGate.getSafeCheckpointVersion(bucketId, memberId)).isEqualTo(63L);
    assertThat(matchCallbacks.get()).isZero();
    assertThat(zeroCallbacks.get()).isZero();
    assertThat(service.getDispatcher().getTotalIdleHeartbeatsFlushed()).isEqualTo(1L);

    // Repeating immediately must not emit redundant heartbeat since eventsSinceHeartbeat reset to 0
    assertThat(service.flushIdleHeartbeats(baseNow + 120_000L, 60_000L)).isZero();

    // Verify background idle scheduler also flushes a subsequent sub-H tail (64..68 -> 68)
    service.startIdleHeartbeatScheduler(25L, 15L);
    try {
      for (long v = 64L; v <= 68L; v++) {
        service.onBucketMutation(bucketId, memberId, v, "idle-doc-" + v, nonMatchVec, true);
      }
      long deadline = System.currentTimeMillis() + 5_000L;
      while (clientGate.getSafeCheckpointVersion(bucketId, memberId) < 68L
          && System.currentTimeMillis() < deadline) {
        Thread.sleep(20L);
      }
      assertThat(clientGate.getSafeCheckpointVersion(bucketId, memberId)).isEqualTo(68L);
      assertThat(matchCallbacks.get()).isZero();
      assertThat(zeroCallbacks.get()).isZero();
    } finally {
      service.stopIdleHeartbeatScheduler();
    }
  }

  @Test
  public void idempotentRvvConsumerSurvivesBothClientCrashBoundariesWithZeroLossAndZeroDuplicates() {
    float[] matchVec = new float[] {1.0f, 0.0f, 0.0f, 0.0f};
    float[] zeroMatchQuery = new float[] {0.0f, 0.0f, 0.0f, 1.0f};

    ContinuousVectorQueryService server = new ContinuousVectorQueryService(4);
    server.setHeartbeatIntervalEvents(4);
    server.registerSubscription("sub-match", matchVec, 0.90f);
    server.registerSubscription("sub-zero", zeroMatchQuery, 0.99f);

    IdempotentRvvConsumer.InMemoryDurableDeliveryStore durableStore =
        new IdempotentRvvConsumer.InMemoryDurableDeliveryStore();
    List<IdempotentRvvConsumer.EventDeliveryId> appliedEffectLog = new ArrayList<>();
    Set<IdempotentRvvConsumer.EventDeliveryId> idempotentExternalSink = new HashSet<>();

    int bucketId = 6;
    String memberId = "srv-crash-test";

    class SimulatedClientCrashException extends RuntimeException {
      private static final long serialVersionUID = 1L;
    }

    // --- Process Instance 1: Crash Boundary A (after watermark/intent persist, BEFORE effect) ---
    RvvClientGate gate1 = new RvvClientGate();
    server.attachClientGate(gate1);
    IdempotentRvvConsumer consumer1 = new IdempotentRvvConsumer(gate1, durableStore,
        (ev, id) -> {
          if (idempotentExternalSink.add(id)) {
            appliedEffectLog.add(id);
          }
        },
        (phase, ev, id) -> {
          if (phase == IdempotentRvvConsumer.CrashPhase.AFTER_WATERMARK_PERSIST_BEFORE_EFFECT
              && ev.getRegionVersion() == 3L) {
            throw new SimulatedClientCrashException();
          }
        });
    server.addEventSink(consumer1);

    server.onBucketMutation(bucketId, memberId, 1L, "k1", matchVec, true);
    server.onBucketMutation(bucketId, memberId, 2L, "k2", matchVec, true);
    boolean crashedAtBoundaryA = false;
    try {
      server.onBucketMutation(bucketId, memberId, 3L, "k3", matchVec, true);
    } catch (SimulatedClientCrashException expected) {
      crashedAtBoundaryA = true;
    }
    assertThat(crashedAtBoundaryA).isTrue();
    // Pre-effect crash left receivedWatermark at 3, committedWatermark at 2, and v=3 in pending
    // WAL!
    assertThat(durableStore.getReceivedWatermark("sub-match", bucketId, memberId)).isEqualTo(3L);
    assertThat(durableStore.getCommittedWatermark("sub-match", bucketId, memberId)).isEqualTo(2L);
    assertThat(appliedEffectLog).hasSize(2);

    // --- Process Instance 2: Recover Boundary A & trigger Boundary B (after effect, BEFORE
    // committed watermark persist) ---
    server.removeEventSink(consumer1);
    server.detachClientGate(gate1);

    RvvClientGate gate2 = new RvvClientGate();
    server.attachClientGate(gate2);
    IdempotentRvvConsumer consumer2 = new IdempotentRvvConsumer(gate2, durableStore,
        (ev, id) -> {
          if (idempotentExternalSink.add(id)) {
            appliedEffectLog.add(id);
          }
        },
        (phase, ev, id) -> {
          if (phase == IdempotentRvvConsumer.CrashPhase.AFTER_EFFECT_BEFORE_WATERMARK_COMMIT
              && ev.getRegionVersion() == 4L) {
            throw new SimulatedClientCrashException();
          }
        });
    int recoveredA = consumer2.recover();
    assertThat(recoveredA).isEqualTo(1);
    assertThat(appliedEffectLog).extracting(IdempotentRvvConsumer.EventDeliveryId::getRegionVersion)
        .containsExactly(1L, 2L, 3L);
    assertThat(durableStore.getCommittedWatermark("sub-match", bucketId, memberId)).isEqualTo(3L);

    server.addEventSink(consumer2);
    boolean crashedAtBoundaryB = false;
    try {
      server.onBucketMutation(bucketId, memberId, 4L, "k4", matchVec, true);
    } catch (SimulatedClientCrashException expected) {
      crashedAtBoundaryB = true;
    }
    assertThat(crashedAtBoundaryB).isTrue();
    // Boundary B executed effect #4 & marked applied ledger, BEFORE committed watermark updated!
    assertThat(appliedEffectLog).extracting(IdempotentRvvConsumer.EventDeliveryId::getRegionVersion)
        .containsExactly(1L, 2L, 3L, 4L);
    assertThat(durableStore.getCommittedWatermark("sub-match", bucketId, memberId)).isEqualTo(3L);

    // --- Process Instance 3: Crash recovery + server reconnect replay + continued live writes ---
    server.removeEventSink(consumer2);
    server.detachClientGate(gate2);

    RvvClientGate gate3 = new RvvClientGate();
    server.attachClientGate(gate3);
    IdempotentRvvConsumer consumer3 = new IdempotentRvvConsumer(gate3, durableStore,
        (ev, id) -> {
          if (idempotentExternalSink.add(id)) {
            appliedEffectLog.add(id);
          }
        });

    // Test server reconnect replay from committedWatermark=3 (replays v=4) -> ledger deduplicates!
    server.addEventSink(consumer3);
    Map<String, Long> reconnectHw = consumer3.getCommittedHighWaterByMember(bucketId);
    // 0%-match sub had heartbeat at v=4 skipped during crash B -> committed safe HW is 0 -> replays
    // v=1..4 safely while applied ledger suppresses duplicates 1..4!
    server.replayUncheckpointedForReconnect(bucketId, reconnectHw);
    int recoveredB = consumer3.recover();
    assertThat(recoveredB).isZero();
    assertThat(consumer3.getDeduplicatedEffectsCount()).isGreaterThanOrEqualTo(4L);
    assertThat(appliedEffectLog).extracting(IdempotentRvvConsumer.EventDeliveryId::getRegionVersion)
        .containsExactly(1L, 2L, 3L, 4L);

    // Continue live mutations 5..8 -> zero losses, zero duplicates, committed watermarks advance
    for (long v = 5L; v <= 8L; v++) {
      server.onBucketMutation(bucketId, memberId, v, "k" + v, matchVec, true);
    }
    assertThat(appliedEffectLog).extracting(IdempotentRvvConsumer.EventDeliveryId::getRegionVersion)
        .containsExactly(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L);
    assertThat(appliedEffectLog).doesNotHaveDuplicates();
    assertThat(durableStore.getCommittedHighWaterByMember(bucketId,
        gate3.getActiveSubscriptionIds())).containsEntry(memberId, 8L);
    assertThat(gate3.getSafeCheckpointVersion(bucketId, memberId)).isEqualTo(8L);
  }

  @Test
  public void steadyTrickleZeroMatchStreamFiresAgeBoundedHeartbeatsWithoutStarvation()
      throws Exception {
    float[] matchVec = new float[] {1.0f, 0.0f, 0.0f, 0.0f};
    float[] nonMatchVec = new float[] {0.0f, 1.0f, 0.0f, 0.0f};
    float[] zeroMatchQuery = new float[] {0.0f, 0.0f, 0.0f, 1.0f};

    ContinuousVectorQueryService service = new ContinuousVectorQueryService(4);
    assertThat(service.getHeartbeatIntervalEvents()).isEqualTo(64);
    service.registerSubscription("sub-match", matchVec, 0.90f);
    service.registerSubscription("sub-zero", zeroMatchQuery, 0.99f);

    RvvClientGate clientGate = new RvvClientGate();
    AtomicInteger callbacks = new AtomicInteger();
    clientGate.addListener(ev -> callbacks.incrementAndGet());
    service.attachClientGate(clientGate);
    service.addEventSink(clientGate::tryAccept);

    int bucketId = 2;
    String memberId = "srv-trickle";

    // H=64, maxAge=100 ms, checkPeriod=20 ms while writing every 40 ms (< 100 ms): an inactivity
    // timer would reset on every write and starve for 64 * 40ms = 2.56 s; oldest-uncovered age
    // must fire periodically ~every 100-120 ms!
    service.startIdleHeartbeatScheduler(100L, 20L);
    try {
      for (long v = 1L; v <= 15L; v++) {
        service.onBucketMutation(bucketId, memberId, v, "trickle-" + v, nonMatchVec, true);
        Thread.sleep(40L);
      }
      long deadline = System.currentTimeMillis() + 3_000L;
      while (clientGate.getSafeCheckpointVersion(bucketId, memberId) < 15L
          && System.currentTimeMillis() < deadline) {
        Thread.sleep(20L);
      }
      assertThat(clientGate.getHeartbeatsAcceptedCount()).isGreaterThanOrEqualTo(3L);
      assertThat(clientGate.getHighWaterVersion("sub-zero", bucketId, memberId)).isEqualTo(15L);
      assertThat(clientGate.getSafeCheckpointVersion(bucketId, memberId)).isEqualTo(15L);
      assertThat(callbacks.get()).isZero();
    } finally {
      service.stopIdleHeartbeatScheduler();
    }
  }

  @Test
  public void clientCrashBoundaryA_AfterWatermarkPersistBeforeEffectAndHeartbeatPendingGuard() {
    float[] matchVec = new float[] {1.0f, 0.0f, 0.0f, 0.0f};
    float[] zeroMatchQuery = new float[] {0.0f, 0.0f, 0.0f, 1.0f};

    ContinuousVectorQueryService server = new ContinuousVectorQueryService(4);
    server.setHeartbeatIntervalEvents(4);
    server.registerSubscription("sub-match", matchVec, 0.90f);
    server.registerSubscription("sub-zero", zeroMatchQuery, 0.99f);

    IdempotentRvvConsumer.InMemoryDurableDeliveryStore durableStore =
        new IdempotentRvvConsumer.InMemoryDurableDeliveryStore();
    List<IdempotentRvvConsumer.EventDeliveryId> appliedEffectLog = new ArrayList<>();
    Set<IdempotentRvvConsumer.EventDeliveryId> idempotentExternalSink = new HashSet<>();

    int bucketId = 8;
    String memberId = "srv-crash-a";

    class CrashAException extends RuntimeException {
      private static final long serialVersionUID = 1L;
    }

    RvvClientGate gate1 = new RvvClientGate();
    server.attachClientGate(gate1);
    IdempotentRvvConsumer consumer1 = new IdempotentRvvConsumer(gate1, durableStore,
        (ev, id) -> {
          if (idempotentExternalSink.add(id)) {
            appliedEffectLog.add(id);
          }
        },
        (phase, ev, id) -> {
          if (phase == IdempotentRvvConsumer.CrashPhase.AFTER_WATERMARK_PERSIST_BEFORE_EFFECT
              && ev.getRegionVersion() == 3L) {
            throw new CrashAException();
          }
        });
    server.addEventSink(consumer1);

    server.onBucketMutation(bucketId, memberId, 1L, "k1", matchVec, true);
    server.onBucketMutation(bucketId, memberId, 2L, "k2", matchVec, true);
    boolean crashed = false;
    try {
      server.onBucketMutation(bucketId, memberId, 3L, "k3", matchVec, true);
    } catch (CrashAException expected) {
      crashed = true;
    }
    assertThat(crashed).isTrue();
    assertThat(durableStore.getReceivedWatermark("sub-match", bucketId, memberId)).isEqualTo(3L);
    assertThat(durableStore.getCommittedWatermark("sub-match", bucketId, memberId)).isEqualTo(2L);
    assertThat(durableStore.getLowestPendingVersion("sub-match", bucketId, memberId)).isEqualTo(3L);

    // A heartbeat at v=4 arriving while v=3 is STILL PENDING must NEVER advance committedWatermark
    // past 2 for sub-match (while sub-zero with no pending intents safely advances to 4)!
    consumer1.processEvent(
        ContinuousVectorEvent.watermarkHeartbeat(bucketId, memberId, 4L,
            System.currentTimeMillis()));
    assertThat(durableStore.getCommittedWatermark("sub-match", bucketId, memberId)).isEqualTo(2L);
    assertThat(durableStore.getCommittedWatermark("sub-zero", bucketId, memberId)).isEqualTo(4L);

    // Recover process instance 2 -> drains pending v=3 cleanly (0 lost, 0 duplicates)
    server.removeEventSink(consumer1);
    server.detachClientGate(gate1);
    RvvClientGate gate2 = new RvvClientGate();
    server.attachClientGate(gate2);
    IdempotentRvvConsumer consumer2 = new IdempotentRvvConsumer(gate2, durableStore, (ev, id) -> {
      if (idempotentExternalSink.add(id)) {
        appliedEffectLog.add(id);
      }
    });
    assertThat(consumer2.recover()).isEqualTo(1);
    assertThat(appliedEffectLog).extracting(IdempotentRvvConsumer.EventDeliveryId::getRegionVersion)
        .containsExactly(1L, 2L, 3L);
    assertThat(durableStore.getCommittedWatermark("sub-match", bucketId, memberId)).isEqualTo(3L);
    assertThat(durableStore.getLowestPendingVersion("sub-match", bucketId, memberId))
        .isEqualTo(Long.MAX_VALUE);
  }

  @Test
  public void clientCrashBoundaryB_AfterEffectBeforeWatermarkCommitDeduplicatesOnReconnect() {
    float[] matchVec = new float[] {1.0f, 0.0f, 0.0f, 0.0f};
    float[] zeroMatchQuery = new float[] {0.0f, 0.0f, 0.0f, 1.0f};

    ContinuousVectorQueryService server = new ContinuousVectorQueryService(4);
    server.setHeartbeatIntervalEvents(4);
    server.registerSubscription("sub-match", matchVec, 0.90f);
    server.registerSubscription("sub-zero", zeroMatchQuery, 0.99f);

    IdempotentRvvConsumer.InMemoryDurableDeliveryStore durableStore =
        new IdempotentRvvConsumer.InMemoryDurableDeliveryStore();
    List<IdempotentRvvConsumer.EventDeliveryId> appliedEffectLog = new ArrayList<>();
    Set<IdempotentRvvConsumer.EventDeliveryId> idempotentExternalSink = new HashSet<>();

    int bucketId = 9;
    String memberId = "srv-crash-b";

    class CrashBException extends RuntimeException {
      private static final long serialVersionUID = 1L;
    }

    RvvClientGate gate1 = new RvvClientGate();
    server.attachClientGate(gate1);
    IdempotentRvvConsumer consumer1 = new IdempotentRvvConsumer(gate1, durableStore,
        (ev, id) -> {
          if (idempotentExternalSink.add(id)) {
            appliedEffectLog.add(id);
          }
        },
        (phase, ev, id) -> {
          if (phase == IdempotentRvvConsumer.CrashPhase.AFTER_EFFECT_BEFORE_WATERMARK_COMMIT
              && ev.getRegionVersion() == 4L) {
            throw new CrashBException();
          }
        });
    server.addEventSink(consumer1);

    for (long v = 1L; v <= 3L; v++) {
      server.onBucketMutation(bucketId, memberId, v, "k" + v, matchVec, true);
    }
    boolean crashed = false;
    try {
      server.onBucketMutation(bucketId, memberId, 4L, "k4", matchVec, true);
    } catch (CrashBException expected) {
      crashed = true;
    }
    assertThat(crashed).isTrue();
    // Effect v=4 ran and was durably recorded in applied ledger, while committedWatermark lags at 3
    assertThat(appliedEffectLog).extracting(IdempotentRvvConsumer.EventDeliveryId::getRegionVersion)
        .containsExactly(1L, 2L, 3L, 4L);
    assertThat(durableStore.getCommittedWatermark("sub-match", bucketId, memberId)).isEqualTo(3L);

    // Restart client -> reconnect replay re-sends v=4 -> applied ledger suppresses duplicate!
    server.removeEventSink(consumer1);
    server.detachClientGate(gate1);
    RvvClientGate gate2 = new RvvClientGate();
    server.attachClientGate(gate2);
    IdempotentRvvConsumer consumer2 = new IdempotentRvvConsumer(gate2, durableStore, (ev, id) -> {
      if (idempotentExternalSink.add(id)) {
        appliedEffectLog.add(id);
      }
    });
    server.addEventSink(consumer2);

    Map<String, Long> reconnectHw = consumer2.getCommittedHighWaterByMember(bucketId);
    server.replayUncheckpointedForReconnect(bucketId, reconnectHw);
    assertThat(consumer2.recover()).isZero();
    assertThat(consumer2.getDeduplicatedEffectsCount()).isGreaterThanOrEqualTo(4L);
    assertThat(appliedEffectLog).extracting(IdempotentRvvConsumer.EventDeliveryId::getRegionVersion)
        .containsExactly(1L, 2L, 3L, 4L);

    for (long v = 5L; v <= 8L; v++) {
      server.onBucketMutation(bucketId, memberId, v, "k" + v, matchVec, true);
    }
    assertThat(appliedEffectLog).extracting(IdempotentRvvConsumer.EventDeliveryId::getRegionVersion)
        .containsExactly(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L);
    assertThat(gate2.getSafeCheckpointVersion(bucketId, memberId)).isEqualTo(8L);
  }

  @Test
  public void delayedPredecessorExceedingMaxHoldbackAgeClosesGapEmitsResyncNotificationAndUnblocksZeroMatchCheckpoints() {
    float[] matchVec = new float[] {1.0f, 0.0f, 0.0f, 0.0f};
    float[] nonMatchVec = new float[] {0.0f, 1.0f, 0.0f, 0.0f};
    float[] zeroMatchQuery = new float[] {0.0f, 0.0f, 0.0f, 1.0f};

    ContinuousVectorQueryService service = new ContinuousVectorQueryService(4);
    service.setHeartbeatIntervalEvents(64);
    service.setMaxHoldbackAgeMillis(100L);
    service.registerSubscription("sub-match", matchVec, 0.90f);
    service.registerSubscription("sub-zero", zeroMatchQuery, 0.99f);

    RvvClientGate clientGate = new RvvClientGate();
    List<ContinuousVectorEvent> accepted = new ArrayList<>();
    clientGate.addListener(accepted::add);
    service.attachClientGate(clientGate);
    service.addEventSink(clientGate::tryAccept);

    List<String> gapNotifications = new ArrayList<>();
    service.addHoldbackGapListener(
        (bId, mId, expectedVer, resumedVer, reason) -> gapNotifications
            .add(bId + "|" + mId + "|" + expectedVer + "->" + resumedVer + "|" + reason));

    int bucketId = 10;
    String memberId = "srv-holdback-age";

    // Contiguous prefix 1..2 (non-matching)
    service.onBucketMutation(bucketId, memberId, 1L, "k1", nonMatchVec, true);
    service.onBucketMutation(bucketId, memberId, 2L, "k2", nonMatchVec, true);
    long t0 = System.currentTimeMillis();

    // Out-of-order arrivals v=4 (match) and v=5 (non-match) leaving predecessor v=3 missing
    assertThat(service.onBucketMutation(bucketId, memberId, 4L, "k4", matchVec, true)).isEmpty();
    service.onBucketMutation(bucketId, memberId, 5L, "k5", nonMatchVec, true);
    assertThat(service.getDispatcher().getHoldbackSize(bucketId)).isEqualTo(2);
    assertThat(accepted).isEmpty();
    assertThat(service.isResyncRequired(bucketId, memberId)).isFalse();

    // Before maxHoldbackAgeMillis=100ms expires, holdback must remain intact (no premature skip)
    service.flushIdleHeartbeats(t0 + 40L, 20L);
    assertThat(service.getDispatcher().getHoldbackSize(bucketId)).isEqualTo(2);
    assertThat(service.getHoldbackGapClosedCount()).isZero();
    assertThat(accepted).isEmpty();

    // Once age reaches maxHoldbackAgeMillis=100ms, holdback gap 3 closes deterministically,
    // emits resync notification, dispatches v=4, and emits trailing heartbeat for v=5!
    int flushedHb = service.flushIdleHeartbeats(t0 + 150L, 100L);
    assertThat(flushedHb).isGreaterThanOrEqualTo(1);
    assertThat(service.getDispatcher().getHoldbackSize(bucketId)).isZero();
    assertThat(service.getHoldbackGapClosedCount()).isEqualTo(1L);
    assertThat(service.isResyncRequired(bucketId, memberId)).isTrue();
    assertThat(gapNotifications)
        .containsExactly("10|srv-holdback-age|3->4|MAX_HOLDBACK_AGE_EXCEEDED");
    assertThat(accepted).extracting(ContinuousVectorEvent::getRegionVersion).containsExactly(4L);
    assertThat(clientGate.getSafeCheckpointVersion(bucketId, memberId)).isEqualTo(5L);

    // Late aborted predecessor v=3 arriving after closure is safely ignored without regressing
    assertThat(service.onBucketMutation(bucketId, memberId, 3L, "k3-late", matchVec, true))
        .isEmpty();
    assertThat(accepted).extracting(ContinuousVectorEvent::getRegionVersion).containsExactly(4L);
    service.getDispatcher().clearResyncRequired(bucketId, memberId);
    assertThat(service.isResyncRequired(bucketId, memberId)).isFalse();
  }

  @Test
  public void outOfOrderMatchCommitCannotAdvanceCommittedWatermarkPastEarlierPendingIntent() {
    float[] matchVec = new float[] {1.0f, 0.0f, 0.0f, 0.0f};
    float[] zeroMatchQuery = new float[] {0.0f, 0.0f, 0.0f, 1.0f};

    ContinuousVectorQueryService server = new ContinuousVectorQueryService(4);
    server.setHeartbeatIntervalEvents(8);
    server.registerSubscription("sub-match", matchVec, 0.90f);
    server.registerSubscription("sub-zero", zeroMatchQuery, 0.99f);

    IdempotentRvvConsumer.InMemoryDurableDeliveryStore durableStore =
        new IdempotentRvvConsumer.InMemoryDurableDeliveryStore();
    List<IdempotentRvvConsumer.EventDeliveryId> appliedEffectLog = new ArrayList<>();
    Set<IdempotentRvvConsumer.EventDeliveryId> idempotentExternalSink = new HashSet<>();

    int bucketId = 11;
    String memberId = "srv-ooo-commit";

    class PreEffectCrashException extends RuntimeException {
      private static final long serialVersionUID = 1L;
    }

    RvvClientGate gate1 = new RvvClientGate();
    server.attachClientGate(gate1);
    IdempotentRvvConsumer consumer1 = new IdempotentRvvConsumer(gate1, durableStore,
        (ev, id) -> {
          if (idempotentExternalSink.add(id)) {
            appliedEffectLog.add(id);
          }
        },
        (phase, ev, id) -> {
          if (phase == IdempotentRvvConsumer.CrashPhase.AFTER_WATERMARK_PERSIST_BEFORE_EFFECT
              && ev.getRegionVersion() == 3L) {
            throw new PreEffectCrashException();
          }
        });
    server.addEventSink(consumer1);

    server.onBucketMutation(bucketId, memberId, 1L, "k1", matchVec, true);
    server.onBucketMutation(bucketId, memberId, 2L, "k2", matchVec, true);
    boolean crashed = false;
    try {
      server.onBucketMutation(bucketId, memberId, 3L, "k3", matchVec, true);
    } catch (PreEffectCrashException expected) {
      crashed = true;
    }
    assertThat(crashed).isTrue();
    assertThat(durableStore.getLowestPendingVersion("sub-match", bucketId, memberId)).isEqualTo(3L);
    assertThat(durableStore.getCommittedWatermark("sub-match", bucketId, memberId)).isEqualTo(2L);

    // Before pending v=3 recovers, out-of-order matches v=4 and v=5 execute and complete.
    // Neither match commit nor gate high-water may advance committedWatermark past pending v=3!
    server.removeEventSink(consumer1);
    server.detachClientGate(gate1);
    RvvClientGate gate2 = new RvvClientGate();
    server.attachClientGate(gate2);
    IdempotentRvvConsumer consumer2 = new IdempotentRvvConsumer(gate2, durableStore, (ev, id) -> {
      if (idempotentExternalSink.add(id)) {
        appliedEffectLog.add(id);
      }
    });
    gate2.restoreHighWaterVersion("sub-match", bucketId, memberId, 2L);

    long now = System.currentTimeMillis();
    consumer2.processEvent(new ContinuousVectorEvent("sub-match", "k4", 1.0f, bucketId, memberId,
        4L, 1, now, false));
    consumer2.processEvent(new ContinuousVectorEvent("sub-match", "k5", 1.0f, bucketId, memberId,
        5L, 1, now, false));

    assertThat(appliedEffectLog).extracting(IdempotentRvvConsumer.EventDeliveryId::getRegionVersion)
        .containsExactly(1L, 2L, 4L, 5L);
    assertThat(durableStore.getLowestPendingVersion("sub-match", bucketId, memberId)).isEqualTo(3L);
    assertThat(durableStore.getCommittedWatermark("sub-match", bucketId, memberId)).isEqualTo(2L);
    assertThat(gate2.getHighWaterVersion("sub-match", bucketId, memberId)).isEqualTo(2L);
    assertThat(durableStore.getCommittedHighWaterByMember(bucketId,
        Collections.singletonList("sub-match"))).containsEntry(memberId, 2L);

    // Recover pending v=3 -> drains v=3 once and walks contiguous applied ledger 3->4->5 cleanly
    assertThat(consumer2.recover()).isEqualTo(1);
    assertThat(appliedEffectLog).extracting(IdempotentRvvConsumer.EventDeliveryId::getRegionVersion)
        .containsExactly(1L, 2L, 4L, 5L, 3L);
    assertThat(durableStore.getLowestPendingVersion("sub-match", bucketId, memberId))
        .isEqualTo(Long.MAX_VALUE);
    assertThat(durableStore.getCommittedWatermark("sub-match", bucketId, memberId)).isEqualTo(5L);
    assertThat(gate2.getHighWaterVersion("sub-match", bucketId, memberId)).isEqualTo(5L);

    // Duplicate replay from server for v=3..5 executes zero extra effects
    server.addEventSink(consumer2);
    server.replayUncheckpointedForReconnect(bucketId,
        consumer2.getCommittedHighWaterByMember(bucketId));
    assertThat(appliedEffectLog).hasSize(5).doesNotHaveDuplicates();
  }

  @Test
  public void delayedPendingIntentBlocksLaterMatchAndHeartbeatThroughRestartRecovery()
      throws Exception {
    float[] matchVec = new float[] {1.0f, 0.0f, 0.0f, 0.0f};
    float[] zeroMatchQuery = new float[] {0.0f, 0.0f, 0.0f, 1.0f};

    ContinuousVectorQueryService server = new ContinuousVectorQueryService(4);
    server.setHeartbeatIntervalEvents(8);
    server.registerSubscription("sub-match", matchVec, 0.90f);
    server.registerSubscription("sub-zero", zeroMatchQuery, 0.99f);

    IdempotentRvvConsumer.InMemoryDurableDeliveryStore durableStore =
        new IdempotentRvvConsumer.InMemoryDurableDeliveryStore();
    List<IdempotentRvvConsumer.EventDeliveryId> appliedEffectLog = new CopyOnWriteArrayList<>();
    Set<IdempotentRvvConsumer.EventDeliveryId> idempotentExternalSink =
        java.util.concurrent.ConcurrentHashMap.newKeySet();

    int bucketId = 12;
    String memberId = "srv-delayed-pending";

    CountDownLatch v2EnteredEffect = new CountDownLatch(1);
    CountDownLatch allowV2Abort = new CountDownLatch(1);
    class DelayedAbortException extends RuntimeException {
      private static final long serialVersionUID = 1L;
    }

    RvvClientGate gate1 = new RvvClientGate();
    server.attachClientGate(gate1);
    IdempotentRvvConsumer consumer1 = new IdempotentRvvConsumer(gate1, durableStore, (ev, id) -> {
      if (ev.getRegionVersion() == 2L && id.getMemberId().equals(memberId)) {
        v2EnteredEffect.countDown();
        try {
          allowV2Abort.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        throw new DelayedAbortException();
      }
      if (idempotentExternalSink.add(id)) {
        appliedEffectLog.add(id);
      }
    });

    long now = System.currentTimeMillis();
    consumer1.processEvent(new ContinuousVectorEvent("sub-match", "k1", 1.0f, bucketId, memberId,
        1L, 1, now, false));
    consumer1.processEvent(ContinuousVectorEvent.watermarkHeartbeat(bucketId, memberId, 1L, now));
    assertThat(durableStore.getCommittedWatermark("sub-match", bucketId, memberId)).isEqualTo(1L);
    assertThat(durableStore.getCommittedWatermark("sub-zero", bucketId, memberId)).isEqualTo(1L);

    // Concurrent thread receives earlier match v1=2, stages it in PENDING, and delays inside effect
    AtomicInteger delayedThreadAbortCount = new AtomicInteger();
    Thread delayedThread = new Thread(() -> {
      try {
        consumer1.processEvent(new ContinuousVectorEvent("sub-match", "k2", 1.0f, bucketId,
            memberId, 2L, 1, now + 1L, false));
      } catch (DelayedAbortException expected) {
        delayedThreadAbortCount.incrementAndGet();
      }
    }, "delayed-intent-v2");
    delayedThread.start();
    assertThat(v2EnteredEffect.await(10, TimeUnit.SECONDS)).isTrue();
    assertThat(durableStore.getLowestPendingVersion("sub-match", bucketId, memberId)).isEqualTo(2L);

    // While earlier intent v1=2 is delayed in PENDING inside its per-stream execution fence:
    // (a) staging v2=3 succeeds without blocking and heartbeat v=4 leaves sub-match at 1 (while
    // unblocked sub-zero advances to 4), and (b) executing v2=3 is strictly fenced behind in-flight
    // v1=2 until v1=2 exits the execution fence!
    consumer1.stagePendingIntent(new ContinuousVectorEvent("sub-match", "k3", 1.0f, bucketId,
        memberId, 3L, 1, now + 2L, false));
    consumer1.processEvent(
        ContinuousVectorEvent.watermarkHeartbeat(bucketId, memberId, 4L, now + 3L));
    assertThat(durableStore.getCommittedWatermark("sub-match", bucketId, memberId)).isEqualTo(1L);
    assertThat(gate1.getHighWaterVersion("sub-match", bucketId, memberId)).isEqualTo(1L);
    assertThat(durableStore.getCommittedWatermark("sub-zero", bucketId, memberId)).isEqualTo(4L);

    CountDownLatch v3Executed = new CountDownLatch(1);
    Thread laterMatchThread = new Thread(() -> {
      consumer1.processEvent(new ContinuousVectorEvent("sub-match", "k3", 1.0f, bucketId, memberId,
          3L, 1, now + 2L, false));
      v3Executed.countDown();
    }, "later-match-v3");
    laterMatchThread.start();
    // While v1=2 remains in flight, v2=3 cannot execute out-of-order ahead of it
    assertThat(v3Executed.await(100L, TimeUnit.MILLISECONDS)).isFalse();
    assertThat(appliedEffectLog).extracting(IdempotentRvvConsumer.EventDeliveryId::getRegionVersion)
        .containsExactly(1L);

    // Abort/crash instance 1 while v1=2 stays PENDING -> v2=3 finishes after fence release yet
    // STILL cannot advance committedWatermark or gate past 1 because v1=2 remains PENDING!
    allowV2Abort.countDown();
    delayedThread.join(10_000L);
    assertThat(v3Executed.await(10, TimeUnit.SECONDS)).isTrue();
    laterMatchThread.join(10_000L);
    assertThat(delayedThreadAbortCount.get()).isEqualTo(1);
    assertThat(appliedEffectLog).extracting(IdempotentRvvConsumer.EventDeliveryId::getRegionVersion)
        .containsExactly(1L, 3L);
    assertThat(durableStore.getCommittedWatermark("sub-match", bucketId, memberId)).isEqualTo(1L);
    assertThat(gate1.getHighWaterVersion("sub-match", bucketId, memberId)).isEqualTo(1L);
    assertThat(consumer1.getCommittedHighWaterByMember(bucketId)).containsEntry(memberId, 1L);

    server.detachClientGate(gate1);
    RvvClientGate gate2 = new RvvClientGate();
    server.attachClientGate(gate2);
    IdempotentRvvConsumer consumer2 = new IdempotentRvvConsumer(gate2, durableStore, (ev, id) -> {
      if (idempotentExternalSink.add(id)) {
        appliedEffectLog.add(id);
      }
    });

    assertThat(consumer2.recover()).isEqualTo(1);
    assertThat(appliedEffectLog).extracting(IdempotentRvvConsumer.EventDeliveryId::getRegionVersion)
        .containsExactlyInAnyOrder(1L, 2L, 3L)
        .hasSize(3);
    assertThat(durableStore.getLowestPendingVersion("sub-match", bucketId, memberId))
        .isEqualTo(Long.MAX_VALUE);
    assertThat(durableStore.getCommittedWatermark("sub-match", bucketId, memberId)).isEqualTo(3L);
    assertThat(gate2.getHighWaterVersion("sub-match", bucketId, memberId)).isEqualTo(3L);
    assertThat(consumer2.processEvent(
        ContinuousVectorEvent.watermarkHeartbeat(bucketId, memberId, 4L, now + 4L))).isTrue();
    assertThat(gate2.getSafeCheckpointVersion(bucketId, memberId)).isEqualTo(4L);
  }

  @Test
  public void temporaryPredecessorGapRepairedBeforeExpiryPreservesContiguousDeliveryWithoutResync() {
    float[] matchVec = new float[] {1.0f, 0.0f, 0.0f, 0.0f};
    float[] nonMatchVec = new float[] {0.0f, 1.0f, 0.0f, 0.0f};
    float[] zeroMatchQuery = new float[] {0.0f, 0.0f, 0.0f, 1.0f};

    ContinuousVectorQueryService service = new ContinuousVectorQueryService(4);
    service.setHeartbeatIntervalEvents(64);
    service.setMaxHoldbackAgeMillis(200L);
    service.registerSubscription("sub-match", matchVec, 0.90f);
    service.registerSubscription("sub-zero", zeroMatchQuery, 0.99f);

    RvvClientGate clientGate = new RvvClientGate();
    List<ContinuousVectorEvent> accepted = new ArrayList<>();
    clientGate.addListener(accepted::add);
    service.attachClientGate(clientGate);
    service.addEventSink(clientGate::tryAccept);

    int bucketId = 13;
    String memberId = "srv-repaired-before-expiry";

    service.onBucketMutation(bucketId, memberId, 1L, "k1", nonMatchVec, true);
    service.onBucketMutation(bucketId, memberId, 2L, "k2", nonMatchVec, true);
    long t0 = System.currentTimeMillis();

    // Hold v=4 (match) and v=5 (non-match) behind missing predecessor v=3
    assertThat(service.onBucketMutation(bucketId, memberId, 4L, "k4", matchVec, true)).isEmpty();
    service.onBucketMutation(bucketId, memberId, 5L, "k5", nonMatchVec, true);
    assertThat(service.getDispatcher().getHoldbackSize(bucketId)).isEqualTo(2);

    // Flush BEFORE expiry (t0 + 100ms < 200ms) -> gap remains intact, zero resync triggered
    service.flushIdleHeartbeats(t0 + 100L, 40L);
    assertThat(service.isResyncRequired(bucketId, memberId)).isFalse();
    assertThat(service.getHoldbackGapClosedCount()).isZero();
    assertThat(accepted).isEmpty();

    // Delayed predecessor v=3 arrives BEFORE expiry -> drains 3 -> 4 contiguously with no resync
    service.onBucketMutation(bucketId, memberId, 3L, "k3", matchVec, true);
    assertThat(service.getDispatcher().getHoldbackSize(bucketId)).isZero();
    assertThat(service.isResyncRequired(bucketId, memberId)).isFalse();
    assertThat(service.getHoldbackGapClosedCount()).isZero();
    assertThat(accepted).extracting(ContinuousVectorEvent::getRegionVersion).containsExactly(3L,
        4L);
    assertThat(RvvClientGate.verifyStrictMonotonicity(accepted)).isEmpty();

    // Subsequent idle flush advances 0%-match checkpoint smoothly to v=5 without resync
    service.flushIdleHeartbeats(t0 + 300L, 50L);
    assertThat(service.isResyncRequired(bucketId, memberId)).isFalse();
    assertThat(clientGate.getSafeCheckpointVersion(bucketId, memberId)).isEqualTo(5L);
  }

  @Test
  public void connectionLossAfterDurablePendingStagingBeforeEffectRetires() {
    float[] matchVec = new float[] {1.0f, 0.0f, 0.0f, 0.0f};
    float[] zeroMatchQuery = new float[] {0.0f, 0.0f, 0.0f, 1.0f};

    ContinuousVectorQueryService server = new ContinuousVectorQueryService(4);
    server.setHeartbeatIntervalEvents(8);
    server.registerSubscription("sub-match", matchVec, 0.90f);
    server.registerSubscription("sub-zero", zeroMatchQuery, 0.99f);

    IdempotentRvvConsumer.InMemoryDurableDeliveryStore durableStore =
        new IdempotentRvvConsumer.InMemoryDurableDeliveryStore();
    List<IdempotentRvvConsumer.EventDeliveryId> appliedEffectLog = new CopyOnWriteArrayList<>();
    Set<IdempotentRvvConsumer.EventDeliveryId> idempotentExternalSink =
        java.util.concurrent.ConcurrentHashMap.newKeySet();
    List<ContinuousVectorEvent> gateAccepted = new CopyOnWriteArrayList<>();

    int bucketId = 14;
    String memberId = "srv-conn-loss";

    class PreStageDisconnectException extends RuntimeException {
      private static final long serialVersionUID = 1L;
    }
    class PostStageConnectionLossException extends RuntimeException {
      private static final long serialVersionUID = 1L;
    }

    RvvClientGate gate = new RvvClientGate();
    gate.addListener(gateAccepted::add);
    server.attachClientGate(gate);

    // Phase selector: 1 = fail BEFORE durable PENDING stage; 2 = fail AFTER PENDING stage before
    // effect; 0 = healthy delivery on ordinary reconnect using the same live consumer instance.
    AtomicInteger faultPhase = new AtomicInteger(1);
    IdempotentRvvConsumer consumer = new IdempotentRvvConsumer(gate, durableStore,
        (ev, id) -> {
          if (idempotentExternalSink.add(id)) {
            appliedEffectLog.add(id);
          }
        },
        (phase, ev, id) -> {
          int mode = faultPhase.get();
          if (mode == 1
              && phase == IdempotentRvvConsumer.CrashPhase.BEFORE_DURABLE_PENDING_STAGE
              && ev.getRegionVersion() == 4L) {
            throw new PreStageDisconnectException();
          }
          if (mode == 2
              && phase == IdempotentRvvConsumer.CrashPhase.AFTER_WATERMARK_PERSIST_BEFORE_EFFECT
              && ev.getRegionVersion() == 4L) {
            throw new PostStageConnectionLossException();
          }
        });
    server.addEventSink(consumer);

    // 1. Gate-to-staging boundary test: abort strictly BEFORE durable PENDING WAL commit
    for (long v = 1L; v <= 3L; v++) {
      server.onBucketMutation(bucketId, memberId, v, "k" + v, matchVec, true);
    }
    boolean abortedBeforeStage = false;
    try {
      server.onBucketMutation(bucketId, memberId, 4L, "k4", matchVec, true);
    } catch (PreStageDisconnectException expected) {
      abortedBeforeStage = true;
    }
    assertThat(abortedBeforeStage).isTrue();
    IdempotentRvvConsumer.EventDeliveryId id4 =
        new IdempotentRvvConsumer.EventDeliveryId("sub-match", bucketId, memberId, 4L);
    assertThat(durableStore.isPendingIntent(id4)).isFalse();
    assertThat(consumer.getDurableAdmissionsCount()).isEqualTo(3L);
    assertThat(durableStore.getCommittedWatermark("sub-match", bucketId, memberId)).isEqualTo(3L);
    assertThat(gate.getHighWaterVersion("sub-match", bucketId, memberId)).isEqualTo(3L);
    assertThat(gateAccepted).hasSize(3);

    // 2. Reconnect delivers k4 -> durably stages PENDING WAL (durableAdmissionsCount=4), then loses
    // connection BEFORE effect retires and BEFORE V_client advances!
    faultPhase.set(2);
    boolean droppedAfterStage = false;
    try {
      server.replayUncheckpointedForReconnect(bucketId,
          consumer.getCommittedHighWaterByMember(bucketId));
    } catch (PostStageConnectionLossException expected) {
      droppedAfterStage = true;
    }
    assertThat(droppedAfterStage).isTrue();
    assertThat(durableStore.isPendingIntent(id4)).isTrue();
    assertThat(consumer.getDurableAdmissionsCount()).isEqualTo(4L);
    assertThat(durableStore.getCommittedWatermark("sub-match", bucketId, memberId)).isEqualTo(3L);
    assertThat(gate.getHighWaterVersion("sub-match", bucketId, memberId)).isEqualTo(3L);
    assertThat(appliedEffectLog).hasSize(3);

    // Heartbeat v=4 during disconnect advances unblocked sub-zero to 4 while PENDING v=4 holds
    // sub-match at 3; once reconnect retires PENDING v=4, V_client becomes 4!
    consumer.processEvent(
        ContinuousVectorEvent.watermarkHeartbeat(bucketId, memberId, 4L,
            System.currentTimeMillis()));
    assertThat(durableStore.getCommittedWatermark("sub-zero", bucketId, memberId)).isEqualTo(4L);
    assertThat(durableStore.getCommittedWatermark("sub-match", bucketId, memberId)).isEqualTo(3L);

    // Simulate network disconnect while server ingests k5
    server.removeEventSink(consumer);
    server.onBucketMutation(bucketId, memberId, 5L, "k5", matchVec, true);

    // 3. Ordinary reconnect WITHOUT process restart on the SAME consumer instance: retires durable
    // PENDING v=4 BEFORE replayUncheckpointedForReconnect so v=4 is never delivered a second time!
    faultPhase.set(0);
    server.addEventSink(consumer);
    List<ContinuousVectorEvent> replayed = consumer.reconnectAndReplay(server, bucketId);

    // Server replay only emitted k5 (>4), never re-sending already-staged k4!
    assertThat(replayed).extracting(ContinuousVectorEvent::getRegionVersion).containsExactly(5L);
    assertThat(durableStore.isPendingIntent(id4)).isFalse();
    assertThat(appliedEffectLog).extracting(IdempotentRvvConsumer.EventDeliveryId::getRegionVersion)
        .containsExactly(1L, 2L, 3L, 4L, 5L)
        .doesNotHaveDuplicates();
    assertThat(consumer.getDurableAdmissionsCount()).isEqualTo(5L);
    assertThat(durableStore.getCommittedWatermark("sub-match", bucketId, memberId)).isEqualTo(5L);
    assertThat(gate.getSafeCheckpointVersion(bucketId, memberId)).isEqualTo(5L);
  }

  @Test
  public void staggeredAcksFromMultipleHealthySubscribersAdvanceCheckpointToMinimumOnIdleTail() {
    float[] matchVec = new float[] {1.0f, 0.0f, 0.0f, 0.0f};
    ContinuousVectorQueryService service = new ContinuousVectorQueryService(4);
    service.registerSubscription("sub-stagger", matchVec, 0.90f);
    service.registerSubscriberForCheckpoint("sub-A");
    service.registerSubscriberForCheckpoint("sub-B");
    service.registerSubscriberForCheckpoint("sub-C");

    int bucketId = 15;
    String memberId = "srv-stagger-acks";
    for (long v = 1L; v <= 10L; v++) {
      service.onBucketMutation(bucketId, memberId, v, "doc-" + v, matchVec, false);
    }
    assertThat(service.getDispatcher().getTailSize(bucketId)).isEqualTo(10);
    assertThat(service.getCheckpointVersion(bucketId, memberId)).isZero();

    // Idle tail (no further mutations): staggered ACKs advance V_ckpt unconditionally to min_i
    assertThat(service.acknowledgeSubscriberCheckpoint("sub-A", bucketId, memberId, 10L))
        .isZero();
    assertThat(service.acknowledgeSubscriberCheckpoint("sub-B", bucketId, memberId, 4L)).isZero();
    assertThat(service.acknowledgeSubscriberCheckpoint("sub-C", bucketId, memberId, 6L))
        .isEqualTo(4L);
    assertThat(service.getDispatcher().getTailSize(bucketId)).isEqualTo(6);

    // Reordered stale ACK from sub-A must not regress ACK or V_ckpt
    assertThat(service.acknowledgeSubscriberCheckpoint("sub-A", bucketId, memberId, 2L))
        .isEqualTo(4L);
    assertThat(service.getSubscriberCheckpointAck("sub-A", bucketId, memberId)).isEqualTo(10L);

    // Catching up lagging subscribers advances V_ckpt immediately to new minimum (4 -> 6 -> 8 ->
    // 10)
    assertThat(service.acknowledgeSubscriberCheckpoint("sub-B", bucketId, memberId, 10L))
        .isEqualTo(6L);
    assertThat(service.acknowledgeSubscriberCheckpoint("sub-C", bucketId, memberId, 8L))
        .isEqualTo(8L);
    assertThat(service.acknowledgeSubscriberCheckpoint("sub-C", bucketId, memberId, 10L))
        .isEqualTo(10L);
    assertThat(service.getDispatcher().getTailSize(bucketId)).isZero();

    // Unregistering a lagging subscriber on an idle tail immediately unblocks V_ckpt to remaining
    // min
    for (long v = 11L; v <= 15L; v++) {
      service.onBucketMutation(bucketId, memberId, v, "doc-" + v, matchVec, false);
    }
    assertThat(service.acknowledgeSubscriberCheckpoint("sub-A", bucketId, memberId, 15L))
        .isEqualTo(10L);
    assertThat(service.acknowledgeSubscriberCheckpoint("sub-B", bucketId, memberId, 15L))
        .isEqualTo(10L);
    assertThat(service.acknowledgeSubscriberCheckpoint("sub-C", bucketId, memberId, 12L))
        .isEqualTo(12L);
    assertThat(service.unregisterSubscriberForCheckpoint("sub-C")).isTrue();
    assertThat(service.getCheckpointVersion(bucketId, memberId)).isEqualTo(15L);
    assertThat(service.getDispatcher().getTailSize(bucketId)).isZero();
  }

  @Test
  public void emptySubscriptionIntervalMaintainsVersionContinuityAndAvoidsTailOverflow() {
    float[] vec = new float[] {1.0f, 0.0f, 0.0f, 0.0f};
    ChordalVoronoiRouter router = new ChordalVoronoiRouter(4);
    RvvMonotonicVectorDispatcher dispatcher = new RvvMonotonicVectorDispatcher(router, 4);
    ContinuousVectorQueryService service = new ContinuousVectorQueryService(router, dispatcher);

    List<ContinuousVectorEvent> dispatched = new ArrayList<>();
    service.addEventSink(dispatched::add);

    int bucketId = 16;
    String memberId = "srv-empty-s";

    // Phase 1: Initial |S| = 0 with out-of-order arrival v=4 before v=3 -> contiguous sequencing!
    service.onBucketMutation(bucketId, memberId, 1L, "k1", vec, true);
    service.onBucketMutation(bucketId, memberId, 2L, "k2", vec, true);
    service.onBucketMutation(bucketId, memberId, 4L, "k4", vec, true);
    assertThat(dispatcher.getHoldbackSize(bucketId)).isEqualTo(1);
    service.onBucketMutation(bucketId, memberId, 3L, "k3", vec, true);
    service.onBucketMutation(bucketId, memberId, 5L, "k5", vec, true);

    assertThat(service.getLastDispatchedVersion(bucketId, memberId)).isEqualTo(5L);
    assertThat(service.getNextExpectedVersion(bucketId, memberId)).isEqualTo(6L);
    assertThat(service.getCheckpointVersion(bucketId, memberId)).isEqualTo(5L);
    assertThat(dispatcher.getTailSize(bucketId)).isZero();
    assertThat(dispatcher.getTailOverflowCount()).isZero();
    assertThat(service.isResyncRequired(bucketId, memberId)).isFalse();

    // Phase 2: |S| = 1 interval v=6..8, ack & unregister sub-1 -> |S| returns to 0
    service.registerSubscription("sub-1", vec, 0.90f);
    for (long v = 6L; v <= 8L; v++) {
      service.onBucketMutation(bucketId, memberId, v, "k" + v, vec, true);
    }
    assertThat(dispatched).extracting(ContinuousVectorEvent::getRegionVersion).containsExactly(6L,
        7L, 8L);
    service.acknowledgeCheckpoint(bucketId, memberId, 8L);
    assertThat(service.unregisterSubscription("sub-1")).isTrue();
    assertThat(service.getActiveSubscriptionCount()).isZero();

    // Phase 3: Middle |S| = 0 exceeding maxTailPerStream=4 (20 mutations v=9..28) -> zero overflow
    for (long v = 9L; v <= 28L; v++) {
      service.onBucketMutation(bucketId, memberId, v, "k" + v, vec, true);
    }
    assertThat(service.getLastDispatchedVersion(bucketId, memberId)).isEqualTo(28L);
    assertThat(service.getNextExpectedVersion(bucketId, memberId)).isEqualTo(29L);
    assertThat(service.getCheckpointVersion(bucketId, memberId)).isEqualTo(28L);
    assertThat(dispatcher.getTailSize(bucketId)).isZero();
    assertThat(dispatcher.getTailOverflowCount()).isZero();
    assertThat(service.isResyncRequired(bucketId, memberId)).isFalse();

    // Phase 4: New subscriber joins -> contiguous versions v=29..31 flow & replay cleanly
    dispatched.clear();
    service.registerSubscription("sub-2", vec, 0.90f);
    for (long v = 29L; v <= 31L; v++) {
      service.onBucketMutation(bucketId, memberId, v, "k" + v, vec, true);
    }
    assertThat(dispatched).extracting(ContinuousVectorEvent::getRegionVersion).containsExactly(29L,
        30L, 31L);
    assertThat(dispatcher.getTailSize(bucketId)).isEqualTo(3);
    assertThat(service.onPrimaryPromotion(bucketId))
        .extracting(ContinuousVectorEvent::getRegionVersion)
        .containsExactly(29L, 30L, 31L);
  }

  @Test
  public void ordinaryReconnectFencesInFlightHandlerAndPreventsDuplicateAdmissionOrOverlap()
      throws Exception {
    float[] matchVec = new float[] {1.0f, 0.0f, 0.0f, 0.0f};
    ContinuousVectorQueryService server = new ContinuousVectorQueryService(4);
    server.setHeartbeatIntervalEvents(8);
    server.registerSubscription("sub-fence", matchVec, 0.90f);

    IdempotentRvvConsumer.InMemoryDurableDeliveryStore durableStore =
        new IdempotentRvvConsumer.InMemoryDurableDeliveryStore();
    List<IdempotentRvvConsumer.EventDeliveryId> appliedEffectLog = new CopyOnWriteArrayList<>();
    java.util.concurrent.ConcurrentHashMap<Long, AtomicInteger> handlerInvocationsByVer =
        new java.util.concurrent.ConcurrentHashMap<>();
    AtomicInteger activeHandlers = new AtomicInteger();
    AtomicInteger maxConcurrentHandlers = new AtomicInteger();

    CountDownLatch v3InFlight = new CountDownLatch(1);
    CountDownLatch allowV3Finish = new CountDownLatch(1);

    int bucketId = 17;
    String memberId = "srv-fence";

    RvvClientGate gate = new RvvClientGate();
    server.attachClientGate(gate);
    IdempotentRvvConsumer consumer = new IdempotentRvvConsumer(gate, durableStore, (ev, id) -> {
      int concurrent = activeHandlers.incrementAndGet();
      maxConcurrentHandlers.accumulateAndGet(concurrent, Math::max);
      handlerInvocationsByVer.computeIfAbsent(ev.getRegionVersion(), ignored -> new AtomicInteger())
          .incrementAndGet();
      try {
        if (ev.getRegionVersion() == 3L) {
          v3InFlight.countDown();
          try {
            allowV3Finish.await(10, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
        appliedEffectLog.add(id);
      } finally {
        activeHandlers.decrementAndGet();
      }
    });
    server.addEventSink(consumer);

    server.onBucketMutation(bucketId, memberId, 1L, "k1", matchVec, true);
    server.onBucketMutation(bucketId, memberId, 2L, "k2", matchVec, true);
    assertThat(durableStore.getCommittedWatermark("sub-fence", bucketId, memberId)).isEqualTo(2L);
    assertThat(consumer.getDurableAdmissionsCount()).isEqualTo(2L);

    // Trigger v=3 on background thread so its handler remains actively in flight
    Thread inFlightThread = new Thread(
        () -> server.onBucketMutation(bucketId, memberId, 3L, "k3", matchVec, true),
        "in-flight-handler-v3");
    inFlightThread.start();
    assertThat(v3InFlight.await(10, TimeUnit.SECONDS)).isTrue();
    assertThat(consumer.getDurableAdmissionsCount()).isEqualTo(3L);

    // Concurrent duplicate of in-flight v=3 is rejected atomically without overlap or re-admission
    ContinuousVectorEvent dupV3 = new ContinuousVectorEvent("sub-fence", "k3", 1.0f, bucketId,
        memberId, 3L, 1, System.currentTimeMillis(), true);
    assertThat(consumer.processEvent(dupV3)).isFalse();
    assertThat(consumer.getDurableAdmissionsCount()).isEqualTo(3L);
    assertThat(maxConcurrentHandlers.get()).isEqualTo(1);

    // Ordinary reconnect calls prepareForReconnect(bucketId) WHILE v=3 handler is still in flight;
    // the per-stream execution/recovery fence must block reconnect until v=3 completes cleanly!
    server.removeEventSink(consumer);

    CountDownLatch reconnectCompleted = new CountDownLatch(1);
    java.util.concurrent.atomic.AtomicReference<Map<String, Long>> reconnectHwRef =
        new java.util.concurrent.atomic.AtomicReference<>();
    Thread reconnectThread = new Thread(() -> {
      reconnectHwRef.set(consumer.prepareForReconnect(bucketId));
      reconnectCompleted.countDown();
    }, "ordinary-reconnect-fenced");
    reconnectThread.start();
    assertThat(reconnectCompleted.await(120L, TimeUnit.MILLISECONDS)).isFalse();
    assertThat(maxConcurrentHandlers.get()).isEqualTo(1);

    // Release in-flight handler v=3 -> completes, unblocks reconnect with V_client=3, then
    // server records v=4 while consumer is still detached and replays [4] cleanly on reconnect
    allowV3Finish.countDown();
    inFlightThread.join(10_000L);
    assertThat(reconnectCompleted.await(10, TimeUnit.SECONDS)).isTrue();
    reconnectThread.join(10_000L);

    assertThat(reconnectHwRef.get()).containsEntry(memberId, 3L);
    server.onBucketMutation(bucketId, memberId, 4L, "k4", matchVec, true);
    server.addEventSink(consumer);
    List<ContinuousVectorEvent> replayed =
        server.replayUncheckpointedForReconnect(bucketId, reconnectHwRef.get());
    assertThat(replayed).extracting(ContinuousVectorEvent::getRegionVersion).containsExactly(4L);

    assertThat(maxConcurrentHandlers.get()).isEqualTo(1);
    assertThat(handlerInvocationsByVer.get(3L).get()).isEqualTo(1);
    assertThat(consumer.getDurableAdmissionsCount()).isEqualTo(4L);
    assertThat(appliedEffectLog).extracting(IdempotentRvvConsumer.EventDeliveryId::getRegionVersion)
        .containsExactly(1L, 2L, 3L, 4L)
        .doesNotHaveDuplicates();
    assertThat(durableStore.getCommittedWatermark("sub-fence", bucketId, memberId)).isEqualTo(4L);
    assertThat(gate.getSafeCheckpointVersion(bucketId, memberId)).isEqualTo(4L);
  }

  @Test
  public void subscriberRejoinBelowCompactedReplayFloorImmediatelyFlagsResyncRequired() {
    float[] vec = new float[] {1.0f, 0.0f, 0.0f, 0.0f};
    ContinuousVectorQueryService service = new ContinuousVectorQueryService(4);
    service.registerSubscription("sub-healthy", vec, 0.90f);
    service.registerSubscriberForCheckpoint("sub-healthy");

    List<String> floorAlerts = new ArrayList<>();
    service.addReplayFloorResyncListener((bId, mId, clientHw, floor) -> floorAlerts
        .add(bId + "|" + mId + "|" + clientHw + "<" + floor));

    int bucketId = 18;
    String memberId = "srv-floor";
    for (long v = 1L; v <= 10L; v++) {
      service.onBucketMutation(bucketId, memberId, v, "k" + v, vec, true);
    }
    assertThat(service.getReplayFloorVersion(bucketId, memberId)).isZero();

    // Compact tail through v=6 -> V_floor advances to 6
    service.acknowledgeSubscriberCheckpoint("sub-healthy", bucketId, memberId, 6L);
    assertThat(service.getReplayFloorVersion(bucketId, memberId)).isEqualTo(6L);
    assertThat(service.isResyncRequired(bucketId, memberId)).isFalse();

    // (a) Disconnected subscriber rejoining at V_client=4 < V_floor=6 -> immediate resyncRequired
    assertThat(service.admitSubscriberForContinuity("sub-stale", bucketId, memberId, 4L))
        .isFalse();
    assertThat(service.isResyncRequired(bucketId, memberId)).isTrue();
    assertThat(service.getReplayFloorResyncCount()).isEqualTo(1L);
    assertThat(service.getDispatcher().getRegisteredCheckpointSubscribers())
        .doesNotContain("sub-stale");
    assertThat(floorAlerts).containsExactly("18|srv-floor|4<6");

    // Reconnect replay presenting V_client=4 < V_floor=6 refuses misleading partial replay
    service.getDispatcher().clearResyncRequired(bucketId, memberId);
    assertThat(service.replayUncheckpointedForReconnect(bucketId,
        Collections.singletonMap(memberId, 4L))).isEmpty();
    assertThat(service.isResyncRequired(bucketId, memberId)).isTrue();
    assertThat(service.getReplayFloorResyncCount()).isEqualTo(2L);

    // Healthy subscriber at V_client=6 >= V_floor=6 admits cleanly and replays 7..10
    service.getDispatcher().clearResyncRequired(bucketId, memberId);
    assertThat(service.admitSubscriberForContinuity("sub-caught-up", bucketId, memberId, 6L))
        .isTrue();
    assertThat(service.isResyncRequired(bucketId, memberId)).isFalse();
    assertThat(service.replayUncheckpointedForReconnect(bucketId,
        Collections.singletonMap(memberId, 6L)))
            .extracting(ContinuousVectorEvent::getRegionVersion)
            .containsExactly(7L, 8L, 9L, 10L);

    // (b) Empty |S|=0 interval advances V_floor cleanly; rejoin below it flags resyncRequired
    assertThat(service.unregisterSubscriberForCheckpoint("sub-healthy")).isTrue();
    assertThat(service.unregisterSubscriberForCheckpoint("sub-caught-up")).isTrue();
    assertThat(service.unregisterSubscription("sub-healthy")).isTrue();
    for (long v = 11L; v <= 15L; v++) {
      service.onBucketMutation(bucketId, memberId, v, "k" + v, vec, true);
    }
    assertThat(service.getReplayFloorVersion(bucketId, memberId)).isEqualTo(15L);
    assertThat(service.admitSubscriberForContinuity("sub-from-before-empty", bucketId, memberId,
        10L)).isFalse();
    assertThat(service.isResyncRequired(bucketId, memberId)).isTrue();
  }

  @Test
  public void promotionAtTailCapacityMinusOneVsPlusOneWithSlowConsumerAndPendingCheckpoint() {
    float[] vec = new float[] {1.0f, 0.0f, 0.0f, 0.0f};
    int cTail = 64;
    int bucketId = 19;
    String memberP = "srv-primary-cap";

    // Case A: C_tail - 1 (63 uncheckpointed entries) with slow consumer at 10 and pending
    // un-replicated checkpoint -> promotion replays all 63 entries cleanly with 0 resync!
    ChordalVoronoiRouter routerA = new ChordalVoronoiRouter(4);
    RvvMonotonicVectorDispatcher secA = new RvvMonotonicVectorDispatcher(routerA, cTail);
    ContinuousVectorQueryService serviceA = new ContinuousVectorQueryService(routerA, secA);
    serviceA.registerSubscription("sub-cap", vec, 0.90f);
    RvvClientGate slowGateA = new RvvClientGate();
    List<ContinuousVectorEvent> deliveredA = new ArrayList<>();
    slowGateA.addListener(deliveredA::add);
    serviceA.attachClientGate(slowGateA);
    serviceA.addEventSink(slowGateA::tryAccept);

    // Replicated checkpoint baseline V_ckpt = 10 on secondary (v=1..10 already delivered)
    serviceA.acknowledgeCheckpoint(bucketId, memberP, 10L);
    slowGateA.restoreHighWaterVersion("sub-cap", bucketId, memberP, 10L);
    for (long v = 11L; v <= 10L + (cTail - 1); v++) {
      serviceA.onBucketMutation(bucketId, memberP, v, "k-" + v, vec, false);
    }
    assertThat(secA.getTailSize(bucketId)).isEqualTo(cTail - 1);
    assertThat(secA.getTailOverflowCount()).isZero();
    assertThat(secA.isResyncRequired(bucketId, memberP)).isFalse();
    assertThat(secA.getReplayFloorVersion(bucketId, memberP)).isEqualTo(10L);

    List<ContinuousVectorEvent> promoA = serviceA.onPrimaryPromotion(bucketId);
    assertThat(promoA).hasSize(cTail - 1);
    assertThat(deliveredA).hasSize(cTail - 1);
    assertThat(secA.isResyncRequired(bucketId, memberP)).isFalse();
    assertThat(serviceA.admitSubscriberForContinuity("sub-cap", bucketId, memberP, 10L)).isTrue();
    assertThat(RvvClientGate.verifyStrictMonotonicity(deliveredA)).isEmpty();
    assertThat(slowGateA.getHighWaterVersion("sub-cap", bucketId, memberP))
        .isEqualTo(10L + (cTail - 1));

    // Case B: C_tail + 1 (65 uncheckpointed entries) before pending checkpoint replicates ->
    // deterministic eviction of v=11, V_floor=11, resyncRequired=true, slow rejoin rejected!
    ChordalVoronoiRouter routerB = new ChordalVoronoiRouter(4);
    RvvMonotonicVectorDispatcher secB = new RvvMonotonicVectorDispatcher(routerB, cTail);
    ContinuousVectorQueryService serviceB = new ContinuousVectorQueryService(routerB, secB);
    serviceB.registerSubscription("sub-cap", vec, 0.90f);
    serviceB.acknowledgeCheckpoint(bucketId, memberP, 10L);
    for (long v = 11L; v <= 10L + (cTail + 1); v++) {
      serviceB.onBucketMutation(bucketId, memberP, v, "k-" + v, vec, false);
    }
    assertThat(secB.getTailSize(bucketId)).isEqualTo(cTail);
    assertThat(secB.getTailOverflowCount()).isEqualTo(1L);
    assertThat(secB.isResyncRequired(bucketId, memberP)).isTrue();
    assertThat(secB.getReplayFloorVersion(bucketId, memberP)).isEqualTo(11L);
    assertThat(serviceB.admitSubscriberForContinuity("sub-cap", bucketId, memberP, 10L)).isFalse();

    // Case C: Configured production default C_tail = DEFAULT_MAX_TAIL_PER_STREAM (100,000)
    int defaultCap = RvvMonotonicVectorDispatcher.DEFAULT_MAX_TAIL_PER_STREAM;
    ChordalVoronoiRouter routerDef = new ChordalVoronoiRouter(4);
    RvvMonotonicVectorDispatcher secDef = new RvvMonotonicVectorDispatcher(routerDef, defaultCap);
    routerDef.addSubscription(new VectorSubscription("sub-def", vec, 0.90f));
    for (long v = 1L; v <= defaultCap - 1L; v++) {
      secDef.onBucketMutation(20, "srv-def", v, 1, "k-" + v, vec, false);
    }
    assertThat(secDef.getTailSize(20)).isEqualTo(defaultCap - 1);
    assertThat(secDef.getTailOverflowCount()).isZero();
    assertThat(secDef.isResyncRequired(20, "srv-def")).isFalse();
    secDef.onBucketMutation(20, "srv-def", defaultCap, 1, "k-" + defaultCap, vec, false);
    assertThat(secDef.getTailOverflowCount()).isZero();
    secDef.onBucketMutation(20, "srv-def", defaultCap + 1L, 1, "k-over", vec, false);
    assertThat(secDef.getTailOverflowCount()).isEqualTo(1L);
    assertThat(secDef.isResyncRequired(20, "srv-def")).isTrue();
    assertThat(secDef.getReplayFloorVersion(20, "srv-def")).isEqualTo(1L);
  }

  @Test
  public void repeatedKeyFailoverPreservesEveryPerVersionNotificationWithoutCoalescingLoss() {
    float[] vec = new float[] {1.0f, 0.0f, 0.0f, 0.0f};
    ContinuousVectorQueryService primary = new ContinuousVectorQueryService(4);
    ContinuousVectorQueryService secondary = new ContinuousVectorQueryService(4);
    primary.registerSubscription("sub-hot", vec, 0.90f);
    secondary.registerSubscription("sub-hot", vec, 0.90f);

    RvvClientGate clientGate = new RvvClientGate();
    List<ContinuousVectorEvent> delivered = new ArrayList<>();
    clientGate.addListener(delivered::add);
    primary.attachClientGate(clientGate);
    secondary.attachClientGate(clientGate);
    ContinuousVectorEventSink gateSink = clientGate::tryAccept;
    primary.addEventSink(gateSink);

    int bucketId = 21;
    String memberP = "srv-hot-P";
    String memberS = "srv-hot-S";
    List<String> expectedStreamVersions = new ArrayList<>();

    // Phase 1: 20 mutations repeatedly overwriting the SAME 4 hot keys ("hot-0".."hot-3").
    // Client disconnects after v=12; v=13..20 survive in secondary's per-version tail buffer.
    List<RvvMonotonicVectorDispatcher.TailMutation> coalescedStateSnapshot = new ArrayList<>();
    for (long v = 1L; v <= 20L; v++) {
      String hotKey = "hot-" + (v % 4L);
      expectedStreamVersions.add(bucketId + "|" + memberP + "|" + v);
      if (v == 13L) {
        primary.removeEventSink(gateSink);
      }
      primary.onBucketMutation(bucketId, memberP, v, hotKey, vec, true);
      secondary.onBucketMutation(bucketId, memberP, v, hotKey, vec, false);
      if (v >= 17L) {
        coalescedStateSnapshot.add(new RvvMonotonicVectorDispatcher.TailMutation(hotKey, vec,
            bucketId, memberP, v, (int) v, System.currentTimeMillis()));
      }
    }
    assertThat(coalescedStateSnapshot).hasSize(4); // State store holds only 4 latest key values
    assertThat(delivered).hasSize(12);

    // Phase 2: Primary crashes -> secondary promotes merging 4-key state snapshot with full
    // per-version tail (1..20), then processes 20 more repeated-key updates on m_S
    secondary.addEventSink(gateSink);
    secondary.getDispatcher().onPrimaryPromotion(bucketId, coalescedStateSnapshot);
    for (long v = 1L; v <= 20L; v++) {
      String hotKey = "hot-" + (v % 4L);
      expectedStreamVersions.add(bucketId + "|" + memberS + "|" + v);
      secondary.onBucketMutation(bucketId, memberS, v, hotKey, vec, true);
    }

    // Verify 40/40 per-version (b, m, v) notifications delivered with 0 misses, 0 duplicates,
    // and 0 resyncRequired despite only 4 distinct keys existing in region state!
    List<String> deliveredStreamVersions = new ArrayList<>(delivered.size());
    for (ContinuousVectorEvent ev : delivered) {
      deliveredStreamVersions.add(ev.getBucketId() + "|" + ev.getMemberId() + "|"
          + ev.getRegionVersion());
    }
    assertThat(deliveredStreamVersions).containsExactlyElementsOf(expectedStreamVersions);
    assertThat(deliveredStreamVersions).doesNotHaveDuplicates();
    assertThat(RvvClientGate.verifyStrictMonotonicity(delivered)).isEmpty();
    assertThat(secondary.isResyncRequired(bucketId, memberP)).isFalse();
    assertThat(secondary.isResyncRequired(bucketId, memberS)).isFalse();
  }

  @Test
  public void reconnectBetweenFloorAndCheckpointReplaysIntermediateVersionsAndBelowFloorTriggersResync() {
    float[] vec = new float[] {1.0f, 0.0f, 0.0f, 0.0f};
    ContinuousVectorQueryService service = new ContinuousVectorQueryService(4);
    service.registerSubscription("sub-fast", vec, 0.90f);
    service.registerSubscription("sub-lagging", vec, 0.90f);
    service.registerSubscriberForCheckpoint("sub-fast");

    int bucketId = 22;
    String memberId = "srv-floor-ckpt";
    for (long v = 1L; v <= 70L; v++) {
      service.onBucketMutation(bucketId, memberId, v, "k-" + v, vec, true);
    }

    // Compact tail through V_floor = 60, then advance logical replicated V_ckpt = 64 without
    // compacting [61..64] yet -> V_floor=60 <= V_client=63 < V_ckpt=64, tail retains [61..70]
    service.acknowledgeCheckpoint(bucketId, memberId, 60L);
    service.recordReplicatedCheckpoint(bucketId, memberId, 64L);
    assertThat(service.getReplayFloorVersion(bucketId, memberId)).isEqualTo(60L);
    assertThat(service.getCheckpointVersion(bucketId, memberId)).isEqualTo(64L);
    assertThat(service.getDispatcher().getTailSize(bucketId)).isEqualTo(10);

    // (a) Returning subscriber at V_client=63 (V_floor=60 <= 63 < V_ckpt=64):
    // MUST pass admission, atomically join S_healthy pinned at 63, and replay [64..70] (NOT skip
    // 64!)
    List<ContinuousVectorEvent> laggingReceived = new ArrayList<>();
    ContinuousVectorEventSink laggingSink = laggingReceived::add;
    List<ContinuousVectorEvent> replayed = service.admitAndReplayForReconnect("sub-lagging",
        bucketId, Collections.singletonMap(memberId, 63L), laggingSink, null);
    assertThat(service.isResyncRequired(bucketId, memberId)).isFalse();
    assertThat(service.getDispatcher().getRegisteredCheckpointSubscribers())
        .contains("sub-lagging");
    assertThat(replayed).filteredOn(e -> "sub-lagging".equals(e.getSubscriptionId()))
        .extracting(ContinuousVectorEvent::getRegionVersion)
        .containsExactly(64L, 65L, 66L, 67L, 68L, 69L, 70L);
    assertThat(laggingReceived)
        .filteredOn(e -> !e.isHeartbeat() && "sub-lagging".equals(e.getSubscriptionId()))
        .extracting(ContinuousVectorEvent::getRegionVersion)
        .containsExactly(64L, 65L, 66L, 67L, 68L, 69L, 70L);

    // Atomic S_healthy membership pin: fast subscriber acking 70 CANNOT advance V_floor past 63
    // while sub-lagging has only acknowledged 63!
    service.acknowledgeSubscriberCheckpoint("sub-fast", bucketId, memberId, 70L);
    assertThat(service.getReplayFloorVersion(bucketId, memberId)).isLessThanOrEqualTo(63L);
    assertThat(service.getDispatcher().getTailSize(bucketId)).isGreaterThanOrEqualTo(7);

    // Once sub-lagging acks 70, both V_floor and V_ckpt advance cleanly to 70
    service.acknowledgeSubscriberCheckpoint("sub-lagging", bucketId, memberId, 70L);
    assertThat(service.getReplayFloorVersion(bucketId, memberId)).isEqualTo(70L);
    assertThat(service.getCheckpointVersion(bucketId, memberId)).isEqualTo(70L);
    assertThat(service.getDispatcher().getTailSize(bucketId)).isZero();

    // (b) Stale subscriber below compacted V_floor (V_client=59 < V_floor=60 on fresh stream):
    // IMMEDIATELY rejected with resyncRequired, NOT enrolled in S_healthy, 0 partial replay!
    int bucketId2 = 23;
    for (long v = 1L; v <= 70L; v++) {
      service.onBucketMutation(bucketId2, memberId, v, "k2-" + v, vec, true);
    }
    service.acknowledgeCheckpoint(bucketId2, memberId, 60L);
    service.recordReplicatedCheckpoint(bucketId2, memberId, 64L);

    List<ContinuousVectorEvent> staleSinkEvents = new ArrayList<>();
    List<ContinuousVectorEvent> staleReplayed = service.admitAndReplayForReconnect("sub-stale",
        bucketId2, Collections.singletonMap(memberId, 59L), staleSinkEvents::add, null);
    assertThat(staleReplayed).isEmpty();
    assertThat(staleSinkEvents).isEmpty();
    assertThat(service.isResyncRequired(bucketId2, memberId)).isTrue();
    assertThat(service.getReplayFloorResyncCount()).isEqualTo(1L);
    assertThat(service.getDispatcher().getRegisteredCheckpointSubscribers())
        .doesNotContain("sub-stale");

    // (c) Empty |S_healthy| == 0 AND |S| == 0 cleanly compacts tail and advances V_floor to V_S
    service.unregisterSubscription("sub-fast");
    service.unregisterSubscription("sub-lagging");
    service.unregisterSubscriberForCheckpoint("sub-fast");
    service.unregisterSubscriberForCheckpoint("sub-lagging");
    assertThat(service.getReplayFloorVersion(bucketId2, memberId)).isEqualTo(70L);
    assertThat(service.getDispatcher().getTailSize(bucketId2)).isZero();
  }

  @Test
  public void multiSubscriptionRepeatedKeyPromotionCoveringHeartbeatAndLaggingReconnect()
      throws Exception {
    float[] vec = new float[] {1.0f, 0.0f, 0.0f, 0.0f};
    ContinuousVectorQueryService primary = new ContinuousVectorQueryService(4);
    ContinuousVectorQueryService secondary = new ContinuousVectorQueryService(4);
    primary.setHeartbeatIntervalEvents(4);
    secondary.setHeartbeatIntervalEvents(4);
    primary.registerSubscription("sub-A", vec, 0.90f);
    primary.registerSubscription("sub-B", vec, 0.90f);
    secondary.registerSubscription("sub-A", vec, 0.90f);
    secondary.registerSubscription("sub-B", vec, 0.90f);

    int bucketId = 24;
    String memberP = "srv-multi-P";
    String memberS = "srv-multi-S";

    // Consumer 1 shares ONE IdempotentRvvConsumer + DurableDeliveryStore across sub-A and sub-B
    // to prove EventDeliveryId (subId, b, m, v) never collides across subscriptions on one
    // mutation, and covering HB(64) cannot retire sub-B's delayed v=63 match before
    // admission/commit
    RvvClientGate gate1 = new RvvClientGate();
    gate1.registerSubscription("sub-A");
    gate1.registerSubscription("sub-B");
    IdempotentRvvConsumer.InMemoryDurableDeliveryStore store1 =
        new IdempotentRvvConsumer.InMemoryDurableDeliveryStore();
    List<IdempotentRvvConsumer.EventDeliveryId> applied1 = new CopyOnWriteArrayList<>();
    CountDownLatch subBV63InFlight = new CountDownLatch(1);
    CountDownLatch releaseSubBV63 = new CountDownLatch(1);
    IdempotentRvvConsumer consumer1 = new IdempotentRvvConsumer(gate1, store1, (ev, id) -> {
      if ("sub-B".equals(ev.getSubscriptionId()) && memberP.equals(ev.getMemberId())
          && ev.getRegionVersion() == 63L) {
        subBV63InFlight.countDown();
        try {
          releaseSubBV63.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      applied1.add(id);
    });

    // Consumer 2 is a lagging subscriber that disconnects after v=63 and reconnects after failover
    // in the V_floor=60 <= V_client=63 < V_ckpt=64 window!
    RvvClientGate gate2 = new RvvClientGate();
    gate2.registerSubscription("sub-A");
    gate2.registerSubscription("sub-B");
    IdempotentRvvConsumer.InMemoryDurableDeliveryStore store2 =
        new IdempotentRvvConsumer.InMemoryDurableDeliveryStore();
    List<IdempotentRvvConsumer.EventDeliveryId> applied2 = new CopyOnWriteArrayList<>();
    IdempotentRvvConsumer consumer2 = new IdempotentRvvConsumer(gate2, store2,
        (ev, id) -> applied2.add(id));

    primary.addEventSink(consumer1);
    primary.addEventSink(consumer2);

    // Phase 1a: v=1..62 overwriting ONLY 4 hot keys ("hot-0".."hot-3"), matching BOTH sub-A & sub-B
    Set<IdempotentRvvConsumer.EventDeliveryId> expectedIds = new HashSet<>();
    for (long v = 1L; v <= 62L; v++) {
      String key = "hot-" + (v % 4L);
      expectedIds.add(new IdempotentRvvConsumer.EventDeliveryId("sub-A", bucketId, memberP, v));
      expectedIds.add(new IdempotentRvvConsumer.EventDeliveryId("sub-B", bucketId, memberP, v));
      primary.onBucketMutation(bucketId, memberP, v, key, vec, true);
      secondary.onBucketMutation(bucketId, memberP, v, key, vec, false);
    }
    assertThat(applied1).hasSize(124);
    assertThat(applied2).hasSize(124);

    // Compact V_floor=60 on both primary and secondary, and replicate logical V_ckpt=64 while
    // retaining [61..68] in the tail (V_floor=60 < V_ckpt=64)
    primary.acknowledgeCheckpoint(bucketId, memberP, 60L);
    secondary.acknowledgeCheckpoint(bucketId, memberP, 60L);
    primary.recordReplicatedCheckpoint(bucketId, memberP, 64L);
    secondary.recordReplicatedCheckpoint(bucketId, memberP, 64L);

    // Phase 1b: v=63 executes on background thread so consumer1 holds sub-B v=63 in flight while
    // sub-A v=63 completes cleanly and covering HB(64) arrives concurrently!
    expectedIds.add(new IdempotentRvvConsumer.EventDeliveryId("sub-A", bucketId, memberP, 63L));
    expectedIds.add(new IdempotentRvvConsumer.EventDeliveryId("sub-B", bucketId, memberP, 63L));
    Thread v63Thread = new Thread(
        () -> primary.onBucketMutation(bucketId, memberP, 63L, "hot-3", vec, true),
        "multi-sub-v63-inflight");
    v63Thread.start();
    assertThat(subBV63InFlight.await(10, TimeUnit.SECONDS)).isTrue();
    secondary.onBucketMutation(bucketId, memberP, 63L, "hot-3", vec, false);

    // Covering heartbeat HB(63) arrives on consumer1 while sub-B v=63 is STILL PENDING!
    // (In FIFO server dispatch, a covering HB(v) follows all matches of version v.)
    consumer1.processEvent(ContinuousVectorEvent.watermarkHeartbeat(bucketId, memberP, 63L,
        System.currentTimeMillis()));
    // sub-B MUST stay pinned at 62 (never retired by covering HB(63)!), while sub-A is independent
    assertThat(store1.getCommittedWatermark("sub-B", bucketId, memberP)).isEqualTo(62L);
    assertThat(store1.getCommittedWatermark("sub-A", bucketId, memberP)).isEqualTo(63L);
    assertThat(consumer1.getCommittedHighWaterByMember(bucketId)).containsEntry(memberP, 62L);

    // Release sub-B v=63 and detach both consumers before primary crashes during v=64..68
    releaseSubBV63.countDown();
    v63Thread.join(10_000L);
    primary.removeEventSink(consumer1);
    primary.removeEventSink(consumer2);

    // Phase 1c: v=64..68 (same 4 hot keys!) on primary + secondary; v=64 triggers HB(64)
    List<RvvMonotonicVectorDispatcher.TailMutation> coalescedSnapshot = new ArrayList<>();
    for (long v = 64L; v <= 68L; v++) {
      String key = "hot-" + (v % 4L);
      expectedIds.add(new IdempotentRvvConsumer.EventDeliveryId("sub-A", bucketId, memberP, v));
      expectedIds.add(new IdempotentRvvConsumer.EventDeliveryId("sub-B", bucketId, memberP, v));
      primary.onBucketMutation(bucketId, memberP, v, key, vec, true);
      secondary.onBucketMutation(bucketId, memberP, v, key, vec, false);
      if (v >= 65L) {
        coalescedSnapshot.add(new RvvMonotonicVectorDispatcher.TailMutation(key, vec, bucketId,
            memberP, v, (int) v, System.currentTimeMillis()));
      }
    }

    // Phase 2: Primary crashes -> secondary promotes (with V_floor=60, V_ckpt=64, tail=[61..68])
    // and processes 8 more repeated-key updates on m_S BEFORE lagging consumers reconnect!
    secondary.getDispatcher().onPrimaryPromotion(bucketId, coalescedSnapshot);
    for (long v = 1L; v <= 8L; v++) {
      String key = "hot-" + (v % 4L);
      expectedIds.add(new IdempotentRvvConsumer.EventDeliveryId("sub-A", bucketId, memberS, v));
      expectedIds.add(new IdempotentRvvConsumer.EventDeliveryId("sub-B", bucketId, memberS, v));
      secondary.onBucketMutation(bucketId, memberS, v, key, vec, true);
    }

    // Phase 3: Both consumers reconnect via prepareForReconnect -> admitAndReplayForReconnect!
    // Consumer 2 presents V_client=63 in V_floor=60 <= 63 < V_ckpt=64 -> MUST receive v=64!
    Map<String, Long> hw1 = consumer1.prepareForReconnect(bucketId);
    Map<String, Long> hw2 = consumer2.prepareForReconnect(bucketId);
    assertThat(hw2).containsEntry(memberP, 63L);

    secondary.admitAndReplayForReconnect("consumer-1", bucketId, hw1, consumer1, null);
    secondary.admitAndReplayForReconnect("consumer-2", bucketId, hw2, consumer2, null);

    // Exact 152/152 (subId, b, m, v) effects on BOTH consumers, 0 misses, 0 duplicates, 0 resync!
    assertThat(expectedIds).hasSize(152);
    assertThat(applied1).containsExactlyInAnyOrderElementsOf(expectedIds).doesNotHaveDuplicates();
    assertThat(applied2).containsExactlyInAnyOrderElementsOf(expectedIds).doesNotHaveDuplicates();
    assertThat(applied2).contains(
        new IdempotentRvvConsumer.EventDeliveryId("sub-A", bucketId, memberP, 64L),
        new IdempotentRvvConsumer.EventDeliveryId("sub-B", bucketId, memberP, 64L));
    assertThat(secondary.isResyncRequired(bucketId, memberP)).isFalse();
    assertThat(secondary.isResyncRequired(bucketId, memberS)).isFalse();

    // Verify stale subscriber at V_client=59 < V_floor=60 is rejected immediately
    assertThat(secondary.admitAndReplayForReconnect("stale-sub", bucketId,
        Collections.singletonMap(memberP, 59L))).isEmpty();
    assertThat(secondary.isResyncRequired(bucketId, memberP)).isTrue();
  }

  @Test
  public void tailOverflowAtomicallyEvictsLaggingSubscribersAndPreservesFloorLeqCheckpoint() {
    float[] vec = new float[] {1.0f, 0.0f, 0.0f, 0.0f};
    int cTail = 8;

    // Case A: Non-empty surviving S_healthy' (sub-fast at 6 survives, sub-slow at 2 evicted)
    ChordalVoronoiRouter routerA = new ChordalVoronoiRouter(4);
    RvvMonotonicVectorDispatcher dispA = new RvvMonotonicVectorDispatcher(routerA, cTail);
    ContinuousVectorQueryService svcA = new ContinuousVectorQueryService(routerA, dispA);
    svcA.registerSubscription("sub-fast", vec, 0.90f);
    svcA.registerSubscription("sub-slow", vec, 0.90f);
    svcA.registerSubscriberForCheckpoint("sub-fast");
    svcA.registerSubscriberForCheckpoint("sub-slow");

    int bucketA = 25;
    String memberA = "srv-ovf-a";
    for (long v = 1L; v <= 6L; v++) {
      svcA.onBucketMutation(bucketA, memberA, v, "ka-" + v, vec, true);
    }
    svcA.acknowledgeSubscriberCheckpoint("sub-fast", bucketA, memberA, 6L);
    svcA.acknowledgeSubscriberCheckpoint("sub-slow", bucketA, memberA, 2L);
    assertThat(svcA.getReplayFloorVersion(bucketA, memberA)).isEqualTo(2L);
    assertThat(svcA.getCheckpointVersion(bucketA, memberA)).isEqualTo(2L);

    // Mutations v=7..11 overflow v=3 (V_floor bumps to 3 > sub-slow's 2):
    // atomically evicts sub-slow from S_healthy into resyncRequired and recomputes
    // V_ckpt = max(V_floor, min_{S_healthy'}) = 6 (V_floor == 6 <= V_ckpt == 6)!
    for (long v = 7L; v <= 11L; v++) {
      svcA.onBucketMutation(bucketA, memberA, v, "ka-" + v, vec, true);
      assertThat(svcA.getReplayFloorVersion(bucketA, memberA))
          .isLessThanOrEqualTo(svcA.getCheckpointVersion(bucketA, memberA));
    }
    assertThat(svcA.isResyncRequired(bucketA, memberA)).isTrue();
    assertThat(svcA.isSubscriberResyncRequired("sub-slow", bucketA, memberA)).isTrue();
    assertThat(svcA.isSubscriberResyncRequired("sub-fast", bucketA, memberA)).isFalse();
    assertThat(dispA.getRegisteredCheckpointSubscribers()).containsExactly("sub-fast");
    assertThat(svcA.getReplayFloorVersion(bucketA, memberA)).isEqualTo(6L);
    assertThat(svcA.getCheckpointVersion(bucketA, memberA)).isEqualTo(6L);
    assertThat(svcA.admitSubscriberForContinuity("sub-slow", bucketA, memberA, 2L)).isFalse();

    // Case B: S_healthy' becomes empty when sole lagging subscriber is evicted ->
    // V_ckpt advances atomically to V_head (V_floor <= V_ckpt) while retaining (V_floor, V_head]
    ChordalVoronoiRouter routerB = new ChordalVoronoiRouter(4);
    RvvMonotonicVectorDispatcher dispB = new RvvMonotonicVectorDispatcher(routerB, cTail);
    ContinuousVectorQueryService svcB = new ContinuousVectorQueryService(routerB, dispB);
    svcB.registerSubscription("sub-slow", vec, 0.90f);
    svcB.registerSubscriberForCheckpoint("sub-slow");

    int bucketB = 26;
    String memberB = "srv-ovf-b";
    for (long v = 1L; v <= 11L; v++) {
      svcB.onBucketMutation(bucketB, memberB, v, "kb-" + v, vec, true);
      if (v == 2L) {
        svcB.acknowledgeSubscriberCheckpoint("sub-slow", bucketB, memberB, 2L);
      }
      assertThat(svcB.getReplayFloorVersion(bucketB, memberB))
          .isLessThanOrEqualTo(svcB.getCheckpointVersion(bucketB, memberB));
    }
    assertThat(dispB.getRegisteredCheckpointSubscribers()).isEmpty();
    assertThat(svcB.isSubscriberResyncRequired("sub-slow", bucketB, memberB)).isTrue();
    assertThat(svcB.getReplayFloorVersion(bucketB, memberB)).isEqualTo(3L);
    assertThat(svcB.getCheckpointVersion(bucketB, memberB)).isEqualTo(11L);
    assertThat(dispB.getTailSize(bucketB)).isEqualTo(cTail);
    assertThat(svcB.admitAndReplayForReconnect("sub-slow", bucketB,
        Collections.singletonMap(memberB, 2L))).isEmpty();

    // Caught-up subscriber (V_client=5 >= V_floor=3) re-admits cleanly and replays [6..11]
    List<ContinuousVectorEvent> replayed = svcB.admitAndReplayForReconnect("sub-caught-up",
        bucketB, Collections.singletonMap(memberB, 5L));
    assertThat(replayed).extracting(ContinuousVectorEvent::getRegionVersion)
        .containsExactly(6L, 7L, 8L, 9L, 10L, 11L);
    assertThat(dispB.getRegisteredCheckpointSubscribers()).containsExactly("sub-caught-up");
  }

  @Test
  public void disconnectBetweenMatchBatchAndCoveringHeartbeatCannotAdvanceWatermarkPastDroppedMatch() {
    float[] vec = new float[] {1.0f, 0.0f, 0.0f, 0.0f};
    ContinuousVectorQueryService server = new ContinuousVectorQueryService(4);
    server.setHeartbeatIntervalEvents(4);
    server.registerSubscription("sub-A", vec, 0.90f);
    server.registerSubscription("sub-B", vec, 0.90f);

    RvvClientGate gate = new RvvClientGate();
    gate.registerSubscription("sub-A");
    gate.registerSubscription("sub-B");
    IdempotentRvvConsumer.InMemoryDurableDeliveryStore store =
        new IdempotentRvvConsumer.InMemoryDurableDeliveryStore();
    List<IdempotentRvvConsumer.EventDeliveryId> applied = new ArrayList<>();
    IdempotentRvvConsumer consumer = new IdempotentRvvConsumer(gate, store,
        (ev, id) -> applied.add(id));

    int bucketId = 27;
    String memberId = "srv-session-fence";
    long session1 = consumer.getActiveSessionEpoch(bucketId);
    assertThat(session1).isEqualTo(1L);
    ContinuousVectorEventSink session1Sink = consumer.createSessionSink(bucketId, session1);

    // Capture server wire events to simulate a transport break between match batch v=4 and HB(4)
    List<ContinuousVectorEvent> wireBuffer = new ArrayList<>();
    ContinuousVectorEventSink capturingSink = ev -> {
      wireBuffer.add(ev);
      if (ev.getRegionVersion() <= 3L) {
        session1Sink.onEvent(ev);
      }
    };
    server.addEventSink(capturingSink);

    for (long v = 1L; v <= 3L; v++) {
      server.onBucketMutation(bucketId, memberId, v, "k-" + v, vec, true);
    }
    assertThat(applied).hasSize(6);
    assertThat(consumer.getCommittedHighWaterByMember(bucketId)).containsEntry(memberId, 3L);

    // v=4 generates [match(sub-A, 4), match(sub-B, 4), HB(4)]; disconnect drops the match batch
    // BEFORE it reaches the client!
    server.onBucketMutation(bucketId, memberId, 4L, "k-4", vec, true);
    ContinuousVectorEvent droppedHb4 = wireBuffer.stream()
        .filter(e -> e.isHeartbeat() && e.getRegionVersion() == 4L)
        .findFirst()
        .orElseThrow(AssertionError::new);
    server.removeEventSink(capturingSink);

    // Client detects disconnect and calls prepareForReconnect(bucketId) -> advances sessionEpoch
    // 1->2
    Map<String, Long> clientHw = consumer.prepareForReconnect(bucketId);
    long session2 = consumer.getActiveSessionEpoch(bucketId);
    assertThat(session2).isEqualTo(2L);
    assertThat(clientHw).containsEntry(memberId, 3L);

    // Late covering heartbeat HB(4) from dead session 1 arrives -> MUST be rejected by receiver
    // sessionEpoch fence and MUST NOT advance V_client past 3!
    session1Sink.onEvent(droppedHb4);
    assertThat(consumer.getStaleSessionEventsRejectedCount()).isEqualTo(1L);
    assertThat(store.getCommittedWatermark("sub-A", bucketId, memberId)).isEqualTo(3L);
    assertThat(store.getCommittedWatermark("sub-B", bucketId, memberId)).isEqualTo(3L);
    assertThat(consumer.getCommittedHighWaterByMember(bucketId)).containsEntry(memberId, 3L);

    // Reconnect on new session 2 -> server replays (3, 4] strictly matches-before-heartbeat!
    ContinuousVectorEventSink session2Sink = consumer.createSessionSink(bucketId, session2);
    server.admitAndReplayForReconnect("sub-session-client", bucketId, clientHw, session2Sink, null);

    assertThat(applied).hasSize(8).doesNotHaveDuplicates();
    assertThat(applied).contains(
        new IdempotentRvvConsumer.EventDeliveryId("sub-A", bucketId, memberId, 4L),
        new IdempotentRvvConsumer.EventDeliveryId("sub-B", bucketId, memberId, 4L));
    assertThat(store.getCommittedWatermark("sub-A", bucketId, memberId)).isEqualTo(4L);
    assertThat(store.getCommittedWatermark("sub-B", bucketId, memberId)).isEqualTo(4L);
    assertThat(consumer.getCommittedHighWaterByMember(bucketId)).containsEntry(memberId, 4L);
  }

  @Test
  public void idleHeartbeatAndFloorToCheckpointReconnectNearCapacityAdvancesCheckpointWithoutOverflow() {
    float[] vec = new float[] {1.0f, 0.0f, 0.0f, 0.0f};
    int cTail = 32;
    ChordalVoronoiRouter router = new ChordalVoronoiRouter(4);
    RvvMonotonicVectorDispatcher dispatcher =
        new RvvMonotonicVectorDispatcher(router, cTail, null, 64);
    ContinuousVectorQueryService service = new ContinuousVectorQueryService(router, dispatcher);
    service.registerSubscription("sub-fast", vec, 0.90f);
    service.registerSubscription("sub-lagging", vec, 0.90f);
    service.registerSubscriberForCheckpoint("sub-fast");

    RvvClientGate fastGate = new RvvClientGate();
    fastGate.registerSubscription("sub-fast");
    service.addEventSink(fastGate::tryAccept);

    RvvClientGate lagGate = new RvvClientGate();
    lagGate.registerSubscription("sub-lagging");
    List<ContinuousVectorEvent> lagDelivered = new ArrayList<>();
    lagGate.addListener(ev -> {
      if ("sub-lagging".equals(ev.getSubscriptionId())) {
        lagDelivered.add(ev);
      }
    });
    ContinuousVectorEventSink lagSink = lagGate::tryAccept;
    service.addEventSink(lagSink);

    int bucketId = 28;
    String memberId = "srv-idle-cap";
    for (long v = 1L; v <= 10L; v++) {
      if (v == 9L) {
        service.removeEventSink(lagSink); // sub-lagging disconnects at V_client = 8
      }
      service.onBucketMutation(bucketId, memberId, v, "k-" + v, vec, true);
    }
    assertThat(lagGate.getSafeCheckpointVersion(bucketId, memberId)).isEqualTo(8L);

    // Compact V_floor=6 and advance logical V_ckpt=10 -> retained prefix R_ret = |(6, 10]| = 4
    service.acknowledgeCheckpoint(bucketId, memberId, 6L);
    service.recordReplicatedCheckpoint(bucketId, memberId, 10L);
    assertThat(service.getReplayFloorVersion(bucketId, memberId)).isEqualTo(6L);
    assertThat(service.getCheckpointVersion(bucketId, memberId)).isEqualTo(10L);
    assertThat(dispatcher.getTailSize(bucketId)).isEqualTo(4);

    // Append 27 more mutations (v=11..37, below H=64 count trigger!) -> total occupancy = 4 + 27 =
    // 31 == C_tail - 1 with 0 overflow!
    long v0 = 11L;
    for (long v = v0; v <= 37L; v++) {
      service.onBucketMutation(bucketId, memberId, v, "k-" + v, vec, true);
    }
    assertThat(dispatcher.getTailSize(bucketId)).isEqualTo(cTail - 1);
    assertThat(dispatcher.getTailOverflowCount()).isZero();
    assertThat(service.isResyncRequired(bucketId, memberId)).isFalse();

    // Flush idle heartbeat covering v=37 (>= v0=11) while tail is at C_tail - 1 and reconnect
    // sub-lagging at V_client=8 in V_floor=6 < V_client=8 < V_ckpt=10!
    long hbBefore = dispatcher.getTotalHeartbeatsDispatched();
    int flushed = service.flushIdleHeartbeats(System.currentTimeMillis() + 500L, 100L);
    assertThat(flushed).isEqualTo(1);
    assertThat(dispatcher.getTotalHeartbeatsDispatched()).isEqualTo(hbBefore + 1L);
    assertThat(fastGate.getSafeCheckpointVersion(bucketId, memberId)).isEqualTo(37L);

    service.admitAndReplayForReconnect("sub-lagging", bucketId,
        Collections.singletonMap(memberId, 8L), lagSink, null);
    assertThat(lagDelivered).hasSize(37);
    assertThat(lagDelivered).extracting(ContinuousVectorEvent::getRegionVersion)
        .contains(9L, 10L, 11L, 37L)
        .doesNotHaveDuplicates();
    assertThat(lagGate.getSafeCheckpointVersion(bucketId, memberId)).isEqualTo(37L);

    // Both healthy subscribers ACK covering heartbeat v=37 -> V_ckpt >= v0 (37), V_floor=37,
    // tailSize=0, 0 overflow!
    service.acknowledgeSubscriberCheckpoint("sub-fast", bucketId, memberId, 37L);
    service.acknowledgeSubscriberCheckpoint("sub-lagging", bucketId, memberId, 37L);
    assertThat(service.getCheckpointVersion(bucketId, memberId)).isGreaterThanOrEqualTo(v0)
        .isEqualTo(37L);
    assertThat(service.getReplayFloorVersion(bucketId, memberId)).isEqualTo(37L);
    assertThat(dispatcher.getTailSize(bucketId)).isZero();
    assertThat(dispatcher.getTailOverflowCount()).isZero();
    assertThat(service.isResyncRequired(bucketId, memberId)).isFalse();
  }

  private static void assertDataStrictlyBeforeEqualVersionHeartbeat(
      List<ContinuousVectorEvent> wireEvents) {
    Set<String> seenHeartbeats = new HashSet<>();
    for (ContinuousVectorEvent ev : wireEvents) {
      String coord = ev.getBucketId() + "|" + ev.getMemberId() + "|" + ev.getRegionVersion();
      if (ev.isHeartbeat()) {
        seenHeartbeats.add(coord);
      } else {
        assertThat(seenHeartbeats).doesNotContain(coord);
      }
    }
  }
}
