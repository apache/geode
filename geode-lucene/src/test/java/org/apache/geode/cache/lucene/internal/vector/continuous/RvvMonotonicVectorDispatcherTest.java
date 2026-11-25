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
      assertThat(ckpt).isZero();
      assertThat(maxTail).isEqualTo(4);
      evictedVersions.add(evictedVersion);
    });

    for (long v = 1L; v <= 7L; v++) {
      dispatcher.onBucketMutation(3, "srv-overflow", v, 1, "k-" + v, new float[] {1, 0, 0, 0},
          false);
    }

    assertThat(dispatcher.getTailSize(3)).isEqualTo(4);
    assertThat(dispatcher.getTailOverflowCount()).isEqualTo(3L);
    assertThat(evictedVersions).containsExactly(1L, 2L, 3L);
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
