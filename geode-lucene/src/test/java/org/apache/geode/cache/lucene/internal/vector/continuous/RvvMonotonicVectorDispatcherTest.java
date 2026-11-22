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
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
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
      AtomicInteger singleEventFallbackCalls = new AtomicInteger(0);
      service.addEventSink(new ContinuousVectorEventSink() {
        @Override
        public void onEvent(ContinuousVectorEvent event) {
          singleEventFallbackCalls.incrementAndGet();
          clientGate.tryAccept(event);
        }

        @Override
        public void onEventBatch(List<ContinuousVectorEvent> events) {
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
}
