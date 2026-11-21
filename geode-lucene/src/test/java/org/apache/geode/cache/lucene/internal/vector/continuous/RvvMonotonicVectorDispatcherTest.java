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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.Test;
import org.junit.experimental.categories.Category;

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
}
