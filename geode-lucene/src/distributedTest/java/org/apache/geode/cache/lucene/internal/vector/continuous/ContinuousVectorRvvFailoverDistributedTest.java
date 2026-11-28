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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.lucene.internal.vector.continuous.ChordalVoronoiRouter.RouterConfig;
import org.apache.geode.cache.lucene.internal.vector.continuous.ChordalVoronoiRouter.RoutingMode;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.LuceneTest;

/**
 * 4-node failover chaos DUnit test (1 locator + 3 data servers, {@code PARTITION_REDUNDANT} with
 * {@code redundantCopies=1}) verifying Theorem 1 (Zero-False-Negative Chordal Voronoi pruning) and
 * Theorem 3 (RVV-monotonic failover completeness and idempotent deduplication) when a primary
 * server is forcibly killed mid-ingestion.
 */
@Category({LuceneTest.class})
public class ContinuousVectorRvvFailoverDistributedTest implements Serializable {
  private static final long serialVersionUID = 1L;

  private static final String REGION_NAME = "continuousVectorDocs";
  private static final int VECTOR_DIM = 32;
  private static final int NUM_BUCKETS = 16;
  private static final int NUM_SUBSCRIPTIONS = 40;
  private static final int PHASE1_KEYS = 80;
  private static final int TOTAL_KEYS = 160;

  // Per-JVM server state (isolated inside each DUnit server VM)
  private static volatile ContinuousVectorQueryService serverService;
  private static volatile ExecutorService serverReplayExecutor;
  private static volatile ExecutorService serverDispatchExecutor;
  private static volatile TcpEventSink serverTcpSink;
  private static volatile TcpEventSink serverTcpSinkB;
  private static final AtomicBoolean dropOutboundEvents = new AtomicBoolean(false);
  private static final AtomicBoolean dropOutboundEventsB = new AtomicBoolean(false);
  private static final AtomicInteger phase2WrittenCount = new AtomicInteger(0);
  private static final AtomicBoolean promotionCompleteFlag = new AtomicBoolean(false);
  private static final AtomicBoolean reconnectStartedFlag = new AtomicBoolean(false);

  @Rule
  public transient ClusterStartupRule cluster = new ClusterStartupRule();

  private transient ClientEventReceiver receiver;
  private transient ClientEventReceiver receiverB;

  @After
  public void tearDown() {
    if (receiver != null) {
      receiver.close();
    }
    if (receiverB != null) {
      receiverB.close();
    }
    IgnoredException.removeAllExpectedExceptions();
  }

  @Test
  public void primaryKillMidIngestionGuaranteesZeroMissedEventsZeroDuplicatesAndStrictRvvMonotonicity()
      throws Exception {
    // Expect crash-induced log messages when forcibly killing server1
    IgnoredException.addIgnoredException("MemberDepartedException");
    IgnoredException.addIgnoredException("ForcedDisconnectException");
    IgnoredException.addIgnoredException("PartitionOfflineException");
    IgnoredException.addIgnoredException("CacheClosedException");
    IgnoredException.addIgnoredException("CancelException");
    IgnoredException.addIgnoredException("SocketException");
    IgnoredException.addIgnoredException("EOFException");
    IgnoredException.addIgnoredException("IOException");
    IgnoredException.addIgnoredException("Membership: lead member");
    IgnoredException.addIgnoredException("crashed");

    // 1. Start embedded client RVV gate receiver on ephemeral localhost port
    RvvClientGate clientGate = new RvvClientGate();
    List<ContinuousVectorEvent> acceptedEvents = new CopyOnWriteArrayList<>();
    clientGate.addListener(acceptedEvents::add);
    receiver = new ClientEventReceiver(clientGate);
    int receiverPort = receiver.getPort();

    // 2. Start 4-node Geode cluster: 1 locator + 3 data servers
    MemberVM locator = cluster.startLocatorVM(0);
    int locatorPort = locator.getPort();
    MemberVM server1 = cluster.startServerVM(1, locatorPort);
    MemberVM server2 = cluster.startServerVM(2, locatorPort);
    MemberVM server3 = cluster.startServerVM(3, locatorPort);

    // 3. Build deterministic subscriptions (40 matching + 1 healthy 0%-match subscriber)
    // Put all topic & document vectors in dims 0..(VECTOR_DIM-2) with last coordinate = 0 so
    // "sub-zero-match" aligned along e_{VECTOR_DIM-1} is strictly orthogonal (0% match rate).
    Random rng = new Random(2026L);
    float[][] topics = new float[8][VECTOR_DIM];
    for (int t = 0; t < topics.length; t++) {
      topics[t] = randomUnitVectorZeroLastDim(rng, VECTOR_DIM);
    }

    ArrayList<VectorSubscription> subscriptions = new ArrayList<>(NUM_SUBSCRIPTIONS + 1);
    for (int m = 0; m < NUM_SUBSCRIPTIONS; m++) {
      float[] q = perturbVectorZeroLastDim(rng, topics[m % topics.length], 0.08f);
      float threshold = 0.78f + (m % 5) * 0.03f;
      subscriptions.add(new VectorSubscription("sub-" + m, q, threshold));
    }
    float[] orthogonalQuery = new float[VECTOR_DIM];
    orthogonalQuery[VECTOR_DIM - 1] = 1.0f;
    String zeroMatchSubId = "sub-zero-match";
    subscriptions.add(new VectorSubscription(zeroMatchSubId, orthogonalQuery, 0.99f));

    for (VectorSubscription sub : subscriptions) {
      clientGate.registerSubscription(sub.getSubscriptionId());
    }

    ArrayList<float[]> allVectors = new ArrayList<>(TOTAL_KEYS);
    for (int i = 0; i < TOTAL_KEYS; i++) {
      // Mix topic-aligned vectors (guaranteed matches) with random background vectors
      float[] v = (i % 2 == 0)
          ? perturbVectorZeroLastDim(rng, topics[i % topics.length], 0.09f)
          : randomUnitVectorZeroLastDim(rng, VECTOR_DIM);
      allVectors.add(v);
    }

    // Compute exact brute-force ground-truth match sets for Phase 1 and full run
    Set<String> phase1ExpectedPairs =
        computeBruteForceMatches(subscriptions, allVectors, 0, PHASE1_KEYS);
    Set<String> totalExpectedPairs =
        computeBruteForceMatches(subscriptions, allVectors, 0, TOTAL_KEYS);
    assertThat(phase1ExpectedPairs).isNotEmpty();
    assertThat(totalExpectedPairs.size()).isGreaterThan(phase1ExpectedPairs.size());

    // 4. Configure ContinuousVectorQueryService + PARTITION_REDUNDANT(redundantCopies=1) on all 3
    // servers
    for (MemberVM server : new MemberVM[] {server1, server2, server3}) {
      server.invoke(() -> initializeServerNode(subscriptions, receiverPort));
    }

    // Confirm CHORDAL_VORONOI mode is active on all data servers
    for (MemberVM server : new MemberVM[] {server1, server2, server3}) {
      String modeName = server.invoke(() -> serverService.getRouter().getCurrentMode().name());
      assertThat(modeName).isEqualTo(RoutingMode.CHORDAL_VORONOI.name());
    }

    // 5. Phase 1: Ingest keys 0..79 from server3 and wait until client receives all Phase 1 matches
    server3.invoke(() -> {
      Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
      for (int i = 0; i < PHASE1_KEYS; i++) {
        region.put("doc-" + i, allVectors.get(i));
      }
    });

    await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
      Set<String> receivedPairs = collectKeySubPairs(acceptedEvents);
      assertThat(receivedPairs).containsAll(phase1ExpectedPairs);
    });

    // Ensure server1 actually owns primary buckets so crashing it triggers real primary failover
    int server1Primaries = server1.invoke(() -> {
      PartitionedRegion pr =
          (PartitionedRegion) ClusterStartupRule.getCache().getRegion(REGION_NAME);
      return pr.getDataStore().getAllLocalPrimaryBucketIds().size();
    });
    assertThat(server1Primaries)
        .as("Victim server1 must host primary buckets before failover")
        .isGreaterThan(0);

    // 6. Phase 2: Stall server1's outbound delivery (simulating unreceived pre-crash tail) while
    // server1 still replicates to secondary buckets on server2 & server3, then crash server1
    // mid-ingestion!
    server1.invoke(() -> dropOutboundEvents.set(true));

    // Launch background ingestion of keys 80..159 on server3
    server3.invoke(() -> phase2WrittenCount.set(0));
    Set<String> retriedKeys = Collections.synchronizedSet(new HashSet<>());
    Thread ingestionThread = new Thread(() -> {
      ArrayList<String> retried = server3.invoke(() -> {
        ArrayList<String> localRetried = new ArrayList<>();
        Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
        for (int i = PHASE1_KEYS; i < TOTAL_KEYS; i++) {
          String key = "doc-" + i;
          float[] vec = allVectors.get(i);
          boolean stored = false;
          for (int attempt = 0; attempt < 15 && !stored; attempt++) {
            try {
              region.put(key, vec);
              stored = true;
            } catch (RuntimeException e) {
              localRetried.add(key);
              try {
                Thread.sleep(100L);
              } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
              }
            }
          }
          if (!stored) {
            throw new IllegalStateException("Failed to store key after failover retries: " + key);
          }
          phase2WrittenCount.incrementAndGet();
          try {
            Thread.sleep(15L);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            break;
          }
        }
        return localRetried;
      });
      retriedKeys.addAll(retried);
    }, "mid-ingestion-writer");
    ingestionThread.start();

    // Wait until Phase 2 ingestion is actively underway (at least 25 keys written)
    await().atMost(20, TimeUnit.SECONDS)
        .until(() -> server3.invoke(() -> phase2WrittenCount.get()) >= 25);

    // Forcibly kill primary server1 mid-ingestion!
    cluster.crashVM(1);

    // Wait for ingestion of all 160 keys to complete across surviving servers 2 and 3
    ingestionThread.join(60_000L);
    assertThat(ingestionThread.isAlive())
        .as("Mid-ingestion writer thread should complete cleanly after failover")
        .isFalse();

    // Verify all 160 keys are durably present in the surviving partitioned region
    int survivingSize = server3.invoke(() -> {
      Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
      return region.size();
    });
    assertThat(survivingSize).isEqualTo(TOTAL_KEYS);

    // 7. Wait for promoted secondaries on server2 & server3 to replay uncertain tail and deliver
    // every single matching notification plus promotion watermark heartbeats
    await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
      Set<String> receivedPairs = collectKeySubPairs(acceptedEvents);
      assertThat(receivedPairs).containsAll(totalExpectedPairs);
      assertThat(clientGate.getHeartbeatsAcceptedCount()).isGreaterThan(0L);
    });

    // Brief settle window to ensure no delayed duplicate events sneak past the client gate
    Thread.sleep(500L);

    // 8. Verify ALL formal guarantees (Theorem 1 + Proposition 1 + Theorem 3):
    // (a) Zero missed notifications (FNR == 0) AND zero false positives (FPR == 0)
    Set<String> actualPairs = collectKeySubPairs(acceptedEvents);
    assertThat(actualPairs)
        .as("Zero missed notifications and zero false positives across primary failover")
        .containsExactlyInAnyOrderElementsOf(totalExpectedPairs);

    // (b) Zero duplicate accepted deliveries
    Set<String> seenRvvCoords = new HashSet<>();
    Map<String, Integer> pairCounts = new HashMap<>();
    boolean sawReplayedAccepted = false;
    for (ContinuousVectorEvent e : acceptedEvents) {
      String rvvCoord = e.getSubscriptionId() + "|" + e.getBucketId() + "|" + e.getMemberId() + "|"
          + e.getRegionVersion();
      assertThat(seenRvvCoords.add(rvvCoord))
          .as("Duplicate RVV event accepted: %s", rvvCoord)
          .isTrue();
      String pair = e.getKey() + "|" + e.getSubscriptionId();
      pairCounts.merge(pair, 1, Integer::sum);
      if (e.isReplayed()) {
        sawReplayedAccepted = true;
      }
    }
    for (Map.Entry<String, Integer> entry : pairCounts.entrySet()) {
      String key = entry.getKey().substring(0, entry.getKey().indexOf('|'));
      if (!retriedKeys.contains(key)) {
        assertThat(entry.getValue())
            .as("Zero duplicate accepted deliveries for non-retried key/sub pair %s",
                entry.getKey())
            .isEqualTo(1);
      }
    }

    // (c) Verify failover replay actually recovered dropped server1 mutations AND client gate
    // actively deduplicated replayed Phase 1 overlap
    assertThat(sawReplayedAccepted)
        .as("Promoted secondary must have replayed unreceived tail mutations from crashed server1")
        .isTrue();
    assertThat(clientGate.getDiscardedCount())
        .as("RVV client gate must deterministically discard replayed duplicates in [V_ckpt, V_client]")
        .isGreaterThan(0L);

    // (d) Strict per-(subscriptionId, bucketId, memberId) RVV monotonicity
    List<String> monotonicityViolations = RvvClientGate.verifyStrictMonotonicity(acceptedEvents);
    assertThat(monotonicityViolations)
        .as("Strict per-(subscriptionId, bucketId, memberId) RVV monotonicity must hold")
        .isEmpty();

    // (e) Verify Chordal Voronoi pruning actively pruned cells on surviving servers
    double server2PruneRatio =
        server2.invoke(() -> serverService.getTelemetry().getPruneRatio());
    double server3PruneRatio =
        server3.invoke(() -> serverService.getTelemetry().getPruneRatio());
    assertThat(Math.max(server2PruneRatio, server3PruneRatio))
        .as("Chordal Voronoi routing should actively prune cells on surviving servers")
        .isGreaterThan(0.0d);

    // (f) Verify RVV watermark heartbeats advanced safe checkpoints across active subscriptions
    // (including the healthy 0%-match subscriber) without leaking heartbeats to match listeners
    assertThat(clientGate.getHeartbeatsAcceptedCount())
        .as("Periodic and promotion RVV watermark heartbeats must be received by the client gate")
        .isGreaterThan(0L);
    assertThat(acceptedEvents)
        .as("Watermarks must not be delivered to match listeners and 0%%-match subscriber must have 0 matches")
        .noneMatch(e -> e.isHeartbeat() || zeroMatchSubId.equals(e.getSubscriptionId()));
    Map<RvvStreamKey, Long> safeCheckpoints =
        clientGate.getSafeCheckpointSnapshot();
    long positiveCheckpoints =
        safeCheckpoints.values().stream().filter(v -> v != null && v > 0L).count();
    assertThat(positiveCheckpoints)
        .as("At least every failed-over primary bucket must advance safe V_ckpt > 0 even with a 0%%-match subscriber")
        .isGreaterThanOrEqualTo(server1Primaries);

    // Cleanup server executors & TCP sinks on surviving VMs
    for (MemberVM survivor : new MemberVM[] {server2, server3}) {
      survivor.invoke(ContinuousVectorRvvFailoverDistributedTest::shutdownServerNode);
    }
  }

  @Test
  public void slowReconnectAfterPrimaryKillWithConcurrentPromotedWritesGuaranteesZeroLoss()
      throws Exception {
    IgnoredException.addIgnoredException("MemberDepartedException");
    IgnoredException.addIgnoredException("ForcedDisconnectException");
    IgnoredException.addIgnoredException("PartitionOfflineException");
    IgnoredException.addIgnoredException("CacheClosedException");
    IgnoredException.addIgnoredException("CancelException");
    IgnoredException.addIgnoredException("SocketException");
    IgnoredException.addIgnoredException("EOFException");
    IgnoredException.addIgnoredException("IOException");
    IgnoredException.addIgnoredException("Membership: lead member");
    IgnoredException.addIgnoredException("crashed");

    RvvClientGate clientGate = new RvvClientGate();
    List<ContinuousVectorEvent> acceptedEvents = new CopyOnWriteArrayList<>();
    clientGate.addListener(acceptedEvents::add);
    receiver = new ClientEventReceiver(clientGate);
    int receiverPort = receiver.getPort();

    MemberVM locator = cluster.startLocatorVM(0);
    int locatorPort = locator.getPort();
    MemberVM server1 = cluster.startServerVM(1, locatorPort);
    MemberVM server2 = cluster.startServerVM(2, locatorPort);
    MemberVM server3 = cluster.startServerVM(3, locatorPort);

    Random rng = new Random(20261124L);
    float[][] topics = new float[8][VECTOR_DIM];
    for (int t = 0; t < topics.length; t++) {
      topics[t] = randomUnitVectorZeroLastDim(rng, VECTOR_DIM);
    }

    ArrayList<VectorSubscription> subscriptions = new ArrayList<>(NUM_SUBSCRIPTIONS + 1);
    for (int m = 0; m < NUM_SUBSCRIPTIONS; m++) {
      float[] q = perturbVectorZeroLastDim(rng, topics[m % topics.length], 0.08f);
      float threshold = 0.78f + (m % 5) * 0.03f;
      subscriptions.add(new VectorSubscription("sub-" + m, q, threshold));
    }
    float[] orthogonalQuery = new float[VECTOR_DIM];
    orthogonalQuery[VECTOR_DIM - 1] = 1.0f;
    String zeroMatchSubId = "sub-zero-match";
    subscriptions.add(new VectorSubscription(zeroMatchSubId, orthogonalQuery, 0.99f));
    for (VectorSubscription sub : subscriptions) {
      clientGate.registerSubscription(sub.getSubscriptionId());
    }

    final int reconnectTotalKeys = 240;
    ArrayList<float[]> allVectors = new ArrayList<>(reconnectTotalKeys);
    for (int i = 0; i < reconnectTotalKeys; i++) {
      float[] v = (i % 2 == 0)
          ? perturbVectorZeroLastDim(rng, topics[i % topics.length], 0.09f)
          : randomUnitVectorZeroLastDim(rng, VECTOR_DIM);
      allVectors.add(v);
    }
    Set<String> phase1ExpectedPairs =
        computeBruteForceMatches(subscriptions, allVectors, 0, PHASE1_KEYS);
    Set<String> totalExpectedPairs =
        computeBruteForceMatches(subscriptions, allVectors, 0, reconnectTotalKeys);

    for (MemberVM server : new MemberVM[] {server1, server2, server3}) {
      server.invoke(() -> initializeServerNode(subscriptions, receiverPort));
    }

    // Phase 1: Ingest doc-0..doc-79 and confirm client receives all Phase 1 matches
    server3.invoke(() -> {
      Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
      for (int i = 0; i < PHASE1_KEYS; i++) {
        region.put("doc-" + i, allVectors.get(i));
      }
    });
    await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
      assertThat(collectKeySubPairs(acceptedEvents)).containsAll(phase1ExpectedPairs);
    });

    Set<Integer> server1PrimaryBuckets = server1.invoke(() -> {
      PartitionedRegion pr =
          (PartitionedRegion) ClusterStartupRule.getCache().getRegion(REGION_NAME);
      return new HashSet<>(pr.getDataStore().getAllLocalPrimaryBucketIds());
    });
    assertThat(server1PrimaryBuckets).isNotEmpty();
    String server1MemberId = server1.invoke(() -> {
      PartitionedRegion pr =
          (PartitionedRegion) ClusterStartupRule.getCache().getRegion(REGION_NAME);
      int anyBucket = pr.getDataStore().getAllLocalPrimaryBucketIds().iterator().next();
      return pr.getDataStore().getLocalBucketById(anyBucket).getVersionMember().toString();
    });
    long s1DispatchedBeforePhase2 =
        server1.invoke(() -> serverService.getDispatcher().getTotalEventsDispatched());

    // Simulate slow client disconnect across ALL servers (pre-crash m_P tail + promotion replay +
    // post-promotion m_S writes during the >=1500 ms reconnect gap do not reach the client socket)
    for (MemberVM server : new MemberVM[] {server1, server2, server3}) {
      server.invoke(() -> dropOutboundEvents.set(true));
    }

    server3.invoke(() -> {
      phase2WrittenCount.set(0);
      promotionCompleteFlag.set(false);
      reconnectStartedFlag.set(false);
    });
    Set<String> retriedKeys = Collections.synchronizedSet(new HashSet<>());
    Thread ingestionThread = new Thread(() -> {
      ArrayList<String> retried = server3.invoke(() -> {
        ArrayList<String> localRetried = new ArrayList<>();
        Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
        for (int i = PHASE1_KEYS; i < reconnectTotalKeys; i++) {
          if (i == 160) {
            long deadline = System.currentTimeMillis() + 45_000L;
            while (!promotionCompleteFlag.get() && System.currentTimeMillis() < deadline) {
              try {
                Thread.sleep(25L);
              } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return localRetried;
              }
            }
          } else if (i == 200) {
            long deadline = System.currentTimeMillis() + 45_000L;
            while (!reconnectStartedFlag.get() && System.currentTimeMillis() < deadline) {
              try {
                Thread.sleep(20L);
              } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return localRetried;
              }
            }
          }

          String key = "doc-" + i;
          float[] vec = allVectors.get(i);
          boolean stored = false;
          for (int attempt = 0; attempt < 30 && !stored; attempt++) {
            try {
              region.put(key, vec);
              stored = true;
            } catch (RuntimeException e) {
              localRetried.add(key);
              try {
                Thread.sleep(100L);
              } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return localRetried;
              }
            }
          }
          if (!stored) {
            throw new IllegalStateException("Failed to store key after failover retries: " + key);
          }
          phase2WrittenCount.incrementAndGet();
          long paceMs = (i < 120 || i >= 200) ? 15L : (i < 160 ? 90L : 35L);
          try {
            Thread.sleep(paceMs);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            break;
          }
        }
        return localRetried;
      });
      retriedKeys.addAll(retried);
    }, "slow-reconnect-writer");
    ingestionThread.start();

    await().atMost(30, TimeUnit.SECONDS)
        .until(() -> server3.invoke(() -> phase2WrittenCount.get()) >= 35
            && server1.invoke(() -> serverService.getDispatcher()
                .getTotalEventsDispatched()) > s1DispatchedBeforePhase2);
    cluster.crashVM(1);

    // Wait until GMS finishes detecting server1's crash and survivors promote all crashed
    // primaries (and drain async afterPrimary replay tasks while the client is still disconnected)
    await().atMost(30, TimeUnit.SECONDS).until(() -> {
      Set<Integer> survivorPrimaries = new HashSet<>();
      survivorPrimaries.addAll(server2.invoke(() -> {
        PartitionedRegion pr =
            (PartitionedRegion) ClusterStartupRule.getCache().getRegion(REGION_NAME);
        return new HashSet<>(pr.getDataStore().getAllLocalPrimaryBucketIds());
      }));
      survivorPrimaries.addAll(server3.invoke(() -> {
        PartitionedRegion pr =
            (PartitionedRegion) ClusterStartupRule.getCache().getRegion(REGION_NAME);
        return new HashSet<>(pr.getDataStore().getAllLocalPrimaryBucketIds());
      }));
      long promotions =
          server2.invoke(() -> serverService.getDispatcher().getTotalPromotionsHandled())
              + server3.invoke(() -> serverService.getDispatcher().getTotalPromotionsHandled());
      return survivorPrimaries.size() == NUM_BUCKETS
          && survivorPrimaries.containsAll(server1PrimaryBuckets)
          && promotions >= server1PrimaryBuckets.size();
    });
    for (MemberVM survivor : new MemberVM[] {server2, server3}) {
      survivor.invoke(() -> {
        try {
          serverReplayExecutor.submit(() -> {
          }).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
    }
    server3.invoke(() -> promotionCompleteFlag.set(true));

    // Hold client disconnected for >= 1500 ms post-promotion while m_S writes land into promoted
    // buckets with dropOutboundEvents still true
    Thread.sleep(1500L);
    await().atMost(30, TimeUnit.SECONDS)
        .until(() -> server3.invoke(() -> phase2WrittenCount.get()) >= (195 - PHASE1_KEYS));

    // Client reconnects while Phase 3 (doc-200..239) m_S ingestion executes concurrently!
    HashMap<Integer, HashMap<String, Long>> clientHighWaterByBucket = new HashMap<>();
    for (int b = 0; b < NUM_BUCKETS; b++) {
      clientHighWaterByBucket.put(b, new HashMap<>(clientGate.getSafeHighWaterByMember(b)));
    }
    assertThat(ingestionThread.isAlive()).isTrue();
    server3.invoke(() -> reconnectStartedFlag.set(true));
    for (MemberVM survivor : new MemberVM[] {server2, server3}) {
      survivor.invoke(() -> {
        serverTcpSink.enableReconnectBucketGate();
        dropOutboundEvents.set(false);
        PartitionedRegion pr =
            (PartitionedRegion) ClusterStartupRule.getCache().getRegion(REGION_NAME);
        for (Integer b : pr.getDataStore().getAllLocalPrimaryBucketIds()) {
          Map<String, Long> hw = clientHighWaterByBucket.get(b);
          serverService.replayUncheckpointedForReconnect(b,
              hw != null ? hw : Collections.emptyMap(),
              () -> serverTcpSink.allowLiveBucket(b));
        }
      });
    }

    ingestionThread.join(60_000L);
    assertThat(ingestionThread.isAlive()).isFalse();

    await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
      assertThat(collectKeySubPairs(acceptedEvents)).containsAll(totalExpectedPairs);
      assertThat(clientGate.getHeartbeatsAcceptedCount()).isGreaterThan(0L);
    });
    Thread.sleep(500L);

    Set<String> actualPairs = collectKeySubPairs(acceptedEvents);
    assertThat(actualPairs).containsExactlyInAnyOrderElementsOf(totalExpectedPairs);

    Set<String> seenRvvCoords = new HashSet<>();
    Map<String, Integer> pairCounts = new HashMap<>();
    boolean sawReplayedPreCrash = false;
    boolean sawReplayedPostPromotion = false;
    for (ContinuousVectorEvent e : acceptedEvents) {
      String rvvCoord = e.getSubscriptionId() + "|" + e.getBucketId() + "|" + e.getMemberId() + "|"
          + e.getRegionVersion();
      assertThat(seenRvvCoords.add(rvvCoord)).isTrue();
      pairCounts.merge(e.getKey() + "|" + e.getSubscriptionId(), 1, Integer::sum);
      if (e.isReplayed()) {
        if (server1MemberId.equals(e.getMemberId())) {
          sawReplayedPreCrash = true;
        } else {
          sawReplayedPostPromotion = true;
        }
      }
    }
    for (Map.Entry<String, Integer> entry : pairCounts.entrySet()) {
      String key = entry.getKey().substring(0, entry.getKey().indexOf('|'));
      if (!retriedKeys.contains(key)) {
        assertThat(entry.getValue()).isEqualTo(1);
      }
    }

    assertThat(sawReplayedPreCrash)
        .as("Reconnect replay must recover uncheckpointed pre-crash m_P tail events")
        .isTrue();
    assertThat(sawReplayedPostPromotion)
        .as("Reconnect replay must recover uncheckpointed post-promotion m_S tail events")
        .isTrue();
    assertThat(RvvClientGate.verifyStrictMonotonicity(acceptedEvents)).isEmpty();
    assertThat(acceptedEvents)
        .noneMatch(e -> e.isHeartbeat() || zeroMatchSubId.equals(e.getSubscriptionId()));

    Map<RvvStreamKey, Long> safeCheckpoints = clientGate.getSafeCheckpointSnapshot();
    long positiveCheckpoints =
        safeCheckpoints.values().stream().filter(v -> v != null && v > 0L).count();
    assertThat(positiveCheckpoints).isGreaterThanOrEqualTo(server1PrimaryBuckets.size());

    for (MemberVM survivor : new MemberVM[] {server2, server3}) {
      survivor.invoke(ContinuousVectorRvvFailoverDistributedTest::shutdownServerNode);
    }
  }

  @Test
  public void slowReconnectWithFourThreadExecutorTwoClientsAndMutation64Boundary()
      throws Exception {
    IgnoredException.addIgnoredException("MemberDepartedException");
    IgnoredException.addIgnoredException("ForcedDisconnectException");
    IgnoredException.addIgnoredException("PartitionOfflineException");
    IgnoredException.addIgnoredException("CacheClosedException");
    IgnoredException.addIgnoredException("CancelException");
    IgnoredException.addIgnoredException("SocketException");
    IgnoredException.addIgnoredException("EOFException");
    IgnoredException.addIgnoredException("IOException");
    IgnoredException.addIgnoredException("Membership: lead member");
    IgnoredException.addIgnoredException("crashed");

    RvvClientGate clientGateA = new RvvClientGate();
    RvvClientGate clientGateB = new RvvClientGate();
    List<ContinuousVectorEvent> acceptedA = new CopyOnWriteArrayList<>();
    List<ContinuousVectorEvent> acceptedB = new CopyOnWriteArrayList<>();
    clientGateA.addListener(acceptedA::add);
    clientGateB.addListener(acceptedB::add);
    receiver = new ClientEventReceiver(clientGateA);
    receiverB = new ClientEventReceiver(clientGateB);
    int portA = receiver.getPort();
    int portB = receiverB.getPort();

    MemberVM locator = cluster.startLocatorVM(0);
    int locatorPort = locator.getPort();
    MemberVM server1 = cluster.startServerVM(1, locatorPort);
    MemberVM server2 = cluster.startServerVM(2, locatorPort);
    MemberVM server3 = cluster.startServerVM(3, locatorPort);

    Random rng = new Random(20261125L);
    float[][] topics = new float[8][VECTOR_DIM];
    for (int t = 0; t < topics.length; t++) {
      topics[t] = randomUnitVectorZeroLastDim(rng, VECTOR_DIM);
    }

    ArrayList<VectorSubscription> subscriptions = new ArrayList<>(NUM_SUBSCRIPTIONS + 1);
    for (int m = 0; m < NUM_SUBSCRIPTIONS; m++) {
      float[] q = perturbVectorZeroLastDim(rng, topics[m % topics.length], 0.08f);
      float threshold = 0.78f + (m % 5) * 0.03f;
      subscriptions.add(new VectorSubscription("sub-" + m, q, threshold));
    }
    float[] orthogonalQuery = new float[VECTOR_DIM];
    orthogonalQuery[VECTOR_DIM - 1] = 1.0f;
    String zeroMatchSubId = "sub-zero-match";
    subscriptions.add(new VectorSubscription(zeroMatchSubId, orthogonalQuery, 0.99f));
    for (VectorSubscription sub : subscriptions) {
      clientGateA.registerSubscription(sub.getSubscriptionId());
      clientGateB.registerSubscription(sub.getSubscriptionId());
    }

    final int test3Buckets = 8;
    final int phase1aKeys = 40;
    final int maxPhase1bEnd = 1000;
    final int phase2And3Count = 160;
    final int maxTotalKeys = maxPhase1bEnd + phase2And3Count;
    ArrayList<float[]> allVectors = new ArrayList<>(maxTotalKeys);
    for (int i = 0; i < maxTotalKeys; i++) {
      // Topic-aligned vectors guarantee every doc including mutation #64 matches subscriptions
      allVectors.add(perturbVectorZeroLastDim(rng, topics[i % topics.length], 0.08f));
    }

    for (MemberVM server : new MemberVM[] {server1, server2, server3}) {
      server.invoke(() -> initializeServerNode(subscriptions, portA, portB, test3Buckets,
          RvvMonotonicVectorDispatcher.DEFAULT_HEARTBEAT_INTERVAL_EVENTS, 4));
    }

    // Phase 1a: Ingest doc-0..doc-39 with BOTH clients connected (< 64 writes per bucket yet)
    server3.invoke(() -> {
      Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
      for (int i = 0; i < phase1aKeys; i++) {
        region.put("doc-" + i, allVectors.get(i));
      }
    });
    Set<String> phase1aExpectedPairs =
        computeBruteForceMatches(subscriptions, allVectors, 0, phase1aKeys);
    await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
      assertThat(collectKeySubPairs(acceptedA)).containsAll(phase1aExpectedPairs);
      assertThat(collectKeySubPairs(acceptedB)).containsAll(phase1aExpectedPairs);
    });
    assertThat(clientGateA.getHeartbeatsAcceptedCount()).isZero();
    assertThat(clientGateB.getHeartbeatsAcceptedCount()).isZero();

    Set<Integer> server1PrimaryBuckets = server1.invoke(() -> {
      PartitionedRegion pr =
          (PartitionedRegion) ClusterStartupRule.getCache().getRegion(REGION_NAME);
      return new HashSet<>(pr.getDataStore().getAllLocalPrimaryBucketIds());
    });
    assertThat(server1PrimaryBuckets).isNotEmpty();
    String server1MemberId = server1.invoke(() -> {
      PartitionedRegion pr =
          (PartitionedRegion) ClusterStartupRule.getCache().getRegion(REGION_NAME);
      int anyBucket = pr.getDataStore().getAllLocalPrimaryBucketIds().iterator().next();
      return pr.getDataStore().getLocalBucketById(anyBucket).getVersionMember().toString();
    });

    // Disconnect Client B EARLY so V_client_B freezes at Phase 1a while Client A advances across
    // #64
    for (MemberVM server : new MemberVM[] {server1, server2, server3}) {
      server.invoke(() -> dropOutboundEventsB.set(true));
    }

    // Exercise real Geode afterDestroy -> onNonMatchingBucketVersion without stalling contiguous
    // RVV sequencing (negative last-dim unit vector matches 0 of the 41 subscriptions)
    server3.invoke(() -> {
      Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
      float[] noMatchVec = new float[VECTOR_DIM];
      noMatchVec[VECTOR_DIM - 1] = -1.0f;
      region.put("doc-destroy-probe", noMatchVec);
      region.destroy("doc-destroy-probe");
    });
    long nonMatchObserved =
        server1.invoke(() -> serverService.getDispatcher().getTotalNonMatchingVersionsObserved())
            + server2
                .invoke(() -> serverService.getDispatcher().getTotalNonMatchingVersionsObserved())
            + server3
                .invoke(() -> serverService.getDispatcher().getTotalNonMatchingVersionsObserved());
    assertThat(nonMatchObserved).isGreaterThanOrEqualTo(1L);

    // Phase 1b: Ingest batches until H=64 triggers on at least one bucket -> exact mutation #64
    // delivers match alerts + trailing watermarkHeartbeat(v_h) to Client A via the 4T executor!
    int cursor = phase1aKeys;
    while (cursor < maxPhase1bEnd) {
      final int startIdx = cursor;
      final int endIdx = Math.min(maxPhase1bEnd, startIdx + 32);
      server3.invoke(() -> {
        Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
        for (int i = startIdx; i < endIdx; i++) {
          region.put("doc-" + i, allVectors.get(i));
        }
      });
      cursor = endIdx;
      long hbCount =
          server1.invoke(() -> serverService.getDispatcher().getTotalHeartbeatsDispatched())
              + server2.invoke(() -> serverService.getDispatcher().getTotalHeartbeatsDispatched())
              + server3.invoke(() -> serverService.getDispatcher().getTotalHeartbeatsDispatched());
      if (hbCount > 0L) {
        break;
      }
    }
    final int phase1bEnd = cursor;
    for (MemberVM server : new MemberVM[] {server1, server2, server3}) {
      server.invoke(() -> {
        try {
          assertThat(serverService.awaitPendingDispatches(10_000L)).isTrue();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      });
    }
    Set<String> phase1bExpectedPairs =
        computeBruteForceMatches(subscriptions, allVectors, 0, phase1bEnd);
    await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
      assertThat(collectKeySubPairs(acceptedA)).containsAll(phase1bExpectedPairs);
      assertThat(clientGateA.getHeartbeatsAcceptedCount()).isGreaterThan(0L);
    });
    assertThat(clientGateB.getHeartbeatsAcceptedCount()).isZero();
    assertThat(clientGateA.getSafeCheckpointSnapshot()).isNotEmpty();
    assertThat(clientGateB.getSafeCheckpointSnapshot()).isEmpty();
    assertThat(clientGateA.getDiscardedCount()).isZero();

    // Now disconnect Client A too while preserving V_client_A != V_client_B
    for (MemberVM server : new MemberVM[] {server1, server2, server3}) {
      server.invoke(() -> dropOutboundEvents.set(true));
    }
    long s1DispatchedBeforePhase2 =
        server1.invoke(() -> serverService.getDispatcher().getTotalEventsDispatched());

    final int test3TotalKeys = phase1bEnd + phase2And3Count;
    final int gatePostPromoIdx = phase1bEnd + 80;
    final int gateReconnectIdx = phase1bEnd + 120;
    Set<String> totalExpectedPairs =
        computeBruteForceMatches(subscriptions, allVectors, 0, test3TotalKeys);

    server3.invoke(() -> {
      phase2WrittenCount.set(0);
      promotionCompleteFlag.set(false);
      reconnectStartedFlag.set(false);
    });
    Set<String> retriedKeys = Collections.synchronizedSet(new HashSet<>());
    Thread ingestionThread = new Thread(() -> {
      ArrayList<String> retried = server3.invoke(() -> {
        ArrayList<String> localRetried = new ArrayList<>();
        Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
        for (int i = phase1bEnd; i < test3TotalKeys; i++) {
          if (i == gatePostPromoIdx) {
            long deadline = System.currentTimeMillis() + 45_000L;
            while (!promotionCompleteFlag.get() && System.currentTimeMillis() < deadline) {
              try {
                Thread.sleep(25L);
              } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return localRetried;
              }
            }
          } else if (i == gateReconnectIdx) {
            long deadline = System.currentTimeMillis() + 45_000L;
            while (!reconnectStartedFlag.get() && System.currentTimeMillis() < deadline) {
              try {
                Thread.sleep(20L);
              } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return localRetried;
              }
            }
          }
          String key = "doc-" + i;
          float[] vec = allVectors.get(i);
          boolean stored = false;
          for (int attempt = 0; attempt < 30 && !stored; attempt++) {
            try {
              region.put(key, vec);
              stored = true;
            } catch (RuntimeException e) {
              localRetried.add(key);
              try {
                Thread.sleep(100L);
              } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return localRetried;
              }
            }
          }
          if (!stored) {
            throw new IllegalStateException("Failed to store key after retries: " + key);
          }
          phase2WrittenCount.incrementAndGet();
          long paceMs = (i < phase1bEnd + 40 || i >= gateReconnectIdx) ? 15L
              : (i < gatePostPromoIdx ? 90L : 35L);
          try {
            Thread.sleep(paceMs);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            break;
          }
        }
        return localRetried;
      });
      retriedKeys.addAll(retried);
    }, "v10-dual-reconnect-writer");
    ingestionThread.start();

    await().atMost(30, TimeUnit.SECONDS)
        .until(() -> server3.invoke(() -> phase2WrittenCount.get()) >= 35
            && server1.invoke(() -> serverService.getDispatcher()
                .getTotalEventsDispatched()) > s1DispatchedBeforePhase2);
    cluster.crashVM(1);

    await().atMost(30, TimeUnit.SECONDS).until(() -> {
      Set<Integer> survivorPrimaries = new HashSet<>();
      survivorPrimaries.addAll(server2.invoke(() -> {
        PartitionedRegion pr =
            (PartitionedRegion) ClusterStartupRule.getCache().getRegion(REGION_NAME);
        return new HashSet<>(pr.getDataStore().getAllLocalPrimaryBucketIds());
      }));
      survivorPrimaries.addAll(server3.invoke(() -> {
        PartitionedRegion pr =
            (PartitionedRegion) ClusterStartupRule.getCache().getRegion(REGION_NAME);
        return new HashSet<>(pr.getDataStore().getAllLocalPrimaryBucketIds());
      }));
      long promotions =
          server2.invoke(() -> serverService.getDispatcher().getTotalPromotionsHandled())
              + server3.invoke(() -> serverService.getDispatcher().getTotalPromotionsHandled());
      return survivorPrimaries.size() == test3Buckets
          && survivorPrimaries.containsAll(server1PrimaryBuckets)
          && promotions >= server1PrimaryBuckets.size();
    });
    for (MemberVM survivor : new MemberVM[] {server2, server3}) {
      survivor.invoke(() -> {
        try {
          serverReplayExecutor.submit(() -> {
          }).get(10, TimeUnit.SECONDS);
          assertThat(serverService.awaitPendingDispatches(10_000L)).isTrue();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
    }
    server3.invoke(() -> promotionCompleteFlag.set(true));

    Thread.sleep(1500L);
    await().atMost(30, TimeUnit.SECONDS).until(() -> server3
        .invoke(() -> phase2WrittenCount.get()) >= (gateReconnectIdx - 5 - phase1bEnd));

    HashMap<Integer, HashMap<String, Long>> hwAByBucket = new HashMap<>();
    HashMap<Integer, HashMap<String, Long>> hwBByBucket = new HashMap<>();
    for (int b = 0; b < test3Buckets; b++) {
      hwAByBucket.put(b, new HashMap<>(clientGateA.getSafeHighWaterByMember(b)));
      hwBByBucket.put(b, new HashMap<>(clientGateB.getSafeHighWaterByMember(b)));
    }
    assertThat(ingestionThread.isAlive()).isTrue();
    server3.invoke(() -> reconnectStartedFlag.set(true));

    for (MemberVM survivor : new MemberVM[] {server2, server3}) {
      survivor.invoke(() -> {
        serverTcpSink.enableReconnectBucketGate();
        serverTcpSinkB.enableReconnectBucketGate();
        dropOutboundEvents.set(false);
        dropOutboundEventsB.set(false);
        PartitionedRegion pr =
            (PartitionedRegion) ClusterStartupRule.getCache().getRegion(REGION_NAME);
        for (Integer b : pr.getDataStore().getAllLocalPrimaryBucketIds()) {
          Map<String, Long> hwA = hwAByBucket.get(b);
          Map<String, Long> hwB = hwBByBucket.get(b);
          serverService.replayUncheckpointedForReconnect(b,
              hwA != null ? hwA : Collections.emptyMap(),
              serverTcpSink, () -> serverTcpSink.allowLiveBucket(b));
          serverService.replayUncheckpointedForReconnect(b,
              hwB != null ? hwB : Collections.emptyMap(),
              serverTcpSinkB, () -> serverTcpSinkB.allowLiveBucket(b));
        }
      });
    }

    ingestionThread.join(60_000L);
    assertThat(ingestionThread.isAlive()).isFalse();
    for (MemberVM survivor : new MemberVM[] {server2, server3}) {
      survivor.invoke(() -> {
        try {
          assertThat(serverService.awaitPendingDispatches(10_000L)).isTrue();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      });
    }

    await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
      assertThat(collectKeySubPairs(acceptedA)).containsAll(totalExpectedPairs);
      assertThat(collectKeySubPairs(acceptedB)).containsAll(totalExpectedPairs);
      assertThat(clientGateA.getHeartbeatsAcceptedCount()).isGreaterThan(0L);
      assertThat(clientGateB.getHeartbeatsAcceptedCount()).isGreaterThan(0L);
    });
    Thread.sleep(300L);

    assertThat(collectKeySubPairs(acceptedA))
        .containsExactlyInAnyOrderElementsOf(totalExpectedPairs);
    assertThat(collectKeySubPairs(acceptedB))
        .containsExactlyInAnyOrderElementsOf(totalExpectedPairs);
    assertThat(RvvClientGate.verifyStrictMonotonicity(acceptedA)).isEmpty();
    assertThat(RvvClientGate.verifyStrictMonotonicity(acceptedB)).isEmpty();
    assertThat(acceptedA)
        .noneMatch(e -> e.isHeartbeat() || zeroMatchSubId.equals(e.getSubscriptionId()));
    assertThat(acceptedB)
        .noneMatch(e -> e.isHeartbeat() || zeroMatchSubId.equals(e.getSubscriptionId()));

    long replayedACount = acceptedA.stream().filter(ContinuousVectorEvent::isReplayed).count();
    long replayedBCount = acceptedB.stream().filter(ContinuousVectorEvent::isReplayed).count();
    assertThat(replayedBCount)
        .as("Slower client B must replay strictly more events across (V_client_B, V_S] than client A")
        .isGreaterThan(replayedACount);
    assertThat(acceptedA.stream().anyMatch(
        e -> e.isReplayed() && server1MemberId.equals(e.getMemberId()))).isTrue();
    assertThat(acceptedA.stream().anyMatch(
        e -> e.isReplayed() && !server1MemberId.equals(e.getMemberId()))).isTrue();

    for (MemberVM survivor : new MemberVM[] {server2, server3}) {
      survivor.invoke(ContinuousVectorRvvFailoverDistributedTest::shutdownServerNode);
    }
  }

  /**
   * M4 DUnit regression: proves (a) a steady stream of zero-match bucket puts spaced 40 ms apart
   * (&lt; maxAge=100 ms, total &lt; H=64) advances {@code V_ckpt} continuously via oldest-uncovered
   * heartbeat age tracking instead of starving indefinitely, and (b) an idle tail of 63 rapid
   * zero-match mutations (&lt; H=64) advances {@code V_ckpt} across the cluster when flushed.
   */
  @Test
  public void steadyTrickleAndIdleTailHeartbeatFlushAdvanceCheckpointAcrossCluster()
      throws Exception {
    RvvClientGate clientGate = new RvvClientGate();
    List<ContinuousVectorEvent> accepted = new CopyOnWriteArrayList<>();
    clientGate.addListener(accepted::add);
    receiver = new ClientEventReceiver(clientGate);
    int receiverPort = receiver.getPort();

    MemberVM locator = cluster.startLocatorVM(0);
    int locatorPort = locator.getPort();
    MemberVM server1 = cluster.startServerVM(1, locatorPort);
    MemberVM server2 = cluster.startServerVM(2, locatorPort);
    MemberVM server3 = cluster.startServerVM(3, locatorPort);

    try {
      Random rng = new Random(20261126L);
      ArrayList<VectorSubscription> subscriptions = new ArrayList<>(5);
      for (int m = 0; m < 4; m++) {
        subscriptions.add(new VectorSubscription("sub-trickle-" + m,
            randomUnitVectorZeroLastDim(rng, VECTOR_DIM), 0.78f));
      }
      float[] orthogonalQuery = new float[VECTOR_DIM];
      orthogonalQuery[VECTOR_DIM - 1] = 1.0f;
      subscriptions.add(new VectorSubscription("sub-zero-match", orthogonalQuery, 0.99f));
      for (VectorSubscription sub : subscriptions) {
        clientGate.registerSubscription(sub.getSubscriptionId());
      }

      for (MemberVM server : new MemberVM[] {server1, server2, server3}) {
        server.invoke(() -> {
          initializeServerNode(subscriptions, receiverPort, -1, 1,
              RvvMonotonicVectorDispatcher.DEFAULT_HEARTBEAT_INTERVAL_EVENTS, 1);
          serverService.startIdleHeartbeatScheduler(100L, 20L);
        });
      }

      final float[] nonMatchVec = new float[VECTOR_DIM];
      nonMatchVec[VECTOR_DIM - 1] = -1.0f;

      // Seed bucket 0 with a single non-matching vector to bind primary member ID and baseline.
      server3.invoke(() -> {
        Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
        region.put("seed-nonmatch-0", nonMatchVec);
      });

      String primaryMemberId = null;
      for (MemberVM server : new MemberVM[] {server1, server2, server3}) {
        String candidate = server.invoke(() -> {
          PartitionedRegion pr =
              (PartitionedRegion) ClusterStartupRule.getCache().getRegion(REGION_NAME);
          if (pr.getDataStore().getAllLocalPrimaryBucketIds().contains(0)) {
            return pr.getDataStore().getLocalBucketById(0).getVersionMember().toString();
          }
          return null;
        });
        if (candidate != null) {
          primaryMemberId = candidate;
          break;
        }
      }
      assertThat(primaryMemberId).isNotNull();
      final String bucket0Member = primaryMemberId;

      await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
        assertThat(clientGate.getSafeCheckpointVersion(0, bucket0Member))
            .isGreaterThanOrEqualTo(1L);
        assertThat(clientGate.getHeartbeatsAcceptedCount()).isGreaterThanOrEqualTo(1L);
      });
      long baseCkpt = clientGate.getSafeCheckpointVersion(0, bucket0Member);

      // Phase A: Steady 40 ms zero-match trickle (< 100 ms maxAge, 15 writes < H=64).
      // Oldest-uncovered age tracking guarantees periodic heartbeats rather than resetting age.
      server3.invoke(() -> {
        Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
        for (int i = 0; i < 15; i++) {
          region.put("trickle-" + i, nonMatchVec);
          try {
            Thread.sleep(40L);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
        }
      });

      await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
        assertThat(clientGate.getSafeCheckpointVersion(0, bucket0Member))
            .isEqualTo(baseCkpt + 15L);
        assertThat(clientGate.getHeartbeatsAcceptedCount()).isGreaterThanOrEqualTo(3L);
      });
      assertThat(accepted).isEmpty();

      // Phase B: Stop background scheduler, verify quiescent baseline, then write 63 rapid
      // zero-match puts (< H=64) and flush idle tail across the cluster.
      long flushNow = System.currentTimeMillis() + 500L;
      for (MemberVM server : new MemberVM[] {server1, server2, server3}) {
        server.invoke(() -> {
          serverService.stopIdleHeartbeatScheduler();
          serverService.flushIdleHeartbeats(flushNow, 100L);
          try {
            assertThat(serverService.awaitPendingDispatches(10_000L)).isTrue();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
        });
      }
      long ckptBeforeTail = clientGate.getSafeCheckpointVersion(0, bucket0Member);
      assertThat(ckptBeforeTail).isEqualTo(baseCkpt + 15L);
      long hbBeforeTail = clientGate.getHeartbeatsAcceptedCount();

      server3.invoke(() -> {
        Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
        for (int i = 0; i < 63; i++) {
          region.put("idle-tail-" + i, nonMatchVec);
        }
      });
      assertThat(clientGate.getHeartbeatsAcceptedCount()).isEqualTo(hbBeforeTail);
      assertThat(clientGate.getSafeCheckpointVersion(0, bucket0Member)).isEqualTo(ckptBeforeTail);

      long tailFlushNow = System.currentTimeMillis() + 500L;
      int flushedTotal = 0;
      for (MemberVM server : new MemberVM[] {server1, server2, server3}) {
        flushedTotal += server.invoke(() -> serverService.flushIdleHeartbeats(tailFlushNow, 100L));
      }
      assertThat(flushedTotal).isGreaterThanOrEqualTo(1);

      await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
        assertThat(clientGate.getHeartbeatsAcceptedCount()).isGreaterThan(hbBeforeTail);
        assertThat(clientGate.getSafeCheckpointVersion(0, bucket0Member))
            .isEqualTo(ckptBeforeTail + 63L);
      });
      assertThat(accepted).isEmpty();
    } finally {
      for (MemberVM server : new MemberVM[] {server1, server2, server3}) {
        server.invoke(ContinuousVectorRvvFailoverDistributedTest::shutdownServerNode);
      }
    }
  }

  /**
   * R1 M2 / R2 M5 DUnit regression: verifies delayed-predecessor holdback gap closure across a
   * cluster emits a resync-required notification after {@code maxHoldbackAgeMillis}, unblocks held
   * successor events without stalling, and advances client checkpoints monotonically.
   */
  @Test
  public void delayedPredecessorExceedingMaxHoldbackAgeClosesGapAcrossCluster() throws Exception {
    RvvClientGate clientGate = new RvvClientGate();
    List<ContinuousVectorEvent> accepted = new CopyOnWriteArrayList<>();
    clientGate.addListener(accepted::add);
    receiver = new ClientEventReceiver(clientGate);
    int receiverPort = receiver.getPort();

    MemberVM locator = cluster.startLocatorVM(0);
    int locatorPort = locator.getPort();
    MemberVM server1 = cluster.startServerVM(1, locatorPort);
    MemberVM server2 = cluster.startServerVM(2, locatorPort);

    try {
      Random rng = new Random(20261127L);
      float[] queryVec = randomUnitVectorZeroLastDim(rng, VECTOR_DIM);
      List<VectorSubscription> subscriptions = Collections
          .singletonList(new VectorSubscription("sub-gap-cluster", queryVec, 0.90f));
      clientGate.registerSubscription("sub-gap-cluster");

      for (MemberVM server : new MemberVM[] {server1, server2}) {
        server.invoke(() -> {
          initializeServerNode(subscriptions, receiverPort, -1, 1, 64, 1);
          serverService.setMaxHoldbackAgeMillis(2_000L);
          serverService.startIdleHeartbeatScheduler(50L, 25L);
        });
      }

      // Seed bucket 0 with v=1 match put to resolve its primary VM and member ID
      server2.invoke(() -> {
        Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
        region.put("seed-gap-1", queryVec);
      });

      MemberVM primaryVm = null;
      String primaryMemberId = null;
      for (MemberVM server : new MemberVM[] {server1, server2}) {
        String candidate = server.invoke(() -> {
          PartitionedRegion pr =
              (PartitionedRegion) ClusterStartupRule.getCache().getRegion(REGION_NAME);
          if (pr.getDataStore().getAllLocalPrimaryBucketIds().contains(0)) {
            return pr.getDataStore().getLocalBucketById(0).getVersionMember().toString();
          }
          return null;
        });
        if (candidate != null) {
          primaryVm = server;
          primaryMemberId = candidate;
          break;
        }
      }
      assertThat(primaryVm).isNotNull();
      assertThat(primaryMemberId).isNotNull();
      final MemberVM resolvedPrimary = primaryVm;
      final String bucket0Member = primaryMemberId;

      await().atMost(15, TimeUnit.SECONDS)
          .until(() -> clientGate.getSafeCheckpointVersion(0, bucket0Member) >= 1L);
      final long seedVer = clientGate.getSafeCheckpointVersion(0, bucket0Member);

      // Phase (a): Temporary predecessor gap REPAIRED BEFORE maxHoldbackAgeMillis expiry (2000 ms)
      // must preserve contiguous delivery (v=seedVer+1 before seedVer+2) with resyncRequired=false!
      final long phaseAPredVer = seedVer + 1L;
      final long phaseASuccVer = seedVer + 2L;
      resolvedPrimary.invoke(() -> serverService.onBucketMutation(0, bucket0Member, phaseASuccVer,
          "gap-before-expiry-succ", queryVec, true));
      Thread.sleep(100L);
      assertThat(resolvedPrimary.invoke(() -> serverService.isResyncRequired(0, bucket0Member)))
          .isFalse();
      assertThat(resolvedPrimary.invoke(() -> serverService.getHoldbackGapClosedCount())).isZero();
      assertThat(resolvedPrimary.invoke(() -> serverService.getDispatcher().getHoldbackSize(0)))
          .isEqualTo(1);
      assertThat(accepted).extracting(ContinuousVectorEvent::getKey).containsExactly("seed-gap-1");

      resolvedPrimary.invoke(() -> serverService.onBucketMutation(0, bucket0Member, phaseAPredVer,
          "gap-before-expiry-pred", queryVec, true));
      await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
        assertThat(resolvedPrimary.invoke(() -> serverService.isResyncRequired(0, bucket0Member)))
            .isFalse();
        assertThat(resolvedPrimary.invoke(() -> serverService.getHoldbackGapClosedCount()))
            .isZero();
        assertThat(
            resolvedPrimary.invoke(() -> serverService.getDispatcher().getHoldbackSize(0)))
                .isZero();
        assertThat(accepted).extracting(ContinuousVectorEvent::getKey).containsExactly(
            "seed-gap-1", "gap-before-expiry-pred", "gap-before-expiry-succ");
        assertThat(clientGate.getSafeCheckpointVersion(0, bucket0Member))
            .isGreaterThanOrEqualTo(phaseASuccVer);
      });

      // Phase (b): Delayed predecessor EXCEEDING maxHoldbackAgeMillis (150 ms) closes missing gap
      // deterministically, flags resyncRequired=true, and unblocks successor + checkpoints!
      final long phaseBSkipSuccVer = seedVer + 4L;
      resolvedPrimary.invoke(() -> {
        serverService.setMaxHoldbackAgeMillis(150L);
        serverService.onBucketMutation(0, bucket0Member, phaseBSkipSuccVer, "gap-successor-3",
            queryVec, true);
      });

      await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
        assertThat(resolvedPrimary.invoke(() -> serverService.isResyncRequired(0, bucket0Member)))
            .isTrue();
        assertThat(resolvedPrimary.invoke(() -> serverService.getHoldbackGapClosedCount()))
            .isGreaterThanOrEqualTo(1L);
        assertThat(
            resolvedPrimary.invoke(() -> serverService.getDispatcher().getHoldbackSize(0)))
                .isZero();
        assertThat(accepted).extracting(ContinuousVectorEvent::getKey).containsExactly(
            "seed-gap-1", "gap-before-expiry-pred", "gap-before-expiry-succ", "gap-successor-3");
        assertThat(clientGate.getSafeCheckpointVersion(0, bucket0Member))
            .isGreaterThanOrEqualTo(phaseBSkipSuccVer);
      });
    } finally {
      for (MemberVM server : new MemberVM[] {server1, server2}) {
        server.invoke(ContinuousVectorRvvFailoverDistributedTest::shutdownServerNode);
      }
    }
  }

  /**
   * R1 M2 / R2 M2 DUnit regression: proves that when TCP connection loss occurs strictly AFTER a
   * match notification is durably staged into client {@code PENDING} WAL and BEFORE its effect
   * retires, ordinary reconnect invokes {@code prepareForReconnect()} before server replay so the
   * notification is retired once and never re-delivered twice (while post-disconnect writes replay
   * seamlessly).
   */
  @Test
  public void connectionLossAfterDurablePendingStagingBeforeEffectRetires() throws Exception {
    RvvClientGate clientGate = new RvvClientGate();
    IdempotentRvvConsumer.InMemoryDurableDeliveryStore durableStore =
        new IdempotentRvvConsumer.InMemoryDurableDeliveryStore();
    List<IdempotentRvvConsumer.EventDeliveryId> appliedEffectLog = new CopyOnWriteArrayList<>();
    Set<IdempotentRvvConsumer.EventDeliveryId> externalIdempotentSink =
        ConcurrentHashMap.newKeySet();

    CountDownLatch v4StagedLatch = new CountDownLatch(1);
    class TransientDisconnectException extends RuntimeException {
      private static final long serialVersionUID = 1L;
    }

    IdempotentRvvConsumer consumer = new IdempotentRvvConsumer(clientGate, durableStore,
        (ev, id) -> {
          if (externalIdempotentSink.add(id)) {
            appliedEffectLog.add(id);
          }
        },
        (phase, ev, id) -> {
          if (phase == IdempotentRvvConsumer.CrashPhase.AFTER_WATERMARK_PERSIST_BEFORE_EFFECT
              && "pending-doc-4".equals(ev.getKey())) {
            v4StagedLatch.countDown();
            throw new TransientDisconnectException();
          }
        });
    receiver = new ClientEventReceiver(consumer);
    int initialPort = receiver.getPort();

    MemberVM locator = cluster.startLocatorVM(0);
    int locatorPort = locator.getPort();
    MemberVM server1 = cluster.startServerVM(1, locatorPort);
    MemberVM server2 = cluster.startServerVM(2, locatorPort);

    try {
      Random rng = new Random(20261128L);
      float[] queryVec = randomUnitVectorZeroLastDim(rng, VECTOR_DIM);
      List<VectorSubscription> subscriptions = Collections
          .singletonList(new VectorSubscription("sub-conn-loss", queryVec, 0.90f));
      clientGate.registerSubscription("sub-conn-loss");

      for (MemberVM server : new MemberVM[] {server1, server2}) {
        server.invoke(() -> initializeServerNode(subscriptions, initialPort, -1, 1, 64, 1));
      }

      // Put v=1..3 -> all 3 apply and commit cleanly
      server2.invoke(() -> {
        Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
        region.put("doc-1", queryVec);
        region.put("doc-2", queryVec);
        region.put("doc-3", queryVec);
      });

      String primaryMemberId = null;
      for (MemberVM server : new MemberVM[] {server1, server2}) {
        String candidate = server.invoke(() -> {
          PartitionedRegion pr =
              (PartitionedRegion) ClusterStartupRule.getCache().getRegion(REGION_NAME);
          if (pr.getDataStore().getAllLocalPrimaryBucketIds().contains(0)) {
            return pr.getDataStore().getLocalBucketById(0).getVersionMember().toString();
          }
          return null;
        });
        if (candidate != null) {
          primaryMemberId = candidate;
          break;
        }
      }
      assertThat(primaryMemberId).isNotNull();
      final String bucket0Member = primaryMemberId;

      await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
        assertThat(appliedEffectLog).hasSize(3);
        assertThat(durableStore.getCommittedWatermark("sub-conn-loss", 0, bucket0Member))
            .isEqualTo(3L);
        assertThat(clientGate.getHighWaterVersion("sub-conn-loss", 0, bucket0Member))
            .isEqualTo(3L);
      });

      // Put pending-doc-4 -> stages PENDING WAL, drops before effect retires, then sever TCP link
      server2.invoke(() -> {
        Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
        region.put("pending-doc-4", queryVec);
      });
      assertThat(v4StagedLatch.await(15, TimeUnit.SECONDS)).isTrue();

      for (MemberVM server : new MemberVM[] {server1, server2}) {
        server.invoke(() -> dropOutboundEvents.set(true));
      }
      receiver.close();

      long v4Admissions = consumer.getDurableAdmissionsCount();
      assertThat(v4Admissions).isEqualTo(4L);
      assertThat(appliedEffectLog).hasSize(3);
      assertThat(durableStore.getCommittedWatermark("sub-conn-loss", 0, bucket0Member))
          .isEqualTo(3L);

      // Server continues accepting doc-5 while disconnected
      server2.invoke(() -> {
        Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
        region.put("doc-5", queryVec);
      });

      // Ordinary reconnect WITHOUT crash/restart: create non-failing consumer on same durableStore
      // & clientGate, run prepareForReconnect(0) to retire PENDING pending-doc-4 FIRST, then
      // attach new TCP receiver and replay strictly (4, V_S] = [doc-5]!
      IdempotentRvvConsumer reconnectedConsumer = new IdempotentRvvConsumer(clientGate,
          durableStore, (ev, id) -> {
            if (externalIdempotentSink.add(id)) {
              appliedEffectLog.add(id);
            }
          });
      receiver = new ClientEventReceiver(reconnectedConsumer);
      int reconnectPort = receiver.getPort();
      Map<String, Long> clientHw = reconnectedConsumer.prepareForReconnect(0);
      assertThat(clientHw).containsEntry(bucket0Member, 4L);
      assertThat(appliedEffectLog).hasSize(4);

      for (MemberVM server : new MemberVM[] {server1, server2}) {
        server.invoke(() -> {
          if (serverTcpSink != null) {
            serverService.removeEventSink(serverTcpSink);
            serverTcpSink.close();
          }
          dropOutboundEvents.set(false);
          serverTcpSink = new TcpEventSink(reconnectPort, dropOutboundEvents);
          serverService.addEventSink(serverTcpSink);
          PartitionedRegion pr =
              (PartitionedRegion) ClusterStartupRule.getCache().getRegion(REGION_NAME);
          if (pr.getDataStore().getAllLocalPrimaryBucketIds().contains(0)) {
            serverService.replayUncheckpointedForReconnect(0, clientHw);
          }
        });
      }

      await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
        assertThat(appliedEffectLog)
            .extracting(IdempotentRvvConsumer.EventDeliveryId::getRegionVersion)
            .containsExactly(1L, 2L, 3L, 4L, 5L)
            .doesNotHaveDuplicates();
        assertThat(durableStore.getCommittedWatermark("sub-conn-loss", 0, bucket0Member))
            .isEqualTo(5L);
        assertThat(clientGate.getSafeCheckpointVersion(0, bucket0Member)).isEqualTo(5L);
        assertThat(reconnectedConsumer.getDurableAdmissionsCount()).isEqualTo(1L);
      });
    } finally {
      for (MemberVM server : new MemberVM[] {server1, server2}) {
        server.invoke(ContinuousVectorRvvFailoverDistributedTest::shutdownServerNode);
      }
    }
  }

  /**
   * R2 M3 &amp; R1 M3 / R2 m2 DUnit regression: proves (a) empty {@code |S| = 0} subscription
   * intervals preserve strict server version continuity with zero tail overflow, and (b) staggered
   * ACKs from multiple healthy subscribers advance {@code V_ckpt} unconditionally to
   * {@code min_i ack[i]} on an idle tail (including unblocking when a lagging subscriber exits).
   */
  @Test
  public void staggeredSubscriberAcksAndEmptySubscriptionIntervalMaintainContinuityAcrossCluster()
      throws Exception {
    RvvClientGate clientGate = new RvvClientGate();
    List<ContinuousVectorEvent> accepted = new CopyOnWriteArrayList<>();
    clientGate.addListener(accepted::add);
    receiver = new ClientEventReceiver(clientGate);
    int receiverPort = receiver.getPort();

    MemberVM locator = cluster.startLocatorVM(0);
    int locatorPort = locator.getPort();
    MemberVM server1 = cluster.startServerVM(1, locatorPort);
    MemberVM server2 = cluster.startServerVM(2, locatorPort);

    try {
      // Phase (a): Start both servers with |S| = 0 (empty subscription set) and ingest 5 puts
      for (MemberVM server : new MemberVM[] {server1, server2}) {
        server.invoke(() -> initializeServerNode(Collections.emptyList(), receiverPort, -1, 1,
            64, 1));
      }

      Random rng = new Random(20261129L);
      float[] queryVec = randomUnitVectorZeroLastDim(rng, VECTOR_DIM);

      server2.invoke(() -> {
        Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
        for (int i = 1; i <= 5; i++) {
          region.put("empty-s-" + i, queryVec);
        }
      });

      MemberVM primaryVm = null;
      String primaryMemberId = null;
      for (MemberVM server : new MemberVM[] {server1, server2}) {
        String candidate = server.invoke(() -> {
          PartitionedRegion pr =
              (PartitionedRegion) ClusterStartupRule.getCache().getRegion(REGION_NAME);
          if (pr.getDataStore().getAllLocalPrimaryBucketIds().contains(0)) {
            return pr.getDataStore().getLocalBucketById(0).getVersionMember().toString();
          }
          return null;
        });
        if (candidate != null) {
          primaryVm = server;
          primaryMemberId = candidate;
          break;
        }
      }
      assertThat(primaryVm).isNotNull();
      assertThat(primaryMemberId).isNotNull();
      final MemberVM resolvedPrimary = primaryVm;
      final String bucket0Member = primaryMemberId;

      long emptyLastDispatched =
          resolvedPrimary.invoke(() -> serverService.getLastDispatchedVersion(0, bucket0Member));
      long emptyNextExpected =
          resolvedPrimary.invoke(() -> serverService.getNextExpectedVersion(0, bucket0Member));
      assertThat(emptyLastDispatched).isGreaterThanOrEqualTo(5L);
      assertThat(emptyNextExpected).isEqualTo(emptyLastDispatched + 1L);
      assertThat(resolvedPrimary.invoke(() -> serverService.getDispatcher().getTailSize(0)))
          .isZero();
      assertThat(resolvedPrimary.invoke(() -> serverService.isResyncRequired(0, bucket0Member)))
          .isFalse();
      assertThat(accepted).isEmpty();

      // Phase (b): Transition |S| 0 -> 2 subscribers, ingest 6 puts onto an idle tail, then
      // deliver staggered ACKs -> V_ckpt advances unconditionally to min_i ack[i]
      List<VectorSubscription> activeSubs = Arrays.asList(
          new VectorSubscription("sub-A", queryVec, 0.90f),
          new VectorSubscription("sub-B", queryVec, 0.90f));
      clientGate.registerSubscription("sub-A");
      clientGate.registerSubscription("sub-B");
      for (MemberVM server : new MemberVM[] {server1, server2}) {
        server.invoke(() -> {
          serverService.registerSubscriptions(activeSubs);
          serverService.registerSubscriberForCheckpoint("sub-A");
          serverService.registerSubscriberForCheckpoint("sub-B");
        });
      }

      server2.invoke(() -> {
        Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
        for (int i = 1; i <= 6; i++) {
          region.put("active-doc-" + i, queryVec);
        }
      });

      final long targetTip = emptyLastDispatched + 6L;
      await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
        assertThat(clientGate.getSafeCheckpointVersion(0, bucket0Member)).isEqualTo(targetTip);
        assertThat(accepted).hasSize(12);
      });
      assertThat(resolvedPrimary.invoke(() -> serverService.getDispatcher().getTailSize(0)))
          .isEqualTo(6);

      // Staggered ACKs on idle tail: sub-A acks tip, sub-B acks behind -> V_ckpt = min -> tip
      long ackMid = emptyLastDispatched + 2L;
      assertThat(resolvedPrimary.invoke(() -> serverService.acknowledgeSubscriberCheckpoint(
          "sub-A", 0, bucket0Member, targetTip)))
              .isEqualTo(emptyLastDispatched);
      assertThat(resolvedPrimary.invoke(
          () -> serverService.acknowledgeSubscriberCheckpoint("sub-B", 0, bucket0Member, ackMid)))
              .isEqualTo(ackMid);
      assertThat(resolvedPrimary.invoke(() -> serverService.getDispatcher().getTailSize(0)))
          .isEqualTo(4);
      assertThat(resolvedPrimary.invoke(() -> serverService.acknowledgeSubscriberCheckpoint(
          "sub-B", 0, bucket0Member, targetTip)))
              .isEqualTo(targetTip);
      assertThat(resolvedPrimary.invoke(() -> serverService.getDispatcher().getTailSize(0)))
          .isZero();

      // Ingest 2 more mutations and unregister lagging sub-B -> V_ckpt immediately unblocks
      server2.invoke(() -> {
        Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
        region.put("active-doc-7", queryVec);
        region.put("active-doc-8", queryVec);
      });
      final long finalTip = targetTip + 2L;
      await().atMost(15, TimeUnit.SECONDS).until(
          () -> clientGate.getSafeCheckpointVersion(0, bucket0Member) == finalTip);
      assertThat(resolvedPrimary.invoke(() -> serverService.acknowledgeSubscriberCheckpoint(
          "sub-A", 0, bucket0Member, finalTip)))
              .isEqualTo(targetTip);
      assertThat(resolvedPrimary
          .invoke(() -> serverService.unregisterSubscriberForCheckpoint("sub-B"))).isTrue();
      assertThat(resolvedPrimary
          .invoke(() -> serverService.getCheckpointVersion(0, bucket0Member)))
              .isEqualTo(finalTip);
      assertThat(resolvedPrimary.invoke(() -> serverService.getDispatcher().getTailSize(0)))
          .isZero();
    } finally {
      for (MemberVM server : new MemberVM[] {server1, server2}) {
        server.invoke(ContinuousVectorRvvFailoverDistributedTest::shutdownServerNode);
      }
    }
  }

  /**
   * R1 M2 / R2 M3 DUnit regression: proves that an ordinary reconnect invoking
   * {@code prepareForReconnect(0)} while a previously staged match handler is actively executing in
   * flight blocks on the per-stream execution/recovery fence, suppresses duplicate concurrent
   * admission, prevents handler overlap ({@code maxConcurrentHandlers == 1}), and cleanly replays
   * strictly post-handler writes.
   */
  @Test
  public void ordinaryReconnectFencesInFlightHandlerAndPreventsDuplicateAdmissionOrOverlap()
      throws Exception {
    RvvClientGate clientGate = new RvvClientGate();
    IdempotentRvvConsumer.InMemoryDurableDeliveryStore durableStore =
        new IdempotentRvvConsumer.InMemoryDurableDeliveryStore();
    List<IdempotentRvvConsumer.EventDeliveryId> appliedEffectLog = new CopyOnWriteArrayList<>();
    AtomicInteger activeHandlers = new AtomicInteger();
    AtomicInteger maxConcurrentHandlers = new AtomicInteger();
    CountDownLatch v3InFlight = new CountDownLatch(1);
    CountDownLatch allowV3Finish = new CountDownLatch(1);

    IdempotentRvvConsumer consumer = new IdempotentRvvConsumer(clientGate, durableStore,
        (ev, id) -> {
          int concurrent = activeHandlers.incrementAndGet();
          maxConcurrentHandlers.accumulateAndGet(concurrent, Math::max);
          try {
            if ("inflight-doc-3".equals(ev.getKey())) {
              v3InFlight.countDown();
              try {
                allowV3Finish.await(15, TimeUnit.SECONDS);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            }
            appliedEffectLog.add(id);
          } finally {
            activeHandlers.decrementAndGet();
          }
        });
    receiver = new ClientEventReceiver(consumer);
    int initialPort = receiver.getPort();

    MemberVM locator = cluster.startLocatorVM(0);
    int locatorPort = locator.getPort();
    MemberVM server1 = cluster.startServerVM(1, locatorPort);
    MemberVM server2 = cluster.startServerVM(2, locatorPort);

    try {
      Random rng = new Random(20261130L);
      float[] queryVec = randomUnitVectorZeroLastDim(rng, VECTOR_DIM);
      List<VectorSubscription> subscriptions = Collections
          .singletonList(new VectorSubscription("sub-fence-dunit", queryVec, 0.90f));
      clientGate.registerSubscription("sub-fence-dunit");

      for (MemberVM server : new MemberVM[] {server1, server2}) {
        server.invoke(() -> initializeServerNode(subscriptions, initialPort, -1, 1, 64, 1));
      }

      server2.invoke(() -> {
        Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
        region.put("doc-1", queryVec);
        region.put("doc-2", queryVec);
      });

      String primaryMemberId = null;
      for (MemberVM server : new MemberVM[] {server1, server2}) {
        String candidate = server.invoke(() -> {
          PartitionedRegion pr =
              (PartitionedRegion) ClusterStartupRule.getCache().getRegion(REGION_NAME);
          if (pr.getDataStore().getAllLocalPrimaryBucketIds().contains(0)) {
            return pr.getDataStore().getLocalBucketById(0).getVersionMember().toString();
          }
          return null;
        });
        if (candidate != null) {
          primaryMemberId = candidate;
          break;
        }
      }
      assertThat(primaryMemberId).isNotNull();
      final String bucket0Member = primaryMemberId;

      await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
        assertThat(appliedEffectLog).hasSize(2);
        assertThat(durableStore.getCommittedWatermark("sub-fence-dunit", 0, bucket0Member))
            .isEqualTo(2L);
      });

      // Put inflight-doc-3 -> receiver thread enters effectHandler and stalls inside execFence
      server2.invoke(() -> {
        Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
        region.put("inflight-doc-3", queryVec);
      });
      assertThat(v3InFlight.await(15, TimeUnit.SECONDS)).isTrue();
      assertThat(consumer.getDurableAdmissionsCount()).isEqualTo(3L);

      // Sever wire & ingest doc-4 while inflight-doc-3 is still executing
      for (MemberVM server : new MemberVM[] {server1, server2}) {
        server.invoke(() -> dropOutboundEvents.set(true));
      }
      server2.invoke(() -> {
        Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
        region.put("doc-4", queryVec);
      });

      // Concurrent duplicate of in-flight v=3 is rejected atomically without overlap
      ContinuousVectorEvent dupV3 = new ContinuousVectorEvent("sub-fence-dunit", "inflight-doc-3",
          1.0f, 0, bucket0Member, 3L, 1, System.currentTimeMillis(), true);
      assertThat(consumer.processEvent(dupV3)).isFalse();
      assertThat(consumer.getDurableAdmissionsCount()).isEqualTo(3L);

      // Ordinary reconnect prepareForReconnect(0) must block behind in-flight handler v=3
      CountDownLatch reconnectDone = new CountDownLatch(1);
      java.util.concurrent.atomic.AtomicReference<Map<String, Long>> hwRef =
          new java.util.concurrent.atomic.AtomicReference<>();
      Thread reconnectThread = new Thread(() -> {
        hwRef.set(consumer.prepareForReconnect(0));
        reconnectDone.countDown();
      }, "dunit-ordinary-reconnect");
      reconnectThread.start();
      assertThat(reconnectDone.await(150L, TimeUnit.MILLISECONDS)).isFalse();
      assertThat(maxConcurrentHandlers.get()).isEqualTo(1);

      // Release in-flight v=3 -> finishes cleanly, unblocks reconnect with V_client=3, replays [4]
      allowV3Finish.countDown();
      assertThat(reconnectDone.await(15, TimeUnit.SECONDS)).isTrue();
      reconnectThread.join(10_000L);
      Map<String, Long> clientHw = hwRef.get();
      assertThat(clientHw).containsEntry(bucket0Member, 3L);

      receiver.close();
      receiver = new ClientEventReceiver(consumer);
      int reconnectPort = receiver.getPort();
      for (MemberVM server : new MemberVM[] {server1, server2}) {
        server.invoke(() -> {
          if (serverTcpSink != null) {
            serverService.removeEventSink(serverTcpSink);
            serverTcpSink.close();
          }
          dropOutboundEvents.set(false);
          serverTcpSink = new TcpEventSink(reconnectPort, dropOutboundEvents);
          serverService.addEventSink(serverTcpSink);
          PartitionedRegion pr =
              (PartitionedRegion) ClusterStartupRule.getCache().getRegion(REGION_NAME);
          if (pr.getDataStore().getAllLocalPrimaryBucketIds().contains(0)) {
            serverService.replayUncheckpointedForReconnect(0, clientHw);
          }
        });
      }

      await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
        assertThat(appliedEffectLog)
            .extracting(IdempotentRvvConsumer.EventDeliveryId::getRegionVersion)
            .containsExactly(1L, 2L, 3L, 4L)
            .doesNotHaveDuplicates();
        assertThat(maxConcurrentHandlers.get()).isEqualTo(1);
        assertThat(consumer.getDurableAdmissionsCount()).isEqualTo(4L);
        assertThat(durableStore.getCommittedWatermark("sub-fence-dunit", 0, bucket0Member))
            .isEqualTo(4L);
        assertThat(clientGate.getSafeCheckpointVersion(0, bucket0Member)).isEqualTo(4L);
      });
    } finally {
      for (MemberVM server : new MemberVM[] {server1, server2}) {
        server.invoke(ContinuousVectorRvvFailoverDistributedTest::shutdownServerNode);
      }
    }
  }

  /**
   * R1 M3, M6 / R2 M2 DUnit regression: exercises (a) primary failover right at {@code C_tail - 1}
   * with a slow consumer and pending un-replicated checkpoint (0 resync, complete replay), (b)
   * {@code C_tail + 1} overflow advancing {@code V_floor} and deterministically flagging
   * {@code resyncRequired}, and (c) subscriber rejoin strictly below compacted {@code V_floor}
   * rejected before continuity admission.
   */
  @Test
  public void promotionAtTailCapacityBoundaryAndRejoinBelowReplayFloorAcrossCluster()
      throws Exception {
    IgnoredException.addIgnoredException("MemberDepartedException");
    IgnoredException.addIgnoredException("ForcedDisconnectException");
    IgnoredException.addIgnoredException("PartitionOfflineException");
    IgnoredException.addIgnoredException("CacheClosedException");
    IgnoredException.addIgnoredException("CancelException");
    IgnoredException.addIgnoredException("SocketException");
    IgnoredException.addIgnoredException("EOFException");
    IgnoredException.addIgnoredException("IOException");
    IgnoredException.addIgnoredException("Membership: lead member");
    IgnoredException.addIgnoredException("crashed");

    RvvClientGate clientGate = new RvvClientGate();
    List<ContinuousVectorEvent> accepted = new CopyOnWriteArrayList<>();
    clientGate.addListener(accepted::add);
    receiver = new ClientEventReceiver(clientGate);
    int receiverPort = receiver.getPort();

    MemberVM locator = cluster.startLocatorVM(0);
    int locatorPort = locator.getPort();
    MemberVM server1 = cluster.startServerVM(1, locatorPort);
    MemberVM server2 = cluster.startServerVM(2, locatorPort);
    MemberVM server3 = cluster.startServerVM(3, locatorPort);
    MemberVM[] servers = new MemberVM[] {server1, server2, server3};
    boolean[] alive = new boolean[] {true, true, true};
    final int cTail = 32;

    try {
      Random rng = new Random(20261201L);
      float[] queryVec = randomUnitVectorZeroLastDim(rng, VECTOR_DIM);
      List<VectorSubscription> subscriptions = Collections
          .singletonList(new VectorSubscription("sub-cap-dunit", queryVec, 0.90f));
      clientGate.registerSubscription("sub-cap-dunit");

      for (MemberVM server : servers) {
        server.invoke(() -> initializeServerNode(subscriptions, receiverPort, -1, 1, 64, 1, cTail));
      }

      // Baseline v=1..5 delivered & checkpoint replicated to all servers (V_floor = 5)
      server3.invoke(() -> {
        Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
        for (int i = 1; i <= 5; i++) {
          region.put("base-" + i, queryVec);
        }
      });

      int primaryIdx = -1;
      int secondaryIdx = -1;
      String primaryMemberId = null;
      for (int idx = 0; idx < servers.length; idx++) {
        String roleAndMember = servers[idx].invoke(() -> {
          PartitionedRegion pr =
              (PartitionedRegion) ClusterStartupRule.getCache().getRegion(REGION_NAME);
          if (pr.getDataStore().getAllLocalPrimaryBucketIds().contains(0)) {
            return "P|" + pr.getDataStore().getLocalBucketById(0).getVersionMember().toString();
          }
          if (pr.getDataStore().getAllLocalBucketIds().contains(0)) {
            return "S|secondary";
          }
          return null;
        });
        if (roleAndMember != null && roleAndMember.startsWith("P|")) {
          primaryIdx = idx;
          primaryMemberId = roleAndMember.substring(2);
        } else if (roleAndMember != null && roleAndMember.startsWith("S|")) {
          secondaryIdx = idx;
        }
      }
      assertThat(primaryIdx).isGreaterThanOrEqualTo(0);
      assertThat(secondaryIdx).isGreaterThanOrEqualTo(0);
      final String mP = primaryMemberId;
      final MemberVM survivor = servers[secondaryIdx];

      await().atMost(15, TimeUnit.SECONDS)
          .until(() -> clientGate.getSafeCheckpointVersion(0, mP) == 5L);
      for (MemberVM server : servers) {
        server.invoke(() -> serverService.acknowledgeCheckpoint(0, mP, 5L));
      }

      // Part (a): Disconnect slow client and write exactly C_tail - 1 = 31 entries (v=6..36)
      for (MemberVM server : servers) {
        server.invoke(() -> dropOutboundEvents.set(true));
      }
      survivor.invoke(() -> {
        Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
        for (int i = 6; i <= 5 + (cTail - 1); i++) {
          region.put("cap-a-" + i, queryVec);
        }
      });

      assertThat(survivor.invoke(() -> serverService.getDispatcher().getTailSize(0)))
          .isEqualTo(cTail - 1);
      assertThat(survivor.invoke(() -> serverService.getDispatcher().getTailOverflowCount()))
          .isZero();
      assertThat(survivor.invoke(() -> serverService.isResyncRequired(0, mP))).isFalse();
      assertThat(survivor.invoke(() -> serverService.getReplayFloorVersion(0, mP))).isEqualTo(5L);

      // Crash primary VM -> survivor promotes with all 31 entries intact and 0 resync
      alive[primaryIdx] = false;
      cluster.crashVM(primaryIdx + 1);
      await().atMost(30, TimeUnit.SECONDS).until(() -> survivor.invoke(() -> {
        PartitionedRegion pr =
            (PartitionedRegion) ClusterStartupRule.getCache().getRegion(REGION_NAME);
        return pr.getDataStore().getAllLocalPrimaryBucketIds().contains(0)
            && serverService.getDispatcher().getTotalPromotionsHandled() >= 1L;
      }));
      survivor.invoke(() -> {
        try {
          serverReplayExecutor.submit(() -> {
          }).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      Map<String, Long> slowHw = clientGate.getSafeHighWaterByMember(0);
      assertThat(slowHw).containsEntry(mP, 5L);
      assertThat(survivor.invoke(() -> serverService.admitSubscriberForContinuity("sub-cap-dunit",
          0, mP, 5L))).isTrue();
      survivor.invoke(() -> {
        dropOutboundEvents.set(false);
        serverService.replayUncheckpointedForReconnect(0, slowHw);
      });
      await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
        assertThat(accepted).hasSize(5 + (cTail - 1));
        assertThat(clientGate.getSafeCheckpointVersion(0, mP)).isEqualTo(5L + (cTail - 1));
        assertThat(survivor.invoke(() -> serverService.isResyncRequired(0, mP))).isFalse();
      });

      // Part (b): On promoted primary (m_S), write C_tail + 1 = 33 uncheckpointed entries while
      // client is disconnected -> deterministic overflow advances V_floor and flags resyncRequired!
      String mS = survivor.invoke(() -> {
        PartitionedRegion pr =
            (PartitionedRegion) ClusterStartupRule.getCache().getRegion(REGION_NAME);
        return pr.getDataStore().getLocalBucketById(0).getVersionMember().toString();
      });
      survivor.invoke(() -> {
        dropOutboundEvents.set(true);
        Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
        for (int i = 1; i <= cTail + 1; i++) {
          region.put("cap-b-" + i, queryVec);
        }
      });

      assertThat(survivor.invoke(() -> serverService.getDispatcher().getTailOverflowCount()))
          .isGreaterThanOrEqualTo(1L);
      assertThat(survivor.invoke(() -> serverService.isResyncRequired(0, mS))).isTrue();
      long overflowFloor = survivor.invoke(() -> serverService.getReplayFloorVersion(0, mS));
      assertThat(overflowFloor).isGreaterThanOrEqualTo(1L);

      // Part (c): Rejoin strictly below compacted V_floor -> immediate resyncRequired!
      survivor.invoke(() -> {
        serverService.getDispatcher().clearResyncRequired(0, mS);
        serverService.acknowledgeCheckpoint(0, mS, cTail + 1L);
      });
      assertThat(survivor.invoke(() -> serverService.getReplayFloorVersion(0, mS)))
          .isEqualTo(cTail + 1L);
      assertThat(
          survivor.invoke(() -> serverService.admitSubscriberForContinuity("sub-stale-rejoin",
              0, mS, 10L))).isFalse();
      assertThat(survivor.invoke(() -> serverService.isResyncRequired(0, mS))).isTrue();
      assertThat(survivor.invoke(() -> serverService.getReplayFloorResyncCount()))
          .isGreaterThanOrEqualTo(1L);
    } finally {
      for (int idx = 0; idx < servers.length; idx++) {
        if (alive[idx]) {
          servers[idx].invoke(ContinuousVectorRvvFailoverDistributedTest::shutdownServerNode);
        }
      }
    }
  }

  /**
   * R1 M6 / R2 M4 DUnit regression: overwrites the SAME 4 hot keys repeatedly across a live
   * primary {@code kill -9} failover and verifies every acknowledged per-version
   * {@code (bucketId, memberId, regionVersion)} notification is delivered strictly once with zero
   * misses, zero duplicates, and zero resyncs even though final region state contains only 4 keys.
   */
  @Test
  public void repeatedKeyFailoverTraceDeliversExactPerVersionNotificationsAcrossCluster()
      throws Exception {
    IgnoredException.addIgnoredException("MemberDepartedException");
    IgnoredException.addIgnoredException("ForcedDisconnectException");
    IgnoredException.addIgnoredException("PartitionOfflineException");
    IgnoredException.addIgnoredException("CacheClosedException");
    IgnoredException.addIgnoredException("CancelException");
    IgnoredException.addIgnoredException("SocketException");
    IgnoredException.addIgnoredException("EOFException");
    IgnoredException.addIgnoredException("IOException");
    IgnoredException.addIgnoredException("Membership: lead member");
    IgnoredException.addIgnoredException("crashed");

    RvvClientGate clientGate = new RvvClientGate();
    List<ContinuousVectorEvent> accepted = new CopyOnWriteArrayList<>();
    clientGate.addListener(accepted::add);
    receiver = new ClientEventReceiver(clientGate);
    int receiverPort = receiver.getPort();

    MemberVM locator = cluster.startLocatorVM(0);
    int locatorPort = locator.getPort();
    MemberVM server1 = cluster.startServerVM(1, locatorPort);
    MemberVM server2 = cluster.startServerVM(2, locatorPort);
    MemberVM server3 = cluster.startServerVM(3, locatorPort);
    MemberVM[] servers = new MemberVM[] {server1, server2, server3};
    boolean[] alive = new boolean[] {true, true, true};

    try {
      Random rng = new Random(20261202L);
      float[] queryVec = randomUnitVectorZeroLastDim(rng, VECTOR_DIM);
      List<VectorSubscription> subscriptions = Collections
          .singletonList(new VectorSubscription("sub-hot-dunit", queryVec, 0.90f));
      clientGate.registerSubscription("sub-hot-dunit");

      for (MemberVM server : servers) {
        server.invoke(() -> initializeServerNode(subscriptions, receiverPort, -1, 1, 64, 1));
      }

      // First 10 overwrites of the SAME 4 hot keys delivered live
      server3.invoke(() -> {
        Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
        for (int i = 0; i < 10; i++) {
          region.put("hot-" + (i % 4), queryVec);
        }
      });

      int primaryIdx = -1;
      String primaryMemberId = null;
      for (int idx = 0; idx < servers.length; idx++) {
        String candidate = servers[idx].invoke(() -> {
          PartitionedRegion pr =
              (PartitionedRegion) ClusterStartupRule.getCache().getRegion(REGION_NAME);
          if (pr.getDataStore().getAllLocalPrimaryBucketIds().contains(0)) {
            return pr.getDataStore().getLocalBucketById(0).getVersionMember().toString();
          }
          return null;
        });
        if (candidate != null) {
          primaryIdx = idx;
          primaryMemberId = candidate;
          break;
        }
      }
      assertThat(primaryIdx).isGreaterThanOrEqualTo(0);
      final String mP = primaryMemberId;
      final int writerIdx = (primaryIdx + 1) % servers.length;
      final MemberVM writerVm = servers[writerIdx];

      await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
        assertThat(accepted).hasSize(10);
        assertThat(clientGate.getSafeCheckpointVersion(0, mP)).isEqualTo(10L);
      });

      // Sever wire before 10 more same-key overwrites (v=11..20) + crash primary VM!
      for (MemberVM server : servers) {
        server.invoke(() -> dropOutboundEvents.set(true));
      }
      writerVm.invoke(() -> {
        Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
        for (int i = 10; i < 20; i++) {
          region.put("hot-" + (i % 4), queryVec);
        }
      });

      alive[primaryIdx] = false;
      cluster.crashVM(primaryIdx + 1);

      // Wait for one of the survivors to promote bucket 0 to primary and finish promotion replay
      await().atMost(30, TimeUnit.SECONDS).until(() -> {
        for (int idx = 0; idx < servers.length; idx++) {
          if (!alive[idx]) {
            continue;
          }
          boolean promoted = servers[idx].invoke(() -> {
            PartitionedRegion pr =
                (PartitionedRegion) ClusterStartupRule.getCache().getRegion(REGION_NAME);
            return pr.getDataStore().getAllLocalPrimaryBucketIds().contains(0)
                && serverService.getDispatcher().getTotalPromotionsHandled() >= 1L;
          });
          if (promoted) {
            return true;
          }
        }
        return false;
      });
      MemberVM promotedVm = null;
      String mS = null;
      for (int idx = 0; idx < servers.length; idx++) {
        if (!alive[idx]) {
          continue;
        }
        String candidate = servers[idx].invoke(() -> {
          try {
            serverReplayExecutor.submit(() -> {
            }).get(10, TimeUnit.SECONDS);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          PartitionedRegion pr =
              (PartitionedRegion) ClusterStartupRule.getCache().getRegion(REGION_NAME);
          if (pr.getDataStore().getAllLocalPrimaryBucketIds().contains(0)) {
            return pr.getDataStore().getLocalBucketById(0).getVersionMember().toString();
          }
          return null;
        });
        if (candidate != null) {
          promotedVm = servers[idx];
          mS = candidate;
          break;
        }
      }
      assertThat(promotedVm).isNotNull();
      assertThat(mS).isNotNull().isNotEqualTo(mP);
      final MemberVM resolvedPromoted = promotedVm;
      final String resolvedMS = mS;

      // Ingest 12 more same-key overwrites on promoted primary (m_S), then reconnect client
      writerVm.invoke(() -> {
        Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
        for (int i = 0; i < 12; i++) {
          region.put("hot-" + (i % 4), queryVec);
        }
      });
      int regionKeyCount =
          writerVm.invoke(() -> ClusterStartupRule.getCache().getRegion(REGION_NAME).size());
      assertThat(regionKeyCount).isEqualTo(4);

      Map<String, Long> clientHw = clientGate.getSafeHighWaterByMember(0);
      assertThat(clientHw).containsEntry(mP, 10L);
      resolvedPromoted.invoke(() -> {
        serverTcpSink.enableReconnectBucketGate();
        dropOutboundEvents.set(false);
        serverService.replayUncheckpointedForReconnect(0, clientHw, serverTcpSink,
            () -> serverTcpSink.allowLiveBucket(0));
      });

      // Verify exact 32 per-version notifications (20 on m_P + 12 on m_S) with 0 misses,
      // 0 duplicates, strict monotonicity, and 0 resyncRequired!
      await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
        assertThat(accepted).hasSize(32);
      });
      Set<String> deliveredVersions = new HashSet<>();
      List<Long> mPVersions = new ArrayList<>();
      List<Long> mSVersions = new ArrayList<>();
      for (ContinuousVectorEvent ev : accepted) {
        String id = ev.getSubscriptionId() + "|" + ev.getBucketId() + "|" + ev.getMemberId() + "|"
            + ev.getRegionVersion();
        assertThat(deliveredVersions.add(id)).as("Duplicate version tuple: %s", id).isTrue();
        if (mP.equals(ev.getMemberId())) {
          mPVersions.add(ev.getRegionVersion());
        } else if (resolvedMS.equals(ev.getMemberId())) {
          mSVersions.add(ev.getRegionVersion());
        }
      }
      assertThat(mPVersions).hasSize(20);
      assertThat(mSVersions).hasSize(12);
      assertThat(RvvClientGate.verifyStrictMonotonicity(accepted)).isEmpty();
      assertThat(resolvedPromoted.invoke(() -> serverService.isResyncRequired(0, mP))).isFalse();
      assertThat(resolvedPromoted.invoke(() -> serverService.isResyncRequired(0, resolvedMS)))
          .isFalse();
    } finally {
      for (int idx = 0; idx < servers.length; idx++) {
        if (alive[idx]) {
          servers[idx].invoke(ContinuousVectorRvvFailoverDistributedTest::shutdownServerNode);
        }
      }
    }
  }

  /**
   * R1 M2 / R2 M2 DUnit regression: proves that when {@code V_floor=60 <= V_client=63 < V_ckpt=64},
   * an admitted returning subscriber atomically joins {@code S_healthy} pinned at {@code 63} and
   * replays strictly {@code (63, V_S]} (delivering version {@code 64} with zero skips), while a
   * stale subscriber with {@code V_client=59 < V_floor=60} is immediately rejected with
   * {@code resyncRequired}.
   */
  @Test
  public void reconnectBetweenFloorAndCheckpointReplaysIntermediateVersionsAndBelowFloorTriggersResync()
      throws Exception {
    RvvClientGate clientGate = new RvvClientGate();
    clientGate.registerSubscription("sub-lagging");
    IdempotentRvvConsumer.InMemoryDurableDeliveryStore durableStore =
        new IdempotentRvvConsumer.InMemoryDurableDeliveryStore();
    List<IdempotentRvvConsumer.EventDeliveryId> appliedEffects = new CopyOnWriteArrayList<>();
    IdempotentRvvConsumer consumer =
        new IdempotentRvvConsumer(clientGate, durableStore, (ev, id) -> appliedEffects.add(id));
    receiver = new ClientEventReceiver(consumer);
    int receiverPort = receiver.getPort();

    MemberVM locator = cluster.startLocatorVM(0);
    int locatorPort = locator.getPort();
    MemberVM server1 = cluster.startServerVM(1, locatorPort);
    MemberVM server2 = cluster.startServerVM(2, locatorPort);
    MemberVM[] servers = new MemberVM[] {server1, server2};

    try {
      Random rng = new Random(20261203L);
      float[] queryVec = randomUnitVectorZeroLastDim(rng, VECTOR_DIM);
      List<VectorSubscription> subscriptions = Collections
          .singletonList(new VectorSubscription("sub-lagging", queryVec, 0.90f));

      for (MemberVM server : servers) {
        server.invoke(() -> {
          initializeServerNode(subscriptions, receiverPort, -1, 1, 64, 1);
          serverService.registerSubscriberForCheckpoint("sub-fast");
        });
      }

      // Live puts v=1..63 delivered to sub-lagging -> V_client=63
      server2.invoke(() -> {
        Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
        for (int i = 1; i <= 63; i++) {
          region.put("fc-doc-" + i, queryVec);
        }
      });

      int primaryIdx = -1;
      String primaryMemberId = null;
      for (int idx = 0; idx < servers.length; idx++) {
        String candidate = servers[idx].invoke(() -> {
          PartitionedRegion pr =
              (PartitionedRegion) ClusterStartupRule.getCache().getRegion(REGION_NAME);
          if (pr.getDataStore().getAllLocalPrimaryBucketIds().contains(0)) {
            return pr.getDataStore().getLocalBucketById(0).getVersionMember().toString();
          }
          return null;
        });
        if (candidate != null) {
          primaryIdx = idx;
          primaryMemberId = candidate;
          break;
        }
      }
      assertThat(primaryIdx).isGreaterThanOrEqualTo(0);
      final String mP = primaryMemberId;
      final MemberVM primaryVm = servers[primaryIdx];

      await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
        assertThat(appliedEffects).hasSize(63);
        assertThat(durableStore.getCommittedWatermark("sub-lagging", 0, mP)).isEqualTo(63L);
      });

      // Compact V_floor=60 and advance logical replicated V_ckpt=64 (retaining [61..70] in tail),
      // sever wire, and ingest v=64..70
      for (MemberVM server : servers) {
        server.invoke(() -> {
          serverService.acknowledgeCheckpoint(0, mP, 60L);
          serverService.recordReplicatedCheckpoint(0, mP, 64L);
          dropOutboundEvents.set(true);
        });
      }
      server2.invoke(() -> {
        Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
        for (int i = 64; i <= 70; i++) {
          region.put("fc-doc-" + i, queryVec);
        }
      });

      assertThat(primaryVm.invoke(() -> serverService.getReplayFloorVersion(0, mP)))
          .isEqualTo(60L);
      assertThat(primaryVm.invoke(() -> serverService.getCheckpointVersion(0, mP)))
          .isEqualTo(64L);
      assertThat(primaryVm.invoke(() -> serverService.getDispatcher().getTailSize(0)))
          .isEqualTo(10);

      // Reconnect sub-lagging at V_client=63 (V_floor=60 <= 63 < V_ckpt=64) via atomic
      // admitAndReplayForReconnect; fast subscriber acking 70 cannot push V_floor past 63!
      Map<String, Long> clientHw = consumer.prepareForReconnect(0);
      assertThat(clientHw).containsEntry(mP, 63L);
      primaryVm.invoke(() -> {
        serverTcpSink.enableReconnectBucketGate();
        dropOutboundEvents.set(false);
        serverService.admitAndReplayForReconnect("sub-lagging", 0, clientHw, serverTcpSink,
            () -> serverTcpSink.allowLiveBucket(0));
        serverService.acknowledgeSubscriberCheckpoint("sub-fast", 0, mP, 70L);
      });

      assertThat(primaryVm.invoke(() -> serverService.getReplayFloorVersion(0, mP)))
          .isLessThanOrEqualTo(63L);
      assertThat(primaryVm.invoke(() -> serverService.isResyncRequired(0, mP))).isFalse();

      await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
        assertThat(appliedEffects).hasSize(70);
        assertThat(appliedEffects).doesNotHaveDuplicates();
        assertThat(appliedEffects).contains(
            new IdempotentRvvConsumer.EventDeliveryId("sub-lagging", 0, mP, 64L));
        assertThat(durableStore.getCommittedWatermark("sub-lagging", 0, mP)).isEqualTo(70L);
      });

      // Stale subscriber at V_client=59 < V_floor is immediately rejected with resyncRequired
      int staleReplayCount = primaryVm.invoke(() -> serverService
          .admitAndReplayForReconnect("sub-stale", 0, Collections.singletonMap(mP, 59L)).size());
      assertThat(staleReplayCount).isZero();
      assertThat(primaryVm.invoke(() -> serverService.isResyncRequired(0, mP))).isTrue();
      assertThat(primaryVm.invoke(() -> serverService.getReplayFloorResyncCount()))
          .isGreaterThanOrEqualTo(1L);
      assertThat(primaryVm.invoke(
          () -> serverService.getDispatcher().getRegisteredCheckpointSubscribers()))
              .doesNotContain("sub-stale");
    } finally {
      for (MemberVM server : servers) {
        server.invoke(ContinuousVectorRvvFailoverDistributedTest::shutdownServerNode);
      }
    }
  }

  /**
   * R1 M3 / R1 M6 DUnit regression: combined multi-subscription ({@code sub-A}, {@code sub-B})
   * repeated-key ({@code hot-0..hot-3}) live {@code kill -9} failover crossing promotion, covering
   * heartbeats (with delayed {@code sub-B} v=63 pinned below covering {@code HB(64)}), and lagging
   * subscriber reconnect at {@code V_floor=60 <= V_client=63 < V_ckpt=64}.
   */
  @Test
  public void multiSubscriptionRepeatedKeyPromotionCoveringHeartbeatAndLaggingReconnect()
      throws Exception {
    IgnoredException.addIgnoredException("MemberDepartedException");
    IgnoredException.addIgnoredException("ForcedDisconnectException");
    IgnoredException.addIgnoredException("PartitionOfflineException");
    IgnoredException.addIgnoredException("CacheClosedException");
    IgnoredException.addIgnoredException("CancelException");
    IgnoredException.addIgnoredException("SocketException");
    IgnoredException.addIgnoredException("EOFException");
    IgnoredException.addIgnoredException("IOException");
    IgnoredException.addIgnoredException("Membership: lead member");
    IgnoredException.addIgnoredException("crashed");

    RvvClientGate gate1 = new RvvClientGate();
    gate1.registerSubscription("sub-A");
    gate1.registerSubscription("sub-B");
    IdempotentRvvConsumer.InMemoryDurableDeliveryStore store1 =
        new IdempotentRvvConsumer.InMemoryDurableDeliveryStore();
    List<IdempotentRvvConsumer.EventDeliveryId> applied1 = new CopyOnWriteArrayList<>();
    CountDownLatch subBV63InFlight = new CountDownLatch(1);
    CountDownLatch releaseSubBV63 = new CountDownLatch(1);
    IdempotentRvvConsumer consumer1 = new IdempotentRvvConsumer(gate1, store1, (ev, id) -> {
      if ("sub-B".equals(ev.getSubscriptionId()) && ev.getRegionVersion() == 63L) {
        subBV63InFlight.countDown();
        try {
          releaseSubBV63.await(15, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      applied1.add(id);
    });
    receiver = new ClientEventReceiver(consumer1);
    int receiverPort1 = receiver.getPort();

    RvvClientGate gate2 = new RvvClientGate();
    gate2.registerSubscription("sub-A");
    gate2.registerSubscription("sub-B");
    IdempotentRvvConsumer.InMemoryDurableDeliveryStore store2 =
        new IdempotentRvvConsumer.InMemoryDurableDeliveryStore();
    List<IdempotentRvvConsumer.EventDeliveryId> applied2 = new CopyOnWriteArrayList<>();
    IdempotentRvvConsumer consumer2 =
        new IdempotentRvvConsumer(gate2, store2, (ev, id) -> applied2.add(id));
    receiverB = new ClientEventReceiver(consumer2);
    int receiverPort2 = receiverB.getPort();

    MemberVM locator = cluster.startLocatorVM(0);
    int locatorPort = locator.getPort();
    MemberVM server1 = cluster.startServerVM(1, locatorPort);
    MemberVM server2 = cluster.startServerVM(2, locatorPort);
    MemberVM server3 = cluster.startServerVM(3, locatorPort);
    MemberVM[] servers = new MemberVM[] {server1, server2, server3};
    boolean[] alive = new boolean[] {true, true, true};

    try {
      Random rng = new Random(20261204L);
      float[] queryVec = randomUnitVectorZeroLastDim(rng, VECTOR_DIM);
      List<VectorSubscription> subscriptions = Arrays.asList(
          new VectorSubscription("sub-A", queryVec, 0.90f),
          new VectorSubscription("sub-B", queryVec, 0.90f));

      for (MemberVM server : servers) {
        server.invoke(
            () -> initializeServerNode(subscriptions, receiverPort1, receiverPort2, 1, 4, 1));
      }

      // Phase 1a: 62 repeated-key overwrites of ONLY 4 hot keys ("hot-0".."hot-3")
      server3.invoke(() -> {
        Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
        for (int i = 1; i <= 62; i++) {
          region.put("hot-" + (i % 4), queryVec);
        }
      });

      int primaryIdx = -1;
      int secondaryIdx = -1;
      String primaryMemberId = null;
      for (int idx = 0; idx < servers.length; idx++) {
        String roleAndMember = servers[idx].invoke(() -> {
          PartitionedRegion pr =
              (PartitionedRegion) ClusterStartupRule.getCache().getRegion(REGION_NAME);
          if (pr.getDataStore().getAllLocalPrimaryBucketIds().contains(0)) {
            return "P|" + pr.getDataStore().getLocalBucketById(0).getVersionMember().toString();
          }
          if (pr.getDataStore().getAllLocalBucketIds().contains(0)) {
            return "S|secondary";
          }
          return null;
        });
        if (roleAndMember != null && roleAndMember.startsWith("P|")) {
          primaryIdx = idx;
          primaryMemberId = roleAndMember.substring(2);
        } else if (roleAndMember != null && roleAndMember.startsWith("S|")) {
          secondaryIdx = idx;
        }
      }
      assertThat(primaryIdx).isGreaterThanOrEqualTo(0);
      assertThat(secondaryIdx).isGreaterThanOrEqualTo(0);
      final String mP = primaryMemberId;
      final MemberVM secondaryVm = servers[secondaryIdx];

      await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
        assertThat(applied1).hasSize(124);
        assertThat(applied2).hasSize(124);
        assertThat(store2.getCommittedWatermark("sub-A", 0, mP)).isEqualTo(62L);
        assertThat(store2.getCommittedWatermark("sub-B", 0, mP)).isEqualTo(62L);
      });

      // Compact V_floor=60 & advance logical V_ckpt=64 across cluster (retaining [61..68])
      for (MemberVM server : servers) {
        server.invoke(() -> {
          serverService.acknowledgeCheckpoint(0, mP, 60L);
          serverService.recordReplicatedCheckpoint(0, mP, 64L);
        });
      }

      // Phase 1b: v=63 arrives on both consumers; consumer1 stalls sub-B v=63 in flight while
      // sub-A v=63 completes, and covering HB(64) must NOT retire sub-B v=63!
      secondaryVm.invoke(() -> {
        Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
        region.put("hot-3", queryVec);
      });
      assertThat(subBV63InFlight.await(15, TimeUnit.SECONDS)).isTrue();
      await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
        assertThat(applied2).hasSize(126);
        assertThat(store2.getCommittedWatermark("sub-A", 0, mP)).isEqualTo(63L);
        assertThat(store2.getCommittedWatermark("sub-B", 0, mP)).isEqualTo(63L);
      });

      consumer1.processEvent(
          ContinuousVectorEvent.watermarkHeartbeat(0, mP, 63L, System.currentTimeMillis()));
      assertThat(store1.getCommittedWatermark("sub-B", 0, mP)).isEqualTo(62L);
      assertThat(store1.getCommittedWatermark("sub-A", 0, mP)).isEqualTo(63L);
      assertThat(consumer1.getCommittedHighWaterByMember(0)).containsEntry(mP, 62L);

      // Sever wire for both consumers before v=64..68 (same 4 hot keys!) + crash primary VM!
      for (MemberVM server : servers) {
        server.invoke(() -> {
          dropOutboundEvents.set(true);
          dropOutboundEventsB.set(true);
        });
      }
      releaseSubBV63.countDown();
      secondaryVm.invoke(() -> {
        Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
        for (int i = 64; i <= 68; i++) {
          region.put("hot-" + (i % 4), queryVec);
        }
      });

      assertThat(secondaryVm.invoke(() -> serverService.getDispatcher().getTailSize(0)))
          .isEqualTo(8);

      // Kill primary VM -> secondary promotes to primary (m_S)
      alive[primaryIdx] = false;
      cluster.crashVM(primaryIdx + 1);
      await().atMost(30, TimeUnit.SECONDS).until(() -> secondaryVm.invoke(() -> {
        PartitionedRegion pr =
            (PartitionedRegion) ClusterStartupRule.getCache().getRegion(REGION_NAME);
        return pr.getDataStore().getAllLocalPrimaryBucketIds().contains(0)
            && serverService.getDispatcher().getTotalPromotionsHandled() >= 1L;
      }));
      String mS = secondaryVm.invoke(() -> {
        try {
          serverReplayExecutor.submit(() -> {
          }).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        PartitionedRegion pr =
            (PartitionedRegion) ClusterStartupRule.getCache().getRegion(REGION_NAME);
        return pr.getDataStore().getLocalBucketById(0).getVersionMember().toString();
      });
      assertThat(mS).isNotNull().isNotEqualTo(mP);

      // Ingest 8 more repeated-key overwrites on promoted primary (m_S) before reconnect
      secondaryVm.invoke(() -> {
        Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
        for (int i = 1; i <= 8; i++) {
          region.put("hot-" + (i % 4), queryVec);
        }
      });
      assertThat(
          secondaryVm.invoke(() -> ClusterStartupRule.getCache().getRegion(REGION_NAME).size()))
              .isEqualTo(4);

      // Reconnect both consumers via prepareForReconnect -> atomic admitAndReplayForReconnect;
      // consumer2 presents V_client=63 in V_floor=60 <= 63 < V_ckpt=64 -> MUST receive v=64!
      Map<String, Long> hw1 = consumer1.prepareForReconnect(0);
      Map<String, Long> hw2 = consumer2.prepareForReconnect(0);
      assertThat(hw2).containsEntry(mP, 63L);

      secondaryVm.invoke(() -> {
        serverTcpSink.enableReconnectBucketGate();
        serverTcpSinkB.enableReconnectBucketGate();
        dropOutboundEvents.set(false);
        dropOutboundEventsB.set(false);
        serverService.admitAndReplayForReconnect("consumer-1", 0, hw1, serverTcpSink,
            () -> serverTcpSink.allowLiveBucket(0));
        serverService.admitAndReplayForReconnect("consumer-2", 0, hw2, serverTcpSinkB,
            () -> serverTcpSinkB.allowLiveBucket(0));
      });

      Set<IdempotentRvvConsumer.EventDeliveryId> expectedIds = new HashSet<>();
      for (long v = 1L; v <= 68L; v++) {
        expectedIds.add(new IdempotentRvvConsumer.EventDeliveryId("sub-A", 0, mP, v));
        expectedIds.add(new IdempotentRvvConsumer.EventDeliveryId("sub-B", 0, mP, v));
      }
      for (long v = 1L; v <= 8L; v++) {
        expectedIds.add(new IdempotentRvvConsumer.EventDeliveryId("sub-A", 0, mS, v));
        expectedIds.add(new IdempotentRvvConsumer.EventDeliveryId("sub-B", 0, mS, v));
      }

      await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
        assertThat(applied1).hasSize(152);
        assertThat(applied2).hasSize(152);
      });
      assertThat(applied1).containsExactlyInAnyOrderElementsOf(expectedIds).doesNotHaveDuplicates();
      assertThat(applied2).containsExactlyInAnyOrderElementsOf(expectedIds).doesNotHaveDuplicates();
      assertThat(applied2).contains(
          new IdempotentRvvConsumer.EventDeliveryId("sub-A", 0, mP, 64L),
          new IdempotentRvvConsumer.EventDeliveryId("sub-B", 0, mP, 64L));
      assertThat(secondaryVm.invoke(() -> serverService.isResyncRequired(0, mP))).isFalse();
      assertThat(secondaryVm.invoke(() -> serverService.isResyncRequired(0, mS))).isFalse();

      // Stale probe below V_floor=60 triggers immediate resyncRequired
      assertThat(secondaryVm.invoke(() -> serverService
          .admitAndReplayForReconnect("sub-stale", 0, Collections.singletonMap(mP, 59L))))
              .isEmpty();
      assertThat(secondaryVm.invoke(() -> serverService.isResyncRequired(0, mP))).isTrue();
    } finally {
      for (int idx = 0; idx < servers.length; idx++) {
        if (alive[idx]) {
          servers[idx].invoke(ContinuousVectorRvvFailoverDistributedTest::shutdownServerNode);
        }
      }
    }
  }

  /**
   * R1 M3 DUnit regression: proves that when a client disconnects between a match batch
   * ({@code v=4}
   * matching {@code sub-A} and {@code sub-B}) and its covering heartbeat {@code HB(4)}, the
   * receiver-side monotonic {@code sessionEpoch} fence rejects the late dead-session heartbeat
   * before advancing {@code V_client}, and reconnect on the new session replays {@code (3, 4]}
   * strictly matches-before-heartbeat across a live multi-JVM cluster.
   */
  @Test
  public void disconnectBetweenMatchBatchAndCoveringHeartbeatCannotAdvanceWatermarkPastDroppedMatch()
      throws Exception {
    RvvClientGate clientGate = new RvvClientGate();
    clientGate.registerSubscription("sub-A");
    clientGate.registerSubscription("sub-B");
    IdempotentRvvConsumer.InMemoryDurableDeliveryStore durableStore =
        new IdempotentRvvConsumer.InMemoryDurableDeliveryStore();
    List<IdempotentRvvConsumer.EventDeliveryId> appliedEffects = new CopyOnWriteArrayList<>();
    IdempotentRvvConsumer consumer =
        new IdempotentRvvConsumer(clientGate, durableStore, (ev, id) -> appliedEffects.add(id));

    long session1 = consumer.getActiveSessionEpoch(0);
    assertThat(session1).isEqualTo(1L);
    ContinuousVectorEventSink session1Sink = consumer.createSessionSink(0, session1);
    receiver = new ClientEventReceiver(session1Sink);
    int receiverPort1 = receiver.getPort();

    MemberVM locator = cluster.startLocatorVM(0);
    int locatorPort = locator.getPort();
    MemberVM server1 = cluster.startServerVM(1, locatorPort);
    MemberVM server2 = cluster.startServerVM(2, locatorPort);
    MemberVM[] servers = new MemberVM[] {server1, server2};

    try {
      Random rng = new Random(20261205L);
      float[] queryVec = randomUnitVectorZeroLastDim(rng, VECTOR_DIM);
      List<VectorSubscription> subscriptions = Arrays.asList(
          new VectorSubscription("sub-A", queryVec, 0.90f),
          new VectorSubscription("sub-B", queryVec, 0.90f));

      for (MemberVM server : servers) {
        server.invoke(() -> initializeServerNode(subscriptions, receiverPort1, -1, 1, 4, 1));
      }

      // Live v=1..3 delivered on session 1 -> 6 effects, V_client=3
      server2.invoke(() -> {
        Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
        for (int i = 1; i <= 3; i++) {
          region.put("sf-doc-" + i, queryVec);
        }
      });

      int primaryIdx = -1;
      String primaryMemberId = null;
      for (int idx = 0; idx < servers.length; idx++) {
        String candidate = servers[idx].invoke(() -> {
          PartitionedRegion pr =
              (PartitionedRegion) ClusterStartupRule.getCache().getRegion(REGION_NAME);
          if (pr.getDataStore().getAllLocalPrimaryBucketIds().contains(0)) {
            return pr.getDataStore().getLocalBucketById(0).getVersionMember().toString();
          }
          return null;
        });
        if (candidate != null) {
          primaryIdx = idx;
          primaryMemberId = candidate;
          break;
        }
      }
      assertThat(primaryIdx).isGreaterThanOrEqualTo(0);
      final String mP = primaryMemberId;
      final MemberVM primaryVm = servers[primaryIdx];

      await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
        assertThat(appliedEffects).hasSize(6);
        assertThat(durableStore.getCommittedWatermark("sub-A", 0, mP)).isEqualTo(3L);
        assertThat(durableStore.getCommittedWatermark("sub-B", 0, mP)).isEqualTo(3L);
      });

      // Disconnect drops match batch v=4 (and triggers HB(4) at H=4 on primary!)
      for (MemberVM server : servers) {
        server.invoke(() -> dropOutboundEvents.set(true));
      }
      server2.invoke(() -> {
        Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
        region.put("sf-doc-4", queryVec);
      });
      assertThat(
          primaryVm.invoke(() -> serverService.getDispatcher().getTotalHeartbeatsDispatched()))
              .isGreaterThanOrEqualTo(1L);

      // Client prepares reconnect -> advances sessionEpoch 1 -> 2; late covering HB(4) from dead
      // session 1 MUST be rejected and MUST NOT advance V_client past 3!
      Map<String, Long> clientHw = consumer.prepareForReconnect(0);
      long session2 = consumer.getActiveSessionEpoch(0);
      assertThat(session2).isEqualTo(2L);
      assertThat(clientHw).containsEntry(mP, 3L);

      session1Sink.onEvent(
          ContinuousVectorEvent.watermarkHeartbeat(0, mP, 4L, System.currentTimeMillis()));
      assertThat(consumer.getStaleSessionEventsRejectedCount()).isEqualTo(1L);
      assertThat(durableStore.getCommittedWatermark("sub-A", 0, mP)).isEqualTo(3L);
      assertThat(durableStore.getCommittedWatermark("sub-B", 0, mP)).isEqualTo(3L);

      // Reconnect on new session 2 -> server replays (3, 4] strictly matches-before-heartbeat
      receiver.close();
      ContinuousVectorEventSink session2Sink = consumer.createSessionSink(0, session2);
      receiver = new ClientEventReceiver(session2Sink);
      int receiverPort2 = receiver.getPort();
      for (MemberVM server : servers) {
        server.invoke(() -> {
          if (serverTcpSink != null) {
            serverService.removeEventSink(serverTcpSink);
            serverTcpSink.close();
          }
          dropOutboundEvents.set(false);
          serverTcpSink = new TcpEventSink(receiverPort2, dropOutboundEvents);
          serverService.addEventSink(serverTcpSink);
        });
      }
      primaryVm.invoke(() -> {
        serverTcpSink.enableReconnectBucketGate();
        serverService.admitAndReplayForReconnect("sub-session-dunit", 0, clientHw, serverTcpSink,
            () -> serverTcpSink.allowLiveBucket(0));
      });

      await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
        assertThat(appliedEffects).hasSize(8).doesNotHaveDuplicates();
        assertThat(appliedEffects).contains(
            new IdempotentRvvConsumer.EventDeliveryId("sub-A", 0, mP, 4L),
            new IdempotentRvvConsumer.EventDeliveryId("sub-B", 0, mP, 4L));
        assertThat(durableStore.getCommittedWatermark("sub-A", 0, mP)).isEqualTo(4L);
        assertThat(durableStore.getCommittedWatermark("sub-B", 0, mP)).isEqualTo(4L);
      });
    } finally {
      for (MemberVM server : servers) {
        server.invoke(ContinuousVectorRvvFailoverDistributedTest::shutdownServerNode);
      }
    }
  }

  /**
   * R1 M2 / R2 M2 / R2 M3 DUnit regression: exercises (a) an idle heartbeat + floor-to-checkpoint
   * reconnect
   * ({@code V_floor=6 < V_client=8 < V_ckpt=10}) right at tail capacity {@code C_tail - 1}
   * with zero overflow and exact {@code V_ckpt >= v_0} progress, and (b) subsequent tail overflow
   * atomically evicting a lagging subscriber from {@code S_healthy} into {@code resyncRequired}
   * while preserving {@code V_floor <= V_ckpt}.
   */
  @Test
  public void idleHeartbeatAndFloorToCheckpointReconnectNearCapacityAndOverflowAcrossCluster()
      throws Exception {
    RvvClientGate fastGate = new RvvClientGate();
    fastGate.registerSubscription("sub-fast");
    receiver = new ClientEventReceiver(fastGate);
    int fastPort = receiver.getPort();

    RvvClientGate lagGate = new RvvClientGate();
    lagGate.registerSubscription("sub-lagging");
    List<ContinuousVectorEvent> lagAccepted = new CopyOnWriteArrayList<>();
    lagGate.addListener(ev -> {
      if ("sub-lagging".equals(ev.getSubscriptionId())) {
        lagAccepted.add(ev);
      }
    });
    receiverB = new ClientEventReceiver(lagGate);
    int lagPort = receiverB.getPort();

    MemberVM locator = cluster.startLocatorVM(0);
    int locatorPort = locator.getPort();
    MemberVM server1 = cluster.startServerVM(1, locatorPort);
    MemberVM server2 = cluster.startServerVM(2, locatorPort);
    MemberVM[] servers = new MemberVM[] {server1, server2};
    final int cTail = 32;

    try {
      Random rng = new Random(20261206L);
      float[] queryVec = randomUnitVectorZeroLastDim(rng, VECTOR_DIM);
      List<VectorSubscription> subscriptions = Arrays.asList(
          new VectorSubscription("sub-fast", queryVec, 0.90f),
          new VectorSubscription("sub-lagging", queryVec, 0.90f));

      for (MemberVM server : servers) {
        server.invoke(() -> {
          initializeServerNode(subscriptions, fastPort, lagPort, 1, 64, 1, cTail);
          serverService.registerSubscriberForCheckpoint("sub-fast");
        });
      }

      // Baseline v=1..8 live on both fast and lagging clients, then disconnect lagging at
      // V_client=8
      server2.invoke(() -> {
        Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
        for (int i = 1; i <= 8; i++) {
          region.put("cap-idle-" + i, queryVec);
        }
      });

      int primaryIdx = -1;
      String primaryMemberId = null;
      for (int idx = 0; idx < servers.length; idx++) {
        String candidate = servers[idx].invoke(() -> {
          PartitionedRegion pr =
              (PartitionedRegion) ClusterStartupRule.getCache().getRegion(REGION_NAME);
          if (pr.getDataStore().getAllLocalPrimaryBucketIds().contains(0)) {
            return pr.getDataStore().getLocalBucketById(0).getVersionMember().toString();
          }
          return null;
        });
        if (candidate != null) {
          primaryIdx = idx;
          primaryMemberId = candidate;
          break;
        }
      }
      assertThat(primaryIdx).isGreaterThanOrEqualTo(0);
      final String mP = primaryMemberId;
      final MemberVM primaryVm = servers[primaryIdx];

      await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
        assertThat(lagAccepted).hasSize(8);
        assertThat(lagGate.getSafeCheckpointVersion(0, mP)).isEqualTo(8L);
      });

      for (MemberVM server : servers) {
        server.invoke(() -> dropOutboundEventsB.set(true));
      }
      server2.invoke(() -> {
        Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
        region.put("cap-idle-9", queryVec);
        region.put("cap-idle-10", queryVec);
      });
      await().atMost(15, TimeUnit.SECONDS)
          .until(() -> fastGate.getSafeCheckpointVersion(0, mP) == 10L);

      // Compact V_floor=6 and advance logical V_ckpt=10 -> R_ret=4 retained entries [7..10]
      for (MemberVM server : servers) {
        server.invoke(() -> {
          serverService.acknowledgeCheckpoint(0, mP, 6L);
          serverService.recordReplicatedCheckpoint(0, mP, 10L);
        });
      }

      // Write 27 more mutations (v=11..37, < H=64) -> occupancy = 4 + 27 = 31 == C_tail - 1!
      final long v0 = 11L;
      server2.invoke(() -> {
        Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
        for (int i = 11; i <= 37; i++) {
          region.put("cap-idle-" + i, queryVec);
        }
      });
      assertThat(primaryVm.invoke(() -> serverService.getDispatcher().getTailSize(0)))
          .isEqualTo(cTail - 1);
      assertThat(primaryVm.invoke(() -> serverService.getDispatcher().getTailOverflowCount()))
          .isZero();

      // Trigger idle heartbeat covering v=37 (>= v0) AND reconnect sub-lagging at V_client=8
      long flushNow = System.currentTimeMillis() + 500L;
      int flushed = primaryVm.invoke(() -> serverService.flushIdleHeartbeats(flushNow, 100L));
      assertThat(flushed).isGreaterThanOrEqualTo(1);

      Map<String, Long> lagHw = lagGate.getSafeHighWaterByMember(0);
      assertThat(lagHw).containsEntry(mP, 8L);
      primaryVm.invoke(() -> {
        serverTcpSinkB.enableReconnectBucketGate();
        dropOutboundEventsB.set(false);
        serverService.admitAndReplayForReconnect("sub-lagging", 0, lagHw, serverTcpSinkB,
            () -> serverTcpSinkB.allowLiveBucket(0));
      });

      await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
        assertThat(lagAccepted).hasSize(37);
        assertThat(lagAccepted).extracting(ContinuousVectorEvent::getRegionVersion)
            .contains(9L, 10L, 11L, 37L)
            .doesNotHaveDuplicates();
        assertThat(fastGate.getSafeCheckpointVersion(0, mP)).isEqualTo(37L);
        assertThat(lagGate.getSafeCheckpointVersion(0, mP)).isEqualTo(37L);
      });

      // Both subscribers ACK covering idle heartbeat 37 -> V_ckpt >= v0 (37), V_floor=37, tail=0
      primaryVm.invoke(() -> {
        serverService.acknowledgeSubscriberCheckpoint("sub-fast", 0, mP, 37L);
        serverService.acknowledgeSubscriberCheckpoint("sub-lagging", 0, mP, 37L);
      });
      assertThat(primaryVm.invoke(() -> serverService.getCheckpointVersion(0, mP)))
          .isGreaterThanOrEqualTo(v0)
          .isEqualTo(37L);
      assertThat(primaryVm.invoke(() -> serverService.getReplayFloorVersion(0, mP)))
          .isEqualTo(37L);
      assertThat(primaryVm.invoke(() -> serverService.getDispatcher().getTailSize(0))).isZero();
      assertThat(primaryVm.invoke(() -> serverService.getDispatcher().getTailOverflowCount()))
          .isZero();

      // Part (b) R2 M2: Advance sub-fast to 45 while sub-lagging stalls at 37; ingest 33 puts
      // (v=38..70 -> overflow evicts v=38) -> atomically evicts sub-lagging into resyncRequired,
      // keeps sub-fast in S_healthy, and advances V_ckpt = 45 (V_floor <= V_ckpt)!
      server2.invoke(() -> {
        Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
        for (int i = 38; i <= 45; i++) {
          region.put("ovf-" + i, queryVec);
        }
      });
      primaryVm.invoke(() -> serverService.acknowledgeSubscriberCheckpoint("sub-fast", 0, mP, 45L));
      server2.invoke(() -> {
        Region<String, float[]> region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
        for (int i = 46; i <= 70; i++) {
          region.put("ovf-" + i, queryVec);
        }
      });

      assertThat(primaryVm.invoke(() -> serverService.isResyncRequired(0, mP))).isTrue();
      assertThat(primaryVm
          .invoke(() -> serverService.isSubscriberResyncRequired("sub-lagging", 0, mP))).isTrue();
      assertThat(
          primaryVm.invoke(() -> serverService.isSubscriberResyncRequired("sub-fast", 0, mP)))
              .isFalse();
      assertThat(primaryVm.invoke(
          () -> serverService.getDispatcher().getRegisteredCheckpointSubscribers()))
              .containsExactly("sub-fast");
      long floorAfter = primaryVm.invoke(() -> serverService.getReplayFloorVersion(0, mP));
      long ckptAfter = primaryVm.invoke(() -> serverService.getCheckpointVersion(0, mP));
      assertThat(floorAfter).isEqualTo(45L);
      assertThat(ckptAfter).isEqualTo(45L);
      assertThat(floorAfter).isLessThanOrEqualTo(ckptAfter);
    } finally {
      for (MemberVM server : servers) {
        server.invoke(ContinuousVectorRvvFailoverDistributedTest::shutdownServerNode);
      }
    }
  }

  private static void initializeServerNode(List<VectorSubscription> subscriptions,
      int receiverPort) throws IOException {
    initializeServerNode(subscriptions, receiverPort, -1, NUM_BUCKETS, 4, 1);
  }

  private static void initializeServerNode(List<VectorSubscription> subscriptions,
      int receiverPortA, int receiverPortB, int numBuckets, int heartbeatInterval,
      int asyncDispatchThreads) throws IOException {
    initializeServerNode(subscriptions, receiverPortA, receiverPortB, numBuckets, heartbeatInterval,
        asyncDispatchThreads, RvvMonotonicVectorDispatcher.DEFAULT_MAX_TAIL_PER_STREAM);
  }

  private static void initializeServerNode(List<VectorSubscription> subscriptions,
      int receiverPortA, int receiverPortB, int numBuckets, int heartbeatInterval,
      int asyncDispatchThreads, int maxTailPerStream) throws IOException {
    dropOutboundEvents.set(false);
    dropOutboundEventsB.set(false);
    RouterConfig config = RouterConfig.builder(VECTOR_DIM)
        .numClusters(8)
        .expectedSurvivalFraction(0.20d)
        .hysteresisDelta(0.15d)
        .randomSeed(777L)
        .build();
    ChordalVoronoiRouter router = new ChordalVoronoiRouter(config);
    RvvMonotonicVectorDispatcher dispatcher =
        new RvvMonotonicVectorDispatcher(router, maxTailPerStream);
    serverService = new ContinuousVectorQueryService(router, dispatcher);
    serverService.setHeartbeatIntervalEvents(heartbeatInterval);
    serverService.registerSubscriptions(subscriptions);
    if (asyncDispatchThreads > 1) {
      serverDispatchExecutor = Executors.newFixedThreadPool(asyncDispatchThreads, r -> {
        Thread t = new Thread(r, "rvv-failover-async-dispatch");
        t.setDaemon(true);
        return t;
      });
      serverService.setAsyncDispatchExecutor(serverDispatchExecutor);
    }

    serverTcpSink = new TcpEventSink(receiverPortA, dropOutboundEvents);
    serverService.addEventSink(serverTcpSink);
    if (receiverPortB > 0) {
      serverTcpSinkB = new TcpEventSink(receiverPortB, dropOutboundEventsB);
      serverService.addEventSink(serverTcpSinkB);
    }

    serverReplayExecutor = Executors.newSingleThreadExecutor(r -> {
      Thread t = new Thread(r, "rvv-failover-replay");
      t.setDaemon(true);
      return t;
    });

    Cache cache = ClusterStartupRule.getCache();
    PartitionAttributesFactory<String, float[]> paf = new PartitionAttributesFactory<>();
    paf.setRedundantCopies(1);
    paf.setTotalNumBuckets(numBuckets);
    paf.addPartitionListener(
        serverService.createPartitionListener(val -> (float[]) val, serverReplayExecutor));

    cache.<String, float[]>createRegionFactory(RegionShortcut.PARTITION_REDUNDANT)
        .setPartitionAttributes(paf.create())
        .addCacheListener(serverService.createCacheListener(val -> (float[]) val))
        .create(REGION_NAME);
  }

  private static void shutdownServerNode() {
    if (serverReplayExecutor != null) {
      serverReplayExecutor.shutdownNow();
      serverReplayExecutor = null;
    }
    if (serverDispatchExecutor != null) {
      serverDispatchExecutor.shutdownNow();
      serverDispatchExecutor = null;
    }
    if (serverTcpSink != null) {
      serverTcpSink.close();
      serverTcpSink = null;
    }
    if (serverTcpSinkB != null) {
      serverTcpSinkB.close();
      serverTcpSinkB = null;
    }
    if (serverService != null) {
      serverService.stopIdleHeartbeatScheduler();
      serverService = null;
    }
  }

  private static Set<String> computeBruteForceMatches(List<VectorSubscription> subscriptions,
      List<float[]> vectors, int startInclusive, int endExclusive) {
    Set<String> matches = new HashSet<>();
    for (int i = startInclusive; i < endExclusive; i++) {
      float[] unit = VectorSubscription.normalize(vectors.get(i));
      for (VectorSubscription sub : subscriptions) {
        float sim = VectorSubscription.dotProductContiguous(unit,
            sub.getNormalizedQueryVector(), 0, VECTOR_DIM);
        if (sim >= sub.getSimilarityThreshold()) {
          matches.add("doc-" + i + "|" + sub.getSubscriptionId());
        }
      }
    }
    return matches;
  }

  private static Set<String> collectKeySubPairs(List<ContinuousVectorEvent> events) {
    Set<String> pairs = new HashSet<>(events.size() * 2);
    for (ContinuousVectorEvent e : events) {
      pairs.add(e.getKey() + "|" + e.getSubscriptionId());
    }
    return pairs;
  }

  private static float[] randomUnitVectorZeroLastDim(Random rng, int dim) {
    float[] v = new float[dim];
    for (int i = 0; i < dim - 1; i++) {
      v[i] = (float) rng.nextGaussian();
    }
    v[dim - 1] = 0.0f;
    return VectorSubscription.normalize(v);
  }

  private static float[] perturbVectorZeroLastDim(Random rng, float[] base, float noiseStd) {
    float[] v = new float[base.length];
    for (int i = 0; i < base.length - 1; i++) {
      v[i] = base[i] + (float) (rng.nextGaussian() * noiseStd);
    }
    v[base.length - 1] = 0.0f;
    return VectorSubscription.normalize(v);
  }

  /** Ordered thread-safe TCP sink forwarding RVV-stamped events to the client gate receiver. */
  private static final class TcpEventSink implements ContinuousVectorEventSink {
    private final Socket socket;
    private final ObjectOutputStream out;
    private final AtomicBoolean dropFlag;
    private volatile boolean enforceLiveBucketGate = false;
    private final Set<Integer> liveEnabledBuckets = ConcurrentHashMap.newKeySet();

    TcpEventSink(int port, AtomicBoolean dropFlag) throws IOException {
      this.socket = new Socket(InetAddress.getLoopbackAddress(), port);
      this.out = new ObjectOutputStream(socket.getOutputStream());
      this.out.flush();
      this.dropFlag = dropFlag;
    }

    void enableReconnectBucketGate() {
      liveEnabledBuckets.clear();
      enforceLiveBucketGate = true;
    }

    void allowLiveBucket(int bucketId) {
      liveEnabledBuckets.add(bucketId);
    }

    @Override
    public synchronized void onEvent(ContinuousVectorEvent event) {
      if (dropFlag.get()) {
        return;
      }
      if (enforceLiveBucketGate && !event.isReplayed()
          && !liveEnabledBuckets.contains(event.getBucketId())) {
        return;
      }
      try {
        out.writeObject(event);
        out.flush();
        out.reset();
      } catch (IOException ignored) {
        // Expected if test closes or server crashes
      }
    }

    synchronized void close() {
      try {
        out.close();
      } catch (IOException ignored) {
      }
      try {
        socket.close();
      } catch (IOException ignored) {
      }
    }
  }

  /**
   * Embedded client-side TCP receiver feeding incoming server events into {@link RvvClientGate}.
   */
  private static final class ClientEventReceiver {
    private final ServerSocket serverSocket;
    private final ExecutorService executor = Executors.newCachedThreadPool(r -> {
      Thread t = new Thread(r, "rvv-client-receiver");
      t.setDaemon(true);
      return t;
    });
    private final AtomicBoolean running = new AtomicBoolean(true);

    ClientEventReceiver(RvvClientGate gate) throws IOException {
      this(gate::tryAccept);
    }

    ClientEventReceiver(ContinuousVectorEventSink sink) throws IOException {
      this.serverSocket = new ServerSocket(0, 16, InetAddress.getLoopbackAddress());
      executor.submit(() -> {
        while (running.get()) {
          try {
            Socket client = serverSocket.accept();
            executor.submit(() -> handleConnection(client, sink));
          } catch (SocketException e) {
            if (!running.get()) {
              break;
            }
          } catch (IOException ignored) {
          }
        }
      });
    }

    int getPort() {
      return serverSocket.getLocalPort();
    }

    private void handleConnection(Socket socket, ContinuousVectorEventSink sink) {
      try (Socket s = socket; ObjectInputStream in = new ObjectInputStream(s.getInputStream())) {
        while (running.get()) {
          Object obj = in.readObject();
          if (obj instanceof ContinuousVectorEvent) {
            sink.onEvent((ContinuousVectorEvent) obj);
          }
        }
      } catch (EOFException | SocketException ignored) {
        // Expected when server VM crashes or closes cleanly
      } catch (IOException | ClassNotFoundException ignored) {
      }
    }

    void close() {
      running.set(false);
      try {
        serverSocket.close();
      } catch (IOException ignored) {
      }
      executor.shutdownNow();
    }
  }
}
