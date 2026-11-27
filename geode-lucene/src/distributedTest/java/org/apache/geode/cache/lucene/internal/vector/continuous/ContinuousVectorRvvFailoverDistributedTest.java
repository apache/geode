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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
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
          serverService.setMaxHoldbackAgeMillis(150L);
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
      final long successorVer = seedVer + 2L;

      // Inject out-of-order v=seedVer+2 ahead of missing predecessor seedVer+1 -> held briefly
      // behind gap, then closed automatically once age exceeds maxHoldbackAgeMillis=150 ms
      resolvedPrimary.invoke(() -> serverService.onBucketMutation(0, bucket0Member, successorVer,
          "gap-successor-3", queryVec, true));

      await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
        assertThat(resolvedPrimary.invoke(() -> serverService.isResyncRequired(0, bucket0Member)))
            .isTrue();
        assertThat(resolvedPrimary.invoke(() -> serverService.getHoldbackGapClosedCount()))
            .isGreaterThanOrEqualTo(1L);
        assertThat(
            resolvedPrimary.invoke(() -> serverService.getDispatcher().getHoldbackSize(0)))
                .isZero();
        assertThat(accepted).extracting(ContinuousVectorEvent::getKey)
            .contains("seed-gap-1", "gap-successor-3");
        assertThat(clientGate.getSafeCheckpointVersion(0, bucket0Member))
            .isGreaterThanOrEqualTo(successorVer);
      });
    } finally {
      for (MemberVM server : new MemberVM[] {server1, server2}) {
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
    dropOutboundEvents.set(false);
    dropOutboundEventsB.set(false);
    RouterConfig config = RouterConfig.builder(VECTOR_DIM)
        .numClusters(8)
        .expectedSurvivalFraction(0.20d)
        .hysteresisDelta(0.15d)
        .randomSeed(777L)
        .build();
    serverService = new ContinuousVectorQueryService(config);
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
      this.serverSocket = new ServerSocket(0, 16, InetAddress.getLoopbackAddress());
      executor.submit(() -> {
        while (running.get()) {
          try {
            Socket client = serverSocket.accept();
            executor.submit(() -> handleConnection(client, gate));
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

    private void handleConnection(Socket socket, RvvClientGate gate) {
      try (Socket s = socket; ObjectInputStream in = new ObjectInputStream(s.getInputStream())) {
        while (running.get()) {
          Object obj = in.readObject();
          if (obj instanceof ContinuousVectorEvent) {
            gate.tryAccept((ContinuousVectorEvent) obj);
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
