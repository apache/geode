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
  private static volatile TcpEventSink serverTcpSink;
  private static final AtomicBoolean dropOutboundEvents = new AtomicBoolean(false);
  private static final AtomicInteger phase2WrittenCount = new AtomicInteger(0);
  private static final AtomicBoolean promotionCompleteFlag = new AtomicBoolean(false);
  private static final AtomicBoolean reconnectStartedFlag = new AtomicBoolean(false);

  @Rule
  public transient ClusterStartupRule cluster = new ClusterStartupRule();

  private transient ClientEventReceiver receiver;

  @After
  public void tearDown() {
    if (receiver != null) {
      receiver.close();
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

  private static void initializeServerNode(List<VectorSubscription> subscriptions,
      int receiverPort) throws IOException {
    dropOutboundEvents.set(false);
    RouterConfig config = RouterConfig.builder(VECTOR_DIM)
        .numClusters(8)
        .expectedSurvivalFraction(0.20d)
        .hysteresisDelta(0.15d)
        .randomSeed(777L)
        .build();
    serverService = new ContinuousVectorQueryService(config);
    serverService.setHeartbeatIntervalEvents(4);
    serverService.registerSubscriptions(subscriptions);

    serverTcpSink = new TcpEventSink(receiverPort, dropOutboundEvents);
    serverService.addEventSink(serverTcpSink);

    serverReplayExecutor = Executors.newSingleThreadExecutor(r -> {
      Thread t = new Thread(r, "rvv-failover-replay");
      t.setDaemon(true);
      return t;
    });

    Cache cache = ClusterStartupRule.getCache();
    PartitionAttributesFactory<String, float[]> paf = new PartitionAttributesFactory<>();
    paf.setRedundantCopies(1);
    paf.setTotalNumBuckets(NUM_BUCKETS);
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
    if (serverTcpSink != null) {
      serverTcpSink.close();
      serverTcpSink = null;
    }
    serverService = null;
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
