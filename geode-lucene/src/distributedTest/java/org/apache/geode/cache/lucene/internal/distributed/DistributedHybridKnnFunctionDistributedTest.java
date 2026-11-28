/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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

package org.apache.geode.cache.lucene.internal.distributed;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.cache.control.RebalanceFactory;
import org.apache.geode.cache.control.RebalanceOperation;
import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.cache.lucene.LuceneServiceProvider;
import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.cache.lucene.internal.vector.Tier0VectorBucketListener;
import org.apache.geode.cache.lucene.internal.vector.Tier0VectorDeltaBuffer;
import org.apache.geode.cache.lucene.internal.vector.Tier0VectorRegistry;
import org.apache.geode.cache.lucene.internal.vector.VectorSimilarityScorer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.cache.AbstractBucketRegionQueue;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderQueue;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.LuceneTest;

/**
 * Multi-bucket DUnit tests for {@link DistributedHybridKnnFunction} and
 * {@link Tier0VectorDeltaBuffer} across partitioned regions on multiple server VMs:
 * <ul>
 * <li>Read-after-write visibility before AEQ flush, tombstone/update masking, and post-flush
 * parity.</li>
 * <li>Pre-filtered k-NN selectivity regimes (1%, 5%, 50%) against exact brute-force ground
 * truth.</li>
 * <li>Concurrent writes, updates, and destroys interleaved with continuous hybrid queries.</li>
 * <li>Bucket rebalancing with both committed Lucene HNSW segments and in-flight Tier-0
 * entries.</li>
 * </ul>
 */
@Category({LuceneTest.class})
public class DistributedHybridKnnFunctionDistributedTest extends JUnit4CacheTestCase {

  private static final String REGION_NAME = "hybridVectors";
  private static final String INDEX_NAME = "hybridVecIdx";
  private static final String VECTOR_FIELD = "embedding";
  private static final int NUM_BUCKETS = 16;
  private static final int DIM = 16;
  private static final AtomicBoolean REMOTE_WRITER_STOP = new AtomicBoolean(false);
  private static volatile CountDownLatch OWNERSHIP_HOOK_ENTERED;
  private static volatile CountDownLatch OWNERSHIP_HOOK_RELEASE;

  private VM dataStore1;
  private VM dataStore2;
  private VM accessor;

  @Before
  public void setUpVMs() {
    Host host = Host.getHost(0);
    dataStore1 = host.getVM(0);
    dataStore2 = host.getVM(1);
    accessor = host.getVM(2);
  }

  @After
  public void cleanUpRegistries() {
    for (VM vm : Arrays.asList(dataStore1, dataStore2, accessor)) {
      if (vm != null) {
        vm.invoke(() -> {
          DistributedHybridKnnFunction.beforeAcquireSearcherHookForTest = null;
          CountDownLatch release = OWNERSHIP_HOOK_RELEASE;
          if (release != null) {
            release.countDown();
          }
          OWNERSHIP_HOOK_ENTERED = null;
          OWNERSHIP_HOOK_RELEASE = null;
          if (hasCache()) {
            Tier0VectorRegistry.removeCache(getCache());
          }
        });
      }
    }
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties props = super.getDistributedSystemProperties();
    props.put(ConfigurationProperties.DISABLE_AUTO_RECONNECT, "true");
    props.put(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
        VectorRecord.class.getName() + ";"
            + VectorRecordLuceneSerializer.class.getName() + ";"
            + "org.apache.geode.test.dunit.**");
    return props;
  }

  @Test
  public void readAfterWriteVisibilityBeforeAeqFlushAndAfterCommit() {
    dataStore1.invoke(() -> initServer(1));
    dataStore2.invoke(() -> initServer(1));
    accessor.invoke(() -> initAccessor(1));

    Random rng = new Random(101L);
    float[] query = new float[DIM];
    query[0] = 1.0f;

    // Seed all buckets and flush once so every bucket's Lucene IndexRepository is initialized
    // before pausing AEQ (otherwise first-touch repository creation runs initial bucket reindex).
    float[] orthogonalSeed = new float[DIM];
    orthogonalSeed[DIM - 1] = 1.0f;
    Map<String, VectorRecord> groundTruth = new HashMap<>();
    accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      for (int i = 0; i < NUM_BUCKETS * 2; i++) {
        String seedKey = "seed-" + i;
        region.put(seedKey, new VectorRecord(orthogonalSeed, "seed", "all", "all"));
      }
    });
    for (int i = 0; i < NUM_BUCKETS * 2; i++) {
      groundTruth.put("seed-" + i, new VectorRecord(orthogonalSeed, "seed", "all", "all"));
    }
    waitForLuceneFlush(accessor);

    // Pause AEQ on both servers so none of the newly written "raw-*" docs flush to Lucene yet
    dataStore1.invoke(() -> pauseIndexAeq(getCache()));
    dataStore2.invoke(() -> pauseIndexAeq(getCache()));

    Map<String, VectorRecord> freshRawDocs = new HashMap<>();
    for (int i = 0; i < 80; i++) {
      String key = "raw-" + i;
      float[] vec = randomUnitVector(rng, DIM);
      // Ensure positive alignment with query so fresh docs outrank orthogonal seeds
      vec[0] = Math.abs(vec[0]) + 0.5f;
      normalizeInPlace(vec);
      VectorRecord rec = new VectorRecord(vec, "cat-" + (i % 4), "all", "all");
      freshRawDocs.put(key, rec);
      groundTruth.put(key, rec);
    }

    // Put all 80 fresh records across all 16 buckets while AEQ is paused
    accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      for (Map.Entry<String, VectorRecord> entry : freshRawDocs.entrySet()) {
        region.put(entry.getKey(), entry.getValue());
      }
    });

    List<String> expectedTop10 =
        computeBruteForceTopK(groundTruth, query, 10, VectorSimilarityFunction.COSINE, null);
    assertThat(expectedTop10).allMatch(k -> k.startsWith("raw-"));

    // 1. Lucene-only query (includeTier0=false) must see ZERO fresh "raw-*" docs (AEQ is paused)
    List<String> luceneOnlyKeys = accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      HybridKnnQueryContext ctx = HybridKnnQueryContext
          .builder(INDEX_NAME, VECTOR_FIELD, query, 10)
          .similarityFunction(VectorSimilarityFunction.COSINE)
          .includeTier0(false)
          .build();
      return extractKeys(DistributedHybridKnnFunction.executeOnRegion(region, ctx));
    });
    assertThat(luceneOnlyKeys).noneMatch(k -> k.startsWith("raw-"));

    // 2. Hybrid query (includeTier0=true) BEFORE AEQ flush must achieve 100% top-10 recall parity
    List<String> hybridBeforeFlush = accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      HybridKnnQueryContext ctx = HybridKnnQueryContext
          .builder(INDEX_NAME, VECTOR_FIELD, query, 10)
          .similarityFunction(VectorSimilarityFunction.COSINE)
          .includeTier0(true)
          .build();
      return extractKeys(DistributedHybridKnnFunction.executeOnRegion(region, ctx));
    });
    assertThat(hybridBeforeFlush).containsExactlyElementsOf(expectedTop10);

    // 3. Mutate top-1 (flip to opposite vector) and destroy top-2 BEFORE flush
    String top1Key = expectedTop10.get(0);
    String top2Key = expectedTop10.get(1);
    float[] opposite = negate(query);
    groundTruth.put(top1Key, new VectorRecord(opposite, "cat-0", "all", "all"));
    groundTruth.remove(top2Key);

    accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      region.put(top1Key, new VectorRecord(opposite, "cat-0", "all", "all"));
      region.destroy(top2Key);
    });

    List<String> expectedAfterMutation =
        computeBruteForceTopK(groundTruth, query, 10, VectorSimilarityFunction.COSINE, null);

    List<String> hybridAfterMutation = accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      HybridKnnQueryContext ctx = HybridKnnQueryContext
          .builder(INDEX_NAME, VECTOR_FIELD, query, 10)
          .similarityFunction(VectorSimilarityFunction.COSINE)
          .build();
      return extractKeys(DistributedHybridKnnFunction.executeOnRegion(region, ctx));
    });
    assertThat(hybridAfterMutation).containsExactlyElementsOf(expectedAfterMutation);
    assertThat(hybridAfterMutation).doesNotContain(top1Key, top2Key);

    // 4. Resume AEQ, wait until flushed, verify Tier-0 drained and post-flush recall parity is 100%
    dataStore1.invoke(() -> resumeIndexAeq(getCache()));
    dataStore2.invoke(() -> resumeIndexAeq(getCache()));
    waitForLuceneFlush(accessor);

    int totalTier0Remaining = dataStore1.invoke(this::countTier0Entries)
        + dataStore2.invoke(this::countTier0Entries);
    assertThat(totalTier0Remaining).isEqualTo(0);

    List<String> afterFlushKeys = accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      HybridKnnQueryContext ctx = HybridKnnQueryContext
          .builder(INDEX_NAME, VECTOR_FIELD, query, 10)
          .similarityFunction(VectorSimilarityFunction.COSINE)
          .build();
      return extractKeys(DistributedHybridKnnFunction.executeOnRegion(region, ctx));
    });
    assertThat(afterFlushKeys).containsExactlyElementsOf(expectedAfterMutation);
  }

  @Test
  public void filteredSelectivityRegimesOneFiveAndFiftyPercent() {
    dataStore1.invoke(() -> initServer(1));
    dataStore2.invoke(() -> initServer(1));
    accessor.invoke(() -> initAccessor(1));

    final int totalDocs = 200;
    Random rng = new Random(202L);
    float[] query = randomUnitVector(rng, DIM);
    Map<String, VectorRecord> dataset = new HashMap<>();

    // Build 200 docs with exact selectivity tags:
    // - sel1 = "hit" for exactly 2 docs (1% selectivity: i == 0, 180)
    // - sel5 = "hit" for exactly 10 docs (5% selectivity: i % 20 == 0)
    // - sel50 = "hit" for exactly 100 docs (50% selectivity: i % 2 == 0)
    for (int i = 0; i < totalDocs; i++) {
      String key = "sel-" + i;
      float[] vec = randomUnitVector(rng, DIM);
      String sel1 = (i == 0 || i == 180) ? "hit" : "miss";
      String sel5 = (i % 20 == 0) ? "hit" : "miss";
      String sel50 = (i % 2 == 0) ? "hit" : "miss";
      dataset.put(key, new VectorRecord(vec, sel1, sel5, sel50));
    }

    // Commit first 160 docs to Lucene HNSW segments
    accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      for (int i = 0; i < 160; i++) {
        String key = "sel-" + i;
        region.put(key, dataset.get(key));
      }
    });
    waitForLuceneFlush(accessor);

    // Pause AEQ and write remaining 40 docs (including sel-180 for 1%, sel-160/180 for 5%, etc.)
    // so every selectivity regime spans BOTH committed Lucene HNSW AND live Tier-0!
    dataStore1.invoke(() -> pauseIndexAeq(getCache()));
    dataStore2.invoke(() -> pauseIndexAeq(getCache()));
    accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      for (int i = 160; i < totalDocs; i++) {
        String key = "sel-" + i;
        region.put(key, dataset.get(key));
      }
    });

    // Verify 1%, 5%, and 50% selectivity while mixed (Lucene HNSW + Tier-0)
    verifySelectivityRegime(dataset, query, "sel1", 2, 1.0);
    verifySelectivityRegime(dataset, query, "sel5", 5, 1.0);
    verifySelectivityRegime(dataset, query, "sel50", 10, 0.9);

    // Resume AEQ, flush all 200 docs into Lucene HNSW, and re-verify all 3 selectivity regimes
    dataStore1.invoke(() -> resumeIndexAeq(getCache()));
    dataStore2.invoke(() -> resumeIndexAeq(getCache()));
    waitForLuceneFlush(accessor);

    verifySelectivityRegime(dataset, query, "sel1", 2, 1.0);
    verifySelectivityRegime(dataset, query, "sel5", 5, 1.0);
    verifySelectivityRegime(dataset, query, "sel50", 10, 0.9);
  }

  @Test
  public void concurrentWritesAndContinuousHybridQueriesMaintainRecallParity() {
    dataStore1.invoke(() -> initServer(1));
    dataStore2.invoke(() -> initServer(1));
    accessor.invoke(() -> initAccessor(1));

    Random rng = new Random(303L);
    float[] query = randomUnitVector(rng, DIM);

    // Pre-generate deterministic sequence of writes + updates + destroys
    Map<String, VectorRecord> initialRecords = new HashMap<>();
    Map<String, VectorRecord> updatedRecords = new HashMap<>();
    Set<String> destroyedKeys = new HashSet<>();
    for (int i = 0; i < 120; i++) {
      String key = "conc-" + i;
      initialRecords.put(key,
          new VectorRecord(randomUnitVector(rng, DIM), "g" + (i % 5), "m", "m"));
      if (i % 4 == 0) {
        updatedRecords.put(key,
            new VectorRecord(randomUnitVector(rng, DIM), "g" + (i % 5), "m", "m"));
      } else if (i % 5 == 0) {
        destroyedKeys.add(key);
      }
    }

    Map<String, VectorRecord> finalGroundTruth = new HashMap<>(initialRecords);
    finalGroundTruth.putAll(updatedRecords);
    destroyedKeys.forEach(finalGroundTruth::remove);

    accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      AtomicBoolean writerDone = new AtomicBoolean(false);
      ExecutorService pool = Executors.newFixedThreadPool(2);
      try {
        Future<?> writer = pool.submit(() -> {
          for (Map.Entry<String, VectorRecord> e : initialRecords.entrySet()) {
            region.put(e.getKey(), e.getValue());
          }
          for (Map.Entry<String, VectorRecord> e : updatedRecords.entrySet()) {
            region.put(e.getKey(), e.getValue());
          }
          for (String k : destroyedKeys) {
            region.destroy(k);
          }
        });

        Future<Integer> reader = pool.submit(() -> {
          int queryCount = 0;
          while (!writerDone.get() || queryCount < 5) {
            HybridKnnQueryContext ctx = HybridKnnQueryContext
                .builder(INDEX_NAME, VECTOR_FIELD, query, 10)
                .similarityFunction(VectorSimilarityFunction.COSINE)
                .build();
            TopEntries<String> res = DistributedHybridKnnFunction.executeOnRegion(region, ctx);
            assertThat(res).isNotNull();
            assertThat(res.size()).isLessThanOrEqualTo(10);
            queryCount++;
            if (writerDone.get() && queryCount >= 5) {
              break;
            }
          }
          return queryCount;
        });

        writer.get();
        writerDone.set(true);
        int executedQueries = reader.get();
        assertThat(executedQueries).isGreaterThanOrEqualTo(5);
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        pool.shutdownNow();
      }
    });

    // Immediately after writers finish (even before AEQ flush completes), check recall parity
    List<String> expectedTop10 =
        computeBruteForceTopK(finalGroundTruth, query, 10, VectorSimilarityFunction.COSINE, null);

    List<String> immediateKeys = accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      HybridKnnQueryContext ctx = HybridKnnQueryContext
          .builder(INDEX_NAME, VECTOR_FIELD, query, 10)
          .similarityFunction(VectorSimilarityFunction.COSINE)
          .build();
      return extractKeys(DistributedHybridKnnFunction.executeOnRegion(region, ctx));
    });
    double immediateRecall = computeRecall(expectedTop10, immediateKeys);
    assertThat(immediateRecall).isGreaterThanOrEqualTo(0.9);
    assertThat(immediateKeys).doesNotContainAnyElementsOf(destroyedKeys);

    // After AEQ flush, verify exact recall parity
    waitForLuceneFlush(accessor);
    List<String> finalKeys = accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      HybridKnnQueryContext ctx = HybridKnnQueryContext
          .builder(INDEX_NAME, VECTOR_FIELD, query, 10)
          .similarityFunction(VectorSimilarityFunction.COSINE)
          .build();
      return extractKeys(DistributedHybridKnnFunction.executeOnRegion(region, ctx));
    });
    assertThat(computeRecall(expectedTop10, finalKeys)).isGreaterThanOrEqualTo(0.9);
    assertThat(finalKeys).doesNotContainAnyElementsOf(destroyedKeys);
  }

  @Test
  public void bucketRebalancingPreservesTopKRecallParity() {
    // Start with ONLY dataStore1 + accessor (all 16 buckets on dataStore1)
    dataStore1.invoke(() -> initServer(1));
    accessor.invoke(() -> initAccessor(1));

    Random rng = new Random(404L);
    float[] query = randomUnitVector(rng, DIM);
    Map<String, VectorRecord> dataset = new HashMap<>();
    for (int i = 0; i < 130; i++) {
      dataset.put("rebal-" + i,
          new VectorRecord(randomUnitVector(rng, DIM), "cat-" + (i % 3), "m", "m"));
    }

    // Put first 100 docs and flush to Lucene on dataStore1
    accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      for (int i = 0; i < 100; i++) {
        String key = "rebal-" + i;
        region.put(key, dataset.get(key));
      }
    });
    waitForLuceneFlush(accessor);

    // Pause AEQ on dataStore1 and put 30 more live Tier-0 entries
    dataStore1.invoke(() -> pauseIndexAeq(getCache()));
    accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      for (int i = 100; i < 130; i++) {
        String key = "rebal-" + i;
        region.put(key, dataset.get(key));
      }
    });

    List<String> expectedTop10 =
        computeBruteForceTopK(dataset, query, 10, VectorSimilarityFunction.COSINE, null);

    // Verify top-10 before rebalance
    List<String> beforeRebalance = accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      HybridKnnQueryContext ctx = HybridKnnQueryContext
          .builder(INDEX_NAME, VECTOR_FIELD, query, 10)
          .similarityFunction(VectorSimilarityFunction.COSINE)
          .build();
      return extractKeys(DistributedHybridKnnFunction.executeOnRegion(region, ctx));
    });
    assertThat(computeRecall(expectedTop10, beforeRebalance)).isGreaterThanOrEqualTo(0.9);

    // Bring up dataStore2 (with AEQ paused as well) and trigger cluster rebalance
    dataStore2.invoke(() -> {
      initServer(1);
      pauseIndexAeq(getCache());
    });

    dataStore2.invoke(() -> {
      RebalanceFactory rf = getCache().getResourceManager().createRebalanceFactory();
      RebalanceOperation op = rf.start();
      try {
        op.getResults();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    });

    // Confirm both servers now host primary buckets
    int ds1Primaries = dataStore1.invoke(this::countLocalPrimaryBuckets);
    int ds2Primaries = dataStore2.invoke(this::countLocalPrimaryBuckets);
    assertThat(ds1Primaries).isGreaterThan(0);
    assertThat(ds2Primaries).isGreaterThan(0);
    assertThat(ds1Primaries + ds2Primaries).isEqualTo(NUM_BUCKETS);

    // Query immediately AFTER rebalance (while AEQ is still paused!)
    List<String> afterRebalanceBeforeFlush = accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      HybridKnnQueryContext ctx = HybridKnnQueryContext
          .builder(INDEX_NAME, VECTOR_FIELD, query, 10)
          .similarityFunction(VectorSimilarityFunction.COSINE)
          .build();
      return extractKeys(DistributedHybridKnnFunction.executeOnRegion(region, ctx));
    });
    assertThat(computeRecall(expectedTop10, afterRebalanceBeforeFlush))
        .isGreaterThanOrEqualTo(0.9);

    // Resume AEQ on both servers, flush, and verify recall parity after flush
    dataStore1.invoke(() -> resumeIndexAeq(getCache()));
    dataStore2.invoke(() -> resumeIndexAeq(getCache()));
    waitForLuceneFlush(accessor);

    List<String> afterRebalanceAfterFlush = accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      HybridKnnQueryContext ctx = HybridKnnQueryContext
          .builder(INDEX_NAME, VECTOR_FIELD, query, 10)
          .similarityFunction(VectorSimilarityFunction.COSINE)
          .build();
      return extractKeys(DistributedHybridKnnFunction.executeOnRegion(region, ctx));
    });
    assertThat(computeRecall(expectedTop10, afterRebalanceAfterFlush))
        .isGreaterThanOrEqualTo(0.9);
  }

  @Test
  public void destroyAndRecreateIndexClearsTier0AndLeaksZeroStaleEntries() {
    dataStore1.invoke(() -> initServer(1));
    dataStore2.invoke(() -> initServer(1));
    accessor.invoke(() -> initAccessor(1));

    // Pause AEQ so entries live strictly in Tier-0 buffers across both servers
    dataStore1.invoke(() -> pauseIndexAeq(getCache()));
    dataStore2.invoke(() -> pauseIndexAeq(getCache()));

    float[] query = new float[DIM];
    query[0] = 1.0f;
    accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      for (int i = 0; i < 32; i++) {
        region.put("stale-" + i, new VectorRecord(query, "old", "all", "all"));
      }
    });

    int tier0BeforeDestroy = dataStore1.invoke(this::countTier0Entries)
        + dataStore2.invoke(this::countTier0Entries);
    assertThat(tier0BeforeDestroy).isGreaterThan(0);

    // Destroy the Lucene index from dataStore1 (propagates to dataStore2 and accessor)
    dataStore1.invoke(() -> {
      LuceneServiceProvider.get(getCache()).destroyIndex(INDEX_NAME, REGION_NAME);
    });

    // Tier-0 registry on BOTH servers must be completely empty (zero leaked entries/tombstones)
    assertThat(dataStore1.invoke(() -> Tier0VectorRegistry.forCache(getCache())
        .getBuckets("/" + REGION_NAME, INDEX_NAME).isEmpty())).isTrue();
    assertThat(dataStore2.invoke(() -> Tier0VectorRegistry.forCache(getCache())
        .getBuckets("/" + REGION_NAME, INDEX_NAME).isEmpty())).isTrue();

    // Clear the region and recreate region + index with the SAME (regionPath, indexName)
    accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      region.destroyRegion();
    });
    dataStore1.invoke(() -> initServer(1));
    dataStore2.invoke(() -> initServer(1));
    accessor.invoke(() -> initAccessor(1));

    // Querying the newly recreated empty index must return ZERO leaked stale Tier-0 hits
    List<String> recreatedHits = accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      HybridKnnQueryContext ctx = HybridKnnQueryContext
          .builder(INDEX_NAME, VECTOR_FIELD, query, 10)
          .similarityFunction(VectorSimilarityFunction.COSINE)
          .build();
      return extractKeys(DistributedHybridKnnFunction.executeOnRegion(region, ctx));
    });
    assertThat(recreatedHits).isEmpty();
  }

  @Test
  public void unsupportedFuzzyFilterQueryFlushesTier0AndReturnsFreshWritesWithoutSilentDrops() {
    dataStore1.invoke(() -> initServer(1));
    dataStore2.invoke(() -> initServer(1));
    accessor.invoke(() -> initAccessor(1));

    // Seed all buckets and flush once so every bucket's IndexRepository is initialized
    float[] orthogonalSeed = new float[DIM];
    orthogonalSeed[DIM - 1] = 1.0f;
    accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      for (int i = 0; i < NUM_BUCKETS * 2; i++) {
        region.put("seed-" + i, new VectorRecord(orthogonalSeed, "other", "all", "all"));
      }
    });
    waitForLuceneFlush(accessor);

    // Pause AEQ on both servers so fresh "books" and "music" docs live ONLY in Tier-0
    dataStore1.invoke(() -> pauseIndexAeq(getCache()));
    dataStore2.invoke(() -> pauseIndexAeq(getCache()));

    float[] query = new float[DIM];
    query[0] = 1.0f;
    Random rng = new Random(707L);
    Map<String, VectorRecord> freshDocs = new HashMap<>();
    for (int i = 0; i < 32; i++) {
      float[] vec = randomUnitVector(rng, DIM);
      vec[0] = Math.abs(vec[0]) + 0.5f;
      normalizeInPlace(vec);
      String tag = (i % 4 == 0) ? "books" : "music";
      freshDocs.put("fuzzy-" + i, new VectorRecord(vec, tag, "all", "all"));
    }

    accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      for (Map.Entry<String, VectorRecord> e : freshDocs.entrySet()) {
        region.put(e.getKey(), e.getValue());
      }
    });

    List<String> expectedTop5 = computeBruteForceTopK(
        freshDocs, query, 5, VectorSimilarityFunction.COSINE,
        rec -> "books".equals(rec.getMetadata().get("sel1")));
    assertThat(expectedTop5).hasSize(5);

    // Execute hybrid k-NN with a FuzzyQuery ("sel1:boks~1" matching "books" with edit distance 1)
    // while AEQ is still paused: automatic flush-and-refresh fallback must return ALL fresh hits
    List<String> actualKeys = accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      HybridKnnQueryContext ctx = HybridKnnQueryContext
          .builder(INDEX_NAME, VECTOR_FIELD, query, 5)
          .similarityFunction(VectorSimilarityFunction.COSINE)
          .filterQuery("sel1:boks~1", "sel1")
          .build();
      return extractKeys(DistributedHybridKnnFunction.executeOnRegion(region, ctx));
    });

    assertThat(actualKeys).containsExactlyElementsOf(expectedTop5);

    long fallbackCount = dataStore1.invoke(
        () -> Tier0VectorRegistry.forCache(getCache()).getUnsupportedFilterFlushFallbackCount())
        + dataStore2.invoke(
            () -> Tier0VectorRegistry.forCache(getCache())
                .getUnsupportedFilterFlushFallbackCount());
    assertThat(fallbackCount).isGreaterThan(0L);

    // Resume AEQ cleanly for teardown
    dataStore1.invoke(() -> resumeIndexAeq(getCache()));
    dataStore2.invoke(() -> resumeIndexAeq(getCache()));
  }

  @Test
  public void failoverAeqDeltaHydrationReconcilesPreCrashTombstonesWithZeroGhostsAndDrainsToZero() {
    dataStore1.invoke(() -> initServer(1));
    dataStore2.invoke(() -> initServer(1));
    accessor.invoke(() -> initAccessor(1));

    // Seed 160 committed docs across all 16 buckets with strictly distinct cosine similarities
    // (0.40 .. 0.877) so top-k ranking is tie-free and deterministic.
    float[] query = new float[DIM];
    query[0] = 1.0f;

    Map<String, VectorRecord> groundTruth = new HashMap<>();
    for (int i = 0; i < NUM_BUCKETS * 10; i++) {
      float cos = 0.40f + 0.003f * i;
      float[] vec = new float[DIM];
      vec[0] = cos;
      vec[1] = (float) Math.sqrt(1.0 - (double) cos * cos);
      groundTruth.put("seed-" + i, new VectorRecord(vec, "all", "all", "all"));
    }
    accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      for (Map.Entry<String, VectorRecord> e : groundTruth.entrySet()) {
        region.put(e.getKey(), e.getValue());
      }
    });
    waitForLuceneFlush(accessor);

    // Ensure redundancy is established on dataStore2 and secondary shadow queues are drained to 0
    // before pausing AEQ so |AEQ_b| reflects strictly the uncommitted pre-crash mutations.
    GeodeAwaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
      int ds2Buckets = dataStore2.invoke(this::countLocalBuckets);
      assertThat(ds2Buckets).isEqualTo(NUM_BUCKETS);
      int shadowSize = dataStore1.invoke(() -> countShadowQueueLocalSize(getCache()))
          + dataStore2.invoke(() -> countShadowQueueLocalSize(getCache()));
      assertThat(shadowSize).isEqualTo(0);
    });

    // Identify primary buckets hosted on dataStore1 and choose:
    // - emptiedBucketId: all keys destroyed pre-crash so bucket.isEmpty() == true on failover
    // - untouchedBucketId: zero uncommitted mutations so |AEQ_b| == 0 and |T_mask| == 0
    // - partialDestroyBuckets: subset of keys destroyed + fresh uncommitted puts
    Map<Integer, List<String>> ds1BucketKeys = dataStore1.invoke(() -> {
      PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(REGION_NAME);
      Map<Integer, List<String>> map = new HashMap<>();
      for (int bid : pr.getDataStore().getAllLocalPrimaryBucketIds()) {
        BucketRegion br = pr.getDataStore().getLocalBucketById(bid);
        List<String> keys = new ArrayList<>();
        for (Object k : br.keySet()) {
          keys.add((String) k);
        }
        Collections.sort(keys);
        map.put(bid, keys);
      }
      return map;
    });
    assertThat(ds1BucketKeys.size()).isGreaterThanOrEqualTo(3);

    List<Integer> ds1Bids = new ArrayList<>(ds1BucketKeys.keySet());
    Collections.sort(ds1Bids);
    int emptiedBucketId = ds1Bids.get(0);
    int untouchedBucketId = ds1Bids.get(1);
    Set<Integer> activeDeltaBuckets = new HashSet<>(ds1Bids.subList(2, ds1Bids.size()));

    Set<String> destroyedKeys = new HashSet<>();
    for (String k : ds1BucketKeys.get(emptiedBucketId)) {
      destroyedKeys.add(k);
    }
    for (int bid : activeDeltaBuckets) {
      List<String> bKeys = ds1BucketKeys.get(bid);
      for (int idx = 0; idx < bKeys.size(); idx += 2) {
        destroyedKeys.add(bKeys.get(idx));
      }
    }

    // Pause AEQ on both servers so destroys and puts stay uncommitted in AEQ shadow buckets
    dataStore1.invoke(() -> pauseIndexAeq(getCache()));
    dataStore2.invoke(() -> pauseIndexAeq(getCache()));

    // Pre-crash destroys (including emptying emptiedBucketId completely) + fresh uncommitted puts
    // routed to activeDeltaBuckets
    List<String> freshPutKeys = new ArrayList<>();
    Map<String, VectorRecord> freshPutRecords = new HashMap<>();
    for (int i = 0; i < 160 && freshPutKeys.size() < 8; i++) {
      String candidateKey = "fresh-put-" + i;
      int targetBucket = accessor.invoke(() -> {
        PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(REGION_NAME);
        return pr.getKeyInfo(candidateKey).getBucketId();
      });
      if (activeDeltaBuckets.contains(targetBucket)) {
        float cos = 0.95f + 0.005f * freshPutKeys.size();
        float[] freshVec = new float[DIM];
        freshVec[0] = cos;
        freshVec[1] = (float) Math.sqrt(1.0 - (double) cos * cos);
        freshPutKeys.add(candidateKey);
        freshPutRecords.put(candidateKey, new VectorRecord(freshVec, "all", "all", "all"));
      }
    }
    assertThat(freshPutKeys).isNotEmpty();

    accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      for (String dk : destroyedKeys) {
        region.destroy(dk);
      }
      for (Map.Entry<String, VectorRecord> e : freshPutRecords.entrySet()) {
        region.put(e.getKey(), e.getValue());
      }
    });
    destroyedKeys.forEach(groundTruth::remove);
    groundTruth.putAll(freshPutRecords);

    // Secondary dataStore2 must have 0 Tier-0 entries before failover
    assertThat(dataStore2.invoke(this::countTier0Entries)).isEqualTo(0);

    // Crash primary dataStore1 abruptly; wait for dataStore2 to take ownership of all 16 buckets
    DistributedTestUtils.crashDistributedSystem(dataStore1);
    GeodeAwaitility.await().atMost(30, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(dataStore2.invoke(this::countLocalPrimaryBuckets))
            .isEqualTo(NUM_BUCKETS));

    // Verify AEQ-delta hydration + unconditional completeness reconciliation BEFORE AEQ resumes
    dataStore2.invoke(() -> {
      Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(getCache());
      assertThat(registry.getAeqDeltaHydrationCount())
          .isGreaterThanOrEqualTo(ds1Bids.size());
      assertThat(registry.getFullScanHydrationCount()).isEqualTo(0L);
      assertThat(registry.getHydratedTombstonesCount()).isEqualTo(destroyedKeys.size());
      assertThat(registry.getHydratedKeysCount())
          .isGreaterThanOrEqualTo(destroyedKeys.size() + freshPutKeys.size());

      // Untouched bucket with |AEQ_b| == 0 must have 0 tombstones in Tier-0
      Tier0VectorDeltaBuffer<Object> untouchedBuf =
          registry.getBuffer("/" + REGION_NAME, INDEX_NAME, untouchedBucketId);
      if (untouchedBuf != null) {
        assertThat(untouchedBuf.snapshot().getTombstonedKeys()).isEmpty();
      }

      // Emptied bucket (bucket.isEmpty() == true) MUST still hold all destroyed tombstones
      PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(REGION_NAME);
      BucketRegion emptiedBucket = pr.getDataStore().getLocalBucketById(emptiedBucketId);
      assertThat(emptiedBucket.isEmpty()).isTrue();
      Tier0VectorDeltaBuffer<Object> emptiedBuf =
          registry.getBuffer("/" + REGION_NAME, INDEX_NAME, emptiedBucketId);
      assertThat(emptiedBuf).isNotNull();
      assertThat(emptiedBuf.snapshot().getTombstonedKeys())
          .containsExactlyInAnyOrderElementsOf(ds1BucketKeys.get(emptiedBucketId));
    });

    // Query immediately BEFORE AEQ resumes on dataStore2: ZERO tombstone ghosts, 100% recall!
    List<String> expectedTop25 =
        computeBruteForceTopK(groundTruth, query, 25, VectorSimilarityFunction.COSINE, null);
    List<String> preFlushHits = accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      HybridKnnQueryContext ctx = HybridKnnQueryContext
          .builder(INDEX_NAME, VECTOR_FIELD, query, 25)
          .similarityFunction(VectorSimilarityFunction.COSINE)
          .build();
      return extractKeys(DistributedHybridKnnFunction.executeOnRegion(region, ctx));
    });
    assertThat(preFlushHits)
        .as("Pre-crash destroyed keys must NEVER leak as tombstone ghosts post-failover")
        .doesNotContainAnyElementsOf(destroyedKeys);
    assertThat(preFlushHits).containsAll(freshPutKeys);
    assertThat(computeRecall(expectedTop25, preFlushHits)).isEqualTo(1.0);

    // Exhaustive cluster-wide k=200 query (> total docs) to prove ZERO ghosts across ALL buckets
    List<String> preFlushAllHits = accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      HybridKnnQueryContext ctx = HybridKnnQueryContext
          .builder(INDEX_NAME, VECTOR_FIELD, query, 200)
          .similarityFunction(VectorSimilarityFunction.COSINE)
          .build();
      return extractKeys(DistributedHybridKnnFunction.executeOnRegion(region, ctx));
    });
    assertThat(preFlushAllHits).doesNotContainAnyElementsOf(destroyedKeys);
    assertThat(preFlushAllHits).containsExactlyInAnyOrderElementsOf(groundTruth.keySet());

    // Resume AEQ on surviving dataStore2, wait for flush, verify tombstones and AEQ-delta keys
    // drain completely via evictCommitted while post-flush queries have 0 ghosts and 100% recall
    dataStore2.invoke(() -> resumeIndexAeq(getCache()));
    waitForLuceneFlush(accessor);

    assertThat(dataStore2.invoke(this::countTier0Tombstones)).isEqualTo(0);
    dataStore2.invoke(() -> assertTier0DoesNotMaskKeys(destroyedKeys, freshPutKeys));

    List<String> postFlushHits = accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      HybridKnnQueryContext ctx = HybridKnnQueryContext
          .builder(INDEX_NAME, VECTOR_FIELD, query, 25)
          .similarityFunction(VectorSimilarityFunction.COSINE)
          .build();
      return extractKeys(DistributedHybridKnnFunction.executeOnRegion(region, ctx));
    });
    assertThat(postFlushHits).doesNotContainAnyElementsOf(destroyedKeys);
    assertThat(computeRecall(expectedTop25, postFlushHits)).isEqualTo(1.0);

    List<String> postFlushAllHits = accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      HybridKnnQueryContext ctx = HybridKnnQueryContext
          .builder(INDEX_NAME, VECTOR_FIELD, query, 200)
          .similarityFunction(VectorSimilarityFunction.COSINE)
          .build();
      return extractKeys(DistributedHybridKnnFunction.executeOnRegion(region, ctx));
    });
    assertThat(postFlushAllHits).doesNotContainAnyElementsOf(destroyedKeys);
    assertThat(postFlushAllHits).containsExactlyInAnyOrderElementsOf(groundTruth.keySet());
  }

  @Test
  public void pathBUnsupportedFuzzyFilterZeroDroppedMatchingEntriesUnderConcurrentOverwrites()
      throws Exception {
    dataStore1.invoke(() -> initServer(1));
    dataStore2.invoke(() -> initServer(1));
    accessor.invoke(() -> initAccessor(1));

    float[] query = new float[DIM];
    query[0] = 1.0f;

    List<String> stableKeys = new ArrayList<>();
    for (int i = 0; i < 12; i++) {
      String key = "stable-books-" + i;
      stableKeys.add(key);
      float cos = 0.70f + 0.01f * i;
      float[] vec = new float[DIM];
      vec[0] = cos;
      vec[1] = (float) Math.sqrt(1.0 - (double) cos * cos);
      String finalKey = key;
      accessor.invoke(() -> getCache().<String, VectorRecord>getRegion(REGION_NAME)
          .put(finalKey, new VectorRecord(vec, "books", "all", "all")));
    }
    for (int i = 0; i < 12; i++) {
      String noiseKey = "noise-" + i;
      float cos = 0.90f - 0.005f * i;
      float[] vec = new float[DIM];
      vec[0] = cos;
      vec[1] = (float) Math.sqrt(1.0 - (double) cos * cos);
      accessor.invoke(() -> getCache().<String, VectorRecord>getRegion(REGION_NAME)
          .put(noiseKey, new VectorRecord(vec, "other", "all", "all")));
    }
    waitForLuceneFlush(accessor);

    // Pause AEQ so concurrent overwrites and fresh puts stay in Tier-0 E_active during Path B.
    dataStore1.invoke(() -> pauseIndexAeq(getCache()));
    dataStore2.invoke(() -> pauseIndexAeq(getCache()));

    List<String> freshBooksKeys = List.of("fresh-books-0", "fresh-books-1");
    accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      for (int i = 0; i < freshBooksKeys.size(); i++) {
        float cos = 0.85f + 0.02f * i;
        float[] vec = new float[DIM];
        vec[0] = cos;
        vec[1] = (float) Math.sqrt(1.0 - (double) cos * cos);
        region.put(freshBooksKeys.get(i), new VectorRecord(vec, "books", "all", "all"));
      }
    });

    accessor.invoke(() -> REMOTE_WRITER_STOP.set(false));
    ExecutorService writerPool = Executors.newSingleThreadExecutor();
    try {
      Future<?> writerFuture = writerPool.submit(() -> {
        accessor.invoke(() -> {
          Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
          long deadlineMs = System.currentTimeMillis() + 60_000L;
          int iter = 0;
          while (!REMOTE_WRITER_STOP.get() && System.currentTimeMillis() < deadlineMs) {
            String targetKey = stableKeys.get(iter % stableKeys.size());
            float cos = 0.72f + 0.005f * (iter % 10);
            float[] vec = new float[DIM];
            vec[0] = cos;
            vec[1] = (float) Math.sqrt(1.0 - (double) cos * cos);
            region.put(targetKey, new VectorRecord(vec, "books", "all", "all"));
            iter++;
          }
          return null;
        });
        return null;
      });

      List<String> expectedAllMatching = new ArrayList<>(stableKeys);
      expectedAllMatching.addAll(freshBooksKeys);

      for (int q = 0; q < 20; q++) {
        List<String> hits = accessor.invoke(() -> {
          Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
          HybridKnnQueryContext ctx = HybridKnnQueryContext
              .builder(INDEX_NAME, VECTOR_FIELD, query, 50)
              .similarityFunction(VectorSimilarityFunction.COSINE)
              .filterQuery("sel1:boks~1", "sel1")
              .build();
          return extractKeys(DistributedHybridKnnFunction.executeOnRegion(region, ctx));
        });
        assertThat(hits)
            .as("Path-B FuzzyQuery iteration %d must never drop concurrently overwritten keys", q)
            .containsAll(expectedAllMatching);
        for (String hit : hits) {
          assertThat(hit).doesNotStartWith("noise-");
        }
      }

      accessor.invoke(() -> REMOTE_WRITER_STOP.set(true));
      writerFuture.get(30, TimeUnit.SECONDS);
    } finally {
      accessor.invoke(() -> REMOTE_WRITER_STOP.set(true));
      writerPool.shutdownNow();
    }

    dataStore1.invoke(() -> {
      resumeIndexAeq(getCache());
      for (Tier0VectorDeltaBuffer<Object> buf : Tier0VectorRegistry.forCache(getCache())
          .getBuckets("/" + REGION_NAME, INDEX_NAME).values()) {
        assertThat(buf.isPathBWriteHeldByCurrentThread()).isFalse();
      }
    });
    dataStore2.invoke(() -> {
      resumeIndexAeq(getCache());
      for (Tier0VectorDeltaBuffer<Object> buf : Tier0VectorRegistry.forCache(getCache())
          .getBuckets("/" + REGION_NAME, INDEX_NAME).values()) {
        assertThat(buf.isPathBWriteHeldByCurrentThread()).isFalse();
      }
    });
  }

  @Test
  public void failoverSameKeyPutDestroyRecreateAndPutDestroyWithAeqDeltaHydrationTelemetry() {
    dataStore1.invoke(() -> initServer(1));
    dataStore2.invoke(() -> initServer(1));
    accessor.invoke(() -> initAccessor(1));

    float[] query = new float[DIM];
    query[0] = 1.0f;

    Map<String, VectorRecord> groundTruth = new HashMap<>();
    List<String> committedToDestroy = new ArrayList<>();
    List<String> committedToRecreate = new ArrayList<>();
    for (int i = 0; i < NUM_BUCKETS * 6; i++) {
      float cos = 0.50f + 0.002f * i;
      float[] vec = new float[DIM];
      vec[0] = cos;
      vec[1] = (float) Math.sqrt(1.0 - (double) cos * cos);
      String key = "base-" + i;
      groundTruth.put(key, new VectorRecord(vec, "all", "all", "all"));
      if (i < NUM_BUCKETS) {
        committedToDestroy.add(key);
      } else if (i < NUM_BUCKETS * 2) {
        committedToRecreate.add(key);
      }
    }
    accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      for (Map.Entry<String, VectorRecord> e : groundTruth.entrySet()) {
        region.put(e.getKey(), e.getValue());
      }
    });
    waitForLuceneFlush(accessor);

    GeodeAwaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
      assertThat(dataStore2.invoke(this::countLocalBuckets)).isEqualTo(NUM_BUCKETS);
      int shadowSize = dataStore1.invoke(() -> countShadowQueueLocalSize(getCache()))
          + dataStore2.invoke(() -> countShadowQueueLocalSize(getCache()));
      assertThat(shadowSize).isEqualTo(0);
    });

    List<Integer> ds1Bids = dataStore1.invoke(() -> {
      PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(REGION_NAME);
      List<Integer> bids = new ArrayList<>(pr.getDataStore().getAllLocalPrimaryBucketIds());
      Collections.sort(bids);
      return bids;
    });
    assertThat(ds1Bids).isNotEmpty();

    // Pause AEQ so all same-key sequences remain uncommitted in AEQ shadow queues until crash
    dataStore1.invoke(() -> pauseIndexAeq(getCache()));
    dataStore2.invoke(() -> pauseIndexAeq(getCache()));

    // Find one uncommitted recreate key AND one uncommitted put->destroy key per dataStore1 bucket
    Map<Integer, String> uncommittedRecreateByBucket = new HashMap<>();
    Map<Integer, String> uncommittedDestroyByBucket = new HashMap<>();
    Set<Integer> ds1BidSet = new HashSet<>(ds1Bids);
    for (int i = 0; i < 1000
        && (uncommittedRecreateByBucket.size() < ds1BidSet.size()
            || uncommittedDestroyByBucket.size() < ds1BidSet.size()); i++) {
      String candRec = "rec-seq-" + i;
      int recBid = accessor.invoke(() -> ((PartitionedRegion) getCache().getRegion(REGION_NAME))
          .getKeyInfo(candRec).getBucketId());
      if (ds1BidSet.contains(recBid) && !uncommittedRecreateByBucket.containsKey(recBid)) {
        uncommittedRecreateByBucket.put(recBid, candRec);
      }
      String candDes = "des-seq-" + i;
      int desBid = accessor.invoke(() -> ((PartitionedRegion) getCache().getRegion(REGION_NAME))
          .getKeyInfo(candDes).getBucketId());
      if (ds1BidSet.contains(desBid) && !uncommittedDestroyByBucket.containsKey(desBid)) {
        uncommittedDestroyByBucket.put(desBid, candDes);
      }
    }
    assertThat(uncommittedRecreateByBucket).hasSameSizeAs(ds1BidSet);
    assertThat(uncommittedDestroyByBucket).hasSameSizeAs(ds1BidSet);

    float[] vOld = new float[DIM];
    vOld[0] = 0.60f;
    vOld[1] = (float) Math.sqrt(1.0 - 0.36);
    float[] vRecreate = new float[DIM];
    vRecreate[0] = 0.99f;
    vRecreate[1] = (float) Math.sqrt(1.0 - 0.99 * 0.99);

    Set<String> allDestroyedKeys = new HashSet<>(committedToDestroy);
    allDestroyedKeys.addAll(uncommittedDestroyByBucket.values());
    Set<String> allRecreatedKeys = new HashSet<>(committedToRecreate);
    allRecreatedKeys.addAll(uncommittedRecreateByBucket.values());

    accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      for (String key : committedToDestroy) {
        region.put(key, new VectorRecord(vOld, "all", "all", "all"));
        region.destroy(key);
      }
      for (String key : committedToRecreate) {
        region.destroy(key);
        region.put(key, new VectorRecord(vRecreate, "all", "all", "all"));
      }
      for (String key : uncommittedDestroyByBucket.values()) {
        region.put(key, new VectorRecord(vOld, "all", "all", "all"));
        region.destroy(key);
      }
      for (String key : uncommittedRecreateByBucket.values()) {
        region.put(key, new VectorRecord(vOld, "all", "all", "all"));
        region.destroy(key);
        region.put(key, new VectorRecord(vRecreate, "all", "all", "all"));
      }
    });

    for (String dk : allDestroyedKeys) {
      groundTruth.remove(dk);
    }
    for (String rk : allRecreatedKeys) {
      groundTruth.put(rk, new VectorRecord(vRecreate, "all", "all", "all"));
    }

    // Crash primary dataStore1; wait for dataStore2 promotion + AEQ-delta hydration
    DistributedTestUtils.crashDistributedSystem(dataStore1);
    GeodeAwaitility.await().atMost(30, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(dataStore2.invoke(this::countLocalPrimaryBuckets))
            .isEqualTo(NUM_BUCKETS));

    dataStore2.invoke(() -> {
      Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(getCache());
      assertThat(registry.getAeqDeltaHydrationCount()).isGreaterThanOrEqualTo(ds1Bids.size());
      assertThat(registry.getFullScanHydrationCount()).isEqualTo(0L);
      assertThat(registry.getLastHydrationEventsInspected()).isGreaterThan(0L);
      assertThat(registry.getLastHydrationKeysRestored()).isGreaterThan(0L);
      assertThat(registry.getHydratedTombstonesCount()).isGreaterThanOrEqualTo(
          uncommittedDestroyByBucket.size());
      assertThat(registry.getHydratedKeysCount()).isGreaterThanOrEqualTo(
          uncommittedDestroyByBucket.size() + uncommittedRecreateByBucket.size());
    });

    // Exhaustive pre-flush verification: 100% recreated live, 0 destroyed ghosts
    List<String> preFlushHits = accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      HybridKnnQueryContext ctx = HybridKnnQueryContext
          .builder(INDEX_NAME, VECTOR_FIELD, query, 300)
          .similarityFunction(VectorSimilarityFunction.COSINE)
          .build();
      return extractKeys(DistributedHybridKnnFunction.executeOnRegion(region, ctx));
    });
    assertThat(preFlushHits).doesNotContainAnyElementsOf(allDestroyedKeys);
    assertThat(preFlushHits).containsAll(allRecreatedKeys);
    assertThat(preFlushHits).containsExactlyInAnyOrderElementsOf(groundTruth.keySet());

    // Resume AEQ, drain tombstones & AEQ keys -> 0, and re-verify parity post-flush
    dataStore2.invoke(() -> resumeIndexAeq(getCache()));
    waitForLuceneFlush(accessor);
    assertThat(dataStore2.invoke(this::countTier0Tombstones)).isEqualTo(0);
    dataStore2.invoke(() -> assertTier0DoesNotMaskKeys(allDestroyedKeys, allRecreatedKeys));

    List<String> postFlushHits = accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      HybridKnnQueryContext ctx = HybridKnnQueryContext
          .builder(INDEX_NAME, VECTOR_FIELD, query, 300)
          .similarityFunction(VectorSimilarityFunction.COSINE)
          .build();
      return extractKeys(DistributedHybridKnnFunction.executeOnRegion(region, ctx));
    });
    assertThat(postFlushHits).doesNotContainAnyElementsOf(allDestroyedKeys);
    assertThat(postFlushHits).containsExactlyInAnyOrderElementsOf(groundTruth.keySet());
  }

  @Test
  public void supportedFilterSearcherEpochPreventsTwoKeyTornCutAcrossServers() throws Exception {
    dataStore1.invoke(() -> initServer(1));
    dataStore2.invoke(() -> initServer(1));
    accessor.invoke(() -> initAccessor(1));

    float[] query = new float[DIM];
    query[0] = 1.0f;

    // Seed warmup keys first so PartitionedRegion allocates primary buckets before inspection
    accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      float[] warmupVec = new float[DIM];
      warmupVec[1] = 1.0f;
      for (int i = 0; i < NUM_BUCKETS * 2; i++) {
        region.put("warmup-" + i, new VectorRecord(warmupVec, "warmup", "all", "all"));
      }
    });
    waitForLuceneFlush(accessor);

    // Select a primary bucket on dataStore1 and find two keys (x_c, y_c) per cycle routing to it
    int targetBucketId = dataStore1.invoke(() -> {
      PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(REGION_NAME);
      return pr.getDataStore().getAllLocalPrimaryBucketIds().iterator().next();
    });

    int cycles = 8;
    List<String> keysX = new ArrayList<>();
    List<String> keysY = new ArrayList<>();
    for (int i = 0; i < 1000 && keysX.size() < cycles; i++) {
      String candidateX = "cut-x-" + i;
      int bx = accessor.invoke(() -> ((PartitionedRegion) getCache().getRegion(REGION_NAME))
          .getKeyInfo(candidateX).getBucketId());
      if (bx == targetBucketId) {
        keysX.add(candidateX);
      }
    }
    for (int i = 0; i < 1000 && keysY.size() < cycles; i++) {
      String candidateY = "cut-y-" + i;
      int by = accessor.invoke(() -> ((PartitionedRegion) getCache().getRegion(REGION_NAME))
          .getKeyInfo(candidateY).getBucketId());
      if (by == targetBucketId) {
        keysY.add(candidateY);
      }
    }
    assertThat(keysX).hasSize(cycles);
    assertThat(keysY).hasSize(cycles);

    float[] v0 = new float[DIM];
    v0[0] = 0.20f;
    v0[1] = (float) Math.sqrt(1.0 - 0.04);
    float[] v1 = new float[DIM];
    v1[0] = 0.85f;
    v1[1] = (float) Math.sqrt(1.0 - 0.85 * 0.85);
    float[] v2 = new float[DIM];
    v2[0] = 0.99f;
    v2[1] = (float) Math.sqrt(1.0 - 0.99 * 0.99);
    float[] vY = new float[DIM];
    vY[0] = 0.90f;
    vY[1] = (float) Math.sqrt(1.0 - 0.81);
    float expectedV2Score =
        VectorSimilarityScorer.score(query, v2, VectorSimilarityFunction.COSINE);

    for (int c = 0; c < cycles; c++) {
      String kx = keysX.get(c);
      String ky = keysY.get(c);
      String tag = "cycle-" + c;

      // Commit baseline {kx=v0} while ky is absent
      accessor.invoke(() -> {
        getCache().<String, VectorRecord>getRegion(REGION_NAME)
            .put(kx, new VectorRecord(v0, tag, "all", "all"));
      });
      waitForLuceneFlush(accessor);

      // Pause AEQ and write kx=v1 into Tier-0
      dataStore1.invoke(() -> pauseIndexAeq(getCache()));
      dataStore2.invoke(() -> pauseIndexAeq(getCache()));
      accessor.invoke(() -> {
        getCache().<String, VectorRecord>getRegion(REGION_NAME)
            .put(kx, new VectorRecord(v1, tag, "all", "all"));
      });

      accessor.invoke(() -> REMOTE_WRITER_STOP.set(false));
      ExecutorService readerPool = Executors.newSingleThreadExecutor();
      try {
        Future<?> readerFuture = readerPool.submit(() -> {
          accessor.invoke(() -> {
            Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
            while (!REMOTE_WRITER_STOP.get()) {
              HybridKnnQueryContext ctx = HybridKnnQueryContext
                  .builder(INDEX_NAME, VECTOR_FIELD, query, 5)
                  .similarityFunction(VectorSimilarityFunction.COSINE)
                  .addMetadataFilter("sel1", tag)
                  .build();
              TopEntries<String> res = DistributedHybridKnnFunction.executeOnRegion(region, ctx);
              Map<String, Float> scoreByKey = new HashMap<>();
              for (EntryScore<String> hit : res.getHits()) {
                scoreByKey.put(hit.getKey(), hit.getScore());
              }
              if (scoreByKey.containsKey(ky)) {
                assertThat(scoreByKey).containsKey(kx);
                assertThat(scoreByKey.get(kx))
                    .as("Torn cut {x=v1, y} detected on cycle %d",
                        Integer.valueOf(tag.substring(6)))
                    .isGreaterThanOrEqualTo(expectedV2Score - 1e-3f);
              }
            }
            return null;
          });
          return null;
        });

        // Write kx=v2, ky=vY and resume AEQ so Path A commits {kx=v2, ky} concurrently with readers
        accessor.invoke(() -> {
          Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
          region.put(kx, new VectorRecord(v2, tag, "all", "all"));
          region.put(ky, new VectorRecord(vY, tag, "all", "all"));
        });
        dataStore1.invoke(() -> resumeIndexAeq(getCache()));
        dataStore2.invoke(() -> resumeIndexAeq(getCache()));
        waitForLuceneFlush(accessor);

        accessor.invoke(() -> REMOTE_WRITER_STOP.set(true));
        readerFuture.get(30, TimeUnit.SECONDS);
      } finally {
        accessor.invoke(() -> REMOTE_WRITER_STOP.set(true));
        readerPool.shutdownNow();
      }
    }

    // Verify true flush-cycle telemetry was captured cleanly on dataStore1 across the flushes
    int recordedSamples = dataStore1.invoke(() -> Tier0VectorRegistry.forCache(getCache())
        .drainFlushCycleSamples("/" + REGION_NAME, INDEX_NAME).size());
    assertThat(recordedSamples).isGreaterThan(0);
  }

  @Test
  public void promotionStaleLuceneDocWithMissingAeqAndExpiredTombstoneReconcilesZeroGhosts()
      throws Exception {
    dataStore1.invoke(() -> initServer(1));
    dataStore2.invoke(() -> initServer(1));
    accessor.invoke(() -> initAccessor(1));

    float[] query = new float[DIM];
    query[0] = 1.0f;

    Map<String, VectorRecord> seedData = new HashMap<>();
    for (int i = 0; i < NUM_BUCKETS * 4; i++) {
      float cos = 0.45f + 0.004f * i;
      float[] vec = new float[DIM];
      vec[0] = cos;
      vec[1] = (float) Math.sqrt(1.0 - (double) cos * cos);
      seedData.put("live-base-" + i, new VectorRecord(vec, "all", "all", "all"));
    }
    accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      for (Map.Entry<String, VectorRecord> e : seedData.entrySet()) {
        region.put(e.getKey(), e.getValue());
      }
    });
    waitForLuceneFlush(accessor);

    // Find a key whose primary bucket lives on dataStore1 and seed it with top-1 cosine 0.99
    Set<Integer> ds1Primaries = dataStore1.invoke(() -> {
      PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(REGION_NAME);
      return new HashSet<>(pr.getDataStore().getAllLocalPrimaryBucketIds());
    });
    String staleKey = null;
    int staleBucketId = -1;
    for (int i = 0; i < 500; i++) {
      String candidate = "stale-expired-" + i;
      int bid = accessor.invoke(() -> ((PartitionedRegion) getCache().getRegion(REGION_NAME))
          .getKeyInfo(candidate).getBucketId());
      if (ds1Primaries.contains(bid)) {
        staleKey = candidate;
        staleBucketId = bid;
        break;
      }
    }
    assertThat(staleKey).isNotNull();

    float[] staleVec = new float[DIM];
    staleVec[0] = 0.99f;
    staleVec[1] = (float) Math.sqrt(1.0 - 0.99 * 0.99);
    String finalStaleKey = staleKey;
    int finalStaleBucketId = staleBucketId;

    accessor.invoke(() -> {
      getCache().<String, VectorRecord>getRegion(REGION_NAME)
          .put(finalStaleKey, new VectorRecord(staleVec, "all", "all", "all"));
    });
    waitForLuceneFlush(accessor);

    // Wait until secondary fileAndChunkRegion replicates APACHE_GEODE_INDEX_COMPLETE and shadow=0
    GeodeAwaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
      assertThat(dataStore2.invoke(this::countLocalBuckets)).isEqualTo(NUM_BUCKETS);
      int shadowSize = dataStore1.invoke(() -> countShadowQueueLocalSize(getCache()))
          + dataStore2.invoke(() -> countShadowQueueLocalSize(getCache()));
      assertThat(shadowSize).isEqualTo(0);
    });

    // Pause AEQ, destroy staleKey from region, then expire region tombstone & remove local AEQ
    // coverage on dataStore2 so staleKey remains ONLY as a committed stale doc in Lucene.
    dataStore1.invoke(() -> pauseIndexAeq(getCache()));
    dataStore2.invoke(() -> pauseIndexAeq(getCache()));
    accessor.invoke(() -> getCache().getRegion(REGION_NAME).destroy(finalStaleKey));

    dataStore2.invoke(() -> {
      PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(REGION_NAME);
      BucketRegion localBucket = pr.getDataStore().getLocalBucketById(finalStaleBucketId);
      RegionEntry re = localBucket.getRegionMap().getEntry(finalStaleKey);
      assertThat(re).isNotNull();
      assertThat(re.isTombstone()).isTrue();
      boolean removed =
          localBucket.getRegionMap().removeTombstone(re, re.getVersionStamp().asVersionTag());
      assertThat(removed).isTrue();
      assertThat(localBucket.getRegionMap().getEntry(finalStaleKey)).isNull();

      String aeqId = LuceneServiceImpl.getUniqueIndexName(INDEX_NAME, REGION_NAME);
      AsyncEventQueueImpl q = (AsyncEventQueueImpl) getCache().getAsyncEventQueue(aeqId);
      if (q != null) {
        q.stop();
        ((InternalCache) getCache()).removeAsyncEventQueue(q);
      }
      Tier0VectorRegistry.forCache(getCache())
          .removeBucket("/" + REGION_NAME, INDEX_NAME, finalStaleBucketId);
    });

    // Crash primary dataStore1: dataStore2 promotes without AEQ coverage and without tombstone!
    DistributedTestUtils.crashDistributedSystem(dataStore1);
    GeodeAwaitility.await().atMost(30, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(dataStore2.invoke(this::countLocalPrimaryBuckets))
            .isEqualTo(NUM_BUCKETS));

    dataStore2.invoke(() -> {
      Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(getCache());
      assertThat(registry.getFullScanHydrationCount()).isGreaterThan(0L);
      assertThat(registry.getReconciledStaleIndexKeysCount()).isGreaterThanOrEqualTo(1L);
      assertThat(registry.getHydratedTombstonesCount()).isGreaterThanOrEqualTo(1L);
      Tier0VectorDeltaBuffer<Object> buf =
          registry.getBuffer("/" + REGION_NAME, INDEX_NAME, finalStaleBucketId);
      assertThat(buf).isNotNull();
      assertThat(buf.snapshot().getTombstonedKeys()).contains(finalStaleKey);
    });

    // Verify 0 resurrected ghosts in exhaustive query across the promoted node
    List<String> hits = accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      HybridKnnQueryContext ctx = HybridKnnQueryContext
          .builder(INDEX_NAME, VECTOR_FIELD, query, 200)
          .similarityFunction(VectorSimilarityFunction.COSINE)
          .build();
      return extractKeys(DistributedHybridKnnFunction.executeOnRegion(region, ctx));
    });
    assertThat(hits).doesNotContain(finalStaleKey);
    assertThat(hits).containsExactlyInAnyOrderElementsOf(seedData.keySet());
  }

  @Test
  public void promotionFallbackWithMissingAeqCoversOverwriteUnindexedInsertAndDeleteRecreate()
      throws Exception {
    dataStore1.invoke(() -> initServer(1));
    dataStore2.invoke(() -> initServer(1));
    accessor.invoke(() -> initAccessor(1));

    float[] query = new float[DIM];
    query[0] = 1.0f;

    Map<String, VectorRecord> seedData = new HashMap<>();
    for (int i = 0; i < NUM_BUCKETS * 4; i++) {
      float cos = 0.40f + 0.003f * i;
      float[] vec = new float[DIM];
      vec[0] = cos;
      vec[1] = (float) Math.sqrt(1.0 - (double) cos * cos);
      seedData.put("seed-" + i, new VectorRecord(vec, "all", "all", "all"));
    }
    accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      for (Map.Entry<String, VectorRecord> e : seedData.entrySet()) {
        region.put(e.getKey(), e.getValue());
      }
    });
    waitForLuceneFlush(accessor);

    Set<Integer> ds1Primaries = dataStore1.invoke(() -> {
      PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(REGION_NAME);
      return new HashSet<>(pr.getDataStore().getAllLocalPrimaryBucketIds());
    });
    assertThat(ds1Primaries).isNotEmpty();
    int targetBucketId = ds1Primaries.iterator().next();

    List<String> bucketKeys = new ArrayList<>(4);
    for (int i = 0; i < 2000 && bucketKeys.size() < 4; i++) {
      String candidate = "fallback-case-" + i;
      int bid = accessor.invoke(() -> ((PartitionedRegion) getCache().getRegion(REGION_NAME))
          .getKeyInfo(candidate).getBucketId());
      if (bid == targetBucketId) {
        bucketKeys.add(candidate);
      }
    }
    assertThat(bucketKeys).hasSize(4);

    String overwriteKey = bucketKeys.get(0);
    String unindexedKey = bucketKeys.get(1);
    String delRecreateKey = bucketKeys.get(2);
    String expiredKey = bucketKeys.get(3);

    float[] lowCos30 = new float[DIM];
    lowCos30[0] = 0.30f;
    lowCos30[1] = (float) Math.sqrt(1.0 - 0.09);
    float[] lowCos25 = new float[DIM];
    lowCos25[0] = 0.25f;
    lowCos25[1] = (float) Math.sqrt(1.0 - 0.0625);
    float[] expiredCos99 = new float[DIM];
    expiredCos99[0] = 0.99f;
    expiredCos99[1] = (float) Math.sqrt(1.0 - 0.99 * 0.99);

    accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      region.put(overwriteKey, new VectorRecord(lowCos30, "all", "all", "all"));
      region.put(delRecreateKey, new VectorRecord(lowCos25, "all", "all", "all"));
      region.put(expiredKey, new VectorRecord(expiredCos99, "all", "all", "all"));
    });
    waitForLuceneFlush(accessor);

    GeodeAwaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
      assertThat(dataStore2.invoke(this::countLocalBuckets)).isEqualTo(NUM_BUCKETS);
      int shadowSize = dataStore1.invoke(() -> countShadowQueueLocalSize(getCache()))
          + dataStore2.invoke(() -> countShadowQueueLocalSize(getCache()));
      assertThat(shadowSize).isEqualTo(0);
    });

    // Pause AEQ so the acknowledged overwrite, unindexed insert, delete-recreate, and expired
    // tombstone reside only in replicated bucket state and are absent from AEQ delta coverage.
    dataStore1.invoke(() -> pauseIndexAeq(getCache()));
    dataStore2.invoke(() -> pauseIndexAeq(getCache()));

    float[] overwriteV2 = new float[DIM];
    overwriteV2[0] = 0.98f;
    overwriteV2[1] = (float) Math.sqrt(1.0 - 0.98 * 0.98);
    float[] unindexedV1 = new float[DIM];
    unindexedV1[0] = 0.97f;
    unindexedV1[1] = (float) Math.sqrt(1.0 - 0.97 * 0.97);
    float[] recreateV3 = new float[DIM];
    recreateV3[0] = 0.96f;
    recreateV3[1] = (float) Math.sqrt(1.0 - 0.96 * 0.96);

    accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      region.put(overwriteKey, new VectorRecord(overwriteV2, "all", "all", "all"));
      region.put(unindexedKey, new VectorRecord(unindexedV1, "all", "all", "all"));
      region.destroy(delRecreateKey);
      region.put(delRecreateKey, new VectorRecord(recreateV3, "all", "all", "all"));
      region.destroy(expiredKey);
    });

    dataStore2.invoke(() -> {
      PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(REGION_NAME);
      BucketRegion localBucket = pr.getDataStore().getLocalBucketById(targetBucketId);
      RegionEntry re = localBucket.getRegionMap().getEntry(expiredKey);
      assertThat(re).isNotNull();
      assertThat(re.isTombstone()).isTrue();
      assertThat(
          localBucket.getRegionMap().removeTombstone(re, re.getVersionStamp().asVersionTag()))
              .isTrue();

      String aeqId = LuceneServiceImpl.getUniqueIndexName(INDEX_NAME, REGION_NAME);
      AsyncEventQueueImpl q = (AsyncEventQueueImpl) getCache().getAsyncEventQueue(aeqId);
      if (q != null) {
        q.stop();
        ((InternalCache) getCache()).removeAsyncEventQueue(q);
      }
      Tier0VectorRegistry.forCache(getCache())
          .removeBucket("/" + REGION_NAME, INDEX_NAME, targetBucketId);
    });

    DistributedTestUtils.crashDistributedSystem(dataStore1);
    GeodeAwaitility.await().atMost(30, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(dataStore2.invoke(this::countLocalPrimaryBuckets))
            .isEqualTo(NUM_BUCKETS));

    dataStore2.invoke(() -> {
      Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(getCache());
      assertThat(registry.getFullScanHydrationCount()).isGreaterThan(0L);
      assertThat(registry.getReconciledStaleIndexKeysCount()).isGreaterThanOrEqualTo(1L);
      Tier0VectorDeltaBuffer<Object> buf =
          registry.getBuffer("/" + REGION_NAME, INDEX_NAME, targetBucketId);
      assertThat(buf).isNotNull();
      Tier0VectorDeltaBuffer.Snapshot<Object> snap = buf.snapshot();
      assertThat(snap.getLiveEntries()).containsKeys(overwriteKey, unindexedKey, delRecreateKey);
      assertThat(snap.getLiveEntries().get(overwriteKey).getVector()[0]).isCloseTo(0.98f,
          within(1e-5f));
      assertThat(snap.getLiveEntries().get(unindexedKey).getVector()[0]).isCloseTo(0.97f,
          within(1e-5f));
      assertThat(snap.getLiveEntries().get(delRecreateKey).getVector()[0]).isCloseTo(0.96f,
          within(1e-5f));
      assertThat(snap.getTombstonedKeys()).contains(expiredKey);
    });

    List<String> top3Hits = accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      HybridKnnQueryContext ctx = HybridKnnQueryContext
          .builder(INDEX_NAME, VECTOR_FIELD, query, 3)
          .similarityFunction(VectorSimilarityFunction.COSINE)
          .build();
      return extractKeys(DistributedHybridKnnFunction.executeOnRegion(region, ctx));
    });
    assertThat(top3Hits).containsExactly(overwriteKey, unindexedKey, delRecreateKey);

    List<String> allHits = accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      HybridKnnQueryContext ctx = HybridKnnQueryContext
          .builder(INDEX_NAME, VECTOR_FIELD, query, 200)
          .similarityFunction(VectorSimilarityFunction.COSINE)
          .build();
      return extractKeys(DistributedHybridKnnFunction.executeOnRegion(region, ctx));
    });
    Set<String> expectedAll = new HashSet<>(seedData.keySet());
    expectedAll.add(overwriteKey);
    expectedAll.add(unindexedKey);
    expectedAll.add(delRecreateKey);
    assertThat(allHits).doesNotContain(expiredKey);
    assertThat(allHits).containsExactlyInAnyOrderElementsOf(expectedAll);
  }

  @Test
  public void partialNonEmptyAeqOnPromotionReconcilesMissingAckedOverwriteInsertAndDelete()
      throws Exception {
    dataStore1.invoke(() -> initServer(1));
    dataStore2.invoke(() -> initServer(1));
    accessor.invoke(() -> initAccessor(1));

    float[] query = new float[DIM];
    query[0] = 1.0f;

    Map<String, VectorRecord> seedData = new HashMap<>();
    for (int i = 0; i < NUM_BUCKETS * 4; i++) {
      float cos = 0.40f + 0.003f * i;
      float[] vec = new float[DIM];
      vec[0] = cos;
      vec[1] = (float) Math.sqrt(1.0 - (double) cos * cos);
      seedData.put("paeq-seed-" + i, new VectorRecord(vec, "all", "all", "all"));
    }
    accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      for (Map.Entry<String, VectorRecord> e : seedData.entrySet()) {
        region.put(e.getKey(), e.getValue());
      }
    });
    waitForLuceneFlush(accessor);

    Set<Integer> ds1Primaries = dataStore1.invoke(() -> {
      PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(REGION_NAME);
      return new HashSet<>(pr.getDataStore().getAllLocalPrimaryBucketIds());
    });
    assertThat(ds1Primaries).isNotEmpty();
    int targetBucketId = ds1Primaries.iterator().next();

    List<String> bucketKeys = new ArrayList<>(5);
    for (int i = 0; i < 3000 && bucketKeys.size() < 5; i++) {
      String candidate = "paeq-case-" + i;
      int bid = accessor.invoke(() -> ((PartitionedRegion) getCache().getRegion(REGION_NAME))
          .getKeyInfo(candidate).getBucketId());
      if (bid == targetBucketId) {
        bucketKeys.add(candidate);
      }
    }
    assertThat(bucketKeys).hasSize(5);

    String aeqAnchorKey = bucketKeys.get(0);
    String overwriteKey = bucketKeys.get(1);
    String unindexedKey = bucketKeys.get(2);
    String tombstoneKey = bucketKeys.get(3);
    String expiredKey = bucketKeys.get(4);

    float[] lowCos30 = new float[DIM];
    lowCos30[0] = 0.30f;
    lowCos30[1] = (float) Math.sqrt(1.0 - 0.09);
    float[] highCos99 = new float[DIM];
    highCos99[0] = 0.99f;
    highCos99[1] = (float) Math.sqrt(1.0 - 0.99 * 0.99);
    float[] highCos985 = new float[DIM];
    highCos985[0] = 0.985f;
    highCos985[1] = (float) Math.sqrt(1.0 - 0.985 * 0.985);

    accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      region.put(overwriteKey, new VectorRecord(lowCos30, "all", "all", "all"));
      region.put(tombstoneKey, new VectorRecord(highCos99, "all", "all", "all"));
      region.put(expiredKey, new VectorRecord(highCos985, "all", "all", "all"));
    });
    waitForLuceneFlush(accessor);

    GeodeAwaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
      assertThat(dataStore2.invoke(this::countLocalBuckets)).isEqualTo(NUM_BUCKETS);
      int shadowSize = dataStore1.invoke(() -> countShadowQueueLocalSize(getCache()))
          + dataStore2.invoke(() -> countShadowQueueLocalSize(getCache()));
      assertThat(shadowSize).isEqualTo(0);
    });

    dataStore1.invoke(() -> pauseIndexAeq(getCache()));
    dataStore2.invoke(() -> pauseIndexAeq(getCache()));

    float[] anchorVec = new float[DIM];
    anchorVec[0] = 0.50f;
    anchorVec[1] = (float) Math.sqrt(1.0 - 0.25);
    float[] overwriteV2 = new float[DIM];
    overwriteV2[0] = 0.98f;
    overwriteV2[1] = (float) Math.sqrt(1.0 - 0.98 * 0.98);
    float[] unindexedV1 = new float[DIM];
    unindexedV1[0] = 0.97f;
    unindexedV1[1] = (float) Math.sqrt(1.0 - 0.97 * 0.97);

    accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      region.put(aeqAnchorKey, new VectorRecord(anchorVec, "all", "all", "all"));
      region.put(overwriteKey, new VectorRecord(overwriteV2, "all", "all", "all"));
      region.put(unindexedKey, new VectorRecord(unindexedV1, "all", "all", "all"));
      region.destroy(tombstoneKey);
      region.destroy(expiredKey);
    });

    // Drop AEQ shadow entries for overwrite/unindexed/deleted keys on BOTH primary (dataStore1)
    // and secondary (dataStore2) locally because BucketRegionQueue.destroyKey does not replicate
    // removals synchronously; leave ONLY aeqAnchorKey in shadowBucket -> non-empty partial AEQ.
    Set<String> dropFromAeq =
        new HashSet<>(Arrays.asList(overwriteKey, unindexedKey, tombstoneKey, expiredKey));
    dataStore1.invoke(() -> dropKeysFromAeqShadowQueue(getCache(), targetBucketId, dropFromAeq));

    dataStore2.invoke(() -> {
      PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(REGION_NAME);
      BucketRegion localBucket = pr.getDataStore().getLocalBucketById(targetBucketId);
      RegionEntry re = localBucket.getRegionMap().getEntry(expiredKey);
      assertThat(re).isNotNull();
      assertThat(re.isTombstone()).isTrue();
      assertThat(
          localBucket.getRegionMap().removeTombstone(re, re.getVersionStamp().asVersionTag()))
              .isTrue();

      dropKeysFromAeqShadowQueue(getCache(), targetBucketId, dropFromAeq);

      String aeqId = LuceneServiceImpl.getUniqueIndexName(INDEX_NAME, REGION_NAME);
      AsyncEventQueueImpl q = (AsyncEventQueueImpl) getCache().getAsyncEventQueue(aeqId);
      ConcurrentParallelGatewaySenderQueue cq =
          (ConcurrentParallelGatewaySenderQueue) q.getSender().getEventProcessor().getQueue();
      BucketRegion shadowBucket = cq.getRegion("/" + REGION_NAME)
          .getDataStore().getLocalBucketById(targetBucketId);
      assertThat(shadowBucket.isEmpty()).isFalse();
      for (Object sk : new ArrayList<>(shadowBucket.keySet())) {
        Object val =
            shadowBucket.getEntry(sk) != null ? shadowBucket.getEntry(sk).getValue() : null;
        if (val instanceof GatewaySenderEventImpl) {
          assertThat(dropFromAeq).doesNotContain((String) ((GatewaySenderEventImpl) val).getKey());
        }
      }
    });

    DistributedTestUtils.crashDistributedSystem(dataStore1);
    GeodeAwaitility.await().atMost(30, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(dataStore2.invoke(this::countLocalPrimaryBuckets))
            .isEqualTo(NUM_BUCKETS));

    dataStore2.invoke(() -> {
      Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(getCache());
      assertThat(registry.getAeqDeltaHydrationCount()).isGreaterThanOrEqualTo(ds1Primaries.size());
      assertThat(registry.getFullScanHydrationCount()).isEqualTo(0L);
      assertThat(registry.getReconciledStaleIndexKeysCount()).isGreaterThanOrEqualTo(1L);
      Tier0VectorDeltaBuffer<Object> buf =
          registry.getBuffer("/" + REGION_NAME, INDEX_NAME, targetBucketId);
      assertThat(buf).isNotNull();
      Tier0VectorDeltaBuffer.Snapshot<Object> snap = buf.snapshot();
      assertThat(snap.getLiveEntries()).containsKeys(aeqAnchorKey, overwriteKey, unindexedKey);
      assertThat(snap.getLiveEntries().get(overwriteKey).getVector()[0]).isCloseTo(0.98f,
          within(1e-5f));
      assertThat(snap.getLiveEntries().get(unindexedKey).getVector()[0]).isCloseTo(0.97f,
          within(1e-5f));
      assertThat(snap.getTombstonedKeys()).contains(tombstoneKey, expiredKey);
    });

    List<String> top2Hits = accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      HybridKnnQueryContext ctx = HybridKnnQueryContext
          .builder(INDEX_NAME, VECTOR_FIELD, query, 2)
          .similarityFunction(VectorSimilarityFunction.COSINE)
          .build();
      return extractKeys(DistributedHybridKnnFunction.executeOnRegion(region, ctx));
    });
    assertThat(top2Hits).containsExactly(overwriteKey, unindexedKey);

    List<String> allHits = accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      HybridKnnQueryContext ctx = HybridKnnQueryContext
          .builder(INDEX_NAME, VECTOR_FIELD, query, 200)
          .similarityFunction(VectorSimilarityFunction.COSINE)
          .build();
      return extractKeys(DistributedHybridKnnFunction.executeOnRegion(region, ctx));
    });
    Set<String> expectedAll = new HashSet<>(seedData.keySet());
    expectedAll.add(aeqAnchorKey);
    expectedAll.add(overwriteKey);
    expectedAll.add(unindexedKey);
    assertThat(allHits).doesNotContain(tombstoneKey, expiredKey);
    assertThat(allHits).containsExactlyInAnyOrderElementsOf(expectedAll);
  }

  @Test
  public void promotionOverwritesConflictingAeqWithAuthoritativeBucketLiveAndTombstoneAndDrains()
      throws Exception {
    dataStore1.invoke(() -> initServer(1));
    dataStore2.invoke(() -> initServer(1));
    accessor.invoke(() -> initAccessor(1));

    Map<String, VectorRecord> seedData = new HashMap<>();
    Random seedRand = new Random(20260714L);
    int seedCount = Math.max(60, NUM_BUCKETS * 4);
    for (int i = 0; i < seedCount; i++) {
      float[] vec = randomUnitVector(seedRand, DIM);
      vec[0] = -Math.abs(vec[0]) * 0.5f;
      normalizeInPlace(vec);
      seedData.put("seed-" + i, new VectorRecord(vec, "all", "all", "all"));
    }

    float[] query = new float[DIM];
    query[0] = 1.0f;

    accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      region.putAll(seedData);
    });
    waitForLuceneFlush(accessor);

    Set<Integer> ds1Primaries = dataStore1.invoke(() -> {
      PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(REGION_NAME);
      return new HashSet<>(pr.getDataStore().getAllLocalPrimaryBucketIds());
    });
    assertThat(ds1Primaries).isNotEmpty();
    int targetBucketId = ds1Primaries.iterator().next();

    List<String> bucketKeys = accessor.invoke(() -> {
      PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(REGION_NAME);
      List<String> keys = new ArrayList<>();
      for (int idx = 0; keys.size() < 4 && idx < 10_000; idx++) {
        String candidate = "conf-case-" + idx;
        if (pr.getKeyInfo(candidate).getBucketId() == targetBucketId) {
          keys.add(candidate);
        }
      }
      return keys;
    });
    assertThat(bucketKeys).hasSize(4);

    String aeqDestroyVsBucketLiveKey = bucketKeys.get(0);
    String aeqPutVsBucketTombKey = bucketKeys.get(1);
    String staleBufVsBucketLiveKey = bucketKeys.get(2);
    String postPromoLiveKey = bucketKeys.get(3);

    float[] lowCos25 = new float[DIM];
    lowCos25[0] = 0.25f;
    lowCos25[1] = (float) Math.sqrt(1.0 - 0.25 * 0.25);
    float[] highCos99 = new float[DIM];
    highCos99[0] = 0.99f;
    highCos99[1] = (float) Math.sqrt(1.0 - 0.99 * 0.99);
    float[] highCos97 = new float[DIM];
    highCos97[0] = 0.97f;
    highCos97[1] = (float) Math.sqrt(1.0 - 0.97 * 0.97);
    float[] postPromoVec = new float[DIM];
    postPromoVec[0] = 0.995f;
    postPromoVec[1] = (float) Math.sqrt(1.0 - 0.995 * 0.995);

    // Commit v1 of aeqPutVsBucketTombKey and staleBufVsBucketLiveKey into Lucene first
    accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      region.put(aeqPutVsBucketTombKey, new VectorRecord(highCos99, "all", "all", "all"));
      region.put(staleBufVsBucketLiveKey, new VectorRecord(lowCos25, "all", "all", "all"));
    });
    waitForLuceneFlush(accessor);

    GeodeAwaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
      assertThat(dataStore2.invoke(this::countLocalBuckets)).isEqualTo(NUM_BUCKETS);
      int shadowSize = dataStore1.invoke(() -> countShadowQueueLocalSize(getCache()))
          + dataStore2.invoke(() -> countShadowQueueLocalSize(getCache()));
      assertThat(shadowSize).isEqualTo(0);
    });

    dataStore1.invoke(() -> pauseIndexAeq(getCache()));
    dataStore2.invoke(() -> pauseIndexAeq(getCache()));

    accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      // Case 1: AEQ keeps DESTROY while authoritative bucket holds live highCos99
      region.put(aeqDestroyVsBucketLiveKey, new VectorRecord(lowCos25, "all", "all", "all"));
      region.destroy(aeqDestroyVsBucketLiveKey);
      region.put(aeqDestroyVsBucketLiveKey, new VectorRecord(highCos99, "all", "all", "all"));

      // Case 2: AEQ keeps UPDATE(lowCos25) while authoritative bucket holds TOMBSTONE
      region.put(aeqPutVsBucketTombKey, new VectorRecord(lowCos25, "all", "all", "all"));
      region.destroy(aeqPutVsBucketTombKey);

      // Case 3: Committed v1 overwritten by v2 while v2 AEQ event is dropped -> authoritative
      // bucket wins without needing AEQ event for v2
      region.put(staleBufVsBucketLiveKey, new VectorRecord(highCos97, "all", "all", "all"));
    });

    // Forge conflicting AEQ shadow queues on both dataStore1 and dataStore2 so shadowBucket stays
    // non-empty with DESTROY for aeqDestroyVsBucketLiveKey and UPDATE for aeqPutVsBucketTombKey,
    // while staleBufVsBucketLiveKey has no v2 AEQ event.
    dataStore1.invoke(() -> {
      dropSelectiveAeqShadowEvents(getCache(), targetBucketId, aeqDestroyVsBucketLiveKey, false);
      dropSelectiveAeqShadowEvents(getCache(), targetBucketId, aeqPutVsBucketTombKey, true);
      dropKeysFromAeqShadowQueue(
          getCache(), targetBucketId, Collections.singleton(staleBufVsBucketLiveKey));
    });
    dataStore2.invoke(() -> {
      dropSelectiveAeqShadowEvents(getCache(), targetBucketId, aeqDestroyVsBucketLiveKey, false);
      dropSelectiveAeqShadowEvents(getCache(), targetBucketId, aeqPutVsBucketTombKey, true);
      dropKeysFromAeqShadowQueue(
          getCache(), targetBucketId, Collections.singleton(staleBufVsBucketLiveKey));
    });

    DistributedTestUtils.crashDistributedSystem(dataStore1);
    GeodeAwaitility.await().atMost(30, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(dataStore2.invoke(this::countLocalPrimaryBuckets))
            .isEqualTo(NUM_BUCKETS));

    dataStore2.invoke(() -> {
      Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(getCache());
      Tier0VectorDeltaBuffer<Object> buf =
          registry.getBuffer("/" + REGION_NAME, INDEX_NAME, targetBucketId);
      assertThat(buf).isNotNull();
      Tier0VectorDeltaBuffer.Snapshot<Object> snap = buf.snapshot();
      assertThat(snap.getLiveEntries()).containsKeys(
          aeqDestroyVsBucketLiveKey, staleBufVsBucketLiveKey);
      assertThat(snap.getLiveEntries().get(aeqDestroyVsBucketLiveKey).getVector()[0])
          .isCloseTo(0.99f, within(1e-5f));
      assertThat(snap.getLiveEntries().get(staleBufVsBucketLiveKey).getVector()[0])
          .isCloseTo(0.97f, within(1e-5f));
      assertThat(snap.getTombstonedKeys()).contains(aeqPutVsBucketTombKey);
    });

    List<String> top2PostPromo = accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      HybridKnnQueryContext ctx = HybridKnnQueryContext
          .builder(INDEX_NAME, VECTOR_FIELD, query, 2)
          .similarityFunction(VectorSimilarityFunction.COSINE)
          .build();
      return extractKeys(DistributedHybridKnnFunction.executeOnRegion(region, ctx));
    });
    assertThat(top2PostPromo)
        .containsExactly(aeqDestroyVsBucketLiveKey, staleBufVsBucketLiveKey);

    // Live post-promotion put succeeds cleanly and participates in top-k
    accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      region.put(postPromoLiveKey, new VectorRecord(postPromoVec, "all", "all", "all"));
    });

    List<String> top3WithPostPromo = accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      HybridKnnQueryContext ctx = HybridKnnQueryContext
          .builder(INDEX_NAME, VECTOR_FIELD, query, 3)
          .similarityFunction(VectorSimilarityFunction.COSINE)
          .build();
      return extractKeys(DistributedHybridKnnFunction.executeOnRegion(region, ctx));
    });
    assertThat(top3WithPostPromo).containsExactly(
        postPromoLiveKey, aeqDestroyVsBucketLiveKey, staleBufVsBucketLiveKey);

    dataStore2.invoke(() -> resumeIndexAeq(getCache()));
    waitForLuceneFlush(accessor);
    dataStore2.invoke(() -> {
      assertThat(countTier0Tombstones()).isEqualTo(0);
      assertTier0DoesNotMaskKeys(
          Collections.singletonList(aeqPutVsBucketTombKey),
          Arrays.asList(aeqDestroyVsBucketLiveKey, postPromoLiveKey));
    });

    List<String> allPostFlush = accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      HybridKnnQueryContext ctx = HybridKnnQueryContext
          .builder(INDEX_NAME, VECTOR_FIELD, query, 200)
          .similarityFunction(VectorSimilarityFunction.COSINE)
          .build();
      return extractKeys(DistributedHybridKnnFunction.executeOnRegion(region, ctx));
    });
    Set<String> expectedPostFlush = new HashSet<>(seedData.keySet());
    expectedPostFlush.add(aeqDestroyVsBucketLiveKey);
    expectedPostFlush.add(staleBufVsBucketLiveKey);
    expectedPostFlush.add(postPromoLiveKey);
    assertThat(allPostFlush).doesNotContain(aeqPutVsBucketTombKey);
    assertThat(allPostFlush).containsExactlyInAnyOrderElementsOf(expectedPostFlush);
  }

  @Test
  public void inFlightReadInvalidatesAndRetriesAcrossBucketDemotionAndRePromotion()
      throws Exception {
    dataStore1.invoke(() -> initServer(1));
    dataStore2.invoke(() -> initServer(1));
    accessor.invoke(() -> initAccessor(1));

    // Seed committed Lucene segments across all 16 buckets so every bucket repository is ready
    // before pausing AEQ.
    float[] query = new float[DIM];
    query[0] = 1.0f;
    float[] orthogonalSeed = new float[DIM];
    orthogonalSeed[DIM - 1] = 1.0f;
    accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      for (int i = 0; i < NUM_BUCKETS * 2; i++) {
        region.put("seed-" + i, new VectorRecord(orthogonalSeed, "other", "all", "all"));
      }
    });
    waitForLuceneFlush(accessor);

    GeodeAwaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
      assertThat(dataStore1.invoke(this::countLocalBuckets)).isEqualTo(NUM_BUCKETS);
      assertThat(dataStore2.invoke(this::countLocalBuckets)).isEqualTo(NUM_BUCKETS);
      int shadowSize = dataStore1.invoke(() -> countShadowQueueLocalSize(getCache()))
          + dataStore2.invoke(() -> countShadowQueueLocalSize(getCache()));
      assertThat(shadowSize).isEqualTo(0);
    });

    int targetBucketId = dataStore1.invoke(() -> {
      PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(REGION_NAME);
      return pr.getDataStore().getAllLocalPrimaryBucketIds().iterator().next();
    });

    List<String> targetKeys = accessor.invoke(() -> {
      PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(REGION_NAME);
      List<String> keys = new ArrayList<>();
      for (int i = 0; keys.size() < 6 && i < 20_000; i++) {
        String candidate = "own-fence-" + i;
        if (pr.getKeyInfo(candidate).getBucketId() == targetBucketId) {
          keys.add(candidate);
        }
      }
      return keys;
    });
    assertThat(targetKeys).hasSize(6);

    String kTerm1Stale = targetKeys.get(0);
    String kTerm1Keep = targetKeys.get(1);
    String kTerm2Fresh = targetKeys.get(2);
    String kFuzzyStale = targetKeys.get(3);
    String kFuzzyFresh = targetKeys.get(4);
    String kDs2Only = targetKeys.get(5);

    float[] vecCos85 = unitVectorWithCos0(0.85f);
    float[] vecCos96 = unitVectorWithCos0(0.96f);
    float[] vecCos98 = unitVectorWithCos0(0.98f);
    float[] vecCos99 = unitVectorWithCos0(0.99f);
    float[] vecCos995 = unitVectorWithCos0(0.995f);

    // Pause AEQ on both servers so Term-1 / Term-2 mutations stay in Tier-0 / replicated bucket
    dataStore1.invoke(() -> pauseIndexAeq(getCache()));
    dataStore2.invoke(() -> pauseIndexAeq(getCache()));

    ExecutorService queryPool = Executors.newSingleThreadExecutor();
    try {
      // Case 1 (Path A supported filter): pause in-flight read on dataStore1 strictly between
      // buffer snapshot and searcher acquisition, demote targetBucketId ds1 -> ds2, mutate on ds2,
      // re-promote ds2 -> ds1, then release read. Ownership fence MUST invalidate Term-1 snapshot
      // and retry against the re-promoted cut.
      accessor.invoke(() -> {
        Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
        region.put(kTerm1Stale, new VectorRecord(vecCos96, "pathA", "all", "all"));
        region.put(kTerm1Keep, new VectorRecord(vecCos85, "pathA", "all", "all"));
      });

      long genBeforePathA = dataStore1.invoke(() -> Tier0VectorRegistry.forCache(getCache())
          .getOwnershipGeneration("/" + REGION_NAME, INDEX_NAME, targetBucketId));
      long retriesBeforePathA = dataStore1.invoke(
          () -> Tier0VectorRegistry.forCache(getCache()).getOwnershipFenceRetryCount());

      armOneShotBeforeAcquireHook(dataStore1, targetBucketId);

      Future<List<String>> pathAFuture = queryPool.submit(() -> accessor.invoke(() -> {
        Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
        HybridKnnQueryContext ctx = HybridKnnQueryContext
            .builder(INDEX_NAME, VECTOR_FIELD, query, 5)
            .similarityFunction(VectorSimilarityFunction.COSINE)
            .addMetadataFilter("sel1", "pathA")
            .rerankBeamCandidates(true)
            .build();
        return extractKeys(DistributedHybridKnnFunction.executeOnRegion(region, ctx));
      }));

      awaitBeforeAcquireHookEntered(dataStore1);

      transferPrimaryBucket(dataStore2, dataStore1, targetBucketId);
      accessor.invoke(() -> {
        Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
        region.destroy(kTerm1Stale);
        region.put(kTerm2Fresh, new VectorRecord(vecCos99, "pathA", "all", "all"));
      });
      transferPrimaryBucket(dataStore1, dataStore2, targetBucketId);

      releaseBeforeAcquireHook(dataStore1);

      List<String> pathAHits = pathAFuture.get(30, TimeUnit.SECONDS);
      assertThat(pathAHits).containsExactly(kTerm2Fresh, kTerm1Keep);
      assertThat(dataStore1.invoke(() -> Tier0VectorRegistry.forCache(getCache())
          .getOwnershipGeneration("/" + REGION_NAME, INDEX_NAME, targetBucketId)))
              .isGreaterThan(genBeforePathA);
      assertThat(dataStore1.invoke(
          () -> Tier0VectorRegistry.forCache(getCache()).getOwnershipFenceRetryCount()))
              .isGreaterThan(retriesBeforePathA);

      // Case 2 (Path B unsupported FuzzyQuery): pause Path B on dataStore1 strictly after flush +
      // snapshot while holding pathBWrite and before searcher acquisition, demote ds1 -> ds2,
      // mutate on ds2, re-promote ds2 -> ds1 (proving zero deadlock against pathBWrite!), then
      // release read. Path B MUST invalidate and return the exact re-promoted fuzzy cut.
      accessor.invoke(() -> {
        Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
        region.put(kFuzzyStale, new VectorRecord(vecCos96, "books", "all", "all"));
      });

      long genBeforePathB = dataStore1.invoke(() -> Tier0VectorRegistry.forCache(getCache())
          .getOwnershipGeneration("/" + REGION_NAME, INDEX_NAME, targetBucketId));
      long retriesBeforePathB = dataStore1.invoke(
          () -> Tier0VectorRegistry.forCache(getCache()).getOwnershipFenceRetryCount());

      armOneShotBeforeAcquireHook(dataStore1, targetBucketId);

      Future<List<String>> pathBFuture = queryPool.submit(() -> accessor.invoke(() -> {
        Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
        HybridKnnQueryContext ctx = HybridKnnQueryContext
            .builder(INDEX_NAME, VECTOR_FIELD, query, 5)
            .similarityFunction(VectorSimilarityFunction.COSINE)
            .filterQuery("sel1:boks~1", "sel1")
            .build();
        return extractKeys(DistributedHybridKnnFunction.executeOnRegion(region, ctx));
      }));

      awaitBeforeAcquireHookEntered(dataStore1);

      transferPrimaryBucket(dataStore2, dataStore1, targetBucketId);
      accessor.invoke(() -> {
        Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
        region.destroy(kFuzzyStale);
        region.put(kFuzzyFresh, new VectorRecord(vecCos98, "books", "all", "all"));
      });
      transferPrimaryBucket(dataStore1, dataStore2, targetBucketId);

      releaseBeforeAcquireHook(dataStore1);

      List<String> pathBHits = pathBFuture.get(30, TimeUnit.SECONDS);
      assertThat(pathBHits).containsExactly(kFuzzyFresh);
      assertThat(dataStore1.invoke(() -> Tier0VectorRegistry.forCache(getCache())
          .getOwnershipGeneration("/" + REGION_NAME, INDEX_NAME, targetBucketId)))
              .isGreaterThan(genBeforePathB);
      assertThat(dataStore1.invoke(
          () -> Tier0VectorRegistry.forCache(getCache()).getOwnershipFenceRetryCount()))
              .isGreaterThan(retriesBeforePathB);

      // Case 3 (Permanent demotion ds1 -> ds2 without re-promotion): dataStore1 MUST reject the
      // stale cut via PrimaryBucketException so Geode HA transparently retries on dataStore2 and
      // returns the exact post-demotion cut from dataStore2.
      long genBeforePerm = dataStore1.invoke(() -> Tier0VectorRegistry.forCache(getCache())
          .getOwnershipGeneration("/" + REGION_NAME, INDEX_NAME, targetBucketId));

      armOneShotBeforeAcquireHook(dataStore1, targetBucketId);

      Future<List<String>> permFuture = queryPool.submit(() -> accessor.invoke(() -> {
        Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
        HybridKnnQueryContext ctx = HybridKnnQueryContext
            .builder(INDEX_NAME, VECTOR_FIELD, query, 5)
            .similarityFunction(VectorSimilarityFunction.COSINE)
            .addMetadataFilter("sel1", "pathA")
            .build();
        return extractKeys(DistributedHybridKnnFunction.executeOnRegion(region, ctx));
      }));

      awaitBeforeAcquireHookEntered(dataStore1);

      transferPrimaryBucket(dataStore2, dataStore1, targetBucketId);
      accessor.invoke(() -> {
        Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
        region.destroy(kTerm2Fresh);
        region.put(kDs2Only, new VectorRecord(vecCos995, "pathA", "all", "all"));
      });

      releaseBeforeAcquireHook(dataStore1);

      List<String> permHits = permFuture.get(30, TimeUnit.SECONDS);
      assertThat(permHits).containsExactly(kDs2Only, kTerm1Keep);
      assertThat(dataStore1.invoke(() -> Tier0VectorRegistry.forCache(getCache())
          .getOwnershipGeneration("/" + REGION_NAME, INDEX_NAME, targetBucketId)))
              .isGreaterThan(genBeforePerm);
    } finally {
      clearBeforeAcquireHook(dataStore1);
      clearBeforeAcquireHook(dataStore2);
      queryPool.shutdownNow();
      dataStore1.invoke(() -> resumeIndexAeq(getCache()));
      dataStore2.invoke(() -> resumeIndexAeq(getCache()));
    }
  }

  private void armOneShotBeforeAcquireHook(VM vm, int targetBucketId) {
    vm.invoke(() -> {
      OWNERSHIP_HOOK_ENTERED = new CountDownLatch(1);
      OWNERSHIP_HOOK_RELEASE = new CountDownLatch(1);
      AtomicBoolean fired = new AtomicBoolean(false);
      DistributedHybridKnnFunction.beforeAcquireSearcherHookForTest = bucketId -> {
        if (bucketId == targetBucketId && fired.compareAndSet(false, true)) {
          OWNERSHIP_HOOK_ENTERED.countDown();
          try {
            OWNERSHIP_HOOK_RELEASE.await(30, TimeUnit.SECONDS);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
          }
        }
      };
    });
  }

  private void awaitBeforeAcquireHookEntered(VM vm) {
    boolean entered = vm.invoke(() -> {
      try {
        return OWNERSHIP_HOOK_ENTERED.await(30, TimeUnit.SECONDS);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        return false;
      }
    });
    assertThat(entered).as("In-flight read must pause before searcher acquisition").isTrue();
  }

  private void releaseBeforeAcquireHook(VM vm) {
    vm.invoke(() -> {
      DistributedHybridKnnFunction.beforeAcquireSearcherHookForTest = null;
      CountDownLatch release = OWNERSHIP_HOOK_RELEASE;
      if (release != null) {
        release.countDown();
      }
    });
  }

  private void clearBeforeAcquireHook(VM vm) {
    if (vm != null) {
      vm.invoke(() -> {
        DistributedHybridKnnFunction.beforeAcquireSearcherHookForTest = null;
        CountDownLatch release = OWNERSHIP_HOOK_RELEASE;
        if (release != null) {
          release.countDown();
        }
      });
    }
  }

  private void transferPrimaryBucket(VM targetPrimary, VM formerPrimary, int bucketId) {
    boolean moved = targetPrimary.invoke(() -> {
      PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(REGION_NAME);
      return pr.getRegionAdvisor().getBucketAdvisor(bucketId).becomePrimary(false);
    });
    assertThat(moved).as("Bucket %d must transfer primary cleanly", bucketId).isTrue();
    GeodeAwaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
      assertThat(targetPrimary.invoke(() -> {
        PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(REGION_NAME);
        BucketRegion br = pr.getDataStore().getLocalBucketById(bucketId);
        Tier0VectorRegistry reg = Tier0VectorRegistry.forCache(getCache());
        return br != null && br.getBucketAdvisor().isPrimary()
            && !reg.isHydrating("/" + REGION_NAME, INDEX_NAME, bucketId);
      })).isTrue();
      assertThat(formerPrimary.invoke(() -> {
        PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(REGION_NAME);
        BucketRegion br = pr.getDataStore().getLocalBucketById(bucketId);
        return br == null || !br.getBucketAdvisor().isPrimary();
      })).isTrue();
    });
  }

  private static float[] unitVectorWithCos0(float cos0) {
    float[] vec = new float[DIM];
    vec[0] = cos0;
    vec[1] = (float) Math.sqrt(Math.max(0.0, 1.0 - (double) cos0 * cos0));
    return vec;
  }

  private void verifySelectivityRegime(Map<String, VectorRecord> dataset, float[] query,
      String filterField, int k, double minRecall) {
    List<String> expected = computeBruteForceTopK(
        dataset, query, k, VectorSimilarityFunction.COSINE,
        rec -> "hit".equals(rec.getMetadata().get(filterField)));

    List<String> actual = accessor.invoke(() -> {
      Region<String, VectorRecord> region = getCache().getRegion(REGION_NAME);
      HybridKnnQueryContext ctx = HybridKnnQueryContext
          .builder(INDEX_NAME, VECTOR_FIELD, query, k)
          .similarityFunction(VectorSimilarityFunction.COSINE)
          .addMetadataFilter(filterField, "hit")
          .build();
      return extractKeys(DistributedHybridKnnFunction.executeOnRegion(region, ctx));
    });

    assertThat(actual).hasSize(expected.size());
    for (String key : actual) {
      assertThat(dataset.get(key).getMetadata().get(filterField))
          .as("Returned key %s violated filter %s=hit", key, filterField)
          .isEqualTo("hit");
    }
    double recall = computeRecall(expected, actual);
    assertThat(recall)
        .as("Selectivity regime %s (k=%d) recall", filterField, k)
        .isGreaterThanOrEqualTo(minRecall);
  }

  private void initServer(int redundantCopies) {
    Cache cache = getCache();
    LuceneService luceneService = LuceneServiceProvider.get(cache);
    luceneService.createIndexFactory()
        .setLuceneSerializer(new VectorRecordLuceneSerializer())
        .setFields("sel1", "sel5", "sel50")
        .create(INDEX_NAME, REGION_NAME);

    Tier0VectorBucketListener<String, VectorRecord> tier0Listener =
        new Tier0VectorBucketListener<>(INDEX_NAME, VectorRecord::getVector,
            VectorRecord::getMetadata);

    PartitionAttributes<String, VectorRecord> pa =
        new PartitionAttributesFactory<String, VectorRecord>()
            .setTotalNumBuckets(NUM_BUCKETS)
            .setRedundantCopies(redundantCopies)
            .setLocalMaxMemory(100)
            .addPartitionListener(tier0Listener.createPartitionListener())
            .create();

    cache.<String, VectorRecord>createRegionFactory(RegionShortcut.PARTITION_REDUNDANT)
        .setPartitionAttributes(pa)
        .addCacheListener(tier0Listener)
        .create(REGION_NAME);
  }

  private void initAccessor(int redundantCopies) {
    Cache cache = getCache();
    LuceneService luceneService = LuceneServiceProvider.get(cache);
    luceneService.createIndexFactory()
        .setLuceneSerializer(new VectorRecordLuceneSerializer())
        .setFields("sel1", "sel5", "sel50")
        .create(INDEX_NAME, REGION_NAME);

    Tier0VectorBucketListener<String, VectorRecord> tier0Listener =
        new Tier0VectorBucketListener<>(INDEX_NAME, VectorRecord::getVector,
            VectorRecord::getMetadata);

    PartitionAttributes<String, VectorRecord> pa =
        new PartitionAttributesFactory<String, VectorRecord>()
            .setTotalNumBuckets(NUM_BUCKETS)
            .setRedundantCopies(redundantCopies)
            .setLocalMaxMemory(0)
            .addPartitionListener(tier0Listener.createPartitionListener())
            .create();

    cache.<String, VectorRecord>createRegionFactory(RegionShortcut.PARTITION_PROXY_REDUNDANT)
        .setPartitionAttributes(pa)
        .create(REGION_NAME);
  }

  private void pauseIndexAeq(Cache cache) {
    String aeqId = LuceneServiceImpl.getUniqueIndexName(INDEX_NAME, REGION_NAME);
    AsyncEventQueueImpl queue = (AsyncEventQueueImpl) cache.getAsyncEventQueue(aeqId);
    assertThat(queue).as("AsyncEventQueue %s must exist", aeqId).isNotNull();
    queue.getSender().pause();
    AbstractGatewaySender sender = (AbstractGatewaySender) queue.getSender();
    sender.getEventProcessor().waitForDispatcherToPause();
  }

  private void resumeIndexAeq(Cache cache) {
    String aeqId = LuceneServiceImpl.getUniqueIndexName(INDEX_NAME, REGION_NAME);
    AsyncEventQueueImpl queue = (AsyncEventQueueImpl) cache.getAsyncEventQueue(aeqId);
    assertThat(queue).as("AsyncEventQueue %s must exist", aeqId).isNotNull();
    queue.getSender().resume();
  }

  private void waitForLuceneFlush(VM vm) {
    boolean flushed = vm.invoke(() -> {
      LuceneService service = LuceneServiceProvider.get(getCache());
      try {
        return service.waitUntilFlushed(INDEX_NAME, REGION_NAME, 60, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    });
    assertThat(flushed).isTrue();
  }

  private int countTier0Entries() {
    Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(getCache());
    int total = 0;
    for (Tier0VectorDeltaBuffer<Object> buf : registry.getBuckets("/" + REGION_NAME, INDEX_NAME)
        .values()) {
      total += buf.getTotalSize();
    }
    return total;
  }

  private int countTier0Tombstones() {
    Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(getCache());
    int total = 0;
    for (Tier0VectorDeltaBuffer<Object> buf : registry.getBuckets("/" + REGION_NAME, INDEX_NAME)
        .values()) {
      total += buf.snapshot().getTombstonedKeys().size();
    }
    return total;
  }

  private void assertTier0DoesNotMaskKeys(Collection<String> destroyed, Collection<String> puts) {
    Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(getCache());
    Set<Object> allMasked = new HashSet<>();
    for (Tier0VectorDeltaBuffer<Object> buf : registry.getBuckets("/" + REGION_NAME, INDEX_NAME)
        .values()) {
      allMasked.addAll(buf.snapshot().getMaskedKeys());
    }
    assertThat(allMasked).doesNotContainAnyElementsOf(destroyed);
    assertThat(allMasked).doesNotContainAnyElementsOf(puts);
  }

  private int countLocalPrimaryBuckets() {
    PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(REGION_NAME);
    return pr.getDataStore().getAllLocalPrimaryBucketIds().size();
  }

  private int countLocalBuckets() {
    PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(REGION_NAME);
    return pr.getDataStore().getAllLocalBucketRegions().size();
  }

  private int countShadowQueueLocalSize(Cache cache) {
    String aeqId = LuceneServiceImpl.getUniqueIndexName(INDEX_NAME, REGION_NAME);
    AsyncEventQueueImpl queue = (AsyncEventQueueImpl) cache.getAsyncEventQueue(aeqId);
    if (queue == null || queue.getSender() == null
        || queue.getSender().getEventProcessor() == null) {
      return 0;
    }
    Object q = queue.getSender().getEventProcessor().getQueue();
    if (q instanceof ConcurrentParallelGatewaySenderQueue) {
      return ((ConcurrentParallelGatewaySenderQueue) q).localSize(true);
    }
    return 0;
  }

  private void dropKeysFromAeqShadowQueue(Cache cache, int bucketId, Set<String> keysToDrop)
      throws ForceReattemptException {
    removeMatchingAeqShadowEvents(
        cache, bucketId, ev -> keysToDrop.contains(ev.getKey()));
  }

  private void dropSelectiveAeqShadowEvents(
      Cache cache, int bucketId, String key, boolean dropDestroysOnly)
      throws ForceReattemptException {
    removeMatchingAeqShadowEvents(cache, bucketId, ev -> {
      if (!key.equals(ev.getKey())) {
        return false;
      }
      boolean isDestroyOrInvalidate = ev.getOperation() != null
          && (ev.getOperation().isDestroy() || ev.getOperation().isInvalidate());
      return dropDestroysOnly == isDestroyOrInvalidate;
    });
  }

  private void removeMatchingAeqShadowEvents(Cache cache, int bucketId,
      java.util.function.Predicate<GatewaySenderEventImpl> shouldDrop)
      throws ForceReattemptException {
    String aeqId = LuceneServiceImpl.getUniqueIndexName(INDEX_NAME, REGION_NAME);
    AsyncEventQueueImpl q = (AsyncEventQueueImpl) cache.getAsyncEventQueue(aeqId);
    ConcurrentParallelGatewaySenderQueue cq =
        (ConcurrentParallelGatewaySenderQueue) q.getSender().getEventProcessor().getQueue();
    BlockingQueue<GatewaySenderEventImpl> tmp = cq.getBucketTmpQueue(bucketId);
    if (tmp != null) {
      tmp.removeIf(shouldDrop);
    }
    BucketRegion shadowBucket =
        cq.getRegion("/" + REGION_NAME).getDataStore().getLocalBucketById(bucketId);
    if (shadowBucket == null) {
      return;
    }
    List<Object> shadowKeysToRemove = new ArrayList<>();
    for (Object sk : new ArrayList<>(shadowBucket.keySet())) {
      Object val =
          shadowBucket.getEntry(sk) != null ? shadowBucket.getEntry(sk).getValue() : null;
      if (val instanceof GatewaySenderEventImpl && shouldDrop.test((GatewaySenderEventImpl) val)) {
        shadowKeysToRemove.add(sk);
      }
    }
    for (Object sk : shadowKeysToRemove) {
      ((AbstractBucketRegionQueue) shadowBucket).destroyKey(sk);
    }
  }

  private static List<String> extractKeys(TopEntries<String> topEntries) {
    return topEntries.getHits().stream()
        .map(EntryScore::getKey)
        .collect(Collectors.toList());
  }

  private static List<String> computeBruteForceTopK(Map<String, VectorRecord> data, float[] query,
      int k, VectorSimilarityFunction similarity,
      java.util.function.Predicate<VectorRecord> filter) {
    List<EntryScore<String>> scored = new ArrayList<>();
    for (Map.Entry<String, VectorRecord> e : data.entrySet()) {
      if (filter != null && !filter.test(e.getValue())) {
        continue;
      }
      float s = VectorSimilarityScorer.score(query, e.getValue().getVector(), similarity);
      scored.add(new EntryScore<>(e.getKey(), s));
    }
    scored.sort(Comparator.<EntryScore<String>>comparingDouble(EntryScore::getScore).reversed()
        .thenComparing(EntryScore::getKey));
    return scored.stream()
        .limit(k)
        .map(EntryScore::getKey)
        .collect(Collectors.toList());
  }

  private static double computeRecall(List<String> expected, List<String> actual) {
    if (expected.isEmpty()) {
      return actual.isEmpty() ? 1.0 : 0.0;
    }
    Set<String> actualSet = new HashSet<>(actual);
    int hits = 0;
    for (String key : expected) {
      if (actualSet.contains(key)) {
        hits++;
      }
    }
    return (double) hits / expected.size();
  }

  private static float[] randomUnitVector(Random random, int dim) {
    float[] vec = new float[dim];
    float normSq = 0f;
    for (int i = 0; i < dim; i++) {
      vec[i] = (float) random.nextGaussian();
      normSq += vec[i] * vec[i];
    }
    float invNorm = (float) (1.0 / Math.sqrt(Math.max(normSq, 1e-12f)));
    for (int i = 0; i < dim; i++) {
      vec[i] *= invNorm;
    }
    return vec;
  }

  private static void normalizeInPlace(float[] vec) {
    float normSq = 0f;
    for (float v : vec) {
      normSq += v * v;
    }
    float invNorm = (float) (1.0 / Math.sqrt(Math.max(normSq, 1e-12f)));
    for (int i = 0; i < vec.length; i++) {
      vec[i] *= invNorm;
    }
  }

  private static float[] negate(float[] v) {
    float[] out = new float[v.length];
    for (int i = 0; i < v.length; i++) {
      out[i] = -v[i];
    }
    return out;
  }

  public static class VectorRecord implements Serializable {
    private static final long serialVersionUID = 1L;
    private final float[] vector;
    private final String sel1;
    private final String sel5;
    private final String sel50;

    public VectorRecord(float[] vector, String sel1, String sel5, String sel50) {
      this.vector = Arrays.copyOf(vector, vector.length);
      this.sel1 = sel1;
      this.sel5 = sel5;
      this.sel50 = sel50;
    }

    public float[] getVector() {
      return vector;
    }

    public Map<String, Object> getMetadata() {
      Map<String, Object> map = new HashMap<>(4);
      map.put("sel1", sel1);
      map.put("sel5", sel5);
      map.put("sel50", sel50);
      return map;
    }
  }

  public static class VectorRecordLuceneSerializer implements LuceneSerializer<VectorRecord> {
    private static final long serialVersionUID = 1L;

    @Override
    public Collection<Document> toDocuments(LuceneIndex index, VectorRecord value) {
      Document doc = new Document();
      doc.add(new KnnFloatVectorField(VECTOR_FIELD, value.getVector(),
          VectorSimilarityFunction.COSINE));
      doc.add(new StringField("sel1", value.sel1, Field.Store.NO));
      doc.add(new StringField("sel5", value.sel5, Field.Store.NO));
      doc.add(new StringField("sel50", value.sel50, Field.Store.NO));
      return Collections.singletonList(doc);
    }
  }
}
