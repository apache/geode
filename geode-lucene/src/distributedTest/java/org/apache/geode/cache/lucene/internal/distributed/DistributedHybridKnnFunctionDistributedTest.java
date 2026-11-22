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
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
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

  private int countLocalPrimaryBuckets() {
    PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(REGION_NAME);
    return pr.getDataStore().getAllLocalPrimaryBucketIds().size();
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
