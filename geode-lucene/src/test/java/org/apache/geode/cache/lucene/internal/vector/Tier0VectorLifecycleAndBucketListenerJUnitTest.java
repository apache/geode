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

package org.apache.geode.cache.lucene.internal.vector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.cache.lucene.internal.InternalLuceneService;
import org.apache.geode.cache.lucene.internal.LuceneEventListener;
import org.apache.geode.cache.lucene.internal.LuceneIndexImpl;
import org.apache.geode.cache.lucene.internal.LuceneIndexStats;
import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.cache.lucene.internal.PartitionedRepositoryManager;
import org.apache.geode.cache.lucene.internal.distributed.DistributedHybridKnnFunction;
import org.apache.geode.cache.lucene.internal.distributed.EntryScore;
import org.apache.geode.cache.lucene.internal.distributed.HybridKnnQueryContext;
import org.apache.geode.cache.lucene.internal.distributed.TopEntriesCollector;
import org.apache.geode.cache.lucene.internal.repository.IndexRepository;
import org.apache.geode.cache.lucene.internal.repository.IndexRepositoryImpl;
import org.apache.geode.cache.partition.PartitionListener;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.KeyInfo;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.RegionMap;
import org.apache.geode.internal.cache.execute.InternalRegionFunctionContext;
import org.apache.geode.internal.cache.wan.AbstractGatewaySenderEventProcessor;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.cache.wan.InternalGatewaySender;
import org.apache.geode.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderQueue;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderQueue;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.test.fake.Fakes;
import org.apache.geode.test.junit.categories.LuceneTest;

/**
 * Unit tests covering {@code PR2-B1} ({@link Tier0VectorRegistry} cleanup on index destroy,
 * region destroy, and cache close) and {@code PR2-B3} ({@link Tier0VectorBucketListener} PDX
 * read-serialized hydration and zero tombstone resurrection under concurrent {@code afterPrimary}
 * and {@code afterDestroy}).
 */
@Category({LuceneTest.class})
public class Tier0VectorLifecycleAndBucketListenerJUnitTest {

  private static final String REGION_PATH = "/vectors";
  private static final String INDEX_NAME = "vecIdx";

  private InternalCache cache;

  @Before
  public void setUp() {
    cache = Fakes.cache();
  }

  @After
  public void tearDown() {
    Tier0VectorRegistry.removeCache(cache);
  }

  @Test
  public void luceneServiceDestroyIndexRegionAndCloseCleanTier0Registry() {
    LuceneServiceImpl service = new LuceneServiceImpl();
    service.init(cache);

    Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(cache);
    registry.recordPut(REGION_PATH, INDEX_NAME, 0, "k1", new float[] {1f, 0f}, Map.of());
    registry.recordDestroy(REGION_PATH, INDEX_NAME, 1, "k2");
    assertThat(registry.getBuckets(REGION_PATH, INDEX_NAME)).hasSize(2);

    // 1. Define index and destroy via destroyDefinedIndex -> must clean Tier-0 registry
    LuceneServiceImpl spyService = org.mockito.Mockito.spy(service);
    LuceneIndexImpl mockIndex = mock(LuceneIndexImpl.class);
    when(mockIndex.getName()).thenReturn(INDEX_NAME);
    when(mockIndex.getRegionPath()).thenReturn(REGION_PATH);
    org.mockito.Mockito.doReturn(mockIndex).when(spyService).getIndex(INDEX_NAME, REGION_PATH);

    spyService.destroyIndex(INDEX_NAME, REGION_PATH);
    assertThat(registry.getBuckets(REGION_PATH, INDEX_NAME)).isEmpty();

    // 2. Repopulate two indexes on same region and call destroyIndexes(regionPath)
    registry.recordPut(REGION_PATH, INDEX_NAME, 0, "stale-1", new float[] {1f, 0f}, Map.of());
    registry.recordPut(REGION_PATH, "otherIdx", 2, "stale-2", new float[] {0f, 1f}, Map.of());
    LuceneIndexImpl mockIndex2 = mock(LuceneIndexImpl.class);
    when(mockIndex2.getName()).thenReturn("otherIdx");
    when(mockIndex2.getRegionPath()).thenReturn(REGION_PATH);
    org.mockito.Mockito.doReturn(java.util.List.of(mockIndex, mockIndex2))
        .when(spyService).getAllIndexes();

    spyService.destroyIndexes(REGION_PATH);
    assertThat(registry.getBuckets(REGION_PATH, INDEX_NAME)).isEmpty();
    assertThat(registry.getBuckets(REGION_PATH, "otherIdx")).isEmpty();

    // 3. beforeRegionDestroyed cleans any residual region buffers
    registry.recordPut(REGION_PATH, INDEX_NAME, 3, "stale-3", new float[] {1f, 0f}, Map.of());
    org.mockito.Mockito.doReturn(Collections.emptyList()).when(spyService).getIndexes(REGION_PATH);
    Region<?, ?> mockRegion = mock(Region.class);
    when(mockRegion.getFullPath()).thenReturn(REGION_PATH);
    spyService.beforeRegionDestroyed(mockRegion);
    assertThat(registry.getBuckets(REGION_PATH, INDEX_NAME)).isEmpty();

    // 4. LuceneServiceImpl.close() removes the entire cache registry
    registry.recordPut(REGION_PATH, INDEX_NAME, 4, "stale-4", new float[] {1f, 0f}, Map.of());
    assertThat(Tier0VectorRegistry.hasCache(cache)).isTrue();
    spyService.close();
    assertThat(Tier0VectorRegistry.hasCache(cache)).isFalse();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void afterRegionDestroyCleansUpTier0Registry() {
    Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(cache);
    registry.recordPut(REGION_PATH, INDEX_NAME, 0, "k1", new float[] {1f, 0f}, Map.of());
    assertThat(registry.getBuckets(REGION_PATH, INDEX_NAME)).hasSize(1);

    Tier0VectorBucketListener<String, float[]> listener =
        new Tier0VectorBucketListener<>(INDEX_NAME, v -> v, v -> Map.of());
    Region<String, float[]> mockRegion = mock(Region.class);
    when(mockRegion.getFullPath()).thenReturn(REGION_PATH);
    when(mockRegion.getRegionService()).thenReturn(cache);
    RegionEvent<String, float[]> event = mock(RegionEvent.class);
    when(event.getRegion()).thenReturn(mockRegion);

    listener.afterRegionDestroy(event);
    assertThat(registry.getBuckets(REGION_PATH, INDEX_NAME)).isEmpty();
  }

  @Test
  public void afterPrimaryReadsPdxWithReadSerializedOverrideWithoutDeserializingDomainClass() {
    AtomicBoolean pdxOverrideFlag = new AtomicBoolean(false);
    doAnswer(inv -> {
      pdxOverrideFlag.set(inv.getArgument(0));
      return null;
    }).when(cache).setPdxReadSerializedOverride(anyBoolean());
    when(cache.getPdxReadSerializedOverride()).thenAnswer(inv -> pdxOverrideFlag.get());

    PdxInstance pdx = mock(PdxInstance.class);
    when(pdx.getField("embedding")).thenReturn(new float[] {0.6f, 0.8f});
    when(pdx.getField("cat")).thenReturn("news");
    // Ensure domain-class deserialization (getObject) would explode if ever called
    when(pdx.getObject()).thenThrow(new AssertionError("Must never call PdxInstance.getObject()"));

    EntrySnapshot snapshot = mock(EntrySnapshot.class);
    when(snapshot.getRawValue(true)).thenAnswer(inv -> {
      assertThat(pdxOverrideFlag.get())
          .as("pdxReadSerializedOverride must be true while reading bucket entry")
          .isTrue();
      return pdx;
    });

    BucketRegion bucket = mock(BucketRegion.class);
    when(bucket.isEmpty()).thenReturn(false);
    when(bucket.keySet()).thenReturn(Collections.singleton("pdx-k1"));
    when(bucket.getEntry("pdx-k1")).thenReturn(snapshot);

    PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);
    when(dataStore.getLocalBucketById(5)).thenReturn(bucket);

    PartitionedRegion pr = mock(PartitionedRegion.class);
    when(pr.getFullPath()).thenReturn(REGION_PATH);
    when(pr.getRegionService()).thenReturn(cache);
    when(pr.getDataStore()).thenReturn(dataStore);

    Tier0VectorBucketListener<String, Object> listener = new Tier0VectorBucketListener<>(
        INDEX_NAME,
        v -> (float[]) ((PdxInstance) v).getField("embedding"),
        v -> Map.of("cat", ((PdxInstance) v).getField("cat")));
    PartitionListener partitionListener = listener.createPartitionListener();
    partitionListener.afterRegionCreate(pr);

    partitionListener.afterPrimary(5);

    // Override restored to original false value
    assertThat(pdxOverrideFlag.get()).isFalse();
    verify(pdx, never()).getObject();

    Tier0VectorDeltaBuffer<Object> buffer =
        Tier0VectorRegistry.forCache(cache).getBuffer(REGION_PATH, INDEX_NAME, 5);
    assertThat(buffer).isNotNull();
    Tier0VectorDeltaBuffer.Snapshot<Object> snap = buffer.snapshot();
    assertThat(snap.getLiveEntries()).containsOnlyKeys("pdx-k1");
    assertThat(snap.getLiveEntries().get("pdx-k1").getVector()).containsExactly(0.6f, 0.8f);
    assertThat(snap.getLiveEntries().get("pdx-k1").getMetadata()).containsEntry("cat", "news");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void afterPrimaryHydrationNeverResurrectsConcurrentlyDestroyedTombstone()
      throws Exception {
    final int bucketId = 3;
    final int numKeys = 100;
    Map<Object, float[]> backingEntries = new ConcurrentHashMap<>();
    Set<Object> allKeys = new LinkedHashSet<>();
    for (int i = 0; i < numKeys; i++) {
      String key = "k-" + i;
      backingEntries.put(key, new float[] {1f, 0f});
      allKeys.add(key);
    }

    CountDownLatch hydrationReadFirst = new CountDownLatch(1);
    CountDownLatch destroyDone = new CountDownLatch(1);

    BucketRegion bucket = mock(BucketRegion.class);
    when(bucket.isEmpty()).thenReturn(false);
    when(bucket.keySet()).thenReturn(allKeys);
    when(bucket.getEntry(any())).thenAnswer(inv -> {
      Object k = inv.getArgument(0);
      float[] v = backingEntries.get(k);
      if (v == null) {
        return null;
      }
      EntrySnapshot es = mock(EntrySnapshot.class);
      when(es.getRawValue(true)).thenAnswer(rawInv -> {
        if ("k-0".equals(k)) {
          // Pause hydration mid-flight after reading k-0 so concurrent afterDestroy runs first
          hydrationReadFirst.countDown();
          destroyDone.await(5, TimeUnit.SECONDS);
        }
        return v;
      });
      return es;
    });

    PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);
    when(dataStore.getLocalBucketById(bucketId)).thenReturn(bucket);

    PartitionedRegion pr = mock(PartitionedRegion.class);
    when(pr.getFullPath()).thenReturn(REGION_PATH);
    when(pr.getRegionService()).thenReturn(cache);
    when(pr.getDataStore()).thenReturn(dataStore);
    KeyInfo keyInfo = mock(KeyInfo.class);
    when(keyInfo.getBucketId()).thenReturn(bucketId);
    when(pr.getKeyInfo(any(), any())).thenReturn(keyInfo);

    Tier0VectorBucketListener<String, float[]> listener =
        new Tier0VectorBucketListener<>(INDEX_NAME, v -> v, v -> Map.of());
    PartitionListener partitionListener = listener.createPartitionListener();
    partitionListener.afterRegionCreate(pr);

    // Pre-mark k-1 as updated with a newer vector before afterPrimary even starts
    Tier0VectorRegistry.forCache(cache).recordPut(
        REGION_PATH, INDEX_NAME, bucketId, "k-1", new float[] {0f, 1f}, Map.of());

    Set<String> destroyedKeys = new java.util.HashSet<>();
    ExecutorService exec = Executors.newFixedThreadPool(2);
    try {
      Future<?> hydrator = exec.submit(() -> partitionListener.afterPrimary(bucketId));

      assertThat(hydrationReadFirst.await(5, TimeUnit.SECONDS)).isTrue();

      // Concurrently destroy even keys (including k-0 mid-read!) via afterDestroy
      for (int i = 0; i < numKeys; i += 2) {
        String key = "k-" + i;
        destroyedKeys.add(key);
        backingEntries.remove(key);
        EntryEvent<String, float[]> destroyEvent = mock(EntryEvent.class);
        when(destroyEvent.getRegion()).thenReturn(pr);
        when(destroyEvent.getKey()).thenReturn(key);
        listener.afterDestroy(destroyEvent);
      }
      destroyDone.countDown();

      hydrator.get(10, TimeUnit.SECONDS);
    } finally {
      exec.shutdownNow();
    }

    Tier0VectorDeltaBuffer<Object> buffer =
        Tier0VectorRegistry.forCache(cache).getBuffer(REGION_PATH, INDEX_NAME, bucketId);
    assertThat(buffer).isNotNull();
    Tier0VectorDeltaBuffer.Snapshot<Object> snap = buffer.snapshot();

    // Every concurrently destroyed key MUST remain tombstoned (zero resurrections!)
    for (String destroyed : destroyedKeys) {
      assertThat(snap.isTombstoned(destroyed))
          .as("Destroyed key %s must not be resurrected by afterPrimary", destroyed)
          .isTrue();
      assertThat(snap.getLiveEntries()).doesNotContainKey(destroyed);
    }

    // Newer live put on k-1 must NOT be overwritten by stale bucket value
    assertThat(snap.getLiveEntries().get("k-1").getVector()).containsExactly(0f, 1f);

    // Surviving odd keys were hydrated cleanly
    for (int i = 3; i < numKeys; i += 2) {
      assertThat(snap.getLiveEntries()).containsKey("k-" + i);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void promotionHydrationBarrierBlocksConcurrentHybridReadsUntilHydrationCompletes()
      throws Exception {
    final int bucketId = 7;
    CountDownLatch hydrationEntered = new CountDownLatch(1);
    CountDownLatch allowHydrationFinish = new CountDownLatch(1);

    EntrySnapshot es = mock(EntrySnapshot.class);
    when(es.getRawValue(true)).thenAnswer(inv -> {
      hydrationEntered.countDown();
      allowHydrationFinish.await(5, TimeUnit.SECONDS);
      return new float[] {1f, 0f, 0f, 0f};
    });

    BucketRegion bucket = mock(BucketRegion.class);
    when(bucket.isEmpty()).thenReturn(false);
    when(bucket.keySet()).thenReturn(Collections.singleton("promoted-k1"));
    when(bucket.getEntry("promoted-k1")).thenReturn(es);

    PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);
    when(dataStore.getLocalBucketById(bucketId)).thenReturn(bucket);

    PartitionedRegion pr = mock(PartitionedRegion.class);
    when(pr.getFullPath()).thenReturn(REGION_PATH);
    when(pr.getRegionService()).thenReturn(cache);
    when(pr.getDataStore()).thenReturn(dataStore);

    Tier0VectorBucketListener<String, float[]> listener =
        new Tier0VectorBucketListener<>(INDEX_NAME, v -> v, v -> Map.of());
    PartitionListener partitionListener = listener.createPartitionListener();
    partitionListener.afterRegionCreate(pr);

    // Wire DistributedHybridKnnFunction mocks with an empty Lucene repo (AEQ hasn't caught up)
    InternalLuceneService mockService = mock(InternalLuceneService.class);
    LuceneIndexImpl mockIndex = mock(LuceneIndexImpl.class);
    PartitionedRepositoryManager mockRepoManager = mock(PartitionedRepositoryManager.class);
    IndexRepository emptyLuceneRepo = mock(IndexRepository.class);
    when(emptyLuceneRepo.tryQueryAtEpoch(anyLong(), any(), anyInt(), any())).thenCallRealMethod();
    LuceneIndexStats mockStats = mock(LuceneIndexStats.class);
    InternalRegionFunctionContext fnCtx = mock(InternalRegionFunctionContext.class);
    ResultSender<TopEntriesCollector> resultSender = mock(ResultSender.class);

    when(cache.getService(InternalLuceneService.class)).thenReturn(mockService);
    when(mockService.getIndex(INDEX_NAME, REGION_PATH)).thenReturn(mockIndex);
    when(mockIndex.getRepositoryManager()).thenReturn(mockRepoManager);
    when(mockIndex.getIndexStats()).thenReturn(mockStats);
    when(fnCtx.getCache()).thenReturn(cache);
    when(fnCtx.getDataSet()).thenReturn(pr);
    when(fnCtx.getResultSender()).thenReturn(resultSender);
    when(mockRepoManager.getRepositoriesByBucket(fnCtx, false))
        .thenReturn(Collections.singletonMap(bucketId, emptyLuceneRepo));

    HybridKnnQueryContext queryContext = HybridKnnQueryContext
        .builder(INDEX_NAME, "embedding", new float[] {1f, 0f, 0f, 0f}, 5)
        .build();
    when(fnCtx.getArguments()).thenReturn(queryContext);

    Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(cache);
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      // 1. Start primary promotion hydration and pause mid-hydration
      Future<?> hydrator = pool.submit(() -> partitionListener.afterPrimary(bucketId));
      assertThat(hydrationEntered.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(registry.isHydrating(REGION_PATH, INDEX_NAME, bucketId)).isTrue();

      // 2. Launch a concurrent hybrid k-NN read while hydration is still in flight
      java.util.concurrent.atomic.AtomicReference<TopEntriesCollector> captured =
          new java.util.concurrent.atomic.AtomicReference<>();
      doAnswer(inv -> {
        captured.set(inv.getArgument(0));
        return null;
      }).when(resultSender).lastResult(any(TopEntriesCollector.class));

      Future<?> reader = pool.submit(() -> new DistributedHybridKnnFunction().execute(fnCtx));

      // Wait until reader registers on the hydration barrier (proving it did NOT bypass with empty)
      long waitDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (registry.getHydrationWaitCount() == 0L && System.nanoTime() < waitDeadline) {
        Thread.sleep(5L);
      }
      assertThat(registry.getHydrationWaitCount()).isGreaterThanOrEqualTo(1L);
      assertThat(reader.isDone()).isFalse();

      // 3. Release hydration -> reader unblocks and observes 100% of hydrated Tier-0 entries
      allowHydrationFinish.countDown();
      hydrator.get(5, TimeUnit.SECONDS);
      reader.get(5, TimeUnit.SECONDS);

      assertThat(registry.isHydrating(REGION_PATH, INDEX_NAME, bucketId)).isFalse();
      assertThat(captured.get()).isNotNull();
      java.util.List<EntryScore> hits = captured.get().getEntries().getHits();
      assertThat(hits).extracting(EntryScore::getKey).containsExactly("promoted-k1");
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void afterPrimaryAeqDeltaHydrationReconcilesUncommittedPutsAndTombstonesInOaeqSize() {
    final int bucketId = 4;
    Map<Object, float[]> bucketData = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      bucketData.put("committed-" + i, new float[] {1f, 0f});
    }
    bucketData.put("aeq-live-1", new float[] {0.5f, 0.5f});
    bucketData.put("aeq-recreated", new float[] {0.8f, 0.2f});
    bucketData.put("post-promo-live", new float[] {0f, 1f});

    BucketRegion bucket = mock(BucketRegion.class);
    when(bucket.isEmpty()).thenReturn(false);
    when(bucket.keySet()).thenReturn(bucketData.keySet());
    when(bucket.getEntry(any())).thenAnswer(inv -> {
      Object k = inv.getArgument(0);
      float[] v = bucketData.get(k);
      if (v == null) {
        return null;
      }
      EntrySnapshot es = mock(EntrySnapshot.class);
      when(es.getRawValue(true)).thenReturn(v);
      return es;
    });

    PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);
    when(dataStore.getLocalBucketById(bucketId)).thenReturn(bucket);
    PartitionedRegion pr = mock(PartitionedRegion.class);
    when(pr.getFullPath()).thenReturn(REGION_PATH);
    when(pr.getRegionService()).thenReturn(cache);
    when(pr.getDataStore()).thenReturn(dataStore);

    // Wire shadow bucket with uncommitted puts/destroys (chronological shadowKeys)
    Map<Long, GatewaySenderEventImpl> shadowEntries = new HashMap<>();
    shadowEntries.put(10L, mockShadowEvent("aeq-live-1", Operation.CREATE, 10L));
    shadowEntries.put(11L, mockShadowEvent("aeq-recreated", Operation.DESTROY, 11L));
    shadowEntries.put(12L, mockShadowEvent("aeq-recreated", Operation.CREATE, 12L));
    shadowEntries.put(13L, mockShadowEvent("aeq-deleted", Operation.CREATE, 13L));
    shadowEntries.put(14L, mockShadowEvent("aeq-deleted", Operation.DESTROY, 14L));
    shadowEntries.put(15L, mockShadowEvent("precrash-destroy", Operation.DESTROY, 15L));
    shadowEntries.put(16L, mockShadowEvent("missing-in-bucket", Operation.UPDATE, 16L));
    shadowEntries.put(17L, mockShadowEvent("post-promo-live", Operation.DESTROY, 17L));

    wireMockAeqShadowBucket(pr, bucketId, shadowEntries);

    Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(cache);
    // Simulate a live post-promotion put already recorded before afterPrimary reaches the key
    registry.recordPut(REGION_PATH, INDEX_NAME, bucketId, "post-promo-live",
        new float[] {0f, 1f}, Map.of());

    Tier0VectorBucketListener<String, float[]> listener =
        new Tier0VectorBucketListener<>(INDEX_NAME, v -> v, v -> Map.of());
    PartitionListener partitionListener = listener.createPartitionListener();
    partitionListener.afterRegionCreate(pr);
    partitionListener.afterPrimary(bucketId);

    Tier0VectorDeltaBuffer<Object> buffer = registry.getBuffer(REGION_PATH, INDEX_NAME, bucketId);
    Tier0VectorDeltaBuffer.Snapshot<Object> snap = buffer.snapshot();

    // AEQ-delta path runs first (replaying AEQ puts/tombstones without overwriting post-promo-live)
    // and unconditional full-bucket completeness pass hydrates remaining live entries cleanly.
    assertThat(registry.getAeqDeltaHydrationCount()).isEqualTo(1L);
    assertThat(registry.getFullScanHydrationCount()).isEqualTo(0L);
    assertThat(registry.getHydratedKeysCount()).isEqualTo(105L);
    assertThat(registry.getHydratedTombstonesCount()).isEqualTo(3L);
    assertThat(snap.getLiveEntries()).containsKeys("aeq-live-1", "aeq-recreated",
        "post-promo-live");
    assertThat(snap.getLiveEntries()).hasSize(103);
    assertThat(snap.getLiveEntries().get("post-promo-live").getVector()).containsExactly(0f, 1f);
    assertThat(snap.getTombstonedKeys())
        .containsExactlyInAnyOrder("aeq-deleted", "precrash-destroy", "missing-in-bucket");

    // Draining AEQ via evictCommitted drains |T_mask| for AEQ keys and all tombstones to 0
    Map<Object, Long> committedSeqs = new HashMap<>();
    for (Object k : snap.getTombstonedKeys()) {
      committedSeqs.put(k, buffer.getKeySequence(k));
    }
    committedSeqs.put("aeq-live-1", buffer.getKeySequence("aeq-live-1"));
    committedSeqs.put("aeq-recreated", buffer.getKeySequence("aeq-recreated"));
    committedSeqs.put("post-promo-live", buffer.getKeySequence("post-promo-live"));
    buffer.evictCommitted(committedSeqs);
    assertThat(buffer.snapshot().getTombstonedKeys()).isEmpty();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void afterPrimaryPartialNonEmptyAeqReconcilesMissingOverwriteInsertDeleteAndExpiredTombstone()
      throws Exception {
    int bucketId = 23;
    LuceneSerializer<float[]> serializer = (idx, vec) -> {
      Document doc = new Document();
      doc.add(new KnnFloatVectorField("embedding", vec, VectorSimilarityFunction.COSINE));
      return Collections.singletonList(doc);
    };

    try (ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(new StandardAnalyzer()))) {
      BucketRegion bucket = mock(BucketRegion.class);
      when(bucket.isEmpty()).thenReturn(false);
      when(bucket.keySet())
          .thenReturn(Set.of("aeq-anchor", "overwrite-missing-aeq", "unindexed-missing-aeq"));

      float[] anchorVec = new float[] {0f, 1f};
      float[] overwriteV2 = new float[] {0.99f, 0.14f};
      float[] unindexedV1 = new float[] {0.98f, 0.2f};

      Region.Entry<Object, Object> anchorEntry = mock(Region.Entry.class);
      when(anchorEntry.getValue()).thenReturn(anchorVec);
      Region.Entry<Object, Object> overwriteEntry = mock(Region.Entry.class);
      when(overwriteEntry.getValue()).thenReturn(overwriteV2);
      Region.Entry<Object, Object> unindexedEntry = mock(Region.Entry.class);
      when(unindexedEntry.getValue()).thenReturn(unindexedV1);

      when(bucket.getEntry("aeq-anchor")).thenReturn(anchorEntry);
      when(bucket.getEntry("overwrite-missing-aeq")).thenReturn(overwriteEntry);
      when(bucket.getEntry("unindexed-missing-aeq")).thenReturn(unindexedEntry);
      when(bucket.getEntry("tombstone-missing-aeq")).thenReturn(null);
      when(bucket.getEntry("expired-missing-aeq")).thenReturn(null);

      RegionEntry liveTombstone = mock(RegionEntry.class);
      when(liveTombstone.isTombstone()).thenReturn(true);
      when(liveTombstone.getKey()).thenReturn("tombstone-missing-aeq");
      RegionMap regionMap = mock(RegionMap.class);
      when(regionMap.regionEntries()).thenReturn(List.of(liveTombstone));
      when(bucket.getRegionMap()).thenReturn(regionMap);

      PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);
      when(dataStore.getLocalBucketById(bucketId)).thenReturn(bucket);
      PartitionedRegion pr = mock(PartitionedRegion.class);
      when(pr.getFullPath()).thenReturn(REGION_PATH);
      when(pr.getRegionService()).thenReturn(cache);
      when(pr.getDataStore()).thenReturn(dataStore);
      when(pr.isDestroyed()).thenReturn(false);

      // Non-empty AEQ contains ONLY aeq-anchor; overwrite, unindexed insert, and deletes are absent
      Map<Long, GatewaySenderEventImpl> shadowEntries = new HashMap<>();
      shadowEntries.put(1L, mockShadowEvent("aeq-anchor", Operation.CREATE, 1L));
      wireMockAeqShadowBucket(pr, bucketId, shadowEntries);

      // Lucene holds stale v1 for overwrite-missing-aeq and stale docs for deleted keys
      LuceneIndexImpl mockIndex = mock(LuceneIndexImpl.class);
      LuceneIndexStats mockStats = mock(LuceneIndexStats.class);
      IndexRepositoryImpl repo = new IndexRepositoryImpl(
          bucket, writer, serializer, mockStats, bucket, null, "lock", mockIndex);
      repo.update("overwrite-missing-aeq", new float[] {0.1f, 0.99f});
      repo.update("tombstone-missing-aeq", new float[] {0.9f, 0.4f});
      repo.update("expired-missing-aeq", new float[] {1f, 0f});
      repo.commitAndRefreshBlocking();

      PartitionedRepositoryManager mockRepoManager = mock(PartitionedRepositoryManager.class);
      when(mockRepoManager.getRepositoryIfReady(bucketId)).thenReturn(repo);
      InternalLuceneService mockService = mock(InternalLuceneService.class);
      when(mockService.getIndex(INDEX_NAME, REGION_PATH)).thenReturn(mockIndex);
      when(mockIndex.getRepositoryManager()).thenReturn(mockRepoManager);
      when(cache.getService(InternalLuceneService.class)).thenReturn(mockService);

      Tier0VectorBucketListener<String, float[]> listener =
          new Tier0VectorBucketListener<>(INDEX_NAME, v -> v, v -> Map.of());
      PartitionListener partitionListener = listener.createPartitionListener();
      partitionListener.afterRegionCreate(pr);
      partitionListener.afterPrimary(bucketId);

      Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(cache);
      assertThat(registry.getAeqDeltaHydrationCount()).isEqualTo(1L);
      assertThat(registry.getFullScanHydrationCount()).isEqualTo(0L);
      assertThat(registry.getReconciledStaleIndexKeysCount()).isEqualTo(1L);
      assertThat(registry.getHydratedTombstonesCount()).isEqualTo(2L);

      Tier0VectorDeltaBuffer<Object> buffer = registry.getBuffer(REGION_PATH, INDEX_NAME, bucketId);
      Tier0VectorDeltaBuffer.Snapshot<Object> snap = buffer.snapshot();
      assertThat(snap.getLiveEntries())
          .containsOnlyKeys("aeq-anchor", "overwrite-missing-aeq", "unindexed-missing-aeq");
      assertThat(snap.getLiveEntries().get("overwrite-missing-aeq").getVector())
          .containsExactly(overwriteV2);
      assertThat(snap.getLiveEntries().get("unindexed-missing-aeq").getVector())
          .containsExactly(unindexedV1);
      assertThat(snap.getTombstonedKeys())
          .containsExactlyInAnyOrder("tombstone-missing-aeq", "expired-missing-aeq");
    }
  }

  @Test
  public void afterPrimaryEmptyBucketStillHydratesPreCrashAeqTombstones() {
    final int bucketId = 6;
    BucketRegion bucket = mock(BucketRegion.class);
    when(bucket.isEmpty()).thenReturn(true);
    when(bucket.keySet()).thenReturn(Collections.emptySet());

    PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);
    when(dataStore.getLocalBucketById(bucketId)).thenReturn(bucket);
    PartitionedRegion pr = mock(PartitionedRegion.class);
    when(pr.getFullPath()).thenReturn(REGION_PATH);
    when(pr.getRegionService()).thenReturn(cache);
    when(pr.getDataStore()).thenReturn(dataStore);

    Map<Long, GatewaySenderEventImpl> shadowEntries = new HashMap<>();
    shadowEntries.put(1L, mockShadowEvent("emptied-ghost-1", Operation.DESTROY, 1L));
    shadowEntries.put(2L, mockShadowEvent("emptied-ghost-2", Operation.INVALIDATE, 2L));
    wireMockAeqShadowBucket(pr, bucketId, shadowEntries);

    Tier0VectorBucketListener<String, float[]> listener =
        new Tier0VectorBucketListener<>(INDEX_NAME, v -> v, v -> Map.of());
    PartitionListener partitionListener = listener.createPartitionListener();
    partitionListener.afterRegionCreate(pr);
    partitionListener.afterPrimary(bucketId);

    Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(cache);
    assertThat(registry.getAeqDeltaHydrationCount()).isEqualTo(1L);
    assertThat(registry.getFullScanHydrationCount()).isEqualTo(0L);
    assertThat(registry.getHydratedKeysCount()).isEqualTo(2L);
    assertThat(registry.getHydratedTombstonesCount()).isEqualTo(2L);

    Tier0VectorDeltaBuffer<Object> buffer = registry.getBuffer(REGION_PATH, INDEX_NAME, bucketId);
    assertThat(buffer.snapshot().getTombstonedKeys())
        .containsExactlyInAnyOrder("emptied-ghost-1", "emptied-ghost-2");
  }

  @Test
  public void afterPrimaryFallbackScansRegionMapTombstonesEvenWhenBucketIsEmpty() {
    final int bucketId = 8;
    RegionEntry tombA = mock(RegionEntry.class);
    when(tombA.isTombstone()).thenReturn(true);
    when(tombA.getKey()).thenReturn("tomb-a");

    RegionEntry tombB = mock(RegionEntry.class);
    when(tombB.isTombstone()).thenReturn(true);
    when(tombB.getKey()).thenReturn("tomb-b");

    RegionEntry nonTombRemoved = mock(RegionEntry.class);
    when(nonTombRemoved.isTombstone()).thenReturn(false);
    when(nonTombRemoved.getKey()).thenReturn("removed-c");

    List<RegionEntry> regionEntries = new ArrayList<>();
    regionEntries.add(tombA);
    regionEntries.add(tombB);
    regionEntries.add(nonTombRemoved);

    RegionMap regionMap = mock(RegionMap.class);
    when(regionMap.regionEntries()).thenReturn(regionEntries);

    BucketRegion bucket = mock(BucketRegion.class);
    when(bucket.isEmpty()).thenReturn(true);
    when(bucket.keySet()).thenReturn(Collections.emptySet());
    when(bucket.getRegionMap()).thenReturn(regionMap);

    PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);
    when(dataStore.getLocalBucketById(bucketId)).thenReturn(bucket);
    PartitionedRegion pr = mock(PartitionedRegion.class);
    when(pr.getFullPath()).thenReturn(REGION_PATH);
    when(pr.getRegionService()).thenReturn(cache);
    when(pr.getDataStore()).thenReturn(dataStore);

    // No AEQ registered on cache -> forces fallback full-scan path
    Tier0VectorBucketListener<String, float[]> listener =
        new Tier0VectorBucketListener<>(INDEX_NAME, v -> v, v -> Map.of());
    PartitionListener partitionListener = listener.createPartitionListener();
    partitionListener.afterRegionCreate(pr);
    partitionListener.afterPrimary(bucketId);

    Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(cache);
    assertThat(registry.getFullScanHydrationCount()).isEqualTo(1L);
    assertThat(registry.getAeqDeltaHydrationCount()).isEqualTo(0L);
    assertThat(registry.getHydratedKeysCount()).isEqualTo(2L);
    assertThat(registry.getHydratedTombstonesCount()).isEqualTo(2L);

    Tier0VectorDeltaBuffer<Object> buffer = registry.getBuffer(REGION_PATH, INDEX_NAME, bucketId);
    assertThat(buffer.snapshot().getTombstonedKeys())
        .containsExactlyInAnyOrder("tomb-a", "tomb-b");
    assertThat(registry.getLastHydrationEventsInspected()).isEqualTo(2L);
    assertThat(registry.getLastHydrationKeysRestored()).isEqualTo(2L);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void aeqDeltaHydrationResolvesOutOfOrderSameKeySequencesAndTempQueueTiebreak() {
    int bucketId = 19;

    // 1. Scramble shadowKey iteration order for multi-op same-key sequences:
    // recreate-k: CREATE(10) -> DESTROY(20) -> UPDATE(30) => latest is LIVE
    // deleted-k: CREATE(40) -> DESTROY(50) => latest is TOMBSTONE
    // tiebreak-k: CREATE(60 in shadowBucket) vs DESTROY(60 in tempQueue) => tempQueue wins (TOMB)
    // temp-recreate-k: DESTROY(70 in shadowBucket) vs UPDATE(80 in tempQueue) => LIVE
    Map<Long, GatewaySenderEventImpl> shadowEntries = new LinkedHashMap<>();
    shadowEntries.put(30L, mockShadowEvent("recreate-k", Operation.UPDATE, 30L));
    shadowEntries.put(10L, mockShadowEvent("recreate-k", Operation.CREATE, 10L));
    shadowEntries.put(20L, mockShadowEvent("recreate-k", Operation.DESTROY, 20L));
    shadowEntries.put(50L, mockShadowEvent("deleted-k", Operation.DESTROY, 50L));
    shadowEntries.put(40L, mockShadowEvent("deleted-k", Operation.CREATE, 40L));
    shadowEntries.put(60L, mockShadowEvent("tiebreak-k", Operation.CREATE, 60L));
    shadowEntries.put(70L, mockShadowEvent("temp-recreate-k", Operation.DESTROY, 70L));

    LinkedBlockingQueue<GatewaySenderEventImpl> tempQueue = new LinkedBlockingQueue<>();
    tempQueue.add(mockShadowEvent("tiebreak-k", Operation.DESTROY, 60L));
    tempQueue.add(mockShadowEvent("temp-recreate-k", Operation.UPDATE, 80L));

    Region.Entry<Object, Object> recreateEntry = mock(Region.Entry.class);
    when(recreateEntry.getValue()).thenReturn(new float[] {1f, 0f});
    Region.Entry<Object, Object> tempRecreateEntry = mock(Region.Entry.class);
    when(tempRecreateEntry.getValue()).thenReturn(new float[] {0f, 1f});

    BucketRegion bucket = mock(BucketRegion.class);
    when(bucket.isEmpty()).thenReturn(false);
    when(bucket.getEntry("recreate-k")).thenReturn(recreateEntry);
    when(bucket.getEntry("deleted-k")).thenReturn(null);
    when(bucket.getEntry("tiebreak-k")).thenReturn(null);
    when(bucket.getEntry("temp-recreate-k")).thenReturn(tempRecreateEntry);

    PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);
    when(dataStore.getLocalBucketById(bucketId)).thenReturn(bucket);
    PartitionedRegion pr = mock(PartitionedRegion.class);
    when(pr.getFullPath()).thenReturn(REGION_PATH);
    when(pr.getRegionService()).thenReturn(cache);
    when(pr.getDataStore()).thenReturn(dataStore);

    wireMockAeqShadowBucket(pr, bucketId, shadowEntries, tempQueue);

    Tier0VectorBucketListener<String, float[]> listener =
        new Tier0VectorBucketListener<>(INDEX_NAME, v -> v, v -> Map.of());
    PartitionListener partitionListener = listener.createPartitionListener();
    partitionListener.afterRegionCreate(pr);
    partitionListener.afterPrimary(bucketId);

    Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(cache);
    assertThat(registry.getAeqDeltaHydrationCount()).isEqualTo(1L);
    assertThat(registry.getFullScanHydrationCount()).isEqualTo(0L);
    assertThat(registry.getLastHydrationEventsInspected()).isEqualTo(9L);
    assertThat(registry.getLastHydrationKeysRestored()).isEqualTo(4L);
    assertThat(registry.getHydratedKeysCount()).isEqualTo(4L);
    assertThat(registry.getHydratedTombstonesCount()).isEqualTo(2L);

    Tier0VectorDeltaBuffer<Object> buffer = registry.getBuffer(REGION_PATH, INDEX_NAME, bucketId);
    Tier0VectorDeltaBuffer.Snapshot<Object> snap = buffer.snapshot();
    assertThat(snap.getLiveEntries().keySet())
        .containsExactlyInAnyOrder("recreate-k", "temp-recreate-k");
    assertThat(snap.getLiveEntries().get("recreate-k").getVector())
        .containsExactly(1f, 0f);
    assertThat(snap.getLiveEntries().get("temp-recreate-k").getVector())
        .containsExactly(0f, 1f);
    assertThat(snap.getTombstonedKeys())
        .containsExactlyInAnyOrder("deleted-k", "tiebreak-k");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void afterPrimaryFallbackReconcilesStaleIndexedKeysWithMissingAeqAndExpiredTombstones()
      throws Exception {
    int bucketId = 21;
    LuceneSerializer<float[]> serializer = (idx, vec) -> {
      Document doc = new Document();
      doc.add(new KnnFloatVectorField("embedding", vec, VectorSimilarityFunction.COSINE));
      return Collections.singletonList(doc);
    };

    try (ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(new StandardAnalyzer()))) {
      BucketRegion bucket = mock(BucketRegion.class);
      when(bucket.isEmpty()).thenReturn(false);
      when(bucket.keySet()).thenReturn(Collections.singleton("live-1"));
      Region.Entry<Object, Object> liveEntry = mock(Region.Entry.class);
      when(liveEntry.getValue()).thenReturn(new float[] {0f, 1f});
      when(bucket.getEntry("live-1")).thenReturn(liveEntry);
      when(bucket.getEntry("stale-expired-doc")).thenReturn(null);

      // Expired tombstone GC: RegionMap has 0 tombstones and 0 entries for stale-expired-doc.
      RegionMap regionMap = mock(RegionMap.class);
      when(regionMap.regionEntries()).thenReturn(Collections.emptyList());
      when(bucket.getRegionMap()).thenReturn(regionMap);

      PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);
      when(dataStore.getLocalBucketById(bucketId)).thenReturn(bucket);
      PartitionedRegion pr = mock(PartitionedRegion.class);
      when(pr.getFullPath()).thenReturn(REGION_PATH);
      when(pr.getRegionService()).thenReturn(cache);
      when(pr.getDataStore()).thenReturn(dataStore);
      when(pr.isDestroyed()).thenReturn(false);

      LuceneIndexImpl mockIndex = mock(LuceneIndexImpl.class);
      LuceneIndexStats mockStats = mock(LuceneIndexStats.class);
      IndexRepositoryImpl repo = new IndexRepositoryImpl(
          bucket, writer, serializer, mockStats, bucket, null, "lock", mockIndex);
      repo.update("live-1", new float[] {0f, 1f});
      repo.update("stale-expired-doc", new float[] {1f, 0f});
      repo.commitAndRefreshBlocking();
      assertThat(repo.collectIndexedKeys())
          .containsExactlyInAnyOrder("live-1", "stale-expired-doc");

      PartitionedRepositoryManager mockRepoManager = mock(PartitionedRepositoryManager.class);
      when(mockRepoManager.getRepositoryIfReady(bucketId)).thenReturn(repo);
      InternalLuceneService mockService = mock(InternalLuceneService.class);
      when(mockService.getIndex(INDEX_NAME, REGION_PATH)).thenReturn(mockIndex);
      when(mockIndex.getRepositoryManager()).thenReturn(mockRepoManager);
      when(cache.getService(InternalLuceneService.class)).thenReturn(mockService);

      // No AEQ registered on cache -> forces fallback full-scan path + Lucene reconciliation
      Tier0VectorBucketListener<String, float[]> listener =
          new Tier0VectorBucketListener<>(INDEX_NAME, v -> v, v -> Map.of());
      PartitionListener partitionListener = listener.createPartitionListener();
      partitionListener.afterRegionCreate(pr);
      partitionListener.afterPrimary(bucketId);

      Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(cache);
      assertThat(registry.getFullScanHydrationCount()).isEqualTo(1L);
      assertThat(registry.getAeqDeltaHydrationCount()).isEqualTo(0L);
      assertThat(registry.getReconciledStaleIndexKeysCount()).isEqualTo(1L);
      assertThat(registry.getHydratedTombstonesCount()).isEqualTo(1L);
      assertThat(registry.getHydratedKeysCount()).isEqualTo(2L);

      Tier0VectorDeltaBuffer<Object> buffer = registry.getBuffer(REGION_PATH, INDEX_NAME, bucketId);
      Tier0VectorDeltaBuffer.Snapshot<Object> snap = buffer.snapshot();
      assertThat(snap.getLiveEntries()).containsOnlyKeys("live-1");
      assertThat(snap.getTombstonedKeys()).containsExactly("stale-expired-doc");
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void afterPrimaryFallbackReconcilesOverwritesUnindexedInsertsAndDeleteRecreates()
      throws Exception {
    int bucketId = 22;
    LuceneSerializer<float[]> serializer = (idx, vec) -> {
      Document doc = new Document();
      doc.add(new KnnFloatVectorField("embedding", vec, VectorSimilarityFunction.COSINE));
      return Collections.singletonList(doc);
    };

    try (ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(new StandardAnalyzer()))) {
      BucketRegion bucket = mock(BucketRegion.class);
      when(bucket.isEmpty()).thenReturn(false);
      when(bucket.keySet())
          .thenReturn(Set.of("overwrite-key", "unindexed-insert", "del-recreate-key"));

      float[] overwriteV2 = new float[] {0.98f, 0.2f};
      float[] unindexedV1 = new float[] {0.97f, 0.24f};
      float[] delRecreateV3 = new float[] {0.96f, 0.28f};

      Region.Entry<Object, Object> overwriteEntry = mock(Region.Entry.class);
      when(overwriteEntry.getValue()).thenReturn(overwriteV2);
      Region.Entry<Object, Object> insertEntry = mock(Region.Entry.class);
      when(insertEntry.getValue()).thenReturn(unindexedV1);
      Region.Entry<Object, Object> recreateEntry = mock(Region.Entry.class);
      when(recreateEntry.getValue()).thenReturn(delRecreateV3);

      when(bucket.getEntry("overwrite-key")).thenReturn(overwriteEntry);
      when(bucket.getEntry("unindexed-insert")).thenReturn(insertEntry);
      when(bucket.getEntry("del-recreate-key")).thenReturn(recreateEntry);
      when(bucket.getEntry("expired-tombstone")).thenReturn(null);

      RegionMap regionMap = mock(RegionMap.class);
      when(regionMap.regionEntries()).thenReturn(Collections.emptyList());
      when(bucket.getRegionMap()).thenReturn(regionMap);

      PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);
      when(dataStore.getLocalBucketById(bucketId)).thenReturn(bucket);
      PartitionedRegion pr = mock(PartitionedRegion.class);
      when(pr.getFullPath()).thenReturn(REGION_PATH);
      when(pr.getRegionService()).thenReturn(cache);
      when(pr.getDataStore()).thenReturn(dataStore);
      when(pr.isDestroyed()).thenReturn(false);

      // Lucene holds stale v1 for overwrite-key, stale v1 for del-recreate-key, and stale
      // expired-tombstone; unindexed-insert is absent from Lucene completely.
      LuceneIndexImpl mockIndex = mock(LuceneIndexImpl.class);
      LuceneIndexStats mockStats = mock(LuceneIndexStats.class);
      IndexRepositoryImpl repo = new IndexRepositoryImpl(
          bucket, writer, serializer, mockStats, bucket, null, "lock", mockIndex);
      repo.update("overwrite-key", new float[] {0f, 1f});
      repo.update("del-recreate-key", new float[] {0.1f, 0.99f});
      repo.update("expired-tombstone", new float[] {1f, 0f});
      repo.commitAndRefreshBlocking();

      PartitionedRepositoryManager mockRepoManager = mock(PartitionedRepositoryManager.class);
      when(mockRepoManager.getRepositoryIfReady(bucketId)).thenReturn(repo);
      InternalLuceneService mockService = mock(InternalLuceneService.class);
      when(mockService.getIndex(INDEX_NAME, REGION_PATH)).thenReturn(mockIndex);
      when(mockIndex.getRepositoryManager()).thenReturn(mockRepoManager);
      when(cache.getService(InternalLuceneService.class)).thenReturn(mockService);

      Tier0VectorBucketListener<String, float[]> listener =
          new Tier0VectorBucketListener<>(INDEX_NAME, v -> v, v -> Map.of());
      PartitionListener partitionListener = listener.createPartitionListener();
      partitionListener.afterRegionCreate(pr);
      partitionListener.afterPrimary(bucketId);

      Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(cache);
      assertThat(registry.getFullScanHydrationCount()).isEqualTo(1L);
      assertThat(registry.getAeqDeltaHydrationCount()).isEqualTo(0L);
      assertThat(registry.getReconciledStaleIndexKeysCount()).isEqualTo(1L);
      assertThat(registry.getHydratedTombstonesCount()).isEqualTo(1L);
      assertThat(registry.getHydratedKeysCount()).isEqualTo(4L);

      Tier0VectorDeltaBuffer<Object> buffer = registry.getBuffer(REGION_PATH, INDEX_NAME, bucketId);
      Tier0VectorDeltaBuffer.Snapshot<Object> snap = buffer.snapshot();
      assertThat(snap.getLiveEntries())
          .containsOnlyKeys("overwrite-key", "unindexed-insert", "del-recreate-key");
      assertThat(snap.getLiveEntries().get("overwrite-key").getVector())
          .containsExactly(overwriteV2);
      assertThat(snap.getLiveEntries().get("unindexed-insert").getVector())
          .containsExactly(unindexedV1);
      assertThat(snap.getLiveEntries().get("del-recreate-key").getVector())
          .containsExactly(delRecreateV3);
      assertThat(snap.getTombstonedKeys()).containsExactly("expired-tombstone");
      assertThat(snap.getMaskedKeys()).containsExactlyInAnyOrder(
          "overwrite-key", "unindexed-insert", "del-recreate-key", "expired-tombstone");
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void luceneEventListenerRecordsTrueFlushCycleSamplesAndRespectsDisabledFlag()
      throws Exception {
    int bucketId = 0;
    Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(cache);
    registry.recordPut(REGION_PATH, INDEX_NAME, bucketId, "k0", new float[] {1f, 0f}, Map.of());
    registry.recordPut(REGION_PATH, INDEX_NAME, bucketId, "k1", new float[] {0f, 1f}, Map.of());
    registry.recordPut(REGION_PATH, INDEX_NAME, bucketId, "k2", new float[] {0.5f, 0.5f}, Map.of());

    BucketRegion bucket = mock(BucketRegion.class);
    when(bucket.getId()).thenReturn(bucketId);
    EntrySnapshot es0 = mock(EntrySnapshot.class);
    when(es0.getRawValue(true)).thenReturn(new float[] {1f, 0f});
    EntrySnapshot es1 = mock(EntrySnapshot.class);
    when(es1.getRawValue(true)).thenReturn(new float[] {0f, 1f});
    EntrySnapshot es2 = mock(EntrySnapshot.class);
    when(es2.getRawValue(true)).thenReturn(new float[] {0.5f, 0.5f});

    Region<Object, Object> region = mock(Region.class);
    when(region.getEntry("k0")).thenReturn(es0);
    when(region.getEntry("k1")).thenReturn(es1);
    when(region.getEntry("k2")).thenReturn(es2);

    IndexRepository repo = mock(IndexRepository.class);
    when(repo.getRegion()).thenReturn(bucket);

    LuceneIndexImpl mockIndex = mock(LuceneIndexImpl.class);
    when(mockIndex.getRegionPath()).thenReturn(REGION_PATH);
    when(mockIndex.getName()).thenReturn(INDEX_NAME);

    PartitionedRepositoryManager repoManager = mock(PartitionedRepositoryManager.class);
    when(repoManager.getIndex()).thenReturn(mockIndex);
    when(repoManager.getRepository(any(), any(), any())).thenReturn(repo);

    AsyncEvent e0 = mock(AsyncEvent.class);
    when(e0.getRegion()).thenReturn(region);
    when(e0.getKey()).thenReturn("k0");
    AsyncEvent e1 = mock(AsyncEvent.class);
    when(e1.getRegion()).thenReturn(region);
    when(e1.getKey()).thenReturn("k1");
    AsyncEvent e2 = mock(AsyncEvent.class);
    when(e2.getRegion()).thenReturn(region);
    when(e2.getKey()).thenReturn("k2");

    LuceneEventListener aeqListener = new LuceneEventListener(cache, repoManager);
    assertThat(aeqListener.processEvents(List.of(e0, e1))).isTrue();

    // k2 remains unevicted in Tier-0 after flushing batch [k0, k1]
    assertThat(registry.getBuffer(REGION_PATH, INDEX_NAME, bucketId).getTotalSize()).isEqualTo(1);
    List<Tier0VectorRegistry.FlushCycleSample> samples =
        registry.drainFlushCycleSamples(REGION_PATH, INDEX_NAME, bucketId);
    assertThat(samples).hasSize(1);
    Tier0VectorRegistry.FlushCycleSample sample = samples.get(0);
    assertThat(sample.getBucketId()).isEqualTo(bucketId);
    assertThat(sample.getBatchSize()).isEqualTo(2);
    assertThat(sample.getPreEvictTier0Size()).isEqualTo(3);
    assertThat(sample.getFlushDurationNanos()).isGreaterThanOrEqualTo(0L);
    // Second drain is empty
    assertThat(registry.drainFlushCycleSamples(REGION_PATH, INDEX_NAME)).isEmpty();

    // Disabling sampling skips sample recording with zero allocations
    registry.setFlushCycleSamplingEnabled(false);
    assertThat(aeqListener.processEvents(List.of(e2))).isTrue();
    assertThat(registry.getBuffer(REGION_PATH, INDEX_NAME, bucketId).getTotalSize()).isEqualTo(0);
    assertThat(registry.drainFlushCycleSamples(REGION_PATH, INDEX_NAME, bucketId)).isEmpty();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void pathAFlushDoesNotEvictNewerOverlayPutWrittenAfterPayloadRead() throws Exception {
    final int bucketId = 24;
    Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(cache);
    float[] k1v1 = new float[] {1f, 0f};
    float[] k1v2 = new float[] {0.6f, 0.8f};
    float[] k2v1 = new float[] {0f, 1f};
    long seqK1V1 = registry.recordPut(REGION_PATH, INDEX_NAME, bucketId, "k1", k1v1, Map.of());
    registry.recordPut(REGION_PATH, INDEX_NAME, bucketId, "k2", k2v1, Map.of());
    Tier0VectorDeltaBuffer<Object> buffer = registry.getBuffer(REGION_PATH, INDEX_NAME, bucketId);

    BucketRegion bucket = mock(BucketRegion.class);
    when(bucket.getId()).thenReturn(bucketId);

    long[] concurrentSeqK1V2 = new long[] {-1L};
    long[] concurrentDestroySeqK2 = new long[] {-1L};
    EntrySnapshot es1 = mock(EntrySnapshot.class);
    when(es1.getRawValue(true)).thenAnswer(inv -> {
      // Simulate a concurrent post-read put arriving right after Path-A read payload v1
      concurrentSeqK1V2[0] =
          registry.recordPut(REGION_PATH, INDEX_NAME, bucketId, "k1", k1v2, Map.of());
      return k1v1;
    });
    EntrySnapshot es2 = mock(EntrySnapshot.class);
    when(es2.getRawValue(true)).thenAnswer(inv -> {
      // Simulate a concurrent destroy arriving right after Path-A read payload v1 for k2
      concurrentDestroySeqK2[0] =
          registry.recordDestroy(REGION_PATH, INDEX_NAME, bucketId, "k2");
      return k2v1;
    });

    Region<Object, Object> region = mock(Region.class);
    when(region.getEntry("k1")).thenReturn(es1);
    when(region.getEntry("k2")).thenReturn(es2);

    IndexRepository repo = mock(IndexRepository.class);
    when(repo.getRegion()).thenReturn(bucket);
    LuceneIndexImpl mockIndex = mock(LuceneIndexImpl.class);
    when(mockIndex.getRegionPath()).thenReturn(REGION_PATH);
    when(mockIndex.getName()).thenReturn(INDEX_NAME);
    PartitionedRepositoryManager repoManager = mock(PartitionedRepositoryManager.class);
    when(repoManager.getIndex()).thenReturn(mockIndex);
    when(repoManager.getRepository(any(), any(), any())).thenReturn(repo);

    AsyncEvent e1 = mock(AsyncEvent.class);
    when(e1.getRegion()).thenReturn(region);
    when(e1.getKey()).thenReturn("k1");
    AsyncEvent e2 = mock(AsyncEvent.class);
    when(e2.getRegion()).thenReturn(region);
    when(e2.getKey()).thenReturn("k2");

    LuceneEventListener aeqListener = new LuceneEventListener(cache, repoManager);
    assertThat(aeqListener.processEvents(List.of(e1, e2))).isTrue();

    assertThat(concurrentSeqK1V2[0]).isGreaterThan(seqK1V1);
    assertThat(concurrentDestroySeqK2[0]).isGreaterThan(seqK1V1);
    org.mockito.Mockito.verify(repo).update("k1", k1v1);
    org.mockito.Mockito.verify(repo).update("k2", k2v1);
    org.mockito.Mockito.verify(repo).commitAndRefreshBlocking();

    // Flush #1 installed v1 payloads, so evictCommitted MUST preserve k1=v2 and k2 tombstone
    Tier0VectorDeltaBuffer.Snapshot<Object> snapAfterFirstFlush = buffer.snapshot();
    assertThat(snapAfterFirstFlush.getLiveEntries()).containsOnlyKeys("k1");
    assertThat(snapAfterFirstFlush.getLiveEntries().get("k1").getVector()).containsExactly(k1v2);
    assertThat(snapAfterFirstFlush.getLiveEntries().get("k1").getSequence())
        .isEqualTo(concurrentSeqK1V2[0]);
    assertThat(snapAfterFirstFlush.getTombstonedKeys()).containsExactly("k2");

    // Subsequent batch seeing v2 for k1 and tombstone (null) for k2 drains Tier-0 cleanly to 0
    when(es1.getRawValue(true)).thenReturn(k1v2);
    when(region.getEntry("k2")).thenReturn(null);
    assertThat(aeqListener.processEvents(List.of(e1, e2))).isTrue();
    assertThat(buffer.getTotalSize()).isEqualTo(0);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void afterPrimaryAuthoritativeBucketOverwritesConflictingAeqAndPreservesPostPromotionLiveWrites()
      throws Exception {
    final int bucketId = 25;
    float[] v1Stale = new float[] {0.2f, 0.98f};
    float[] v2Authoritative = new float[] {0.98f, 0.2f};
    float[] v3PostPromo = new float[] {0.99f, 0.14f};

    LuceneSerializer<float[]> serializer = (idx, vec) -> {
      Document doc = new Document();
      doc.add(new KnnFloatVectorField("embedding", vec, VectorSimilarityFunction.COSINE));
      return Collections.singletonList(doc);
    };

    try (ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(new StandardAnalyzer()))) {
      BucketRegion bucket = mock(BucketRegion.class);
      when(bucket.isEmpty()).thenReturn(false);
      when(bucket.keySet()).thenReturn(Set.of(
          "aeq-destroy-bucket-live",
          "aeq-v1-bucket-v2",
          "post-promo-live-race",
          "post-promo-before-afterprimary"));

      Region.Entry<Object, Object> liveV1Entry = mock(Region.Entry.class);
      when(liveV1Entry.getValue()).thenReturn(v1Stale);
      Region.Entry<Object, Object> liveV2Entry1 = mock(Region.Entry.class);
      when(liveV2Entry1.getValue()).thenReturn(v2Authoritative);
      Region.Entry<Object, Object> liveV2Entry2 = mock(Region.Entry.class);
      when(liveV2Entry2.getValue()).thenReturn(v2Authoritative);
      Region.Entry<Object, Object> liveRaceEntry = mock(Region.Entry.class);
      when(liveRaceEntry.getValue()).thenReturn(v1Stale);

      when(bucket.getEntry("aeq-destroy-bucket-live")).thenReturn(liveV2Entry1);
      // tryHydrateFromAeqDelta reads v1Stale (2 getEntry calls), then hydrateFromFullBucketScan
      // reads
      // authoritative v2Authoritative.
      when(bucket.getEntry("aeq-v1-bucket-v2"))
          .thenReturn(liveV1Entry, liveV1Entry, liveV2Entry2);
      // tryHydrateFromAeqDelta reads v1Stale, then bucket becomes tombstone (null) before full
      // scan.
      when(bucket.getEntry("aeq-put-bucket-tomb")).thenReturn(liveV1Entry, liveV1Entry, null);
      when(bucket.getEntry("aeq-put-stale-lucene")).thenReturn(liveV1Entry, liveV1Entry, null);
      when(bucket.getEntry("post-promo-live-race")).thenReturn(liveRaceEntry);
      when(bucket.getEntry("post-promo-before-afterprimary")).thenReturn(liveRaceEntry);
      when(bucket.getEntry("post-promo-destroy-race")).thenReturn(null);

      RegionEntry tombEntry = mock(RegionEntry.class);
      when(tombEntry.isTombstone()).thenReturn(true);
      when(tombEntry.getKey()).thenReturn("aeq-put-bucket-tomb");
      RegionEntry tombPostRaceEntry = mock(RegionEntry.class);
      when(tombPostRaceEntry.isTombstone()).thenReturn(true);
      when(tombPostRaceEntry.getKey()).thenReturn("post-promo-destroy-race");
      RegionMap regionMap = mock(RegionMap.class);
      when(regionMap.regionEntries()).thenReturn(List.of(tombEntry, tombPostRaceEntry));
      when(bucket.getRegionMap()).thenReturn(regionMap);

      PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);
      when(dataStore.getLocalBucketById(bucketId)).thenReturn(bucket);
      PartitionedRegion pr = mock(PartitionedRegion.class);
      when(pr.getFullPath()).thenReturn(REGION_PATH);
      when(pr.getRegionService()).thenReturn(cache);
      when(pr.getDataStore()).thenReturn(dataStore);
      when(pr.isDestroyed()).thenReturn(false);

      // AEQ shadow contains stale DESTROY for aeq-destroy-bucket-live and stale CREATE/UPDATE
      // events for aeq-v1-bucket-v2, aeq-put-bucket-tomb, and aeq-put-stale-lucene.
      Map<Long, GatewaySenderEventImpl> shadowEntries = new HashMap<>();
      shadowEntries.put(1L, mockShadowEvent("aeq-destroy-bucket-live", Operation.DESTROY, 1L));
      shadowEntries.put(2L, mockShadowEvent("aeq-v1-bucket-v2", Operation.CREATE, 2L));
      shadowEntries.put(3L, mockShadowEvent("aeq-put-bucket-tomb", Operation.UPDATE, 3L));
      shadowEntries.put(4L, mockShadowEvent("aeq-put-stale-lucene", Operation.CREATE, 4L));
      wireMockAeqShadowBucket(pr, bucketId, shadowEntries);

      LuceneIndexImpl mockIndex = mock(LuceneIndexImpl.class);
      LuceneIndexStats mockStats = mock(LuceneIndexStats.class);
      IndexRepositoryImpl repo = new IndexRepositoryImpl(
          bucket, writer, serializer, mockStats, bucket, null, "lock", mockIndex);
      repo.update("aeq-put-stale-lucene", v1Stale);
      repo.commitAndRefreshBlocking();
      PartitionedRepositoryManager mockRepoManager = mock(PartitionedRepositoryManager.class);
      when(mockRepoManager.getRepositoryIfReady(bucketId)).thenReturn(repo);
      InternalLuceneService mockService = mock(InternalLuceneService.class);
      when(mockService.getIndex(INDEX_NAME, REGION_PATH)).thenReturn(mockIndex);
      when(mockIndex.getRepositoryManager()).thenReturn(mockRepoManager);
      when(cache.getService(InternalLuceneService.class)).thenReturn(mockService);

      Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(cache);
      // Live write landing ahead of afterPrimary MUST never be overwritten by bucket hydration
      registry.recordPut(REGION_PATH, INDEX_NAME, bucketId, "post-promo-before-afterprimary",
          v3PostPromo, Map.of());

      Tier0VectorBucketListener<String, float[]> listener =
          new Tier0VectorBucketListener<>(INDEX_NAME, v -> v, v -> Map.of());
      @SuppressWarnings("rawtypes")
      Tier0VectorBucketListener.Tier0PartitionListener partitionListener =
          (Tier0VectorBucketListener.Tier0PartitionListener) listener.createPartitionListener();
      partitionListener.afterRegionCreate(pr);

      Tier0VectorDeltaBuffer<Object> preBuffer =
          registry.getBuffer(REGION_PATH, INDEX_NAME, bucketId);
      partitionListener.afterPrimary(bucketId, () -> {
        // Right after tryHydrateFromAeqDelta, AEQ placed a provisional tombstone for
        // aeq-destroy-bucket-live and provisional stale v1 entries for the remaining AEQ keys.
        Tier0VectorDeltaBuffer.Snapshot<Object> midSnap = preBuffer.snapshot();
        assertThat(midSnap.getTombstonedKeys()).contains("aeq-destroy-bucket-live");
        assertThat(midSnap.getLiveEntries().get("aeq-v1-bucket-v2").getVector())
            .containsExactly(v1Stale);
        assertThat(midSnap.getLiveEntries().get("aeq-put-bucket-tomb").getVector())
            .containsExactly(v1Stale);
        assertThat(midSnap.getLiveEntries().get("aeq-put-stale-lucene").getVector())
            .containsExactly(v1Stale);

        // Concurrent live writes arriving during promotion MUST survive hydrateFromFullBucketScan
        registry.recordPut(
            REGION_PATH, INDEX_NAME, bucketId, "post-promo-live-race", v3PostPromo, Map.of());
        registry.recordPut(
            REGION_PATH, INDEX_NAME, bucketId, "post-promo-destroy-race", v3PostPromo, Map.of());
      });

      assertThat(registry.getAeqDeltaHydrationCount()).isEqualTo(1L);
      assertThat(registry.getFullScanHydrationCount()).isEqualTo(0L);
      assertThat(registry.getReconciledStaleIndexKeysCount()).isEqualTo(1L);

      Tier0VectorDeltaBuffer.Snapshot<Object> snap = preBuffer.snapshot();
      assertThat(snap.getLiveEntries()).containsOnlyKeys(
          "aeq-destroy-bucket-live",
          "aeq-v1-bucket-v2",
          "post-promo-before-afterprimary",
          "post-promo-live-race",
          "post-promo-destroy-race");
      assertThat(snap.getLiveEntries().get("aeq-destroy-bucket-live").getVector())
          .containsExactly(v2Authoritative);
      assertThat(snap.getLiveEntries().get("aeq-v1-bucket-v2").getVector())
          .containsExactly(v2Authoritative);
      assertThat(snap.getLiveEntries().get("post-promo-before-afterprimary").getVector())
          .containsExactly(v3PostPromo);
      assertThat(snap.getLiveEntries().get("post-promo-live-race").getVector())
          .containsExactly(v3PostPromo);
      assertThat(snap.getLiveEntries().get("post-promo-destroy-race").getVector())
          .containsExactly(v3PostPromo);
      assertThat(snap.getTombstonedKeys())
          .containsExactlyInAnyOrder("aeq-put-bucket-tomb", "aeq-put-stale-lucene");
    }
  }

  private GatewaySenderEventImpl mockShadowEvent(String key, Operation op, long shadowKey) {
    GatewaySenderEventImpl event = mock(GatewaySenderEventImpl.class);
    when(event.getKey()).thenReturn(key);
    when(event.getOperation()).thenReturn(op);
    when(event.getRegionPath()).thenReturn(REGION_PATH);
    when(event.getShadowKey()).thenReturn(shadowKey);
    return event;
  }

  private void wireMockAeqShadowBucket(PartitionedRegion pr, int bucketId,
      Map<Long, GatewaySenderEventImpl> shadowEntries) {
    wireMockAeqShadowBucket(pr, bucketId, shadowEntries, new LinkedBlockingQueue<>());
  }

  @SuppressWarnings("unchecked")
  private void wireMockAeqShadowBucket(PartitionedRegion pr, int bucketId,
      Map<Long, GatewaySenderEventImpl> shadowEntries,
      LinkedBlockingQueue<GatewaySenderEventImpl> tempQueue) {
    BucketRegion shadowBucket = mock(BucketRegion.class);
    when(shadowBucket.isDestroyed()).thenReturn(false);
    when(shadowBucket.keySet()).thenReturn(new LinkedHashSet<>(shadowEntries.keySet()));
    when(shadowBucket.getValueInVMOrDiskWithoutFaultIn(any())).thenAnswer(inv -> {
      Object sk = inv.getArgument(0);
      return shadowEntries.get(sk);
    });

    PartitionedRegionDataStore shadowStore = mock(PartitionedRegionDataStore.class);
    when(shadowStore.getLocalBucketById(bucketId)).thenReturn(shadowBucket);
    PartitionedRegion shadowPr = mock(PartitionedRegion.class);
    when(shadowPr.isDestroyed()).thenReturn(false);
    when(shadowPr.getDataStore()).thenReturn(shadowStore);

    ParallelGatewaySenderQueue parallelQueue = mock(ParallelGatewaySenderQueue.class);
    when(parallelQueue.getRegion(REGION_PATH)).thenReturn(shadowPr);
    when(parallelQueue.getBucketToTempQueueMap()).thenReturn(Map.of(bucketId, tempQueue));

    ConcurrentParallelGatewaySenderQueue concurrentQueue =
        mock(ConcurrentParallelGatewaySenderQueue.class);
    when(concurrentQueue.getQueueByBucket(bucketId)).thenReturn(parallelQueue);
    when(concurrentQueue.getRegion(REGION_PATH)).thenReturn(shadowPr);

    AbstractGatewaySenderEventProcessor processor = mock(AbstractGatewaySenderEventProcessor.class);
    when(processor.getQueue()).thenReturn(concurrentQueue);
    InternalGatewaySender sender = mock(InternalGatewaySender.class);
    when(sender.getEventProcessor()).thenReturn(processor);
    AsyncEventQueueImpl aeq = mock(AsyncEventQueueImpl.class);
    when(aeq.getSender()).thenReturn(sender);

    String aeqId = LuceneServiceImpl.getUniqueIndexName(INDEX_NAME, REGION_PATH);
    when(cache.getAsyncEventQueue(aeqId)).thenReturn(aeq);
  }
}
