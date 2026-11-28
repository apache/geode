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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene912.Lucene912Codec;
import org.apache.lucene.codecs.lucene99.Lucene99HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.lucene.LuceneIndexNotFoundException;
import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.cache.lucene.internal.InternalLuceneService;
import org.apache.geode.cache.lucene.internal.LuceneEventListener;
import org.apache.geode.cache.lucene.internal.LuceneIndexImpl;
import org.apache.geode.cache.lucene.internal.LuceneIndexStats;
import org.apache.geode.cache.lucene.internal.PartitionedRepositoryManager;
import org.apache.geode.cache.lucene.internal.repository.IndexRepository;
import org.apache.geode.cache.lucene.internal.repository.IndexRepositoryImpl;
import org.apache.geode.cache.lucene.internal.repository.IndexResultCollector;
import org.apache.geode.cache.lucene.internal.vector.Tier0VectorBucketListener;
import org.apache.geode.cache.lucene.internal.vector.Tier0VectorDeltaBuffer;
import org.apache.geode.cache.lucene.internal.vector.Tier0VectorRegistry;
import org.apache.geode.cache.lucene.internal.vector.VectorSimilarityScorer;
import org.apache.geode.cache.partition.PartitionListener;
import org.apache.geode.internal.cache.BucketAdvisor;
import org.apache.geode.internal.cache.BucketNotFoundException;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.cache.PrimaryBucketException;
import org.apache.geode.internal.cache.execute.InternalFunctionInvocationTargetException;
import org.apache.geode.internal.cache.execute.InternalRegionFunctionContext;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class DistributedHybridKnnFunctionJUnitTest {

  private static final String REGION_PATH = SEPARATOR + "vectors";
  private static final String INDEX_NAME = "vecIndex";
  private static final String VECTOR_FIELD = "embedding";

  private InternalCache mockCache;
  private InternalRegionFunctionContext mockContext;
  private ResultSender<TopEntriesCollector> mockResultSender;
  private Region<Object, Object> mockRegion;
  private InternalLuceneService mockService;
  private LuceneIndexImpl mockIndex;
  private LuceneIndexStats mockStats;
  private PartitionedRepositoryManager mockRepoManager;
  private IndexRepository mockRepoBucket0;
  private IndexRepository mockRepoBucket1;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    mockCache = mock(InternalCache.class);
    CancelCriterion cancelCriterion = mock(CancelCriterion.class);
    when(mockCache.getCancelCriterion()).thenReturn(cancelCriterion);

    mockContext = mock(InternalRegionFunctionContext.class);
    mockResultSender = mock(ResultSender.class);
    mockRegion = mock(Region.class);
    mockService = mock(InternalLuceneService.class);
    mockIndex = mock(LuceneIndexImpl.class);
    mockStats = mock(LuceneIndexStats.class);
    mockRepoManager = mock(PartitionedRepositoryManager.class);
    mockRepoBucket0 = mock(IndexRepository.class);
    mockRepoBucket1 = mock(IndexRepository.class);
    wireRealDefaultQueryMethods(mockRepoBucket0);
    wireRealDefaultQueryMethods(mockRepoBucket1);

    when(mockContext.getCache()).thenReturn(mockCache);
    when(mockContext.getDataSet()).thenReturn(mockRegion);
    when(mockContext.getResultSender()).thenReturn(mockResultSender);
    when(mockRegion.getFullPath()).thenReturn(REGION_PATH);
    when(mockCache.getService(InternalLuceneService.class)).thenReturn(mockService);
    when(mockService.getIndex(INDEX_NAME, REGION_PATH)).thenReturn(mockIndex);
    when(mockIndex.getRepositoryManager()).thenReturn(mockRepoManager);
    when(mockIndex.getIndexStats()).thenReturn(mockStats);
    when(mockIndex.getAnalyzer()).thenReturn(new StandardAnalyzer());
  }

  private static void wireRealDefaultQueryMethods(IndexRepository mockRepo) throws IOException {
    doCallRealMethod().when(mockRepo).query(any(), anyInt(), any(), nullable(Runnable.class));
    doCallRealMethod().when(mockRepo).queryWithFp32Rerank(
        any(), anyInt(), anyInt(), any(), any(), any(), any(), nullable(Runnable.class));
    when(mockRepo.tryQueryAtEpoch(anyLong(), any(), anyInt(), any())).thenCallRealMethod();
    when(mockRepo.tryQueryAtEpoch(anyLong(), any(), anyInt(), any(), nullable(Runnable.class)))
        .thenCallRealMethod();
    when(mockRepo.tryQueryWithFp32RerankAtEpoch(
        anyLong(), any(), anyInt(), anyInt(), any(), any(), any(), any())).thenCallRealMethod();
    when(mockRepo.tryQueryWithFp32RerankAtEpoch(anyLong(), any(), anyInt(), anyInt(), any(), any(),
        any(), any(), nullable(Runnable.class))).thenCallRealMethod();
  }

  @After
  public void tearDown() {
    Tier0VectorRegistry.removeCache(mockCache);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void executesMultiBucketHybridQueryUnionsTier0AndMasksTombstones() throws Exception {
    Map<Integer, IndexRepository> reposByBucket = new LinkedHashMap<>();
    reposByBucket.put(0, mockRepoBucket0);
    reposByBucket.put(1, mockRepoBucket1);
    when(mockRepoManager.getRepositoriesByBucket(eq(mockContext), eq(false)))
        .thenReturn(reposByBucket);

    // Lucene bucket 0 has k_tomb (highest stale score), k_sup (superseded), and k_l0
    doAnswer(inv -> {
      IndexResultCollector c = inv.getArgument(2);
      c.collect("k_tomb", 0.99f);
      c.collect("k_sup", 0.95f);
      c.collect("k_l0", 0.80f);
      return null;
    }).when(mockRepoBucket0).query(any(Query.class), anyInt(), any(IndexResultCollector.class));

    // Lucene bucket 1 has k_l1
    doAnswer(inv -> {
      IndexResultCollector c = inv.getArgument(2);
      c.collect("k_l1", 0.88f);
      return null;
    }).when(mockRepoBucket1).query(any(Query.class), anyInt(), any(IndexResultCollector.class));

    // Register Tier-0 mutations on bucket 0 and bucket 1
    Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(mockCache);
    registry.recordDestroy(REGION_PATH, INDEX_NAME, 0, "k_tomb");
    // Supersede k_sup with orthogonal vector (score 0.5 under Lucene COSINE = (1+0)/2)
    registry.recordPut(REGION_PATH, INDEX_NAME, 0, "k_sup", new float[] {0f, 1f, 0f, 0f},
        Map.of("cat", "tech"));
    // Fresh Tier-0 match in bucket 1 with perfect cosine score (1.0)
    registry.recordPut(REGION_PATH, INDEX_NAME, 1, "k_t0_fresh", new float[] {1f, 0f, 0f, 0f},
        Map.of("cat", "tech"));
    // Filtered-out Tier-0 match in bucket 1 (wrong metadata category)
    registry.recordPut(REGION_PATH, INDEX_NAME, 1, "k_t0_wrong_cat", new float[] {1f, 0f, 0f, 0f},
        Map.of("cat", "sports"));

    HybridKnnQueryContext queryContext = HybridKnnQueryContext
        .builder(INDEX_NAME, VECTOR_FIELD, new float[] {1f, 0f, 0f, 0f}, 3)
        .similarityFunction(VectorSimilarityFunction.COSINE)
        .addMetadataFilter("cat", "tech")
        .build();
    when(mockContext.getArguments()).thenReturn(queryContext);

    DistributedHybridKnnFunction fn = new DistributedHybridKnnFunction();
    fn.execute(mockContext);

    // Verify KnnFloatVectorQuery was pushed into bucket 0 with user filter + MUST_NOT masks
    ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
    verify(mockRepoBucket0).query(queryCaptor.capture(), eq(3), any(IndexResultCollector.class));
    assertThat(queryCaptor.getValue()).isInstanceOf(KnnFloatVectorQuery.class);
    KnnFloatVectorQuery capturedKnn = (KnnFloatVectorQuery) queryCaptor.getValue();
    assertThat(capturedKnn.getField()).isEqualTo(VECTOR_FIELD);
    assertThat(capturedKnn.getK()).isEqualTo(3);
    assertThat(capturedKnn.getFilter()).isInstanceOf(BooleanQuery.class);

    // Verify merged top-3 result order: k_t0_fresh (1.0), k_l1 (0.88), k_l0 (0.80)
    // k_tomb is masked out; k_sup was superseded to 0.5 and pushed out of top-3; k_t0_wrong_cat
    // filtered
    ArgumentCaptor<TopEntriesCollector> resultCaptor =
        ArgumentCaptor.forClass(TopEntriesCollector.class);
    verify(mockResultSender).lastResult(resultCaptor.capture());

    List<EntryScore> hits = resultCaptor.getValue().getEntries().getHits();
    assertThat(hits).hasSize(3);
    assertThat(hits.get(0).getKey()).isEqualTo("k_t0_fresh");
    assertThat(hits.get(1).getKey()).isEqualTo("k_l1");
    assertThat(hits.get(2).getKey()).isEqualTo("k_l0");
  }

  @Test
  public void fallsBackToOversamplingWhenMaskedKeysExceedInlineThreshold() throws Exception {
    Map<Integer, IndexRepository> reposByBucket = Map.of(0, mockRepoBucket0);
    when(mockRepoManager.getRepositoriesByBucket(eq(mockContext), eq(false)))
        .thenReturn(reposByBucket);

    Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(mockCache);
    int numMasked = DistributedHybridKnnFunction.MAX_INLINE_MASK_TERMS + 10;
    for (int i = 0; i < numMasked; i++) {
      registry.recordDestroy(REGION_PATH, INDEX_NAME, 0, "tomb-" + i);
    }

    HybridKnnQueryContext queryContext = HybridKnnQueryContext
        .builder(INDEX_NAME, VECTOR_FIELD, new float[] {1f, 0f}, 5)
        .build();
    when(mockContext.getArguments()).thenReturn(queryContext);

    new DistributedHybridKnnFunction().execute(mockContext);

    ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
    ArgumentCaptor<Integer> limitCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(mockRepoBucket0).query(queryCaptor.capture(), limitCaptor.capture(),
        any(IndexResultCollector.class));

    KnnFloatVectorQuery knn = (KnnFloatVectorQuery) queryCaptor.getValue();
    assertThat(knn.getK()).isEqualTo(5 + numMasked);
    assertThat(limitCaptor.getValue()).isEqualTo(5 + numMasked);
    assertThat(knn.getFilter()).isNull();
  }

  @Test
  public void throwsExpectedExceptionsOnMissingIndexOrBucketNotFound() throws Exception {
    HybridKnnQueryContext queryContext = HybridKnnQueryContext
        .builder(INDEX_NAME, VECTOR_FIELD, new float[] {1f, 0f}, 5)
        .build();
    when(mockContext.getArguments()).thenReturn(queryContext);

    // BucketNotFoundException wrapped in InternalFunctionInvocationTargetException for PR retry
    when(mockRepoManager.getRepositoriesByBucket(eq(mockContext), eq(false)))
        .thenThrow(new BucketNotFoundException("moved"));
    assertThatThrownBy(() -> new DistributedHybridKnnFunction().execute(mockContext))
        .isInstanceOf(InternalFunctionInvocationTargetException.class);

    // Missing index throws LuceneIndexNotFoundException
    when(mockService.getIndex(INDEX_NAME, REGION_PATH)).thenReturn(null);
    assertThatThrownBy(() -> new DistributedHybridKnnFunction().execute(mockContext))
        .isInstanceOf(LuceneIndexNotFoundException.class);
  }

  @Test
  public void tier0RegistryFlushBucketCallsCommitAndRefreshBlockingAndAbortsOnFailure()
      throws Exception {
    Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(mockCache);
    registry.recordPut(REGION_PATH, INDEX_NAME, 3, "k1", new float[] {1f, 0f}, Map.of());
    registry.recordDestroy(REGION_PATH, INDEX_NAME, 3, "k2");

    // Simulate failure during commitAndRefreshBlocking -> flush must abort and preserve entries
    doThrow(new IOException("simulated disk failure"))
        .when(mockRepoBucket0).commitAndRefreshBlocking();

    assertThatThrownBy(() -> registry.flushBucket(REGION_PATH, INDEX_NAME, 3, mockRepoBucket0,
        entry -> "val-" + entry.getKey())).isInstanceOf(IOException.class);

    Tier0VectorDeltaBuffer<Object> buffer = registry.getBuffer(REGION_PATH, INDEX_NAME, 3);
    assertThat(buffer.isFlushing()).isFalse();
    assertThat(buffer.getActiveSize()).isEqualTo(2);

    // Now succeed on retry
    IndexRepository healthyRepo = mock(IndexRepository.class);
    boolean flushed = registry.flushBucket(REGION_PATH, INDEX_NAME, 3, healthyRepo,
        entry -> "val-" + entry.getKey());
    assertThat(flushed).isTrue();
    verify(healthyRepo).update("k1", "val-k1");
    verify(healthyRepo).delete("k2");
    verify(healthyRepo).commitAndRefreshBlocking();
    assertThat(buffer.getTotalSize()).isEqualTo(0);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void unsupportedFuzzyFilterTriggersFlushAndRefreshFallbackReturningFreshTier0Entries()
      throws Exception {
    DistributedHybridKnnFunction.resetUnsupportedFilterFlushFallbackCount();

    // Real in-memory Lucene IndexRepository so FuzzyQuery + KnnFloatVectorQuery execute end-to-end
    try (ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(new StandardAnalyzer()));
        SearcherManager searcherManager = new SearcherManager(writer, true, true, null)) {

      IndexRepository realRepo = new IndexRepository() {
        @Override
        public void create(Object key, Object value) throws IOException {
          update(key, value);
        }

        @Override
        public void update(Object key, Object value) throws IOException {
          Object[] pair = (Object[]) value;
          Document doc = new Document();
          org.apache.geode.cache.lucene.internal.repository.serializer.SerializerUtil.addKey(key,
              doc);
          doc.add(new KnnFloatVectorField(VECTOR_FIELD, (float[]) pair[0],
              VectorSimilarityFunction.COSINE));
          doc.add(new StringField("category", (String) pair[1], Field.Store.NO));
          writer.updateDocument(
              org.apache.geode.cache.lucene.internal.repository.serializer.SerializerUtil
                  .toKeyTerm(key),
              doc);
        }

        @Override
        public void delete(Object key) throws IOException {
          writer.deleteDocuments(
              org.apache.geode.cache.lucene.internal.repository.serializer.SerializerUtil
                  .toKeyTerm(key));
        }

        @Override
        public void query(Query query, int limit, IndexResultCollector collector)
            throws IOException {
          IndexSearcher searcher = searcherManager.acquire();
          try {
            TopDocs td = searcher.search(query, limit);
            for (ScoreDoc sd : td.scoreDocs) {
              Object k = org.apache.geode.cache.lucene.internal.repository.serializer.SerializerUtil
                  .getKey(searcher.storedFields().document(sd.doc));
              collector.collect(k, sd.score);
            }
          } finally {
            searcherManager.release(searcher);
          }
        }

        @Override
        public void commit() throws IOException {
          writer.commit();
          searcherManager.maybeRefresh();
        }

        @Override
        public void commitAndRefreshBlocking() throws IOException {
          writer.commit();
          searcherManager.maybeRefreshBlocking();
        }

        @Override
        public Region<?, ?> getRegion() {
          return mockRegion;
        }

        @Override
        public boolean isClosed() {
          return false;
        }

        @Override
        public IndexWriter getWriter() {
          return writer;
        }

        @Override
        public void cleanup() {}
      };

      when(mockRepoManager.getRepositoriesByBucket(eq(mockContext), eq(false)))
          .thenReturn(Map.of(0, realRepo));

      // Fresh uncommitted entries live ONLY in Tier-0 (0 documents in Lucene yet)
      float[] matchVec = new float[] {1f, 0f, 0f, 0f};
      float[] nonMatchVec = new float[] {0.99f, 0.1f, 0f, 0f};
      when(mockRegion.get("fresh-books")).thenReturn(new Object[] {matchVec, "books"});
      when(mockRegion.get("fresh-music")).thenReturn(new Object[] {nonMatchVec, "music"});

      Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(mockCache);
      registry.recordPut(REGION_PATH, INDEX_NAME, 0, "fresh-books", matchVec,
          Map.of("category", "books"));
      registry.recordPut(REGION_PATH, INDEX_NAME, 0, "fresh-music", nonMatchVec,
          Map.of("category", "music"));

      // FuzzyQuery ("category:boks~1") is NOT natively supported by Tier0MetadataMatcher:
      // must flush+refresh Tier-0 into Lucene and return "fresh-books" instead of silently dropping
      HybridKnnQueryContext queryContext = HybridKnnQueryContext
          .builder(INDEX_NAME, VECTOR_FIELD, matchVec, 5)
          .similarityFunction(VectorSimilarityFunction.COSINE)
          .filterQuery("category:boks~1", "category")
          .build();
      when(mockContext.getArguments()).thenReturn(queryContext);

      new DistributedHybridKnnFunction().execute(mockContext);

      ArgumentCaptor<TopEntriesCollector> resultCaptor =
          ArgumentCaptor.forClass(TopEntriesCollector.class);
      verify(mockResultSender).lastResult(resultCaptor.capture());

      List<EntryScore> hits = resultCaptor.getValue().getEntries().getHits();
      assertThat(hits).extracting(EntryScore::getKey).containsExactly("fresh-books");
      assertThat(registry.getUnsupportedFilterFlushFallbackCount()).isEqualTo(1L);
      assertThat(DistributedHybridKnnFunction.getUnsupportedFilterFlushFallbackCount())
          .isEqualTo(1L);
      assertThat(registry.getBuffer(REGION_PATH, INDEX_NAME, 0).getTotalSize()).isEqualTo(0);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void maskTelemetryTracksInlineAndOversampleFallbackPaths() throws Exception {
    DistributedHybridKnnFunction.resetMaskTelemetry();
    when(mockRepoManager.getRepositoriesByBucket(eq(mockContext), eq(false)))
        .thenReturn(Map.of(0, mockRepoBucket0));

    Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(mockCache);
    for (int i = 0; i < 10; i++) {
      registry.recordDestroy(REGION_PATH, INDEX_NAME, 0, "inline-tomb-" + i);
    }

    HybridKnnQueryContext inlineCtx = HybridKnnQueryContext
        .builder(INDEX_NAME, VECTOR_FIELD, new float[] {1f, 0f}, 5)
        .build();
    when(mockContext.getArguments()).thenReturn(inlineCtx);

    new DistributedHybridKnnFunction().execute(mockContext);
    assertThat(DistributedHybridKnnFunction.getInlineMaskQueryCount()).isEqualTo(1L);
    assertThat(DistributedHybridKnnFunction.getOversampleFallbackQueryCount()).isEqualTo(0L);
    assertThat(registry.getInlineMaskQueryCount()).isEqualTo(1L);
    assertThat(registry.getOversampleFallbackQueryCount()).isEqualTo(0L);

    // Push |T_mask| above 512 so the oversampling fallback branch triggers
    for (int i = 0; i < 520; i++) {
      registry.recordDestroy(REGION_PATH, INDEX_NAME, 0, "overflow-tomb-" + i);
    }
    new DistributedHybridKnnFunction().execute(mockContext);

    assertThat(DistributedHybridKnnFunction.getInlineMaskQueryCount()).isEqualTo(1L);
    assertThat(DistributedHybridKnnFunction.getOversampleFallbackQueryCount()).isEqualTo(1L);
    assertThat(registry.getOversampleFallbackQueryCount()).isEqualTo(1L);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void fp32RerankOverSq8QuantizedIndexImprovesRecallAndMatchesExactTier0Fp32Scores()
      throws Exception {
    // Validate rerankOversampleFactor bounds on HybridKnnQueryContext.Builder
    assertThat(HybridKnnQueryContext.builder(INDEX_NAME, VECTOR_FIELD, new float[] {1f, 0f}, 5)
        .build().getRerankOversampleFactor()).isEqualTo(1);
    assertThatThrownBy(() -> HybridKnnQueryContext
        .builder(INDEX_NAME, VECTOR_FIELD, new float[] {1f, 0f}, 5).rerankOversampleFactor(0))
            .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> HybridKnnQueryContext
        .builder(INDEX_NAME, VECTOR_FIELD, new float[] {1f, 0f}, 5).rerankOversampleFactor(65))
            .isInstanceOf(IllegalArgumentException.class);

    final int dim = 256;
    final int numDocs = 600;
    final int k = 10;
    final int numQueries = 20;
    Random rng = new Random(2026L);

    // Generate anisotropic clustered vectors with outlier spikes where SQ8 quantization error
    // perturbs top-10 ranking, while exact FP32 .vec reranking restores exact FP32 recall & scores.
    Map<String, float[]> vectorsByKey = new LinkedHashMap<>(numDocs);
    float[] baseDir = randomUnitVector(rng, dim);
    for (int i = 0; i < numDocs; i++) {
      float[] v = new float[dim];
      for (int d = 0; d < dim; d++) {
        v[d] = baseDir[d] * 0.6f + (float) (rng.nextGaussian() * 0.08);
      }
      // Inject occasional strong outlier coordinate to stress SQ8 dynamic range
      if (i % 15 == 0) {
        v[i % dim] += 3.5f;
      }
      normalize(v);
      vectorsByKey.put("sq8-" + i, v);
    }

    IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
    iwc.setCodec(new Lucene912Codec() {
      @Override
      public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return new Lucene99HnswScalarQuantizedVectorsFormat(16, 100);
      }
    });

    LuceneSerializer<float[]> vecSerializer = (idx, vec) -> {
      Document doc = new Document();
      doc.add(new KnnFloatVectorField(VECTOR_FIELD, vec, VectorSimilarityFunction.COSINE));
      return Collections.singletonList(doc);
    };

    try (ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(dir, iwc)) {
      IndexRepositoryImpl repo = new IndexRepositoryImpl(
          mockRegion, writer, vecSerializer, mockStats, mockRegion, null, "lock", mockIndex);

      // Commit across 3 batches to guarantee multiple Lucene leaf segments (testing per-leaf sort
      // and monotonic FloatVectorValues.advance())
      int count = 0;
      for (Map.Entry<String, float[]> e : vectorsByKey.entrySet()) {
        repo.update(e.getKey(), e.getValue());
        count++;
        if (count % 200 == 0) {
          repo.commitAndRefreshBlocking();
        }
      }
      repo.commitAndRefreshBlocking();

      when(mockRepoManager.getRepositoriesByBucket(eq(mockContext), eq(false)))
          .thenReturn(Map.of(0, repo));

      double totalSq8Recall = 0.0;
      double totalRerankedRecall = 0.0;

      for (int qIdx = 0; qIdx < numQueries; qIdx++) {
        float[] query = new float[dim];
        for (int d = 0; d < dim; d++) {
          query[d] = baseDir[d] * 0.6f + (float) (rng.nextGaussian() * 0.06);
        }
        normalize(query);

        // Exact FP32 ground truth top-k
        List<EntryScore<String>> exactAll = new ArrayList<>(numDocs);
        for (Map.Entry<String, float[]> e : vectorsByKey.entrySet()) {
          exactAll.add(new EntryScore<>(e.getKey(),
              VectorSimilarityScorer.score(query, e.getValue(), VectorSimilarityFunction.COSINE)));
        }
        exactAll.sort(Comparator.<EntryScore<String>>comparingDouble(EntryScore::getScore)
            .reversed().thenComparing(EntryScore::getKey));
        Set<String> groundTruthTopK = new HashSet<>();
        for (int i = 0; i < k; i++) {
          groundTruthTopK.add(exactAll.get(i).getKey());
        }

        // 1. Raw SQ8 (rerankOversampleFactor = 1)
        List<EntryScore<Object>> sq8Hits = executeHybridQuery(query, k, 1);
        totalSq8Recall += recall(groundTruthTopK, sq8Hits);

        // 2. SQ8 + 4x FP32 .vec reranking (rerankOversampleFactor = 4)
        List<EntryScore<Object>> rerankedHits = executeHybridQuery(query, k, 4);
        totalRerankedRecall += recall(groundTruthTopK, rerankedHits);

        // Every FP32-reranked hit MUST carry the exact FP32 similarity score matching Tier-0 FP32
        assertThat(rerankedHits).hasSize(k);
        for (EntryScore<Object> hit : rerankedHits) {
          float expectedFp32 = VectorSimilarityScorer.score(
              query, vectorsByKey.get(hit.getKey()), VectorSimilarityFunction.COSINE);
          assertThat(hit.getScore())
              .as("Reranked FP32 score mismatch for key=%s", hit.getKey())
              .isCloseTo(expectedFp32, within(1e-5f));
        }
      }

      double avgSq8Recall = totalSq8Recall / numQueries;
      double avgRerankedRecall = totalRerankedRecall / numQueries;
      assertThat(avgRerankedRecall)
          .as("4x FP32 .vec reranking (%.4f) must improve/preserve SQ8 Recall@10 (%.4f)",
              avgRerankedRecall, avgSq8Recall)
          .isGreaterThanOrEqualTo(avgSq8Recall)
          .isGreaterThanOrEqualTo(0.95);
    }
  }

  /**
   * Regression test for PR2-V7-B1: verifies {@link IndexRepositoryImpl#queryWithFp32Rerank}
   * strictly upholds {@code DocIdSetIterator.advance(target > docID())} across sparse/mixed leaves
   * (docs lacking the vector field before, between, and past the last vector doc, plus a leaf
   * completely lacking the vector field) under {@code -ea}.
   */
  @Test
  public void queryWithFp32RerankHandlesSparseMixedLeavesWithoutAdvanceContractViolation()
      throws Exception {
    float[] query = new float[] {1.0f, 0.0f, 0.0f, 0.0f};
    float[] noVec = new float[0];
    float[] v1 = new float[] {0.0f, 1.0f, 0.0f, 0.0f}; // cosine = 0.5
    float[] v3 = new float[] {1.0f, 0.0f, 0.0f, 0.0f}; // cosine = 1.0
    float[] v9 = new float[] {-1.0f, 0.0f, 0.0f, 0.0f}; // cosine = 0.0

    LuceneSerializer<float[]> sparseSerializer = (idx, vec) -> {
      Document doc = new Document();
      if (vec != null && vec.length > 0) {
        doc.add(new KnnFloatVectorField(VECTOR_FIELD, vec, VectorSimilarityFunction.COSINE));
      } else {
        doc.add(new StringField("tag", "no_vector", Field.Store.NO));
      }
      return Collections.singletonList(doc);
    };

    IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
    try (ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(dir, iwc)) {
      IndexRepositoryImpl repo = new IndexRepositoryImpl(
          mockRegion, writer, sparseSerializer, mockStats, mockRegion, null, "lock", mockIndex);

      // Leaf 1 (mixed sparse): doc0(no vec), doc1(vec), doc2(no vec), doc3(vec),
      // doc4(no vec -> NO_MORE_DOCS), doc5(no vec -> already at NO_MORE_DOCS)
      repo.update("k0", noVec);
      repo.update("k1", v1);
      repo.update("k2", noVec);
      repo.update("k3", v3);
      repo.update("k4", noVec);
      repo.update("k5", noVec);
      repo.commitAndRefreshBlocking();

      // Leaf 2 (no vector field at all -> getFloatVectorValues() == null)
      repo.update("k6", noVec);
      repo.update("k7", noVec);
      repo.commitAndRefreshBlocking();

      // Leaf 3 (mixed sparse)
      repo.update("k8", noVec);
      repo.update("k9", v9);
      repo.commitAndRefreshBlocking();

      Map<Object, Float> collected = new LinkedHashMap<>();
      IndexResultCollector collector = new IndexResultCollector() {
        @Override
        public String getName() {
          return "sparse-test";
        }

        @Override
        public int size() {
          return collected.size();
        }

        @Override
        public void collect(Object key, float score) {
          collected.put(key, score);
        }
      };
      repo.queryWithFp32Rerank(new MatchAllDocsQuery(), 10, 10, VECTOR_FIELD, query,
          VectorSimilarityFunction.COSINE, collector);

      assertThat(collected).hasSize(10);
      assertThat(collected.get("k1")).isCloseTo(
          VectorSimilarityScorer.score(query, v1, VectorSimilarityFunction.COSINE), within(1e-5f));
      assertThat(collected.get("k3")).isCloseTo(
          VectorSimilarityScorer.score(query, v3, VectorSimilarityFunction.COSINE), within(1e-5f));
      assertThat(collected.get("k9")).isCloseTo(
          VectorSimilarityScorer.score(query, v9, VectorSimilarityFunction.COSINE), within(1e-5f));
      for (String nonVecKey : new String[] {"k0", "k2", "k4", "k5", "k6", "k7", "k8"}) {
        assertThat(collected.get(nonVecKey)).isCloseTo(1.0f, within(1e-5f));
      }
    }
  }

  /**
   * Regression test for PR2-V7-M1: verifies {@link HybridKnnQueryContext#getRerankOversampleFactor}
   * symmetrically clamps both lower ({@code <= 0} -> 1) and upper ({@code > 64} -> 64) bounds on
   * deserialized or crafted payloads.
   */
  @Test
  public void getRerankOversampleFactorClampsDeserializedOutOfRangeValues() throws Exception {
    HybridKnnQueryContext ctx = HybridKnnQueryContext
        .builder(INDEX_NAME, VECTOR_FIELD, new float[] {1.0f, 0.0f}, 5)
        .rerankOversampleFactor(4)
        .build();
    assertThat(ctx.getRerankOversampleFactor()).isEqualTo(4);

    java.lang.reflect.Field field =
        HybridKnnQueryContext.class.getDeclaredField("rerankOversampleFactor");
    field.setAccessible(true);

    field.setInt(ctx, 0);
    assertThat(ctx.getRerankOversampleFactor())
        .isEqualTo(HybridKnnQueryContext.DEFAULT_RERANK_OVERSAMPLE_FACTOR);

    field.setInt(ctx, -10);
    assertThat(ctx.getRerankOversampleFactor())
        .isEqualTo(HybridKnnQueryContext.DEFAULT_RERANK_OVERSAMPLE_FACTOR);

    field.setInt(ctx, 999);
    assertThat(ctx.getRerankOversampleFactor())
        .isEqualTo(HybridKnnQueryContext.MAX_RERANK_OVERSAMPLE_FACTOR);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void isoEfSearchRerankBeamCandidatesExecutesFp32RerankWithoutBeamInflation()
      throws Exception {
    float[] query = new float[] {1f, 0f, 0f, 0f};

    // 1. Default rerankBeamCandidates is false; builder setter and Java serialization preserve it
    HybridKnnQueryContext defaultCtx =
        HybridKnnQueryContext.builder(INDEX_NAME, VECTOR_FIELD, query, 10).build();
    assertThat(defaultCtx.isRerankBeamCandidates()).isFalse();
    assertThat(defaultCtx.getRerankOversampleFactor()).isEqualTo(1);

    HybridKnnQueryContext isoCtx = HybridKnnQueryContext
        .builder(INDEX_NAME, VECTOR_FIELD, query, 10)
        .rerankBeamCandidates(true)
        .build();
    assertThat(isoCtx.isRerankBeamCandidates()).isTrue();
    assertThat(isoCtx.getRerankOversampleFactor()).isEqualTo(1);

    java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
    try (java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(baos)) {
      oos.writeObject(isoCtx);
    }
    try (java.io.ObjectInputStream ois =
        new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(baos.toByteArray()))) {
      HybridKnnQueryContext deserialized = (HybridKnnQueryContext) ois.readObject();
      assertThat(deserialized.isRerankBeamCandidates()).isTrue();
      assertThat(deserialized.getRerankOversampleFactor()).isEqualTo(1);
    }

    // 2. Verify DistributedHybridKnnFunction routes rerankOversampleFactor==1 &&
    // rerankBeamCandidates==true to repo.queryWithFp32Rerank with queryK == luceneK (zero
    // inflation)
    when(mockRepoManager.getRepositoriesByBucket(eq(mockContext), eq(false)))
        .thenReturn(Map.of(0, mockRepoBucket0));
    when(mockContext.getArguments()).thenReturn(isoCtx);

    new DistributedHybridKnnFunction().execute(mockContext);

    ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
    verify(mockRepoBucket0).queryWithFp32Rerank(
        queryCaptor.capture(), eq(10), eq(10), eq(VECTOR_FIELD),
        any(float[].class), eq(VectorSimilarityFunction.COSINE),
        any(IndexResultCollector.class));
    verify(mockRepoBucket0, org.mockito.Mockito.never())
        .query(any(Query.class), anyInt(), any(IndexResultCollector.class));
    assertThat(((KnnFloatVectorQuery) queryCaptor.getValue()).getK()).isEqualTo(10);

    // 3. Verify end-to-end exact FP32 .vec rescoring on real Lucene SQ8 segments at strict iso-efS
    final int dim = 128;
    final int numDocs = 240;
    final int efS = 16;
    Random rng = new Random(909L);
    Map<String, float[]> vectorsByKey = new LinkedHashMap<>(numDocs);
    float[] baseDir = randomUnitVector(rng, dim);
    for (int i = 0; i < numDocs; i++) {
      float[] v = new float[dim];
      for (int d = 0; d < dim; d++) {
        v[d] = baseDir[d] * 0.6f + (float) (rng.nextGaussian() * 0.08);
      }
      if (i % 12 == 0) {
        v[i % dim] += 3.0f;
      }
      normalize(v);
      vectorsByKey.put("iso-" + i, v);
    }

    IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
    iwc.setCodec(new Lucene912Codec() {
      @Override
      public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return new Lucene99HnswScalarQuantizedVectorsFormat(16, 100);
      }
    });
    LuceneSerializer<float[]> vecSerializer = (idx, vec) -> {
      Document doc = new Document();
      doc.add(new KnnFloatVectorField(VECTOR_FIELD, vec, VectorSimilarityFunction.COSINE));
      return Collections.singletonList(doc);
    };

    try (ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(dir, iwc)) {
      IndexRepositoryImpl repo = new IndexRepositoryImpl(
          mockRegion, writer, vecSerializer, mockStats, mockRegion, null, "lock", mockIndex);
      int count = 0;
      for (Map.Entry<String, float[]> e : vectorsByKey.entrySet()) {
        repo.update(e.getKey(), e.getValue());
        if (++count % 80 == 0) {
          repo.commitAndRefreshBlocking();
        }
      }
      repo.commitAndRefreshBlocking();

      when(mockRepoManager.getRepositoriesByBucket(eq(mockContext), eq(false)))
          .thenReturn(Map.of(0, repo));

      float[] q = new float[dim];
      for (int d = 0; d < dim; d++) {
        q[d] = baseDir[d] * 0.6f + (float) (rng.nextGaussian() * 0.05);
      }
      normalize(q);

      ResultSender<TopEntriesCollector> localSender = mock(ResultSender.class);
      when(mockContext.getResultSender()).thenReturn(localSender);
      HybridKnnQueryContext realIsoCtx = HybridKnnQueryContext
          .builder(INDEX_NAME, VECTOR_FIELD, q, efS)
          .similarityFunction(VectorSimilarityFunction.COSINE)
          .rerankOversampleFactor(1)
          .rerankBeamCandidates(true)
          .build();
      when(mockContext.getArguments()).thenReturn(realIsoCtx);

      new DistributedHybridKnnFunction().execute(mockContext);

      ArgumentCaptor<TopEntriesCollector> captor =
          ArgumentCaptor.forClass(TopEntriesCollector.class);
      verify(localSender).lastResult(captor.capture());
      List<EntryScore<Object>> hits = captor.getValue().getEntries().getHits();
      assertThat(hits).hasSize(efS);
      for (int i = 0; i < hits.size(); i++) {
        EntryScore<Object> hit = hits.get(i);
        float exactFp32 = VectorSimilarityScorer.score(
            q, vectorsByKey.get(hit.getKey()), VectorSimilarityFunction.COSINE);
        assertThat(hit.getScore()).isCloseTo(exactFp32, within(1e-5f));
        if (i > 0) {
          assertThat(hits.get(i - 1).getScore()).isGreaterThanOrEqualTo(hit.getScore());
        }
      }
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void pathBBarrierHoldsWriteLockAcrossSearcherCutBlockingConcurrentOverwriteAndRecreation()
      throws Exception {
    DistributedHybridKnnFunction.resetUnsupportedFilterFlushFallbackCount();

    try (ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(new StandardAnalyzer()));
        SearcherManager searcherManager = new SearcherManager(writer, true, true, null)) {

      CountDownLatch commitReached = new CountDownLatch(1);
      AtomicBoolean armed = new AtomicBoolean(false);
      AtomicBoolean concurrentWriterBlockedWhileCommitRunning = new AtomicBoolean(false);
      AtomicBoolean lockHeldAtAcquire = new AtomicBoolean(false);
      AtomicBoolean lockReleasedAtAcquire = new AtomicBoolean(false);
      AtomicBoolean concurrentWriterCompleted = new AtomicBoolean(false);
      AtomicBoolean queryCallbackRan = new AtomicBoolean(false);

      float[] overwriteVec = new float[] {0.95f, 0.31f, 0f, 0f};
      float[] initialBooksVec = new float[] {1f, 0f, 0f, 0f};
      float[] freshPendingVec = new float[] {0.8f, 0.6f, 0f, 0f};

      when(mockRegion.get("doc-books")).thenReturn(new Object[] {initialBooksVec, "books"});
      when(mockRegion.get("doc-init")).thenReturn(new Object[] {freshPendingVec, "books"});

      Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(mockCache);
      Tier0VectorDeltaBuffer<Object> buffer =
          registry.getOrCreateBuffer(REGION_PATH, INDEX_NAME, 0);

      IndexRepository bareRepo = new IndexRepository() {
        @Override
        public void create(Object key, Object value) throws IOException {
          update(key, value);
        }

        @Override
        public void update(Object key, Object value) throws IOException {
          Object[] pair = (Object[]) value;
          Document doc = new Document();
          org.apache.geode.cache.lucene.internal.repository.serializer.SerializerUtil.addKey(key,
              doc);
          doc.add(new KnnFloatVectorField(VECTOR_FIELD, (float[]) pair[0],
              VectorSimilarityFunction.COSINE));
          doc.add(new StringField("category", (String) pair[1], Field.Store.NO));
          writer.updateDocument(
              org.apache.geode.cache.lucene.internal.repository.serializer.SerializerUtil
                  .toKeyTerm(key),
              doc);
        }

        @Override
        public void delete(Object key) throws IOException {
          writer.deleteDocuments(
              org.apache.geode.cache.lucene.internal.repository.serializer.SerializerUtil
                  .toKeyTerm(key));
        }

        @Override
        public void query(Query query, int limit, IndexResultCollector collector)
            throws IOException {
          query(query, limit, collector, null);
        }

        @Override
        public void query(Query query, int limit, IndexResultCollector collector,
            Runnable onSearcherAcquired) throws IOException {
          IndexSearcher searcher = searcherManager.acquire();
          try {
            if (onSearcherAcquired != null) {
              // Concurrent overwrite must still be blocked right at searcher acquisition!
              concurrentWriterBlockedWhileCommitRunning.set(!concurrentWriterCompleted.get());
              lockHeldAtAcquire.set(buffer.isPathBWriteHeldByCurrentThread());
              onSearcherAcquired.run();
              lockReleasedAtAcquire.set(!buffer.isPathBWriteHeldByCurrentThread());
              queryCallbackRan.set(true);
            }
            TopDocs td = searcher.search(query, limit);
            for (ScoreDoc sd : td.scoreDocs) {
              Object k = org.apache.geode.cache.lucene.internal.repository.serializer.SerializerUtil
                  .getKey(searcher.storedFields().document(sd.doc));
              collector.collect(k, sd.score);
            }
          } finally {
            searcherManager.release(searcher);
          }
        }

        @Override
        public void commit() throws IOException {
          writer.commit();
          searcherManager.maybeRefresh();
        }

        @Override
        public void commitAndRefreshBlocking() throws IOException {
          writer.commit();
          searcherManager.maybeRefreshBlocking();
          if (armed.get()) {
            commitReached.countDown();
            // Yield briefly so concurrent writer thread enters readLock().lock() before
            // searcher cut.
            try {
              Thread.sleep(25L);
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
            }
          }
        }

        @Override
        public IndexWriter getWriter() {
          return writer;
        }

        @Override
        public Region<?, ?> getRegion() {
          return mockRegion;
        }

        @Override
        public boolean isClosed() {
          return false;
        }

        @Override
        public void cleanup() {}
      };

      // Seed "doc-books" in Lucene and put "doc-init" into Tier-0 to trigger Path B
      bareRepo.update("doc-books", new Object[] {initialBooksVec, "books"});
      bareRepo.commitAndRefreshBlocking();
      registry.recordPut(REGION_PATH, INDEX_NAME, 0, "doc-init", freshPendingVec,
          Map.of("category", "books"));
      when(mockRepoManager.getRepositoriesByBucket(eq(mockContext), eq(false)))
          .thenReturn(Map.of(0, bareRepo));
      armed.set(true);

      ExecutorService pool = Executors.newSingleThreadExecutor();
      Future<?> writerFuture;
      try {
        writerFuture = pool.submit(() -> {
          if (!commitReached.await(5, TimeUnit.SECONDS)) {
            throw new IllegalStateException("commitReached timed out");
          }
          // Concurrent matching overwrite on already-committed "doc-books" and fresh creation
          buffer.put("doc-books", overwriteVec, Map.of("category", "books"));
          buffer.put("doc-after-cut", overwriteVec, Map.of("category", "books"));
          concurrentWriterCompleted.set(true);
          return null;
        });

        HybridKnnQueryContext fuzzyCtx = HybridKnnQueryContext
            .builder(INDEX_NAME, VECTOR_FIELD, initialBooksVec, 5)
            .similarityFunction(VectorSimilarityFunction.COSINE)
            .filterQuery("category:boks~1", "category")
            .build();
        when(mockContext.getArguments()).thenReturn(fuzzyCtx);

        new DistributedHybridKnnFunction().execute(mockContext);
        writerFuture.get(5, TimeUnit.SECONDS);
      } finally {
        pool.shutdownNow();
      }

      assertThat(queryCallbackRan.get()).isTrue();
      assertThat(lockHeldAtAcquire.get()).isTrue();
      assertThat(lockReleasedAtAcquire.get()).isTrue();
      assertThat(concurrentWriterBlockedWhileCommitRunning.get()).isTrue();
      assertThat(concurrentWriterCompleted.get()).isTrue();
      assertThat(buffer.isPathBWriteHeldByCurrentThread()).isFalse();

      ArgumentCaptor<TopEntriesCollector> resultCaptor =
          ArgumentCaptor.forClass(TopEntriesCollector.class);
      verify(mockResultSender).lastResult(resultCaptor.capture());
      List<EntryScore> hits = resultCaptor.getValue().getEntries().getHits();
      // Both doc-books and doc-init MUST be returned; doc-books was NEVER masked/dropped!
      assertThat(hits).extracting(EntryScore::getKey)
          .containsExactlyInAnyOrder("doc-books", "doc-init");
      // Concurrent writes executed strictly AFTER searcher cut remain safely in Tier-0
      assertThat(buffer.containsKey("doc-books")).isTrue();
      assertThat(buffer.containsKey("doc-after-cut")).isTrue();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void pathBBarrierReleasesWriteLockWhenFp32RerankThrows() throws Exception {
    Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(mockCache);
    Tier0VectorDeltaBuffer<Object> buffer = registry.getOrCreateBuffer(REGION_PATH, INDEX_NAME, 0);
    float[] q = new float[] {1f, 0f, 0f, 0f};
    buffer.put("k-err", q, Map.of("category", "books"));
    when(mockRegion.get("k-err")).thenReturn(new Object[] {q, "books"});

    IndexRepository failingRepo = mock(IndexRepository.class);
    when(mockRepoManager.getRepositoriesByBucket(eq(mockContext), eq(false)))
        .thenReturn(Map.of(0, failingRepo));
    doThrow(new IOException("simulated searcher failure")).when(failingRepo)
        .queryWithFp32Rerank(any(), anyInt(), anyInt(), any(), any(), any(), any(), any());

    HybridKnnQueryContext fuzzyCtx = HybridKnnQueryContext
        .builder(INDEX_NAME, VECTOR_FIELD, q, 5)
        .similarityFunction(VectorSimilarityFunction.COSINE)
        .rerankOversampleFactor(2)
        .filterQuery("category:boks~1", "category")
        .build();
    when(mockContext.getArguments()).thenReturn(fuzzyCtx);

    assertThatThrownBy(() -> new DistributedHybridKnnFunction().execute(mockContext))
        .isInstanceOf(InternalFunctionInvocationTargetException.class)
        .hasCauseInstanceOf(IOException.class);

    assertThat(buffer.isPathBWriteHeldByCurrentThread()).isFalse();
  }

  @Test
  public void indexRepositoryImplInvokesSearcherAcquiredBarrierBeforeScanning() throws Exception {
    float[] q = new float[] {1f, 0f, 0f, 0f};
    LuceneSerializer<float[]> serializer = (idx, vec) -> {
      Document doc = new Document();
      doc.add(new KnnFloatVectorField(VECTOR_FIELD, vec, VectorSimilarityFunction.COSINE));
      return Collections.singletonList(doc);
    };

    try (ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(new StandardAnalyzer()))) {
      IndexRepositoryImpl repo = new IndexRepositoryImpl(
          mockRegion, writer, serializer, mockStats, mockRegion, null, "lock", mockIndex);
      repo.update("k1", q);
      repo.commitAndRefreshBlocking();

      Tier0VectorDeltaBuffer<Object> buffer = new Tier0VectorDeltaBuffer<>(0);
      for (boolean useRerank : new boolean[] {false, true}) {
        AtomicBoolean acquiredBeforeCollect = new AtomicBoolean(false);
        AtomicBoolean writerDone = new AtomicBoolean(false);
        AtomicBoolean writerBlockedAtAcquire = new AtomicBoolean(false);

        buffer.lockPathBWrite();
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
          Future<?> writerFuture = pool.submit(() -> {
            buffer.put("concurrent-" + useRerank, q);
            writerDone.set(true);
          });
          // Let worker park on pathBBarrierLock.readLock() briefly
          Thread.sleep(20L);

          Runnable onAcquired = () -> {
            writerBlockedAtAcquire.set(!writerDone.get());
            buffer.unlockPathBWrite();
            acquiredBeforeCollect.set(true);
          };
          List<Object> keys = new ArrayList<>();
          IndexResultCollector collector = new IndexResultCollector() {
            @Override
            public String getName() {
              return "barrier-test";
            }

            @Override
            public int size() {
              return keys.size();
            }

            @Override
            public void collect(Object key, float score) {
              assertThat(acquiredBeforeCollect.get()).isTrue();
              keys.add(key);
            }
          };

          if (useRerank) {
            repo.queryWithFp32Rerank(new MatchAllDocsQuery(), 5, 5, VECTOR_FIELD, q,
                VectorSimilarityFunction.COSINE, collector, onAcquired);
          } else {
            repo.query(new MatchAllDocsQuery(), 5, collector, onAcquired);
          }

          writerFuture.get(5, TimeUnit.SECONDS);
          assertThat(writerBlockedAtAcquire.get()).isTrue();
          assertThat(acquiredBeforeCollect.get()).isTrue();
          assertThat(keys).containsExactly("k1");
          assertThat(buffer.isPathBWriteHeldByCurrentThread()).isFalse();
        } finally {
          pool.shutdownNow();
        }
      }
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void supportedFilterSearcherEpochValidationPreventsTwoKeyTornCut() throws Exception {
    for (boolean rerankBeam : new boolean[] {false, true}) {
      Tier0VectorRegistry.removeCache(mockCache);
      LuceneSerializer<float[]> serializer = (idx, vec) -> {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField(VECTOR_FIELD, vec, VectorSimilarityFunction.COSINE));
        doc.add(new StringField("tag", "all", Field.Store.NO));
        return Collections.singletonList(doc);
      };

      try (ByteBuffersDirectory dir = new ByteBuffersDirectory();
          IndexWriter writer =
              new IndexWriter(dir, new IndexWriterConfig(new StandardAnalyzer()))) {
        IndexRepositoryImpl delegate = new IndexRepositoryImpl(
            mockRegion, writer, serializer, mockStats, mockRegion, null, "lock", mockIndex);

        float[] v0 = new float[] {0.20f, (float) Math.sqrt(1.0 - 0.04), 0f, 0f};
        float[] v1 = new float[] {0.85f, (float) Math.sqrt(1.0 - 0.85 * 0.85), 0f, 0f};
        float[] v2 = new float[] {0.99f, (float) Math.sqrt(1.0 - 0.99 * 0.99), 0f, 0f};
        float[] vY = new float[] {0.90f, (float) Math.sqrt(1.0 - 0.81), 0f, 0f};
        float[] queryVec = new float[] {1f, 0f, 0f, 0f};

        delegate.update("x", v0);
        delegate.commitAndRefreshBlocking();
        long initialEpoch = delegate.getSearcherEpoch();
        assertThat(initialEpoch % 2L).isEqualTo(0L);

        Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(mockCache);
        Tier0VectorDeltaBuffer<Object> buffer =
            registry.getOrCreateBuffer(REGION_PATH, INDEX_NAME, 0);
        buffer.put("x", v1, Map.of("tag", "all"));

        java.util.concurrent.atomic.AtomicInteger queryAttempts =
            new java.util.concurrent.atomic.AtomicInteger(0);

        IndexRepository interleavingRepo = new IndexRepository() {
          private void triggerTwoKeyInterleavingAfterSnapshotBeforeAcquire() throws IOException {
            // Simulate {x=v2, y} acked + Path A committed & evicted AFTER snapshot captured
            // {x=v1} and strictly BEFORE searcher acquisition validates epoch0.
            long seqX = buffer.put("x", v2, Map.of("tag", "all"));
            long seqY = buffer.put("y", vY, Map.of("tag", "all"));
            delegate.update("x", v2);
            delegate.update("y", vY);
            delegate.commitAndRefreshBlocking();
            buffer.evictCommitted(Map.of("x", seqX, "y", seqY));
          }

          @Override
          public void create(Object key, Object value) throws IOException {
            delegate.create(key, value);
          }

          @Override
          public void update(Object key, Object value) throws IOException {
            delegate.update(key, value);
          }

          @Override
          public void delete(Object key) throws IOException {
            delegate.delete(key);
          }

          @Override
          public void query(Query query, int limit, IndexResultCollector collector)
              throws IOException {
            delegate.query(query, limit, collector);
          }

          @Override
          public long getSearcherEpoch() {
            return delegate.getSearcherEpoch();
          }

          @Override
          public boolean tryQueryAtEpoch(long expectedEpoch, Query query, int limit,
              IndexResultCollector collector, Runnable onSearcherAcquired) throws IOException {
            if (queryAttempts.incrementAndGet() == 1) {
              triggerTwoKeyInterleavingAfterSnapshotBeforeAcquire();
            }
            return delegate.tryQueryAtEpoch(
                expectedEpoch, query, limit, collector, onSearcherAcquired);
          }

          @Override
          public boolean tryQueryWithFp32RerankAtEpoch(long expectedEpoch, Query query, int k,
              int oversampleK, String vectorField, float[] queryVector,
              VectorSimilarityFunction similarity, IndexResultCollector collector,
              Runnable onSearcherAcquired) throws IOException {
            if (queryAttempts.incrementAndGet() == 1) {
              triggerTwoKeyInterleavingAfterSnapshotBeforeAcquire();
            }
            return delegate.tryQueryWithFp32RerankAtEpoch(expectedEpoch, query, k, oversampleK,
                vectorField, queryVector, similarity, collector, onSearcherAcquired);
          }

          @Override
          public void commit() throws IOException {
            delegate.commit();
          }

          @Override
          public void commitAndRefreshBlocking() throws IOException {
            delegate.commitAndRefreshBlocking();
          }

          @Override
          public Region<?, ?> getRegion() {
            return delegate.getRegion();
          }

          @Override
          public boolean isClosed() {
            return delegate.isClosed();
          }

          @Override
          public IndexWriter getWriter() {
            return delegate.getWriter();
          }

          @Override
          public void cleanup() {
            delegate.cleanup();
          }
        };

        when(mockRepoManager.getRepositoriesByBucket(eq(mockContext), eq(false)))
            .thenReturn(Map.of(0, interleavingRepo));
        ResultSender<TopEntriesCollector> localSender = mock(ResultSender.class);
        when(mockContext.getResultSender()).thenReturn(localSender);

        HybridKnnQueryContext ctx = HybridKnnQueryContext
            .builder(INDEX_NAME, VECTOR_FIELD, queryVec, 5)
            .similarityFunction(VectorSimilarityFunction.COSINE)
            .addMetadataFilter("tag", "all")
            .rerankBeamCandidates(rerankBeam)
            .build();
        when(mockContext.getArguments()).thenReturn(ctx);

        new DistributedHybridKnnFunction().execute(mockContext);

        assertThat(queryAttempts.get()).isEqualTo(2);
        assertThat(delegate.getSearcherEpoch()).isGreaterThan(initialEpoch);

        ArgumentCaptor<TopEntriesCollector> captor =
            ArgumentCaptor.forClass(TopEntriesCollector.class);
        verify(localSender).lastResult(captor.capture());
        List<EntryScore<Object>> hits = captor.getValue().getEntries().getHits();
        assertThat(hits).extracting(EntryScore::getKey).containsExactly("x", "y");
        // x MUST hold score of v2 (~0.995 cosine), NEVER stale v1 (~0.925)!
        float expectedScoreX =
            VectorSimilarityScorer.score(queryVec, v2, VectorSimilarityFunction.COSINE);
        assertThat(hits.get(0).getScore()).isCloseTo(expectedScoreX, within(1e-4f));
      }
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void supportedFilterFallsBackToWriteBarrierAfterSixteenContendedSearcherEpochRetries()
      throws Exception {
    Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(mockCache);
    Tier0VectorDeltaBuffer<Object> buffer = registry.getOrCreateBuffer(REGION_PATH, INDEX_NAME, 0);
    float[] q = new float[] {1f, 0f, 0f, 0f};
    buffer.put("k-contended", q, Map.of("tag", "all"));

    java.util.concurrent.atomic.AtomicInteger attempts =
        new java.util.concurrent.atomic.AtomicInteger(0);
    java.util.concurrent.atomic.AtomicLong fakeEpoch =
        new java.util.concurrent.atomic.AtomicLong(2L);
    AtomicBoolean fallbackBarrierHeld = new AtomicBoolean(false);
    AtomicBoolean barrierReleasedAfterAcquire = new AtomicBoolean(false);

    IndexRepository contendedRepo = new IndexRepository() {
      @Override
      public void create(Object key, Object value) {}

      @Override
      public void update(Object key, Object value) {}

      @Override
      public void delete(Object key) {}

      @Override
      public void query(Query query, int limit, IndexResultCollector collector) {}

      @Override
      public long getSearcherEpoch() {
        return fakeEpoch.get();
      }

      @Override
      public boolean tryQueryAtEpoch(long expectedEpoch, Query query, int limit,
          IndexResultCollector collector, Runnable onSearcherAcquired) {
        int call = attempts.incrementAndGet();
        if (call <= DistributedHybridKnnFunction.MAX_OPTIMISTIC_SEARCHER_EPOCH_RETRIES) {
          fakeEpoch.addAndGet(2L);
          return false;
        }
        fallbackBarrierHeld.set(buffer.isPathBWriteHeldByCurrentThread());
        if (onSearcherAcquired != null) {
          onSearcherAcquired.run();
        }
        barrierReleasedAfterAcquire.set(!buffer.isPathBWriteHeldByCurrentThread());
        return true;
      }

      @Override
      public void commit() {}

      @Override
      public Region<?, ?> getRegion() {
        return mockRegion;
      }

      @Override
      public boolean isClosed() {
        return false;
      }

      @Override
      public IndexWriter getWriter() {
        return null;
      }

      @Override
      public void cleanup() {}
    };

    when(mockRepoManager.getRepositoriesByBucket(eq(mockContext), eq(false)))
        .thenReturn(Map.of(0, contendedRepo));
    ResultSender<TopEntriesCollector> localSender = mock(ResultSender.class);
    when(mockContext.getResultSender()).thenReturn(localSender);

    HybridKnnQueryContext ctx = HybridKnnQueryContext
        .builder(INDEX_NAME, VECTOR_FIELD, q, 5)
        .similarityFunction(VectorSimilarityFunction.COSINE)
        .build();
    when(mockContext.getArguments()).thenReturn(ctx);

    new DistributedHybridKnnFunction().execute(mockContext);

    assertThat(attempts.get())
        .isEqualTo(DistributedHybridKnnFunction.MAX_OPTIMISTIC_SEARCHER_EPOCH_RETRIES + 1);
    assertThat(fallbackBarrierHeld.get()).isTrue();
    assertThat(barrierReleasedAfterAcquire.get()).isTrue();
    assertThat(buffer.isPathBWriteHeldByCurrentThread()).isFalse();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void abortFlushPreservesConsistentCutWithSearcherEpochAcrossBothQueryPaths()
      throws Exception {
    for (boolean rerankBeam : new boolean[] {false, true}) {
      Tier0VectorRegistry.removeCache(mockCache);
      LuceneSerializer<float[]> serializer = (idx, vec) -> {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField(VECTOR_FIELD, vec, VectorSimilarityFunction.COSINE));
        doc.add(new StringField("tag", "all", Field.Store.NO));
        return Collections.singletonList(doc);
      };

      try (ByteBuffersDirectory dir = new ByteBuffersDirectory();
          IndexWriter writer =
              new IndexWriter(dir, new IndexWriterConfig(new StandardAnalyzer()))) {
        float[] v0 = new float[] {0.20f, (float) Math.sqrt(1.0 - 0.04), 0f, 0f};
        float[] v1 = new float[] {0.98f, (float) Math.sqrt(1.0 - 0.98 * 0.98), 0f, 0f};
        float[] staleK2 = new float[] {0.99f, (float) Math.sqrt(1.0 - 0.99 * 0.99), 0f, 0f};
        float[] queryVec = new float[] {1f, 0f, 0f, 0f};

        Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(mockCache);
        Tier0VectorDeltaBuffer<Object> buffer =
            registry.getOrCreateBuffer(REGION_PATH, INDEX_NAME, 0);

        IndexRepositoryImpl repo = new IndexRepositoryImpl(
            mockRegion, writer, serializer, mockStats, mockRegion, null, "lock", mockIndex);

        // Initial Lucene segment holds k0 and stale k2; Tier-0 holds k1 (live) and k2 (tombstone)
        repo.update("k0", v0);
        repo.update("k2", staleK2);
        repo.commitAndRefreshBlocking();
        long initialEpoch = repo.getSearcherEpoch();
        assertThat(initialEpoch % 2L).isEqualTo(0L);

        buffer.put("k1", v1, Map.of("tag", "all"));
        buffer.destroy("k2");

        // Sub-case (a): Pre-commit abortFlush (IOException from valueResolver before commit) does
        // not lose Tier-0 visibility or tear epoch cut
        assertThatThrownBy(() -> registry.flushBucket(REGION_PATH, INDEX_NAME, 0, repo, entry -> {
          throw new IllegalStateException("valueResolver transient abort");
        })).isInstanceOf(IllegalStateException.class);
        assertThat(buffer.isFlushing()).isFalse();
        assertThat(buffer.snapshot().getLiveEntries()).containsOnlyKeys("k1");
        assertThat(buffer.snapshot().getTombstonedKeys()).containsExactly("k2");

        // Sub-case (b): Trigger flush commit + abortFlush strictly AFTER acquireSearcher() returned
        // S_0 and BEFORE post-acquire epoch check runs in IndexRepositoryImpl.tryQuery*AtEpoch().
        java.util.concurrent.atomic.AtomicInteger delegatedAcquires =
            new java.util.concurrent.atomic.AtomicInteger(0);
        IndexRepositoryImpl instrumentedRepo = new IndexRepositoryImpl(
            mockRegion, writer, serializer, mockStats, mockRegion, null, "lock", mockIndex) {
          @Override
          protected IndexSearcher acquireSearcher() throws IOException {
            IndexSearcher s = super.acquireSearcher();
            if (delegatedAcquires.incrementAndGet() == 1) {
              Tier0VectorDeltaBuffer.FlushBatch<Object> batch = buffer.beginFlush();
              assertThat(batch).isNotNull();
              try {
                super.update("k1", v1);
                super.delete("k2");
                // Advance searcherEpoch and refresh NRT reader, then abortFlush back into E_active
                commit();
              } finally {
                buffer.abortFlush(batch.getEpoch());
              }
            }
            return s;
          }
        };

        long epochBeforeQuery = instrumentedRepo.getSearcherEpoch();
        when(mockRepoManager.getRepositoriesByBucket(eq(mockContext), eq(false)))
            .thenReturn(Map.of(0, instrumentedRepo));
        ResultSender<TopEntriesCollector> localSender = mock(ResultSender.class);
        when(mockContext.getResultSender()).thenReturn(localSender);

        HybridKnnQueryContext ctx = HybridKnnQueryContext
            .builder(INDEX_NAME, VECTOR_FIELD, queryVec, 5)
            .similarityFunction(VectorSimilarityFunction.COSINE)
            .addMetadataFilter("tag", "all")
            .rerankBeamCandidates(rerankBeam)
            .build();
        when(mockContext.getArguments()).thenReturn(ctx);

        new DistributedHybridKnnFunction().execute(mockContext);

        // Attempt 1 MUST have been rejected after acquireSearcher() saw epoch advance, and
        // attempt 2 succeeded against aborted+restored E_active and post-commit epoch.
        assertThat(delegatedAcquires.get()).isEqualTo(2);
        assertThat(instrumentedRepo.getSearcherEpoch()).isGreaterThan(epochBeforeQuery);
        assertThat(buffer.isFlushing()).isFalse();

        ArgumentCaptor<TopEntriesCollector> captor =
            ArgumentCaptor.forClass(TopEntriesCollector.class);
        verify(localSender).lastResult(captor.capture());
        List<EntryScore<Object>> hits = captor.getValue().getEntries().getHits();
        assertThat(hits).extracting(EntryScore::getKey).containsExactly("k1", "k0");
      }
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void rePromotionDuringOptimisticRetriesAwaitsHydrationAndRebindsBuffer() throws Exception {
    Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(mockCache);
    Tier0VectorDeltaBuffer<Object> oldBuffer =
        registry.getOrCreateBuffer(REGION_PATH, INDEX_NAME, 0);
    float[] oldVec = new float[] {0.1f, 0.99f, 0f, 0f};
    float[] repromotedVec = new float[] {0.99f, 0.14f, 0f, 0f};
    float[] queryVec = new float[] {1f, 0f, 0f, 0f};
    oldBuffer.put("stale-before-demote", oldVec, Map.of("tag", "all"));

    java.util.concurrent.atomic.AtomicInteger attemptCounter =
        new java.util.concurrent.atomic.AtomicInteger(0);
    java.util.concurrent.atomic.AtomicLong epoch = new java.util.concurrent.atomic.AtomicLong(2L);
    ExecutorService promoPool = Executors.newSingleThreadExecutor();
    try {
      IndexRepository repo = new IndexRepository() {
        @Override
        public void create(Object key, Object value) {}

        @Override
        public void update(Object key, Object value) {}

        @Override
        public void delete(Object key) {}

        @Override
        public void query(Query query, int limit, IndexResultCollector collector) {}

        @Override
        public long getSearcherEpoch() {
          return epoch.get();
        }

        @Override
        public boolean tryQueryAtEpoch(long expectedEpoch, Query query, int limit,
            IndexResultCollector collector, Runnable onSearcherAcquired) {
          if (attemptCounter.incrementAndGet() == 1) {
            // Demote (clears oldBuffer) and start re-promotion hydration asynchronously while
            // advancing epoch so attempt 1 retries and MUST wait for new buffer hydration!
            registry.removeBucket(REGION_PATH, INDEX_NAME, 0);
            CountDownLatch latch = registry.beginHydration(REGION_PATH, INDEX_NAME, 0);
            Tier0VectorDeltaBuffer<Object> newBuffer =
                registry.getOrCreateBuffer(REGION_PATH, INDEX_NAME, 0);
            promoPool.submit(() -> {
              try {
                Thread.sleep(25L);
                newBuffer.put("repromoted-key", repromotedVec, Map.of("tag", "all"));
              } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
              } finally {
                registry.completeHydration(REGION_PATH, INDEX_NAME, 0, latch);
              }
            });
            epoch.addAndGet(2L);
            return false;
          }
          if (onSearcherAcquired != null) {
            onSearcherAcquired.run();
          }
          return true;
        }

        @Override
        public void commit() {}

        @Override
        public Region<?, ?> getRegion() {
          return mockRegion;
        }

        @Override
        public boolean isClosed() {
          return false;
        }

        @Override
        public IndexWriter getWriter() {
          return null;
        }

        @Override
        public void cleanup() {}
      };

      when(mockRepoManager.getRepositoriesByBucket(eq(mockContext), eq(false)))
          .thenReturn(Map.of(0, repo));
      ResultSender<TopEntriesCollector> localSender = mock(ResultSender.class);
      when(mockContext.getResultSender()).thenReturn(localSender);

      HybridKnnQueryContext ctx = HybridKnnQueryContext
          .builder(INDEX_NAME, VECTOR_FIELD, queryVec, 5)
          .similarityFunction(VectorSimilarityFunction.COSINE)
          .addMetadataFilter("tag", "all")
          .build();
      when(mockContext.getArguments()).thenReturn(ctx);

      new DistributedHybridKnnFunction().execute(mockContext);

      assertThat(attemptCounter.get()).isEqualTo(2);
      ArgumentCaptor<TopEntriesCollector> captor =
          ArgumentCaptor.forClass(TopEntriesCollector.class);
      verify(localSender).lastResult(captor.capture());
      List<EntryScore<Object>> hits = captor.getValue().getEntries().getHits();
      assertThat(hits).extracting(EntryScore::getKey).containsExactly("repromoted-key");
    } finally {
      promoPool.shutdownNow();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void pathBUnsupportedFilterDoesNotDeadlockAgainstInFlightFlushAbort() throws Exception {
    Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(mockCache);
    Tier0VectorDeltaBuffer<Object> buffer = registry.getOrCreateBuffer(REGION_PATH, INDEX_NAME, 0);
    float[] vec = new float[] {1f, 0f, 0f, 0f};
    buffer.put("k-inflight", vec, Map.of("category", "books"));
    when(mockRegion.get("k-inflight")).thenReturn(new Object[] {vec, "books"});

    // In-flight flush moves k-inflight into E_flushing before Path B starts
    Tier0VectorDeltaBuffer.FlushBatch<Object> inFlightBatch = buffer.beginFlush();
    assertThat(inFlightBatch).isNotNull();

    LuceneSerializer<Object[]> serializer = (idx, pair) -> {
      Document doc = new Document();
      doc.add(new KnnFloatVectorField(VECTOR_FIELD, (float[]) pair[0],
          VectorSimilarityFunction.COSINE));
      doc.add(new StringField("category", (String) pair[1], Field.Store.YES));
      return Collections.singletonList(doc);
    };

    try (ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(new StandardAnalyzer()))) {
      IndexRepositoryImpl repo = new IndexRepositoryImpl(
          mockRegion, writer, serializer, mockStats, mockRegion, null, "lock", mockIndex);
      when(mockRepoManager.getRepositoriesByBucket(eq(mockContext), eq(false)))
          .thenReturn(Map.of(0, repo));
      ResultSender<TopEntriesCollector> localSender = mock(ResultSender.class);
      when(mockContext.getResultSender()).thenReturn(localSender);

      ExecutorService flushPool = Executors.newSingleThreadExecutor();
      long elapsedMs;
      try {
        Future<?> abortFuture = flushPool.submit(() -> {
          Thread.sleep(30L);
          // abortFlush requires pathBBarrierLock.readLock()!
          buffer.abortFlush(inFlightBatch.getEpoch());
          return null;
        });

        HybridKnnQueryContext fuzzyCtx = HybridKnnQueryContext
            .builder(INDEX_NAME, VECTOR_FIELD, vec, 5)
            .similarityFunction(VectorSimilarityFunction.COSINE)
            .filterQuery("category:boks~1", "category")
            .build();
        when(mockContext.getArguments()).thenReturn(fuzzyCtx);

        long startNanos = System.nanoTime();
        new DistributedHybridKnnFunction().execute(mockContext);
        elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        abortFuture.get(5, TimeUnit.SECONDS);
      } finally {
        flushPool.shutdownNow();
      }

      // Must complete far below the 5,000 ms MAX_CONCURRENT_FLUSH_WAIT_MILLIS deadline
      assertThat(elapsedMs).isLessThan(2000L);
      assertThat(buffer.isFlushing()).isFalse();
      assertThat(buffer.getTotalSize()).isEqualTo(0);

      ArgumentCaptor<TopEntriesCollector> captor =
          ArgumentCaptor.forClass(TopEntriesCollector.class);
      verify(localSender).lastResult(captor.capture());
      List<EntryScore<Object>> hits = captor.getValue().getEntries().getHits();
      assertThat(hits).extracting(EntryScore::getKey).containsExactly("k-inflight");
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void inFlightReadInvalidatesAndRetriesAcrossBucketDemotionAndRePromotion()
      throws Exception {
    float[] queryVec = new float[] {1f, 0f, 0f, 0f};
    float[] vBase = new float[] {0.50f, (float) Math.sqrt(1.0 - 0.25), 0f, 0f};
    float[] vOldTerm = new float[] {0.92f, (float) Math.sqrt(1.0 - 0.92 * 0.92), 0f, 0f};
    float[] vStale = new float[] {0.95f, (float) Math.sqrt(1.0 - 0.95 * 0.95), 0f, 0f};
    float[] vNewTerm = new float[] {0.99f, (float) Math.sqrt(1.0 - 0.99 * 0.99), 0f, 0f};

    LuceneSerializer<Object[]> serializer = (idx, pair) -> {
      Document doc = new Document();
      doc.add(new KnnFloatVectorField(VECTOR_FIELD, (float[]) pair[0],
          VectorSimilarityFunction.COSINE));
      doc.add(new StringField("tag", (String) pair[1], Field.Store.YES));
      return Collections.singletonList(doc);
    };

    // Sub-case (a): Path A (both rerankBeam=false and rerankBeam=true) when demotion -> remote
    // mutation -> re-promotion occurs strictly between buffer snapshot and searcher acquisition
    // WITHOUT advancing searcherEpoch. The ownershipGeneration fence MUST invalidate attempt 1 and
    // retry against the re-promoted term cut.
    for (boolean rerankBeam : new boolean[] {false, true}) {
      Tier0VectorRegistry.removeCache(mockCache);
      Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(mockCache);
      AtomicBoolean advisorIsPrimary = new AtomicBoolean(true);
      Map<Object, Object[]> backingEntries = new java.util.concurrent.ConcurrentHashMap<>();
      backingEntries.put("k_base", new Object[] {vBase, "all"});
      backingEntries.put("k_stale", new Object[] {vStale, "all"});
      backingEntries.put("k_old_term", new Object[] {vOldTerm, "all"});

      BucketAdvisor advisor = mock(BucketAdvisor.class);
      when(advisor.isPrimary()).thenAnswer(inv -> advisorIsPrimary.get());

      BucketRegion bucket = mock(BucketRegion.class);
      when(bucket.isDestroyed()).thenReturn(false);
      when(bucket.getBucketAdvisor()).thenReturn(advisor);
      when(bucket.keySet()).thenAnswer(inv -> new HashSet<>(backingEntries.keySet()));
      when(bucket.getEntry(any())).thenAnswer(inv -> {
        Object[] val = backingEntries.get(inv.getArgument(0));
        if (val == null) {
          return null;
        }
        EntrySnapshot es = mock(EntrySnapshot.class);
        when(es.getRawValue(true)).thenReturn(val);
        return es;
      });

      PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);
      when(dataStore.getLocalBucketById(0)).thenReturn(bucket);

      PartitionedRegion pr = mock(PartitionedRegion.class);
      when(pr.isDestroyed()).thenReturn(false);
      when(pr.getFullPath()).thenReturn(REGION_PATH);
      when(pr.getRegionService()).thenReturn(mockCache);
      when(pr.getDataStore()).thenReturn(dataStore);
      when(mockContext.getDataSet()).thenReturn(pr);

      Tier0VectorBucketListener<Object, Object[]> listener = new Tier0VectorBucketListener<>(
          INDEX_NAME, pair -> (float[]) pair[0], pair -> Map.of("tag", pair[1]));
      PartitionListener partitionListener = listener.createPartitionListener();
      partitionListener.afterRegionCreate(pr);

      try (ByteBuffersDirectory dir = new ByteBuffersDirectory();
          IndexWriter writer =
              new IndexWriter(dir, new IndexWriterConfig(new StandardAnalyzer()))) {
        java.util.concurrent.atomic.AtomicInteger acquireCalls =
            new java.util.concurrent.atomic.AtomicInteger(0);
        IndexRepositoryImpl repo = new IndexRepositoryImpl(
            pr, writer, serializer, mockStats, pr, null, "lock", mockIndex) {
          @Override
          protected IndexSearcher acquireSearcher() throws IOException {
            IndexSearcher s = super.acquireSearcher();
            if (acquireCalls.incrementAndGet() == 1) {
              // Demote bucket -> mutate remotely -> re-promote locally, leaving searcherEpoch
              // untouched so ONLY the ownershipGeneration fence catches the stale term!
              advisorIsPrimary.set(false);
              partitionListener.afterSecondary(0);

              backingEntries.remove("k_old_term");
              backingEntries.remove("k_stale");
              backingEntries.put("k_new_term", new Object[] {vNewTerm, "all"});

              advisorIsPrimary.set(true);
              partitionListener.afterPrimary(0);
            }
            return s;
          }
        };

        // Initial Lucene segments hold k_base and k_stale; Term-1 Tier-0 holds k_old_term & k_stale
        repo.update("k_base", new Object[] {vBase, "all"});
        repo.update("k_stale", new Object[] {vStale, "all"});
        repo.commitAndRefreshBlocking();
        long epochBefore = repo.getSearcherEpoch();

        Tier0VectorDeltaBuffer<Object> term1Buffer =
            registry.getOrCreateBuffer(REGION_PATH, INDEX_NAME, 0);
        term1Buffer.put("k_old_term", vOldTerm, Map.of("tag", "all"));
        term1Buffer.put("k_stale", vStale, Map.of("tag", "all"));
        long genBefore = registry.getOwnershipGeneration(REGION_PATH, INDEX_NAME, 0);

        when(mockRepoManager.getRepositoriesByBucket(eq(mockContext), eq(false)))
            .thenReturn(Map.of(0, repo));
        when(mockRepoManager.collectIndexedKeysIfReady(0))
            .thenReturn(Set.of("k_base", "k_stale"));
        ResultSender<TopEntriesCollector> localSender = mock(ResultSender.class);
        when(mockContext.getResultSender()).thenReturn(localSender);

        HybridKnnQueryContext ctx = HybridKnnQueryContext
            .builder(INDEX_NAME, VECTOR_FIELD, queryVec, 5)
            .similarityFunction(VectorSimilarityFunction.COSINE)
            .addMetadataFilter("tag", "all")
            .rerankBeamCandidates(rerankBeam)
            .build();
        when(mockContext.getArguments()).thenReturn(ctx);

        new DistributedHybridKnnFunction().execute(mockContext);

        assertThat(acquireCalls.get()).isEqualTo(2);
        assertThat(repo.getSearcherEpoch()).isEqualTo(epochBefore);
        assertThat(registry.getOwnershipGeneration(REGION_PATH, INDEX_NAME, 0))
            .isGreaterThan(genBefore);
        assertThat(registry.getOwnershipFenceRetryCount()).isGreaterThanOrEqualTo(1L);

        ArgumentCaptor<TopEntriesCollector> captor =
            ArgumentCaptor.forClass(TopEntriesCollector.class);
        verify(localSender).lastResult(captor.capture());
        List<EntryScore<Object>> hits = captor.getValue().getEntries().getHits();
        assertThat(hits).extracting(EntryScore::getKey).containsExactly("k_new_term", "k_base");
      }
    }

    // Sub-case (b): Path B (unsupported FuzzyQuery) when concurrent demotion + re-promotion occurs
    // from another thread right inside acquireSearcher() while Path B holds pathBWrite -> zero
    // deadlock with removeBucket/clear(), Path B invalidates and retries on the new term.
    Tier0VectorRegistry.removeCache(mockCache);
    Tier0VectorRegistry pathBRegistry = Tier0VectorRegistry.forCache(mockCache);
    AtomicBoolean pathBIsPrimary = new AtomicBoolean(true);
    Map<Object, Object[]> pathBEntries = new java.util.concurrent.ConcurrentHashMap<>();
    pathBEntries.put("k_old_fuzzy", new Object[] {vOldTerm, "books"});

    BucketAdvisor pathBAdvisor = mock(BucketAdvisor.class);
    when(pathBAdvisor.isPrimary()).thenAnswer(inv -> pathBIsPrimary.get());

    BucketRegion pathBBucket = mock(BucketRegion.class);
    when(pathBBucket.isDestroyed()).thenReturn(false);
    when(pathBBucket.getBucketAdvisor()).thenReturn(pathBAdvisor);
    when(pathBBucket.keySet()).thenAnswer(inv -> new HashSet<>(pathBEntries.keySet()));
    when(pathBBucket.getEntry(any())).thenAnswer(inv -> {
      Object[] val = pathBEntries.get(inv.getArgument(0));
      if (val == null) {
        return null;
      }
      EntrySnapshot es = mock(EntrySnapshot.class);
      when(es.getRawValue(true)).thenReturn(val);
      return es;
    });

    PartitionedRegionDataStore pathBStore = mock(PartitionedRegionDataStore.class);
    when(pathBStore.getLocalBucketById(0)).thenReturn(pathBBucket);
    PartitionedRegion pathBPr = mock(PartitionedRegion.class);
    when(pathBPr.isDestroyed()).thenReturn(false);
    when(pathBPr.getFullPath()).thenReturn(REGION_PATH);
    when(pathBPr.getRegionService()).thenReturn(mockCache);
    when(pathBPr.getDataStore()).thenReturn(pathBStore);
    when(mockContext.getDataSet()).thenReturn(pathBPr);

    Tier0VectorBucketListener<Object, Object[]> pathBListener = new Tier0VectorBucketListener<>(
        INDEX_NAME, pair -> (float[]) pair[0], pair -> Map.of("tag", pair[1]));
    PartitionListener pathBPartListener = pathBListener.createPartitionListener();
    pathBPartListener.afterRegionCreate(pathBPr);

    ExecutorService ownershipPool = Executors.newSingleThreadExecutor();
    try (ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexWriter writer =
            new IndexWriter(dir, new IndexWriterConfig(new StandardAnalyzer()))) {
      Tier0VectorDeltaBuffer<Object> term1PathBBuffer =
          pathBRegistry.getOrCreateBuffer(REGION_PATH, INDEX_NAME, 0);
      term1PathBBuffer.put("k_old_fuzzy", vOldTerm, Map.of("tag", "books"));
      long pathBGenBefore = pathBRegistry.getOwnershipGeneration(REGION_PATH, INDEX_NAME, 0);

      java.util.concurrent.atomic.AtomicInteger pathBAcquires =
          new java.util.concurrent.atomic.AtomicInteger(0);
      IndexRepositoryImpl pathBRepo = new IndexRepositoryImpl(
          pathBPr, writer, serializer, mockStats, pathBPr, null, "lock", mockIndex) {
        @Override
        protected IndexSearcher acquireSearcher() throws IOException {
          IndexSearcher s = super.acquireSearcher();
          if (pathBAcquires.incrementAndGet() == 1) {
            assertThat(term1PathBBuffer.isPathBWriteHeldByCurrentThread()).isTrue();
            Future<?> transition = ownershipPool.submit(() -> {
              pathBIsPrimary.set(false);
              pathBPartListener.afterSecondary(0);
              pathBEntries.remove("k_old_fuzzy");
              pathBEntries.put("k_repromoted_fuzzy", new Object[] {vNewTerm, "books"});
              pathBIsPrimary.set(true);
              pathBPartListener.afterPrimary(0);
            });
            try {
              // Must finish immediately without deadlocking against pathBWrite!
              transition.get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
          return s;
        }
      };

      when(mockRepoManager.getRepositoriesByBucket(eq(mockContext), eq(false)))
          .thenReturn(Map.of(0, pathBRepo));
      when(mockRepoManager.collectIndexedKeysIfReady(0))
          .thenReturn(Collections.singleton("k_old_fuzzy"));
      ResultSender<TopEntriesCollector> pathBSender = mock(ResultSender.class);
      when(mockContext.getResultSender()).thenReturn(pathBSender);

      HybridKnnQueryContext fuzzyCtx = HybridKnnQueryContext
          .builder(INDEX_NAME, VECTOR_FIELD, queryVec, 5)
          .similarityFunction(VectorSimilarityFunction.COSINE)
          .filterQuery("tag:boks~1", "tag")
          .build();
      when(mockContext.getArguments()).thenReturn(fuzzyCtx);

      new DistributedHybridKnnFunction().execute(mockContext);

      assertThat(pathBAcquires.get()).isEqualTo(2);
      assertThat(term1PathBBuffer.isPathBWriteHeldByCurrentThread()).isFalse();
      Tier0VectorDeltaBuffer<Object> term2PathBBuffer =
          pathBRegistry.getBuffer(REGION_PATH, INDEX_NAME, 0);
      assertThat(term2PathBBuffer).isNotNull().isNotSameAs(term1PathBBuffer);
      assertThat(term2PathBBuffer.isPathBWriteHeldByCurrentThread()).isFalse();
      assertThat(pathBRegistry.getOwnershipGeneration(REGION_PATH, INDEX_NAME, 0))
          .isGreaterThan(pathBGenBefore);
      assertThat(pathBRegistry.getOwnershipFenceRetryCount()).isGreaterThanOrEqualTo(1L);

      ArgumentCaptor<TopEntriesCollector> captor =
          ArgumentCaptor.forClass(TopEntriesCollector.class);
      verify(pathBSender).lastResult(captor.capture());
      List<EntryScore<Object>> hits = captor.getValue().getEntries().getHits();
      assertThat(hits).extracting(EntryScore::getKey).containsExactly("k_repromoted_fuzzy");
    } finally {
      ownershipPool.shutdownNow();
    }

    // Sub-case (c): Permanent demotion during acquireSearcher() without re-promotion MUST fail
    // over via InternalFunctionInvocationTargetException(PrimaryBucketException) and emit 0 hits.
    Tier0VectorRegistry.removeCache(mockCache);
    Tier0VectorRegistry demoteRegistry = Tier0VectorRegistry.forCache(mockCache);
    AtomicBoolean demoteIsPrimary = new AtomicBoolean(true);
    BucketAdvisor demoteAdvisor = mock(BucketAdvisor.class);
    when(demoteAdvisor.isPrimary()).thenAnswer(inv -> demoteIsPrimary.get());
    BucketRegion demoteBucket = mock(BucketRegion.class);
    when(demoteBucket.isDestroyed()).thenReturn(false);
    when(demoteBucket.getBucketAdvisor()).thenReturn(demoteAdvisor);
    PartitionedRegionDataStore demoteStore = mock(PartitionedRegionDataStore.class);
    when(demoteStore.getLocalBucketById(0)).thenReturn(demoteBucket);
    PartitionedRegion demotePr = mock(PartitionedRegion.class);
    when(demotePr.isDestroyed()).thenReturn(false);
    when(demotePr.getFullPath()).thenReturn(REGION_PATH);
    when(demotePr.getRegionService()).thenReturn(mockCache);
    when(demotePr.getDataStore()).thenReturn(demoteStore);
    when(mockContext.getDataSet()).thenReturn(demotePr);

    Tier0VectorBucketListener<Object, Object[]> demoteListener = new Tier0VectorBucketListener<>(
        INDEX_NAME, pair -> (float[]) pair[0], pair -> Map.of("tag", pair[1]));
    PartitionListener demotePartListener = demoteListener.createPartitionListener();
    demotePartListener.afterRegionCreate(demotePr);

    demoteRegistry.getOrCreateBuffer(REGION_PATH, INDEX_NAME, 0)
        .put("k_demoted", vOldTerm, Map.of("tag", "all"));

    try (ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexWriter writer =
            new IndexWriter(dir, new IndexWriterConfig(new StandardAnalyzer()))) {
      IndexRepositoryImpl demoteRepo = new IndexRepositoryImpl(
          demotePr, writer, serializer, mockStats, demotePr, null, "lock", mockIndex) {
        @Override
        protected IndexSearcher acquireSearcher() throws IOException {
          IndexSearcher s = super.acquireSearcher();
          demoteIsPrimary.set(false);
          demotePartListener.afterSecondary(0);
          return s;
        }
      };
      when(mockRepoManager.getRepositoriesByBucket(eq(mockContext), eq(false)))
          .thenReturn(Map.of(0, demoteRepo));
      ResultSender<TopEntriesCollector> demoteSender = mock(ResultSender.class);
      when(mockContext.getResultSender()).thenReturn(demoteSender);

      HybridKnnQueryContext ctx = HybridKnnQueryContext
          .builder(INDEX_NAME, VECTOR_FIELD, queryVec, 5)
          .similarityFunction(VectorSimilarityFunction.COSINE)
          .build();
      when(mockContext.getArguments()).thenReturn(ctx);

      assertThatThrownBy(() -> new DistributedHybridKnnFunction().execute(mockContext))
          .isInstanceOf(InternalFunctionInvocationTargetException.class)
          .hasCauseInstanceOf(PrimaryBucketException.class);
      verify(demoteSender, never()).lastResult(any());
      assertThat(demoteRegistry.getOwnershipFenceRetryCount()).isGreaterThanOrEqualTo(1L);
    } finally {
      when(mockContext.getDataSet()).thenReturn(mockRegion);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void retryExhaustionThrowsInsteadOfMergingUnvalidatedCut() throws Exception {
    float[] queryVec = new float[] {1f, 0f, 0f, 0f};
    float[] vec = new float[] {0.95f, (float) Math.sqrt(1.0 - 0.95 * 0.95), 0f, 0f};
    LuceneSerializer<float[]> serializer = (idx, v) -> {
      Document doc = new Document();
      doc.add(new KnnFloatVectorField(VECTOR_FIELD, v, VectorSimilarityFunction.COSINE));
      doc.add(new StringField("category", "books", Field.Store.YES));
      return Collections.singletonList(doc);
    };

    long savedWaitMs = DistributedHybridKnnFunction.maxConcurrentFlushWaitMillisForTest;
    DistributedHybridKnnFunction.maxConcurrentFlushWaitMillisForTest = 25L;
    try {
      // Sub-case (a): Repeated ownership generation changes exhaust MAX_OWNERSHIP_FENCE_RETRIES
      // -> MUST explicitly throw PrimaryBucketException (wrapped) and emit ZERO hits.
      Tier0VectorRegistry.removeCache(mockCache);
      Tier0VectorRegistry ownRegistry = Tier0VectorRegistry.forCache(mockCache);
      ownRegistry.getOrCreateBuffer(REGION_PATH, INDEX_NAME, 0)
          .put("k-exhaust", vec, Map.of("category", "books"));

      try (ByteBuffersDirectory dir = new ByteBuffersDirectory();
          IndexWriter writer =
              new IndexWriter(dir, new IndexWriterConfig(new StandardAnalyzer()))) {
        java.util.concurrent.atomic.AtomicInteger ownAcquires =
            new java.util.concurrent.atomic.AtomicInteger(0);
        IndexRepositoryImpl flappingRepo = new IndexRepositoryImpl(
            mockRegion, writer, serializer, mockStats, mockRegion, null, "lock", mockIndex) {
          @Override
          protected IndexSearcher acquireSearcher() throws IOException {
            IndexSearcher s = super.acquireSearcher();
            ownAcquires.incrementAndGet();
            ownRegistry.advanceOwnershipGeneration(REGION_PATH, INDEX_NAME, 0);
            return s;
          }
        };
        when(mockRepoManager.getRepositoriesByBucket(eq(mockContext), eq(false)))
            .thenReturn(Map.of(0, flappingRepo));
        ResultSender<TopEntriesCollector> ownSender = mock(ResultSender.class);
        when(mockContext.getResultSender()).thenReturn(ownSender);

        HybridKnnQueryContext ctx = HybridKnnQueryContext
            .builder(INDEX_NAME, VECTOR_FIELD, queryVec, 5)
            .similarityFunction(VectorSimilarityFunction.COSINE)
            .build();
        when(mockContext.getArguments()).thenReturn(ctx);

        assertThatThrownBy(() -> new DistributedHybridKnnFunction().execute(mockContext))
            .isInstanceOf(InternalFunctionInvocationTargetException.class)
            .hasCauseInstanceOf(PrimaryBucketException.class);
        verify(ownSender, never()).lastResult(any());
        assertThat(ownAcquires.get())
            .isEqualTo(DistributedHybridKnnFunction.MAX_OWNERSHIP_FENCE_RETRIES);
        assertThat(ownRegistry.getOwnershipFenceRetryCount())
            .isEqualTo(DistributedHybridKnnFunction.MAX_OWNERSHIP_FENCE_RETRIES);
      }

      // Sub-case (b): Repository stuck at odd searcherEpoch (in-progress commit never completes
      // within wait budget) -> MUST explicitly throw instead of merging an unvalidated odd cut!
      Tier0VectorRegistry.removeCache(mockCache);
      Tier0VectorRegistry oddRegistry = Tier0VectorRegistry.forCache(mockCache);
      Tier0VectorDeltaBuffer<Object> oddBuffer =
          oddRegistry.getOrCreateBuffer(REGION_PATH, INDEX_NAME, 0);
      oddBuffer.put("k-odd", vec, Map.of("category", "books"));

      IndexRepository stuckOddEpochRepo = new IndexRepository() {
        @Override
        public void create(Object key, Object value) {}

        @Override
        public void update(Object key, Object value) {}

        @Override
        public void delete(Object key) {}

        @Override
        public void query(Query query, int limit, IndexResultCollector collector) {
          throw new AssertionError("Must NEVER query while searcherEpoch is odd");
        }

        @Override
        public long getSearcherEpoch() {
          return 1L;
        }

        @Override
        public void commit() {}

        @Override
        public Region<?, ?> getRegion() {
          return mockRegion;
        }

        @Override
        public boolean isClosed() {
          return false;
        }

        @Override
        public IndexWriter getWriter() {
          return null;
        }

        @Override
        public void cleanup() {}
      };
      when(mockRepoManager.getRepositoriesByBucket(eq(mockContext), eq(false)))
          .thenReturn(Map.of(0, stuckOddEpochRepo));
      ResultSender<TopEntriesCollector> oddSender = mock(ResultSender.class);
      when(mockContext.getResultSender()).thenReturn(oddSender);

      HybridKnnQueryContext oddCtx = HybridKnnQueryContext
          .builder(INDEX_NAME, VECTOR_FIELD, queryVec, 5)
          .similarityFunction(VectorSimilarityFunction.COSINE)
          .build();
      when(mockContext.getArguments()).thenReturn(oddCtx);

      assertThatThrownBy(() -> new DistributedHybridKnnFunction().execute(mockContext))
          .isInstanceOf(InternalFunctionInvocationTargetException.class)
          .hasCauseInstanceOf(PrimaryBucketException.class);
      verify(oddSender, never()).lastResult(any());
      assertThat(oddBuffer.isPathBWriteHeldByCurrentThread()).isFalse();

      // Sub-case (c): Path B (unsupported FuzzyQuery) wait exhaustion against a stuck flush ->
      // MUST explicitly throw instead of falling through with un-flushed Tier-0 entries!
      Tier0VectorRegistry.removeCache(mockCache);
      Tier0VectorRegistry pathBRegistry = Tier0VectorRegistry.forCache(mockCache);
      Tier0VectorDeltaBuffer<Object> stuckBuffer =
          pathBRegistry.getOrCreateBuffer(REGION_PATH, INDEX_NAME, 0);
      stuckBuffer.put("k-stuck-fuzzy", vec, Map.of("category", "books"));
      Tier0VectorDeltaBuffer.FlushBatch<Object> stuckBatch = stuckBuffer.beginFlush();
      assertThat(stuckBatch).isNotNull();

      try (ByteBuffersDirectory dir = new ByteBuffersDirectory();
          IndexWriter writer =
              new IndexWriter(dir, new IndexWriterConfig(new StandardAnalyzer()))) {
        IndexRepositoryImpl pathBRepo = new IndexRepositoryImpl(
            mockRegion, writer, serializer, mockStats, mockRegion, null, "lock", mockIndex);
        when(mockRepoManager.getRepositoriesByBucket(eq(mockContext), eq(false)))
            .thenReturn(Map.of(0, pathBRepo));
        ResultSender<TopEntriesCollector> fuzzySender = mock(ResultSender.class);
        when(mockContext.getResultSender()).thenReturn(fuzzySender);

        HybridKnnQueryContext fuzzyCtx = HybridKnnQueryContext
            .builder(INDEX_NAME, VECTOR_FIELD, queryVec, 5)
            .similarityFunction(VectorSimilarityFunction.COSINE)
            .filterQuery("category:boks~1", "category")
            .build();
        when(mockContext.getArguments()).thenReturn(fuzzyCtx);

        assertThatThrownBy(() -> new DistributedHybridKnnFunction().execute(mockContext))
            .isInstanceOf(InternalFunctionInvocationTargetException.class)
            .hasCauseInstanceOf(PrimaryBucketException.class);
        verify(fuzzySender, never()).lastResult(any());
        assertThat(stuckBuffer.isPathBWriteHeldByCurrentThread()).isFalse();
      } finally {
        stuckBuffer.abortFlush(stuckBatch.getEpoch());
      }
    } finally {
      DistributedHybridKnnFunction.maxConcurrentFlushWaitMillisForTest = savedWaitMs;
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void singleFlightFlushLockSerializesAeqCommitEvictAndPathBFlushAbort() throws Exception {
    float[] queryVec = new float[] {1f, 0f, 0f, 0f};
    float[] v1Stale = new float[] {0.30f, (float) Math.sqrt(1.0 - 0.09), 0f, 0f};
    float[] v2Fresh = new float[] {0.99f, (float) Math.sqrt(1.0 - 0.99 * 0.99), 0f, 0f};

    LuceneSerializer<Object[]> serializer = (idx, pair) -> {
      Document doc = new Document();
      doc.add(new KnnFloatVectorField(VECTOR_FIELD, (float[]) pair[0],
          VectorSimilarityFunction.COSINE));
      doc.add(new StringField("category", (String) pair[1], Field.Store.YES));
      return Collections.singletonList(doc);
    };

    // Sub-case (a): Concurrent AEQ LuceneEventListener (holding flushLock while reading v1) vs
    // Path-B FuzzyQuery (holding pathBWrite) MUST serialize single-flight without deadlock and
    // without letting stale AEQ v1 overwrite newer v2 in Lucene or evict v2 from Tier-0!
    Tier0VectorRegistry.removeCache(mockCache);
    Tier0VectorRegistry registry = Tier0VectorRegistry.forCache(mockCache);
    Tier0VectorDeltaBuffer<Object> buffer = registry.getOrCreateBuffer(REGION_PATH, INDEX_NAME, 0);
    buffer.put("k", v1Stale, Map.of("category", "books"));

    java.util.concurrent.ConcurrentHashMap<Object, Object[]> liveRegionValues =
        new java.util.concurrent.ConcurrentHashMap<>();
    liveRegionValues.put("k", new Object[] {v1Stale, "books"});

    CountDownLatch aeqReadV1Entered = new CountDownLatch(1);
    CountDownLatch allowAeqToProceed = new CountDownLatch(1);
    java.util.concurrent.atomic.AtomicInteger activeCommitOrUpdate =
        new java.util.concurrent.atomic.AtomicInteger(0);
    java.util.concurrent.atomic.AtomicInteger maxConcurrentFlushers =
        new java.util.concurrent.atomic.AtomicInteger(0);

    BucketRegion mockBucketRegion = mock(BucketRegion.class);
    when(mockBucketRegion.getId()).thenReturn(0);
    when(mockBucketRegion.isDestroyed()).thenReturn(false);

    Region<Object, Object> aeqRegion = mock(Region.class);
    when(aeqRegion.getFullPath()).thenReturn(REGION_PATH);
    when(aeqRegion.getEntry("k")).thenAnswer(inv -> {
      Object[] snapshotVal = liveRegionValues.get("k");
      EntrySnapshot es = mock(EntrySnapshot.class);
      when(es.getRawValue(true)).thenAnswer(rawInv -> {
        aeqReadV1Entered.countDown();
        allowAeqToProceed.await(5, TimeUnit.SECONDS);
        return snapshotVal;
      });
      return es;
    });

    when(mockRegion.getEntry("k")).thenAnswer(inv -> {
      Object[] val = liveRegionValues.get("k");
      if (val == null) {
        return null;
      }
      EntrySnapshot es = mock(EntrySnapshot.class);
      when(es.getRawValue(true)).thenReturn(val);
      return es;
    });
    when(mockRegion.get("k")).thenAnswer(inv -> liveRegionValues.get("k"));

    when(mockIndex.getRegionPath()).thenReturn(REGION_PATH);
    when(mockIndex.getName()).thenReturn(INDEX_NAME);

    ExecutorService pool = Executors.newFixedThreadPool(2);
    try (ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(new StandardAnalyzer()))) {
      IndexRepositoryImpl repo = new IndexRepositoryImpl(
          mockBucketRegion, writer, serializer, mockStats, mockBucketRegion, null, "lock",
          mockIndex) {
        @Override
        public void update(Object key, Object value) throws IOException {
          int active = activeCommitOrUpdate.incrementAndGet();
          maxConcurrentFlushers.accumulateAndGet(active, Math::max);
          try {
            super.update(key, value);
          } finally {
            activeCommitOrUpdate.decrementAndGet();
          }
        }

        @Override
        public synchronized void commitAndRefreshBlocking() throws IOException {
          int active = activeCommitOrUpdate.incrementAndGet();
          maxConcurrentFlushers.accumulateAndGet(active, Math::max);
          try {
            super.commitAndRefreshBlocking();
          } finally {
            activeCommitOrUpdate.decrementAndGet();
          }
        }
      };

      when(mockRepoManager.getIndex()).thenReturn(mockIndex);
      when(mockRepoManager.getRepository(eq(aeqRegion), eq("k"), any())).thenReturn(repo);
      when(mockRepoManager.getRepositoriesByBucket(eq(mockContext), eq(false)))
          .thenReturn(Map.of(0, repo));

      AsyncEvent aeqEvent = mock(AsyncEvent.class);
      when(aeqEvent.getRegion()).thenReturn(aeqRegion);
      when(aeqEvent.getKey()).thenReturn("k");

      LuceneEventListener aeqListener = new LuceneEventListener(mockCache, mockRepoManager);
      Future<Boolean> aeqFuture = pool.submit(() -> aeqListener.processEvents(List.of(aeqEvent)));

      assertThat(aeqReadV1Entered.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(buffer.isFlushLocked()).isTrue();

      // Client updates "k" -> v2 while AEQ is paused inside flushLock holding stale v1
      liveRegionValues.put("k", new Object[] {v2Fresh, "books"});
      buffer.put("k", v2Fresh, Map.of("category", "books"));

      ResultSender<TopEntriesCollector> pathBSender = mock(ResultSender.class);
      when(mockContext.getResultSender()).thenReturn(pathBSender);
      HybridKnnQueryContext fuzzyCtx = HybridKnnQueryContext
          .builder(INDEX_NAME, VECTOR_FIELD, queryVec, 5)
          .similarityFunction(VectorSimilarityFunction.COSINE)
          .filterQuery("category:boks~1", "category")
          .build();
      when(mockContext.getArguments()).thenReturn(fuzzyCtx);

      Future<?> pathBFuture = pool.submit(() -> new DistributedHybridKnnFunction().execute(
          mockContext));

      // Let Path B contend on flushLock (must drop pathBWrite so AEQ evictCommitted won't
      // deadlock), then release AEQ.
      Thread.sleep(30L);
      assertThat(pathBFuture.isDone()).isFalse();
      allowAeqToProceed.countDown();

      assertThat(aeqFuture.get(5, TimeUnit.SECONDS)).isTrue();
      pathBFuture.get(5, TimeUnit.SECONDS);

      assertThat(maxConcurrentFlushers.get()).isEqualTo(1);
      assertThat(buffer.isFlushLocked()).isFalse();
      assertThat(buffer.isPathBWriteHeldByCurrentThread()).isFalse();
      assertThat(buffer.getTotalSize()).isEqualTo(0);

      ArgumentCaptor<TopEntriesCollector> captor =
          ArgumentCaptor.forClass(TopEntriesCollector.class);
      verify(pathBSender).lastResult(captor.capture());
      List<EntryScore<Object>> hits = captor.getValue().getEntries().getHits();
      assertThat(hits).extracting(EntryScore::getKey).containsExactly("k");
      float expectedScoreV2 =
          VectorSimilarityScorer.score(queryVec, v2Fresh, VectorSimilarityFunction.COSINE);
      assertThat(hits.get(0).getScore()).isCloseTo(expectedScoreV2, within(1e-4f));

      // Sub-case (b): Competing flushBucket abortFlush (post-commit IOException) vs concurrent
      // AEQ LuceneEventListener batch MUST serialize strictly under flushLock!
      buffer.put("k-abort", v2Fresh, Map.of("category", "books"));
      liveRegionValues.put("k-abort", new Object[] {v2Fresh, "books"});
      CountDownLatch abortFlushInsideCommit = new CountDownLatch(1);
      CountDownLatch allowAbortToThrow = new CountDownLatch(1);
      AtomicBoolean failFirstCommit = new AtomicBoolean(true);
      java.util.concurrent.atomic.AtomicInteger subBActive =
          new java.util.concurrent.atomic.AtomicInteger(0);
      java.util.concurrent.atomic.AtomicInteger subBMaxConcurrent =
          new java.util.concurrent.atomic.AtomicInteger(0);

      IndexRepositoryImpl abortingRepo = new IndexRepositoryImpl(
          mockBucketRegion, writer, serializer, mockStats, mockBucketRegion, null, "lock",
          mockIndex) {
        @Override
        public synchronized void commitAndRefreshBlocking() throws IOException {
          int active = subBActive.incrementAndGet();
          subBMaxConcurrent.accumulateAndGet(active, Math::max);
          try {
            if (failFirstCommit.compareAndSet(true, false)) {
              abortFlushInsideCommit.countDown();
              try {
                allowAbortToThrow.await(5, TimeUnit.SECONDS);
              } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
              }
              throw new IOException("injected post-update refresh failure");
            }
            super.commitAndRefreshBlocking();
          } finally {
            subBActive.decrementAndGet();
          }
        }
      };

      Region<Object, Object> aeqRegionB = mock(Region.class);
      when(aeqRegionB.getFullPath()).thenReturn(REGION_PATH);
      when(aeqRegionB.getEntry("k-abort")).thenAnswer(inv -> {
        EntrySnapshot es = mock(EntrySnapshot.class);
        when(es.getRawValue(true)).thenReturn(liveRegionValues.get("k-abort"));
        return es;
      });
      when(mockRepoManager.getRepository(eq(aeqRegionB), eq("k-abort"), any()))
          .thenReturn(abortingRepo);
      AsyncEvent aeqEventB = mock(AsyncEvent.class);
      when(aeqEventB.getRegion()).thenReturn(aeqRegionB);
      when(aeqEventB.getKey()).thenReturn("k-abort");

      Future<?> failingFlushFuture = pool.submit(() -> {
        assertThatThrownBy(() -> registry.flushBucket(REGION_PATH, INDEX_NAME, 0, abortingRepo,
            entry -> liveRegionValues.get(entry.getKey()))).isInstanceOf(IOException.class);
      });
      assertThat(abortFlushInsideCommit.await(5, TimeUnit.SECONDS)).isTrue();

      Future<Boolean> aeqWaitFuture =
          pool.submit(() -> aeqListener.processEvents(List.of(aeqEventB)));
      Thread.sleep(25L);
      // AEQ MUST block on flushLock until failingFlush finishes abortFlush() and unlocks!
      assertThat(aeqWaitFuture.isDone()).isFalse();

      allowAbortToThrow.countDown();
      failingFlushFuture.get(5, TimeUnit.SECONDS);
      assertThat(aeqWaitFuture.get(5, TimeUnit.SECONDS)).isTrue();

      assertThat(subBMaxConcurrent.get()).isEqualTo(1);
      assertThat(buffer.isFlushing()).isFalse();
      assertThat(buffer.isFlushLocked()).isFalse();
      assertThat(buffer.getTotalSize()).isEqualTo(0);
    } finally {
      pool.shutdownNow();
    }
  }

  @SuppressWarnings("unchecked")
  private List<EntryScore<Object>> executeHybridQuery(float[] query, int k, int rerankFactor) {
    ResultSender<TopEntriesCollector> localSender = mock(ResultSender.class);
    when(mockContext.getResultSender()).thenReturn(localSender);
    HybridKnnQueryContext ctx = HybridKnnQueryContext
        .builder(INDEX_NAME, VECTOR_FIELD, query, k)
        .similarityFunction(VectorSimilarityFunction.COSINE)
        .rerankOversampleFactor(rerankFactor)
        .build();
    when(mockContext.getArguments()).thenReturn(ctx);

    new DistributedHybridKnnFunction().execute(mockContext);

    ArgumentCaptor<TopEntriesCollector> captor = ArgumentCaptor.forClass(TopEntriesCollector.class);
    verify(localSender).lastResult(captor.capture());
    return captor.getValue().getEntries().getHits();
  }

  private static double recall(Set<String> groundTruth, List<EntryScore<Object>> hits) {
    int matched = 0;
    for (EntryScore<Object> h : hits) {
      if (groundTruth.contains(h.getKey())) {
        matched++;
      }
    }
    return (double) matched / groundTruth.size();
  }

  private static float[] randomUnitVector(Random rng, int dim) {
    float[] v = new float[dim];
    for (int i = 0; i < dim; i++) {
      v[i] = (float) rng.nextGaussian();
    }
    normalize(v);
    return v;
  }

  private static void normalize(float[] v) {
    double normSq = 0.0;
    for (float x : v) {
      normSq += (double) x * x;
    }
    float inv = (float) (1.0 / Math.sqrt(Math.max(normSq, 1e-12)));
    for (int i = 0; i < v.length; i++) {
      v[i] *= inv;
    }
  }
}
