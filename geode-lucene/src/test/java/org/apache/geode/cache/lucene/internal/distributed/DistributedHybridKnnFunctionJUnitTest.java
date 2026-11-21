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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.lucene.LuceneIndexNotFoundException;
import org.apache.geode.cache.lucene.internal.InternalLuceneService;
import org.apache.geode.cache.lucene.internal.LuceneIndexImpl;
import org.apache.geode.cache.lucene.internal.LuceneIndexStats;
import org.apache.geode.cache.lucene.internal.PartitionedRepositoryManager;
import org.apache.geode.cache.lucene.internal.repository.IndexRepository;
import org.apache.geode.cache.lucene.internal.repository.IndexResultCollector;
import org.apache.geode.cache.lucene.internal.vector.Tier0VectorDeltaBuffer;
import org.apache.geode.cache.lucene.internal.vector.Tier0VectorRegistry;
import org.apache.geode.internal.cache.BucketNotFoundException;
import org.apache.geode.internal.cache.InternalCache;
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
  public void setUp() {
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
}
