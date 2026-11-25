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
package org.apache.geode.cache.lucene.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.cache.lucene.VectorIndexMetadata;
import org.apache.geode.cache.lucene.internal.repository.IndexRepository;
import org.apache.geode.cache.lucene.internal.repository.IndexRepositoryImpl;
import org.apache.geode.cache.lucene.internal.repository.IndexResultCollector;
import org.apache.geode.cache.lucene.internal.repository.serializer.HeterogeneousLuceneSerializer;
import org.apache.geode.cache.lucene.internal.repository.serializer.SerializerUtil;
import org.apache.geode.cache.lucene.internal.repository.serializer.VectorFieldMapper;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class VectorSegmentCompactionJUnitTest {

  private static class VectorDoc {
    @SuppressWarnings("unused")
    private final String category;
    @SuppressWarnings("unused")
    private final float[] embedding;

    VectorDoc(String category, float[] embedding) {
      this.category = category;
      this.embedding = embedding;
    }
  }

  @Test
  public void vectorIndexConfiguresTunedTieredMergePolicyRamBufferAndConcurrentScheduler() {
    VectorIndexMetadata meta =
        new VectorIndexMetadata("embedding", 64, VectorSimilarityFunction.COSINE, 16, 100, false);
    LuceneIndexForPartitionedRegion indexForPR = mock(LuceneIndexForPartitionedRegion.class);
    when(indexForPR.getAnalyzer()).thenReturn(new StandardAnalyzer());
    when(indexForPR.getVectorFields()).thenReturn(Collections.singletonMap("embedding", meta));

    IndexRepositoryFactory factory = new IndexRepositoryFactory();
    IndexWriterConfig config = factory.createIndexWriterConfig(indexForPR);

    assertThat(config.getRAMBufferSizeMB())
        .isEqualTo(VectorFieldMapper.VECTOR_RAM_BUFFER_SIZE_MB);
    assertThat(config.getMergeScheduler()).isInstanceOf(ConcurrentMergeScheduler.class);
    assertThat(config.getCodec()).isNotSameAs(Codec.getDefault());
    assertThat(config.getMergePolicy()).isInstanceOf(TieredMergePolicy.class);

    TieredMergePolicy policy = (TieredMergePolicy) config.getMergePolicy();
    assertThat(policy.getSegmentsPerTier())
        .isEqualTo(VectorFieldMapper.VECTOR_SEGMENTS_PER_TIER);
    assertThat(policy.getMaxMergeAtOnce())
        .isEqualTo(VectorFieldMapper.VECTOR_MAX_MERGE_AT_ONCE);
    assertThat(policy.getFloorSegmentMB())
        .isEqualTo(VectorFieldMapper.VECTOR_FLOOR_SEGMENT_MB);
    assertThat(policy.getMaxMergedSegmentMB())
        .isEqualTo(VectorFieldMapper.VECTOR_MAX_MERGED_SEGMENT_MB);
  }

  @Test
  public void nonVectorIndexLeavesIndexWriterConfigDefaultsUnchanged() {
    IndexRepositoryFactory factory = new IndexRepositoryFactory();
    IndexWriterConfig baseline = new IndexWriterConfig(new StandardAnalyzer());
    TieredMergePolicy baselinePolicy = (TieredMergePolicy) baseline.getMergePolicy();

    for (Map<String, VectorIndexMetadata> emptyOrNull : new Map[] {Collections.emptyMap(), null}) {
      LuceneIndexForPartitionedRegion indexForPR = mock(LuceneIndexForPartitionedRegion.class);
      when(indexForPR.getAnalyzer()).thenReturn(new StandardAnalyzer());
      when(indexForPR.getVectorFields()).thenReturn(emptyOrNull);

      IndexWriterConfig config = factory.createIndexWriterConfig(indexForPR);

      assertThat(config.getRAMBufferSizeMB()).isEqualTo(baseline.getRAMBufferSizeMB());
      assertThat(config.getCodec()).isSameAs(Codec.getDefault());
      assertThat(config.getMergePolicy()).isInstanceOf(TieredMergePolicy.class);

      TieredMergePolicy policy = (TieredMergePolicy) config.getMergePolicy();
      assertThat(policy.getSegmentsPerTier()).isEqualTo(baselinePolicy.getSegmentsPerTier());
      assertThat(policy.getMaxMergeAtOnce()).isEqualTo(baselinePolicy.getMaxMergeAtOnce());
      assertThat(policy.getFloorSegmentMB()).isEqualTo(baselinePolicy.getFloorSegmentMB());
      assertThat(policy.getMaxMergedSegmentMB()).isEqualTo(baselinePolicy.getMaxMergedSegmentMB());
    }
  }

  @Test
  public void multiBatchCommitsStayBoundedAtMostFourSegmentsWithoutForceMergeAndPreserveRecall()
      throws Exception {
    for (boolean sq8 : new boolean[] {false, true}) {
      int dim = 64;
      int numBatches = 50;
      int batchSize = 100;
      int totalDocs = numBatches * batchSize;
      int numClusters = 64;
      int k = 10;
      int numQueries = 20;

      Random rng = new Random(sq8 ? 9001L : 4001L);
      float[][] centroids = generateUnitVectors(rng, numClusters, dim);
      float[][] corpus = new float[totalDocs][dim];
      for (int i = 0; i < totalDocs; i++) {
        corpus[i] = perturbAndNormalize(rng, centroids[i % numClusters], 0.15f);
      }

      VectorIndexMetadata meta =
          new VectorIndexMetadata("embedding", dim, VectorSimilarityFunction.COSINE, 32, 200, sq8);
      Map<String, VectorIndexMetadata> vectorFields = Collections.singletonMap("embedding", meta);

      LuceneIndex index = mock(LuceneIndex.class);
      when(index.getFieldNames()).thenReturn(new String[] {"category"});
      when(index.getVectorFields()).thenReturn(vectorFields);
      HeterogeneousLuceneSerializer serializer = new HeterogeneousLuceneSerializer();

      try (ByteBuffersDirectory dir = new ByteBuffersDirectory()) {
        IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
        VectorFieldMapper.configureVectorIndexWriterConfig(config, vectorFields);

        try (IndexWriter writer = new IndexWriter(dir, config)) {
          for (int b = 0; b < numBatches; b++) {
            int start = b * batchSize;
            for (int i = start; i < start + batchSize; i++) {
              VectorDoc item = new VectorDoc("c-" + (i % numClusters), corpus[i]);
              Document doc = serializer.toDocuments(index, item).iterator().next();
              SerializerUtil.addKey(i, doc);
              writer.addDocument(doc);
            }
            writer.commit();
          }
          // Wait for natural TieredMergePolicy merges only — never call forceMerge(1).
          writer.maybeMerge();
          ((ConcurrentMergeScheduler) writer.getConfig().getMergeScheduler()).sync();
          writer.commit();
        }

        try (DirectoryReader reader = DirectoryReader.open(dir)) {
          assertThat(reader.numDocs()).isEqualTo(totalDocs);
          assertThat(reader.leaves().size())
              .as("Natural segment count after %d commits (sq8=%s) without forceMerge",
                  numBatches, sq8)
              .isLessThanOrEqualTo(4);

          IndexSearcher searcher = new IndexSearcher(reader);
          double totalRecall = 0.0;
          for (int q = 0; q < numQueries; q++) {
            float[] queryVec = perturbAndNormalize(rng, centroids[q % numClusters], 0.10f);
            Set<Integer> exactTopK =
                computeExactTopK(corpus, queryVec, VectorSimilarityFunction.COSINE, k);

            TopDocs hits = searcher.search(new KnnFloatVectorQuery("embedding", queryVec, k), k);
            int overlap = 0;
            for (ScoreDoc sd : hits.scoreDocs) {
              Document doc = searcher.storedFields().document(sd.doc);
              if (exactTopK.contains(SerializerUtil.getKey(doc))) {
                overlap++;
              }
            }
            totalRecall += (double) overlap / k;
          }

          double meanRecall = totalRecall / numQueries;
          double minExpected = sq8 ? 0.90 : 0.95;
          assertThat(meanRecall)
              .as("Mean Recall@%d after %d commits (sq8=%s)", k, numBatches, sq8)
              .isGreaterThanOrEqualTo(minExpected);
        }
      }
    }
  }

  @Test
  public void maybeCompactSegmentsConsolidatesWhenAboveBudgetAndNoOpsWhenWithinBudget()
      throws Exception {
    int dim = 16;
    VectorIndexMetadata meta =
        new VectorIndexMetadata("embedding", dim, VectorSimilarityFunction.COSINE, 16, 100, false);
    Map<String, VectorIndexMetadata> vectorFields = Collections.singletonMap("embedding", meta);

    LuceneIndex index = mock(LuceneIndex.class);
    when(index.getFieldNames()).thenReturn(new String[0]);
    when(index.getVectorFields()).thenReturn(vectorFields);

    Region<?, ?> fileRegion = mock(Region.class);
    Region<?, ?> userRegion = mock(Region.class);
    when(userRegion.isDestroyed()).thenReturn(false);
    LuceneIndexStats stats = mock(LuceneIndexStats.class);

    Random rng = new Random(77L);
    float[][] vectors = generateUnitVectors(rng, 60, dim);

    try (ByteBuffersDirectory dir = new ByteBuffersDirectory()) {
      // Use Lucene default TieredMergePolicy (segmentsPerTier=10) so 6 commits stay at 6 segments.
      IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
      config.setCodec(VectorFieldMapper.createCodec(vectorFields));
      IndexWriter writer = new IndexWriter(dir, config);

      IndexRepositoryImpl repo = new IndexRepositoryImpl(fileRegion, writer,
          new HeterogeneousLuceneSerializer(), stats, userRegion, null, null, index);
      try {
        assertThatThrownBy(() -> repo.maybeCompactSegments(0))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> repo.maybeCompactSegments(-2))
            .isInstanceOf(IllegalArgumentException.class);

        for (int b = 0; b < 6; b++) {
          for (int i = b * 10; i < (b + 1) * 10; i++) {
            repo.create("k-" + i, new VectorDoc("cat", vectors[i]));
          }
          repo.commit();
        }

        try (DirectoryReader beforeReader = DirectoryReader.open(dir)) {
          assertThat(beforeReader.leaves().size()).isEqualTo(6);
        }

        assertThat(repo.maybeCompactSegments(2)).isTrue();

        try (DirectoryReader afterReader = DirectoryReader.open(dir)) {
          assertThat(afterReader.numDocs()).isEqualTo(60);
          assertThat(afterReader.leaves().size()).isLessThanOrEqualTo(2);
        }

        // Second compaction at the same budget should be a no-op.
        assertThat(repo.maybeCompactSegments(2)).isFalse();

        // Querying after compaction still returns exact top-1 match.
        List<Object> matchedKeys = new ArrayList<>();
        repo.query(new KnnFloatVectorQuery("embedding", vectors[37], 3), 3,
            new IndexResultCollector() {
              @Override
              public String getName() {
                return "test";
              }

              @Override
              public int size() {
                return matchedKeys.size();
              }

              @Override
              public void collect(Object key, float score) {
                matchedKeys.add(key);
              }
            });
        assertThat(matchedKeys).isNotEmpty();
        assertThat(matchedKeys.get(0)).isEqualTo("k-37");
      } finally {
        repo.cleanup();
      }

      // Closed repository returns false without throwing.
      assertThat(repo.maybeCompactSegments(2)).isFalse();
    }
  }

  @Test
  public void partitionedRepositoryManagerCompactsOpenRepositoriesAndSkipsClosed()
      throws Exception {
    InternalLuceneIndex index = mock(InternalLuceneIndex.class);
    LuceneSerializer<?> serializer = mock(LuceneSerializer.class);
    ExecutorService executor = mock(ExecutorService.class);

    PartitionedRepositoryManager manager =
        new PartitionedRepositoryManager(index, serializer, executor);

    IndexRepository needsCompaction = mock(IndexRepository.class);
    when(needsCompaction.isClosed()).thenReturn(false);
    when(needsCompaction.maybeCompactSegments(4)).thenReturn(true);

    IndexRepository alreadyCompact = mock(IndexRepository.class);
    when(alreadyCompact.isClosed()).thenReturn(false);
    when(alreadyCompact.maybeCompactSegments(4)).thenReturn(false);

    IndexRepository closedRepo = mock(IndexRepository.class);
    when(closedRepo.isClosed()).thenReturn(true);

    manager.indexRepositories.put(0, needsCompaction);
    manager.indexRepositories.put(1, alreadyCompact);
    manager.indexRepositories.put(2, closedRepo);

    assertThatThrownBy(() -> manager.maybeCompactSegments(0))
        .isInstanceOf(IllegalArgumentException.class);

    assertThat(manager.maybeCompactSegments(4)).isEqualTo(1);
    verify(needsCompaction).maybeCompactSegments(4);
    verify(alreadyCompact).maybeCompactSegments(4);
    verify(closedRepo, never()).maybeCompactSegments(4);
  }

  private static Set<Integer> computeExactTopK(float[][] corpus, float[] query,
      VectorSimilarityFunction similarity, int k) {
    List<Integer> ids = new ArrayList<>(corpus.length);
    float[] scores = new float[corpus.length];
    for (int i = 0; i < corpus.length; i++) {
      scores[i] = similarity.compare(query, corpus[i]);
      ids.add(i);
    }
    ids.sort(Comparator.comparingDouble((Integer id) -> -scores[id]));
    Set<Integer> topK = new HashSet<>();
    for (int i = 0; i < Math.min(k, ids.size()); i++) {
      topK.add(ids.get(i));
    }
    return topK;
  }

  private static float[][] generateUnitVectors(Random rng, int count, int dim) {
    float[][] result = new float[count][dim];
    for (int i = 0; i < count; i++) {
      float[] v = new float[dim];
      for (int d = 0; d < dim; d++) {
        v[d] = (float) rng.nextGaussian();
      }
      result[i] = normalize(v);
    }
    return result;
  }

  private static float[] perturbAndNormalize(Random rng, float[] base, float sigma) {
    float[] v = new float[base.length];
    for (int i = 0; i < base.length; i++) {
      v[i] = base[i] + (float) (rng.nextGaussian() * sigma);
    }
    return normalize(v);
  }

  private static float[] normalize(float[] v) {
    double sumSq = 0.0;
    for (float x : v) {
      sumSq += (double) x * (double) x;
    }
    float invNorm = (float) (1.0 / Math.sqrt(Math.max(sumSq, 1e-12)));
    float[] out = new float[v.length];
    for (int i = 0; i < v.length; i++) {
      out[i] = v[i] * invNorm;
    }
    return out;
  }
}
