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
package org.apache.geode.cache.lucene.internal.repository.serializer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.VectorIndexMetadata;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class VectorCodecRecallJUnitTest {

  private static class ClusteredItem {
    @SuppressWarnings("unused")
    private final String category;
    @SuppressWarnings("unused")
    private final float[] embedding;

    ClusteredItem(String category, float[] embedding) {
      this.category = category;
      this.embedding = embedding;
    }
  }

  @Test
  public void hnswFp32RoundTripAchievesHighRecallAcrossAllSimilarityMetrics() throws Exception {
    for (VectorSimilarityFunction similarity : new VectorSimilarityFunction[] {
        VectorSimilarityFunction.COSINE,
        VectorSimilarityFunction.DOT_PRODUCT,
        VectorSimilarityFunction.EUCLIDEAN}) {
      double recall = runSyntheticClusterRecallTrial(similarity, false, 64, 400, 8, 10, 20);
      assertThat(recall)
          .as("FP32 HNSW Recall@10 for %s", similarity)
          .isGreaterThanOrEqualTo(0.95);
    }
  }

  @Test
  public void hnswSq8QuantizedCodecRoundTripAchievesHighRecallAcrossAllSimilarityMetrics()
      throws Exception {
    for (VectorSimilarityFunction similarity : new VectorSimilarityFunction[] {
        VectorSimilarityFunction.COSINE,
        VectorSimilarityFunction.DOT_PRODUCT,
        VectorSimilarityFunction.EUCLIDEAN}) {
      double recall = runSyntheticClusterRecallTrial(similarity, true, 64, 400, 8, 10, 20);
      assertThat(recall)
          .as("SQ8 HNSW Recall@10 for %s", similarity)
          .isGreaterThanOrEqualTo(0.90);
    }
  }

  @Test
  public void mixedSq8AndFp32VectorFieldsRoundTripWithReopenedReader() throws Exception {
    int dim = 32;
    Map<String, VectorIndexMetadata> vectorFields = new LinkedHashMap<>();
    VectorIndexMetadata sq8Meta =
        new VectorIndexMetadata("sq8Vec", dim, VectorSimilarityFunction.COSINE, 16, 100, true);
    VectorIndexMetadata fp32Meta =
        new VectorIndexMetadata("fp32Vec", dim, VectorSimilarityFunction.EUCLIDEAN, 16, 100, false);
    vectorFields.put("sq8Vec", sq8Meta);
    vectorFields.put("fp32Vec", fp32Meta);

    LuceneIndex index = mock(LuceneIndex.class);
    when(index.getFieldNames()).thenReturn(new String[] {"category"});
    when(index.getVectorFields()).thenReturn(vectorFields);

    HeterogeneousLuceneSerializer serializer = new HeterogeneousLuceneSerializer();
    Random rng = new Random(1337L);
    float[][] vectors = generateUnitVectors(rng, 100, dim);

    try (ByteBuffersDirectory dir = new ByteBuffersDirectory()) {
      IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
      config.setCodec(VectorFieldMapper.createCodec(vectorFields));

      try (IndexWriter writer = new IndexWriter(dir, config)) {
        for (int i = 0; i < vectors.length; i++) {
          PdxInstance pdx = mock(PdxInstance.class);
          when(pdx.hasField("category")).thenReturn(true);
          when(pdx.getField("category")).thenReturn(i % 2 == 0 ? "even" : "odd");
          when(pdx.hasField("sq8Vec")).thenReturn(true);
          when(pdx.getField("sq8Vec")).thenReturn(vectors[i]);
          when(pdx.hasField("fp32Vec")).thenReturn(true);
          when(pdx.getField("fp32Vec")).thenReturn(vectors[i]);

          Document doc = serializer.toDocuments(index, pdx).iterator().next();
          SerializerUtil.addKey("doc-" + i, doc);
          writer.addDocument(doc);
          verify(pdx, never()).getObject();
        }
        writer.commit();
      }

      // Reopen with stock DirectoryReader to verify standard Lucene SPI compatibility
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);

        // Query SQ8 field for exact doc-42 vector
        TopDocs sq8Hits = searcher.search(new KnnFloatVectorQuery("sq8Vec", vectors[42], 5), 5);
        assertThat(sq8Hits.scoreDocs).hasSize(5);
        Document topSq8Doc = searcher.storedFields().document(sq8Hits.scoreDocs[0].doc);
        assertThat(SerializerUtil.getKey(topSq8Doc)).isEqualTo("doc-42");

        // Query FP32 field with pre-filter category:odd for doc-43 vector
        TopDocs filteredHits = searcher.search(
            new KnnFloatVectorQuery("fp32Vec", vectors[43], 5,
                new TermQuery(new Term("category", "odd"))),
            5);
        assertThat(filteredHits.scoreDocs).hasSize(5);
        Document topFilteredDoc = searcher.storedFields().document(filteredHits.scoreDocs[0].doc);
        assertThat(SerializerUtil.getKey(topFilteredDoc)).isEqualTo("doc-43");
        for (ScoreDoc sd : filteredHits.scoreDocs) {
          String key = (String) SerializerUtil.getKey(searcher.storedFields().document(sd.doc));
          int id = Integer.parseInt(key.substring("doc-".length()));
          assertThat(id % 2).isEqualTo(1);
        }
      }
    }
  }

  @Test
  public void highDimension1536Sq8VectorRoundTripSucceeds() throws Exception {
    int dim = 1536;
    VectorIndexMetadata meta =
        new VectorIndexMetadata("embedding", dim, VectorSimilarityFunction.COSINE, 16, 100, true);
    Map<String, VectorIndexMetadata> vectorFields = Collections.singletonMap("embedding", meta);

    Random rng = new Random(2026L);
    float[][] vectors = generateUnitVectors(rng, 40, dim);

    try (ByteBuffersDirectory dir = new ByteBuffersDirectory()) {
      IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
      config.setCodec(VectorFieldMapper.createCodec(vectorFields));

      try (IndexWriter writer = new IndexWriter(dir, config)) {
        for (int i = 0; i < vectors.length; i++) {
          Document doc = new Document();
          SerializerUtil.addKey("hi-" + i, doc);
          VectorFieldMapper.addVectorField(doc, meta,
              new ClusteredItem("c0", vectors[i]));
          writer.addDocument(doc);
        }
        writer.commit();
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs hits = searcher.search(new KnnFloatVectorQuery("embedding", vectors[17], 3), 3);
        assertThat(hits.scoreDocs).hasSize(3);
        Document topDoc = searcher.storedFields().document(hits.scoreDocs[0].doc);
        assertThat(SerializerUtil.getKey(topDoc)).isEqualTo("hi-17");
      }
    }
  }

  private double runSyntheticClusterRecallTrial(VectorSimilarityFunction similarity, boolean sq8,
      int dim, int numDocs, int numClusters, int k, int numQueries) throws Exception {
    Random rng = new Random(42L + similarity.ordinal() * 31L + (sq8 ? 7L : 0L));

    float[][] centroids = generateUnitVectors(rng, numClusters, dim);
    float[][] corpus = new float[numDocs][dim];
    for (int i = 0; i < numDocs; i++) {
      float[] centroid = centroids[i % numClusters];
      corpus[i] = perturbAndNormalize(rng, centroid, 0.15f);
    }

    VectorIndexMetadata meta =
        new VectorIndexMetadata("embedding", dim, similarity, 32, 200, sq8);
    Map<String, VectorIndexMetadata> vectorFields = Collections.singletonMap("embedding", meta);

    LuceneIndex index = mock(LuceneIndex.class);
    when(index.getFieldNames()).thenReturn(new String[] {"category"});
    when(index.getVectorFields()).thenReturn(vectorFields);
    HeterogeneousLuceneSerializer serializer = new HeterogeneousLuceneSerializer();

    try (ByteBuffersDirectory dir = new ByteBuffersDirectory()) {
      IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
      config.setCodec(VectorFieldMapper.createCodec(vectorFields));

      try (IndexWriter writer = new IndexWriter(dir, config)) {
        for (int i = 0; i < numDocs; i++) {
          ClusteredItem item = new ClusteredItem("cluster-" + (i % numClusters), corpus[i]);
          Document doc = serializer.toDocuments(index, item).iterator().next();
          SerializerUtil.addKey(i, doc);
          writer.addDocument(doc);
        }
        writer.forceMerge(1);
        writer.commit();
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        double totalRecall = 0.0;
        for (int q = 0; q < numQueries; q++) {
          float[] queryVec = perturbAndNormalize(rng, centroids[q % numClusters], 0.12f);
          Set<Integer> exactTopK = computeExactTopK(corpus, queryVec, similarity, k);

          TopDocs annHits = searcher.search(new KnnFloatVectorQuery("embedding", queryVec, k), k);
          Set<Integer> annTopK = new HashSet<>();
          for (ScoreDoc sd : annHits.scoreDocs) {
            Document doc = searcher.storedFields().document(sd.doc);
            annTopK.add((Integer) SerializerUtil.getKey(doc));
          }

          int overlap = 0;
          for (Integer id : annTopK) {
            if (exactTopK.contains(id)) {
              overlap++;
            }
          }
          totalRecall += (double) overlap / k;
        }
        return totalRecall / numQueries;
      }
    }
  }

  private static Set<Integer> computeExactTopK(float[][] corpus, float[] query,
      VectorSimilarityFunction similarity, int k) {
    List<int[]> indexed = new ArrayList<>(corpus.length);
    float[] scores = new float[corpus.length];
    for (int i = 0; i < corpus.length; i++) {
      scores[i] = similarity.compare(query, corpus[i]);
      indexed.add(new int[] {i});
    }
    indexed.sort(Comparator.comparingDouble((int[] a) -> -scores[a[0]]));
    Set<Integer> topK = new HashSet<>();
    for (int i = 0; i < Math.min(k, indexed.size()); i++) {
      topK.add(indexed.get(i)[0]);
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
