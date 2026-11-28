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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.lucene.internal.distributed.EntryScore;
import org.apache.geode.cache.lucene.internal.repository.serializer.SerializerUtil;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class Tier0VectorDeltaBufferJUnitTest {

  private static final String VECTOR_FIELD = "embedding";

  @Test
  public void putDestroyAndCardinalityMetricsTrackDualEpochs() {
    Tier0VectorDeltaBuffer<String> buffer = new Tier0VectorDeltaBuffer<>(7);
    assertThat(buffer.getBucketId()).isEqualTo(7);
    assertThat(buffer.getCurrentEpoch()).isEqualTo(0L);
    assertThat(buffer.getActiveSize()).isEqualTo(0);
    assertThat(buffer.getFlushingSize()).isEqualTo(0);
    assertThat(buffer.isFlushing()).isFalse();

    long s1 = buffer.put("k1", new float[] {1f, 0f, 0f}, Map.of("cat", "a"));
    long s2 = buffer.put("k2", new float[] {0f, 1f, 0f}, Map.of("cat", "b"));
    long s3 = buffer.destroy("k3");
    assertThat(s1).isLessThan(s2);
    assertThat(s2).isLessThan(s3);
    assertThat(buffer.getActiveSize()).isEqualTo(3);
    assertThat(buffer.getFlushingSize()).isEqualTo(0);

    // Swap E_active -> E_flushing
    Tier0VectorDeltaBuffer.FlushBatch<String> batch = buffer.beginFlush();
    assertThat(batch).isNotNull();
    assertThat(batch.getEpoch()).isEqualTo(0L);
    assertThat(batch.size()).isEqualTo(3);
    assertThat(buffer.getCurrentEpoch()).isEqualTo(1L);
    assertThat(buffer.getActiveSize()).isEqualTo(0);
    assertThat(buffer.getFlushingSize()).isEqualTo(3);
    assertThat(buffer.isFlushing()).isTrue();

    // Concurrent flush rejected while flushing epoch is open
    assertThatThrownBy(buffer::beginFlush).isInstanceOf(IllegalStateException.class);

    // New writes land in E_active (epoch 1) without disturbing E_flushing (epoch 0)
    buffer.put("k4", new float[] {0f, 0f, 1f});
    assertThat(buffer.getActiveSize()).isEqualTo(1);
    assertThat(buffer.getFlushingSize()).isEqualTo(3);
    assertThat(buffer.getTotalSize()).isEqualTo(4);

    // Complete flush strictly clears E_flushing while retaining E_active
    buffer.completeFlush(batch.getEpoch());
    assertThat(buffer.isFlushing()).isFalse();
    assertThat(buffer.getFlushingSize()).isEqualTo(0);
    assertThat(buffer.getActiveSize()).isEqualTo(1);

    // Empty beginFlush returns null cleanly
    Tier0VectorDeltaBuffer.FlushBatch<String> batch2 = buffer.beginFlush();
    assertThat(batch2).isNotNull();
    buffer.completeFlush(batch2.getEpoch());
    assertThat(buffer.beginFlush()).isNull();
    assertThat(buffer.isFlushing()).isFalse();
  }

  @Test
  public void reverseEpochPrecedenceAndTombstoneMasking() {
    Tier0VectorDeltaBuffer<String> buffer = new Tier0VectorDeltaBuffer<>(0);
    buffer.put("k1", new float[] {1f, 0f, 0f});
    buffer.put("k2", new float[] {1f, 0f, 0f});
    buffer.put("k3", new float[] {0f, 1f, 0f});

    Tier0VectorDeltaBuffer.FlushBatch<String> batch = buffer.beginFlush();
    assertThat(batch).isNotNull();

    // Override k1 with orthogonal vector in E_active, tombstone k2 in E_active
    buffer.put("k1", new float[] {0f, 0f, 1f});
    buffer.destroy("k2");

    Tier0VectorDeltaBuffer.Snapshot<String> snap = buffer.snapshot();
    assertThat(snap.getMaskedKeys()).containsExactlyInAnyOrder("k1", "k2", "k3");
    assertThat(snap.getTombstonedKeys()).containsExactly("k2");
    assertThat(snap.getLiveEntries()).containsOnlyKeys("k1", "k3");
    assertThat(snap.getLiveEntries().get("k1").getVector()).containsExactly(0f, 0f, 1f);

    // Re-put k2 in E_active resurrects it
    buffer.put("k2", new float[] {1f, 0f, 0f});
    Tier0VectorDeltaBuffer.Snapshot<String> snap2 = buffer.snapshot();
    assertThat(snap2.getTombstonedKeys()).isEmpty();
    assertThat(snap2.getLiveEntries()).containsOnlyKeys("k1", "k2", "k3");

    buffer.completeFlush(batch.getEpoch());
  }

  @Test
  public void abortFlushRestoresUnsupersededEntriesWithoutOverwritingNewerWrites() {
    Tier0VectorDeltaBuffer<String> buffer = new Tier0VectorDeltaBuffer<>(1);
    buffer.put("k1", new float[] {1f, 0f});
    buffer.put("k2", new float[] {0f, 1f});

    Tier0VectorDeltaBuffer.FlushBatch<String> batch = buffer.beginFlush();
    assertThat(batch).isNotNull();

    // Update k1 in new E_active before flush aborts
    buffer.put("k1", new float[] {0.5f, 0.5f});

    buffer.abortFlush(batch.getEpoch());
    assertThat(buffer.isFlushing()).isFalse();
    assertThat(buffer.getActiveSize()).isEqualTo(2);

    Tier0VectorDeltaBuffer.Snapshot<String> snap = buffer.snapshot();
    assertThat(snap.getLiveEntries().get("k1").getVector()).containsExactly(0.5f, 0.5f);
    assertThat(snap.getLiveEntries().get("k2").getVector()).containsExactly(0f, 1f);
  }

  @Test
  public void evictCommittedOnlyRemovesStaleSequences() {
    Tier0VectorDeltaBuffer<String> buffer = new Tier0VectorDeltaBuffer<>(2);
    long seq1 = buffer.put("k1", new float[] {1f, 0f});
    long seq2 = buffer.put("k2", new float[] {0f, 1f});

    // k2 receives a newer update after seq2 was captured
    long seq2New = buffer.put("k2", new float[] {0.7f, 0.7f});
    assertThat(seq2New).isGreaterThan(seq2);

    buffer.evictCommitted(Map.of("k1", seq1, "k2", seq2));
    assertThat(buffer.getActiveSize()).isEqualTo(1);
    Tier0VectorDeltaBuffer.Snapshot<String> snap = buffer.snapshot();
    assertThat(snap.getLiveEntries()).containsOnlyKeys("k2");
    assertThat(snap.getLiveEntries().get("k2").getVector()).containsExactly(0.7f, 0.7f);
  }

  @Test
  public void scoreEquivalenceWithLuceneKnnFloatVectorQueryAcrossAllSimilarities()
      throws IOException {
    Random random = new Random(42L);
    int dim = 32;
    int numDocs = 40;
    float[] query = randomUnitVector(random, dim);

    VectorSimilarityFunction[] similarities = new VectorSimilarityFunction[] {
        VectorSimilarityFunction.COSINE,
        VectorSimilarityFunction.DOT_PRODUCT,
        VectorSimilarityFunction.EUCLIDEAN,
        VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT
    };

    for (VectorSimilarityFunction similarity : similarities) {
      Tier0VectorDeltaBuffer<String> buffer = new Tier0VectorDeltaBuffer<>(0);
      try (ByteBuffersDirectory dir = new ByteBuffersDirectory();
          IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(new StandardAnalyzer()));
          SearcherManager searcherManager = new SearcherManager(writer, true, true, null)) {

        for (int i = 0; i < numDocs; i++) {
          String key = "doc-" + i;
          float[] vec = randomUnitVector(random, dim);
          buffer.put(key, vec);

          float scorerVal = VectorSimilarityScorer.score(query, vec, similarity);
          float luceneDirectVal = similarity.compare(query, vec);
          assertThat(scorerVal)
              .as("Direct score mismatch for %s key=%s", similarity, key)
              .isCloseTo(luceneDirectVal, within(1e-5f));

          Document doc = new Document();
          SerializerUtil.addKey(key, doc);
          doc.add(new KnnFloatVectorField(VECTOR_FIELD, vec, similarity));
          writer.addDocument(doc);
        }
        writer.commit();
        searcherManager.maybeRefreshBlocking();

        List<EntryScore<String>> allTier0Hits = buffer.search(query, numDocs, similarity, null);
        assertThat(allTier0Hits).hasSize(numDocs);
        Map<String, Float> tier0ScoreByKey = new HashMap<>();
        for (EntryScore<String> hit : allTier0Hits) {
          tier0ScoreByKey.put(hit.getKey(), hit.getScore());
        }

        IndexSearcher searcher = searcherManager.acquire();
        try {
          TopDocs luceneTopDocs =
              searcher.search(new KnnFloatVectorQuery(VECTOR_FIELD, query, numDocs), numDocs);
          assertThat(luceneTopDocs.scoreDocs).hasSize(numDocs);

          for (int rank = 0; rank < 10; rank++) {
            ScoreDoc sd = luceneTopDocs.scoreDocs[rank];
            String luceneKey =
                (String) SerializerUtil.getKey(searcher.storedFields().document(sd.doc));
            EntryScore<String> t0 = allTier0Hits.get(rank);
            assertThat(tier0ScoreByKey.get(luceneKey))
                .as("Tier-0 vs Lucene KnnFloatVectorQuery score mismatch for %s key=%s", similarity,
                    luceneKey)
                .isCloseTo(sd.score, within(1e-5f));
            assertThat(t0.getScore())
                .as("Rank %d score mismatch for %s", rank, similarity)
                .isCloseTo(sd.score, within(1e-5f));
          }
        } finally {
          searcherManager.release(searcher);
        }
      }
    }
  }

  @Test
  public void tombstoneMasksCommittedLuceneDocuments() throws IOException {
    float[] query = new float[] {1f, 0f, 0f, 0f};
    Tier0VectorDeltaBuffer<String> buffer = new Tier0VectorDeltaBuffer<>(0);

    try (ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(new StandardAnalyzer()));
        SearcherManager searcherManager = new SearcherManager(writer, true, true, null)) {

      // Commit 3 documents into Lucene; k0 is exact match to query
      addVectorDoc(writer, "k0", new float[] {1f, 0f, 0f, 0f}, VectorSimilarityFunction.COSINE);
      addVectorDoc(writer, "k1", new float[] {0.8f, 0.6f, 0f, 0f}, VectorSimilarityFunction.COSINE);
      addVectorDoc(writer, "k2", new float[] {0.6f, 0.8f, 0f, 0f}, VectorSimilarityFunction.COSINE);
      writer.commit();
      searcherManager.maybeRefreshBlocking();

      // Tombstone k0 and add new k3 in Tier-0 (before flush to Lucene)
      buffer.destroy("k0");
      buffer.put("k3", new float[] {0.9f, 0.4358899f, 0f, 0f});

      Tier0VectorDeltaBuffer.Snapshot<String> snap = buffer.snapshot();
      Set<String> masked = snap.getMaskedKeys();

      // Pre-filter Lucene excluding masked keys
      BooleanQuery.Builder filterBuilder = new BooleanQuery.Builder();
      filterBuilder.add(new org.apache.lucene.search.MatchAllDocsQuery(),
          BooleanClause.Occur.FILTER);
      for (String mk : masked) {
        filterBuilder.add(new TermQuery(SerializerUtil.toKeyTerm(mk)),
            BooleanClause.Occur.MUST_NOT);
      }

      List<EntryScore<String>> combined = new ArrayList<>();
      IndexSearcher searcher = searcherManager.acquire();
      try {
        TopDocs topDocs = searcher.search(
            new KnnFloatVectorQuery(VECTOR_FIELD, query, 3, filterBuilder.build()), 3);
        for (ScoreDoc sd : topDocs.scoreDocs) {
          String k = (String) SerializerUtil.getKey(searcher.storedFields().document(sd.doc));
          if (!masked.contains(k)) {
            combined.add(new EntryScore<>(k, sd.score));
          }
        }
      } finally {
        searcherManager.release(searcher);
      }
      combined.addAll(snap.search(query, 3, VectorSimilarityFunction.COSINE, null));
      combined.sort((a, b) -> Float.compare(b.getScore(), a.getScore()));

      assertThat(combined).extracting(EntryScore::getKey).containsExactly("k3", "k1", "k2");
      assertThat(combined).extracting(EntryScore::getKey).doesNotContain("k0");
    }
  }

  @Test
  public void zeroVisibilityGapUnderConcurrentWritesSwapCommitAndRefresh() throws Exception {
    final int dim = 8;
    final int numWriters = 4;
    final int writesPerWriter = 80;
    final float[] query = new float[] {1f, 0f, 0f, 0f, 0f, 0f, 0f, 0f};

    Tier0VectorDeltaBuffer<String> buffer = new Tier0VectorDeltaBuffer<>(0);
    // Keys whose put() has returned and therefore MUST be visible in Tier0 U Lucene at all times
    Set<String> acknowledgedKeys = ConcurrentHashMap.newKeySet();
    AtomicBoolean writersDone = new AtomicBoolean(false);
    AtomicInteger flushCycles = new AtomicInteger(0);
    AtomicInteger visibilityGaps = new AtomicInteger(0);

    try (ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(new StandardAnalyzer()));
        SearcherManager searcherManager = new SearcherManager(writer, true, true, null)) {

      ExecutorService pool = Executors.newFixedThreadPool(numWriters + 3);
      try {
        // Continuous flusher implementing Theorem 2 protocol
        Future<?> flusherFuture = pool.submit(() -> {
          while (!writersDone.get() || buffer.getActiveSize() > 0) {
            Tier0VectorDeltaBuffer.FlushBatch<String> batch = buffer.beginFlush();
            if (batch == null) {
              Thread.yield();
              continue;
            }
            try {
              for (Tier0VectorDeltaBuffer.DeltaEntry<String> entry : batch.getEntries().values()) {
                if (entry.isTombstone()) {
                  writer.deleteDocuments(SerializerUtil.toKeyTerm(entry.getKey()));
                } else {
                  Document doc = new Document();
                  SerializerUtil.addKey(entry.getKey(), doc);
                  doc.add(new KnnFloatVectorField(VECTOR_FIELD, entry.getVector(),
                      VectorSimilarityFunction.COSINE));
                  writer.updateDocument(SerializerUtil.toKeyTerm(entry.getKey()), doc);
                }
              }
              writer.commit();
              searcherManager.maybeRefreshBlocking();
              buffer.completeFlush(batch.getEpoch());
              flushCycles.incrementAndGet();
            } catch (IOException e) {
              buffer.abortFlush(batch.getEpoch());
              throw new RuntimeException(e);
            }
          }
        });

        // Concurrent readers verifying Theorem 2 zero-gap visibility on every snapshot
        List<Future<?>> readerFutures = new ArrayList<>();
        for (int r = 0; r < 2; r++) {
          readerFutures.add(pool.submit(() -> {
            while (!writersDone.get() || buffer.getTotalSize() > 0) {
              // Capture acknowledged set BEFORE snapshot+search to test read-after-write
              List<String> mustBeVisible = new ArrayList<>(acknowledgedKeys);
              if (mustBeVisible.isEmpty()) {
                Thread.yield();
                continue;
              }

              // Theorem 2 ordering: snapshot Tier-0 FIRST, THEN acquire Lucene searcher
              Tier0VectorDeltaBuffer.Snapshot<String> snap = buffer.snapshot();
              Set<String> observed = new HashSet<>(snap.getLiveEntries().keySet());

              try {
                IndexSearcher searcher = searcherManager.acquire();
                try {
                  int maxDocs = Math.max(1, searcher.getIndexReader().numDocs());
                  TopDocs topDocs = searcher.search(
                      new KnnFloatVectorQuery(VECTOR_FIELD, query, maxDocs), maxDocs);
                  for (ScoreDoc sd : topDocs.scoreDocs) {
                    String k = (String) SerializerUtil
                        .getKey(searcher.storedFields().document(sd.doc));
                    if (!snap.isTombstoned(k)) {
                      observed.add(k);
                    }
                  }
                } finally {
                  searcherManager.release(searcher);
                }
              } catch (IOException e) {
                throw new RuntimeException(e);
              }

              for (String ackKey : mustBeVisible) {
                if (!observed.contains(ackKey)) {
                  visibilityGaps.incrementAndGet();
                }
              }
            }
          }));
        }

        // Concurrent writers putting distinct keys
        List<Future<?>> writerFutures = new ArrayList<>();
        for (int w = 0; w < numWriters; w++) {
          final int writerId = w;
          writerFutures.add(pool.submit(() -> {
            Random rng = new Random(1000L + writerId);
            for (int i = 0; i < writesPerWriter; i++) {
              String key = "w" + writerId + "-k" + i;
              float[] vec = randomUnitVector(rng, dim);
              buffer.put(key, vec);
              acknowledgedKeys.add(key);
              if (i % 8 == 0) {
                Thread.yield();
              }
            }
          }));
        }

        for (Future<?> wf : writerFutures) {
          wf.get();
        }
        writersDone.set(true);
        flusherFuture.get();
        for (Future<?> rf : readerFutures) {
          rf.get();
        }
      } finally {
        pool.shutdownNow();
      }

      assertThat(visibilityGaps.get())
          .as("Expected strictly ZERO read-after-write visibility gaps across concurrent flushes")
          .isEqualTo(0);
      assertThat(flushCycles.get()).isGreaterThan(0);
      assertThat(buffer.getTotalSize()).isEqualTo(0);

      // Final verification: all acknowledged keys exist in committed Lucene segments
      IndexSearcher searcher = searcherManager.acquire();
      try {
        assertThat(searcher.getIndexReader().numDocs()).isEqualTo(numWriters * writesPerWriter);
      } finally {
        searcherManager.release(searcher);
      }
    }
  }

  @Test
  public void metadataFilterPredicateMatchesEqualityBooleanAndPointRange() {
    Tier0VectorDeltaBuffer<String> buffer = new Tier0VectorDeltaBuffer<>(0);
    buffer.put("k1", new float[] {1f, 0f},
        Map.of("category", "books", "price", 20, "tag", "scifi"));
    buffer.put("k2", new float[] {0.99f, 0.1f},
        Map.of("category", "books", "price", 150, "tag", "classic"));
    buffer.put("k3", new float[] {0.98f, 0.2f},
        Map.of("category", "music", "price", 25, "tag", "jazz"));

    Query rangeAndNot = new BooleanQuery.Builder()
        .add(IntPoint.newRangeQuery("price_point", 10, 50), BooleanClause.Occur.FILTER)
        .add(new TermQuery(new Term("tag", "jazz")), BooleanClause.Occur.MUST_NOT)
        .build();

    Predicate<Map<String, Object>> pred =
        Tier0MetadataMatcher.buildPredicate(Collections.singletonMap("category", "books"),
            rangeAndNot);

    List<EntryScore<String>> hits =
        buffer.search(new float[] {1f, 0f}, 5, VectorSimilarityFunction.COSINE, pred);
    assertThat(hits).extracting(EntryScore::getKey).containsExactly("k1");
  }

  @Test
  public void evictCommittedRemovesEntriesInFlushingEpochAfterConcurrentSwap() {
    Tier0VectorDeltaBuffer<String> buffer = new Tier0VectorDeltaBuffer<>(4);
    long seq1 = buffer.put("k1", new float[] {1f, 0f});
    long seq2 = buffer.put("k2", new float[] {0f, 1f});

    // beginFlush moves k1 and k2 from E_active to E_flushing
    Tier0VectorDeltaBuffer.FlushBatch<String> batch = buffer.beginFlush();
    assertThat(batch).isNotNull();
    assertThat(buffer.getActiveSize()).isEqualTo(0);
    assertThat(buffer.getFlushingSize()).isEqualTo(2);

    // k2 also gets a newer write in E_active after swap
    long seq2New = buffer.put("k2", new float[] {0.7f, 0.7f});
    assertThat(seq2New).isGreaterThan(seq2);

    // evictCommitted must remove k1 and stale k2 from E_flushing while keeping newer k2 in E_active
    buffer.evictCommitted(Map.of("k1", seq1, "k2", seq2));
    assertThat(buffer.getFlushingSize()).isEqualTo(0);
    assertThat(buffer.getActiveSize()).isEqualTo(1);
    Tier0VectorDeltaBuffer.Snapshot<String> snap = buffer.snapshot();
    assertThat(snap.getLiveEntries()).containsOnlyKeys("k2");
    assertThat(snap.getLiveEntries().get("k2").getVector()).containsExactly(0.7f, 0.7f);

    buffer.completeFlush(batch.getEpoch());
  }

  @Test
  public void abortFlushDoesNotResurrectEntriesAfterConcurrentClear() {
    Tier0VectorDeltaBuffer<String> buffer = new Tier0VectorDeltaBuffer<>(5);
    buffer.put("k1", new float[] {1f, 0f});
    buffer.put("k2", new float[] {0f, 1f});

    Tier0VectorDeltaBuffer.FlushBatch<String> batch = buffer.beginFlush();
    assertThat(batch).isNotNull();

    // Concurrent clear() wipes the buffer before abortFlush runs
    buffer.clear();
    assertThat(buffer.getTotalSize()).isEqualTo(0);
    assertThat(buffer.isFlushing()).isFalse();

    // Calling abortFlush after clear() must be a safe no-op and resurrect ZERO entries
    buffer.abortFlush(batch.getEpoch());
    assertThat(buffer.getTotalSize()).isEqualTo(0);
    assertThat(buffer.snapshot().isEmpty()).isTrue();
  }

  @Test
  public void concurrentStressInterleavingPutEvictFlushAbortAndClearLeavesZeroStrandedEntries()
      throws Exception {
    Tier0VectorDeltaBuffer<String> buffer = new Tier0VectorDeltaBuffer<>(6);
    AtomicBoolean running = new AtomicBoolean(true);
    ExecutorService pool = Executors.newFixedThreadPool(4);
    try {
      // Writer + evictor thread
      Future<?> writerEvictor = pool.submit(() -> {
        for (int i = 0; i < 400; i++) {
          String key = "stress-" + (i % 40);
          long seq = buffer.put(key, new float[] {1f, (float) i});
          if (i % 3 == 0) {
            buffer.evictCommitted(Collections.singletonMap(key, seq));
          }
        }
      });

      // Flusher alternating completeFlush and abortFlush
      Future<?> flusher = pool.submit(() -> {
        int cycle = 0;
        while (running.get()) {
          Tier0VectorDeltaBuffer.FlushBatch<String> batch;
          try {
            batch = buffer.beginFlush();
          } catch (IllegalStateException ignored) {
            Thread.yield();
            continue;
          }
          if (batch == null) {
            Thread.yield();
            continue;
          }
          if ((cycle++ % 2) == 0) {
            buffer.abortFlush(batch.getEpoch());
          } else {
            buffer.completeFlush(batch.getEpoch());
          }
        }
      });

      // Periodic clear() thread racing with abortFlush/put/evictCommitted
      Future<?> clearer = pool.submit(() -> {
        for (int c = 0; c < 30; c++) {
          buffer.clear();
          Thread.yield();
        }
      });

      writerEvictor.get();
      clearer.get();
      running.set(false);
      flusher.get();
    } finally {
      pool.shutdownNow();
    }

    // After quiescence: a final clear() followed by draining any active entries leaves 0 stranded
    buffer.clear();
    assertThat(buffer.getTotalSize()).isEqualTo(0);

    // Put a deterministic batch, beginFlush, then evictCommitted across E_flushing -> 0 stranded
    Map<String, Long> seqs = new HashMap<>();
    for (int i = 0; i < 20; i++) {
      String k = "final-" + i;
      seqs.put(k, buffer.put(k, new float[] {1f, 0f}));
    }
    Tier0VectorDeltaBuffer.FlushBatch<String> finalBatch = buffer.beginFlush();
    assertThat(finalBatch).isNotNull();
    buffer.evictCommitted(seqs);
    assertThat(buffer.getTotalSize()).isEqualTo(0);
    buffer.completeFlush(finalBatch.getEpoch());
    assertThat(buffer.getTotalSize()).isEqualTo(0);
  }

  @Test
  public void snapshotNeverMissesLiveEntriesAcrossTenThousandBeginAndAbortFlushIterations()
      throws Exception {
    final int numPinnedKeys = 32;
    final int iterations = 10_000;
    Tier0VectorDeltaBuffer<String> buffer = new Tier0VectorDeltaBuffer<>(9);

    Set<String> pinnedKeys = new HashSet<>(numPinnedKeys);
    for (int i = 0; i < numPinnedKeys; i++) {
      String key = "pinned-" + i;
      pinnedKeys.add(key);
      buffer.put(key, new float[] {1f, (float) i}, Map.of("slot", i));
    }

    AtomicBoolean flusherDone = new AtomicBoolean(false);
    AtomicInteger missedEntries = new AtomicInteger(0);
    AtomicInteger snapshotsChecked = new AtomicInteger(0);
    AtomicInteger abortedFlushes = new AtomicInteger(0);

    ExecutorService pool = Executors.newFixedThreadPool(4);
    try {
      // Flusher repeatedly executes beginFlush() -> abortFlush() 10,000 times, migrating all
      // pinned entries between E_active and E_flushing continuously.
      Future<?> flusher = pool.submit(() -> {
        for (int i = 0; i < iterations; i++) {
          Tier0VectorDeltaBuffer.FlushBatch<String> batch = buffer.beginFlush();
          if (batch != null) {
            buffer.abortFlush(batch.getEpoch());
            abortedFlushes.incrementAndGet();
          }
        }
      });

      // Concurrent writer continuously updates pinned keys so E_active and E_flushing both hold
      // overlapping versions during abortFlush() restoration.
      Future<?> updater = pool.submit(() -> {
        int step = 0;
        while (!flusherDone.get()) {
          String key = "pinned-" + (step % numPinnedKeys);
          buffer.put(key, new float[] {1f, (float) step}, Map.of("slot", step % numPinnedKeys));
          step++;
        }
      });

      // Two concurrent readers verify that EVERY snapshot sees 100% of pinnedKeys with zero gaps.
      List<Future<?>> readers = new ArrayList<>();
      for (int r = 0; r < 2; r++) {
        readers.add(pool.submit(() -> {
          while (!flusherDone.get() || snapshotsChecked.get() < iterations) {
            Tier0VectorDeltaBuffer.Snapshot<String> snap = buffer.snapshot();
            Map<String, Tier0VectorDeltaBuffer.DeltaEntry<String>> live = snap.getLiveEntries();
            for (String expectedKey : pinnedKeys) {
              if (!live.containsKey(expectedKey)) {
                missedEntries.incrementAndGet();
              }
            }
            if (snapshotsChecked.incrementAndGet() >= iterations && flusherDone.get()) {
              break;
            }
          }
        }));
      }

      flusher.get();
      flusherDone.set(true);
      updater.get();
      for (Future<?> reader : readers) {
        reader.get();
      }
    } finally {
      pool.shutdownNow();
    }

    assertThat(abortedFlushes.get()).isEqualTo(iterations);
    assertThat(snapshotsChecked.get()).isGreaterThanOrEqualTo(iterations);
    assertThat(missedEntries.get())
        .as("snapshot() must NEVER miss a live entry during concurrent beginFlush()/abortFlush()")
        .isEqualTo(0);
    assertThat(buffer.snapshot().getLiveEntries().keySet())
        .containsExactlyInAnyOrderElementsOf(pinnedKeys);
  }

  @Test
  public void concurrentBMaxCappedPartialDrainWithEvictCommittedAndBeginAbortFlushHasZeroRawGaps()
      throws Exception {
    final int bMax = 1_000;
    final int numWriters = 4;
    final int writesPerWriter = 900; // 3,600 total > 3 * B_max
    final int totalWrites = numWriters * writesPerWriter;

    Tier0VectorDeltaBuffer<String> buffer = new Tier0VectorDeltaBuffer<>(11);
    ConcurrentHashMap<String, float[]> regionStore = new ConcurrentHashMap<>();
    ConcurrentHashMap<String, float[]> committedIndex = new ConcurrentHashMap<>();
    java.util.concurrent.ConcurrentLinkedQueue<String> aeqPendingQueue =
        new java.util.concurrent.ConcurrentLinkedQueue<>();
    Set<String> acknowledgedKeys = ConcurrentHashMap.newKeySet();
    AtomicBoolean writersDone = new AtomicBoolean(false);
    AtomicBoolean flusherDone = new AtomicBoolean(false);
    AtomicInteger visibilityGaps = new AtomicInteger(0);
    AtomicInteger partialBatchesDrained = new AtomicInteger(0);
    AtomicInteger snapshotsChecked = new AtomicInteger(0);

    ExecutorService pool = Executors.newFixedThreadPool(numWriters + 3);
    try {
      // Flusher drains AEQ batches capped strictly at B_max = 1,000 using getKeySequence() before
      // reading -> commit -> evictCommitted(), interleaved with periodic beginFlush()/abortFlush().
      Future<?> flusher = pool.submit(() -> {
        int cycle = 0;
        while (!writersDone.get() || !aeqPendingQueue.isEmpty() || buffer.getTotalSize() > 0) {
          if ((cycle % 3) == 1) {
            Tier0VectorDeltaBuffer.FlushBatch<String> aborted = buffer.beginFlush();
            if (aborted != null) {
              buffer.abortFlush(aborted.getEpoch());
            }
          }
          List<String> batchKeys = new ArrayList<>(bMax);
          String pending;
          while (batchKeys.size() < bMax && (pending = aeqPendingQueue.poll()) != null) {
            batchKeys.add(pending);
          }
          if (batchKeys.isEmpty()) {
            // Drain any remaining keys after writers finish
            if (writersDone.get() && buffer.getTotalSize() > 0) {
              Tier0VectorDeltaBuffer.Snapshot<String> rem = buffer.snapshot();
              batchKeys.addAll(rem.getLiveEntries().keySet());
            } else {
              Thread.yield();
              continue;
            }
          }

          // Exact LuceneEventListener partial-drain protocol:
          // 1. Capture seqBeforeRead per key BEFORE reading region value
          Map<String, Long> seqBeforeRead = new HashMap<>(batchKeys.size());
          Map<String, float[]> stagedWrites = new HashMap<>(batchKeys.size());
          for (String k : batchKeys) {
            long seq = buffer.getKeySequence(k);
            if (seq >= 0L) {
              seqBeforeRead.merge(k, seq, Math::max);
            }
            float[] val = regionStore.get(k);
            if (val != null) {
              stagedWrites.put(k, val);
            }
          }
          // 2. Publish batch atomically to committedIndex (simulating commitAndRefreshBlocking)
          committedIndex.putAll(stagedWrites);
          // 3. Evict only entries with seq <= seqBeforeRead
          buffer.evictCommitted(seqBeforeRead);
          partialBatchesDrained.incrementAndGet();
          cycle++;
        }
      });

      // Two concurrent readers verifying Theorem 2 zero-gap visibility across B_max partial drains
      List<Future<?>> readers = new ArrayList<>();
      for (int r = 0; r < 2; r++) {
        readers.add(pool.submit(() -> {
          while (!flusherDone.get()) {
            List<String> mustBeVisible = new ArrayList<>(acknowledgedKeys);
            if (mustBeVisible.isEmpty()) {
              Thread.yield();
              continue;
            }
            // Theorem 2 ordering: snapshot Tier-0 FIRST, THEN read committed index
            Tier0VectorDeltaBuffer.Snapshot<String> snap = buffer.snapshot();
            Map<String, Tier0VectorDeltaBuffer.DeltaEntry<String>> live = snap.getLiveEntries();
            for (String ackKey : mustBeVisible) {
              if (!live.containsKey(ackKey) && !committedIndex.containsKey(ackKey)) {
                visibilityGaps.incrementAndGet();
              }
            }
            snapshotsChecked.incrementAndGet();
          }
        }));
      }

      // Four concurrent writers pushing 3,600 (> 3 * B_max) entries
      List<Future<?>> writers = new ArrayList<>();
      for (int w = 0; w < numWriters; w++) {
        final int writerId = w;
        writers.add(pool.submit(() -> {
          for (int i = 0; i < writesPerWriter; i++) {
            String key = "bmax-w" + writerId + "-k" + i;
            float[] vec = new float[] {1f, (float) i, (float) writerId, 0.5f};
            regionStore.put(key, vec);
            buffer.put(key, vec);
            aeqPendingQueue.add(key);
            acknowledgedKeys.add(key);
          }
        }));
      }

      for (Future<?> wf : writers) {
        wf.get();
      }
      writersDone.set(true);
      flusher.get();
      flusherDone.set(true);
      for (Future<?> rf : readers) {
        rf.get();
      }
    } finally {
      pool.shutdownNow();
    }

    assertThat(visibilityGaps.get())
        .as("Zero RAW visibility gaps across >3*B_max writes and B_max-capped partial drains")
        .isEqualTo(0);
    assertThat(partialBatchesDrained.get()).isGreaterThanOrEqualTo(3);
    assertThat(snapshotsChecked.get()).isGreaterThan(0);
    assertThat(committedIndex).hasSize(totalWrites);
    assertThat(buffer.getTotalSize()).isEqualTo(0);
  }

  @Test
  public void deleteIfAbsentInsertsTombstoneWithoutOverwritingLivePutsOrFlushingEntries() {
    Tier0VectorDeltaBuffer<String> buffer = new Tier0VectorDeltaBuffer<>(12);

    // 1. Inserts tombstone when key is absent from both E_active and E_flushing
    long tombSeq1 = buffer.deleteIfAbsent("ghost-1");
    assertThat(tombSeq1).isPositive();
    assertThat(buffer.deleteIfAbsent("ghost-1")).isEqualTo(-1L);

    // 2. Does not overwrite live put already in E_active
    long putSeq = buffer.put("live-1", new float[] {1f, 0f}, Map.of("cat", "a"));
    assertThat(putSeq).isGreaterThan(tombSeq1);
    assertThat(buffer.deleteIfAbsent("live-1")).isEqualTo(-1L);

    Tier0VectorDeltaBuffer.Snapshot<String> snap = buffer.snapshot();
    assertThat(snap.isTombstoned("ghost-1")).isTrue();
    assertThat(snap.isMasked("ghost-1")).isTrue();
    assertThat(snap.getLiveEntries()).containsOnlyKeys("live-1");

    // 3. Skips key currently residing in E_flushing while allowing new keys in E_active
    Tier0VectorDeltaBuffer.FlushBatch<String> batch = buffer.beginFlush();
    assertThat(batch).isNotNull();
    assertThat(buffer.deleteIfAbsent("live-1")).isEqualTo(-1L);
    assertThat(buffer.deleteIfAbsent("ghost-1")).isEqualTo(-1L);
    long tombSeq2 = buffer.deleteIfAbsent("ghost-2");
    assertThat(tombSeq2).isGreaterThan(putSeq);

    buffer.completeFlush(batch.getEpoch());
    Tier0VectorDeltaBuffer.Snapshot<String> afterFlush = buffer.snapshot();
    assertThat(afterFlush.getLiveEntries()).isEmpty();
    assertThat(afterFlush.getTombstonedKeys()).containsExactly("ghost-2");

    // 4. Drains cleanly on evictCommitted
    buffer.evictCommitted(Collections.singletonMap("ghost-2", tombSeq2));
    assertThat(buffer.getTotalSize()).isEqualTo(0);
  }

  @Test
  public void deleteIfAbsentNeverOverwritesConcurrentLivePostPromotionPuts() throws Exception {
    Tier0VectorDeltaBuffer<String> buffer = new Tier0VectorDeltaBuffer<>(13);
    final int numKeys = 200;
    Set<String> livePutKeys = ConcurrentHashMap.newKeySet();

    ExecutorService pool = Executors.newFixedThreadPool(3);
    try {
      Future<?> writer = pool.submit(() -> {
        for (int i = 0; i < numKeys; i += 2) {
          String key = "race-" + i;
          buffer.put(key, new float[] {0f, 1f}, Map.of("idx", i));
          livePutKeys.add(key);
        }
      });
      Future<?> hydrator = pool.submit(() -> {
        for (int i = 0; i < numKeys; i++) {
          buffer.deleteIfAbsent("race-" + i);
        }
      });
      Future<?> flusher = pool.submit(() -> {
        for (int i = 0; i < 20; i++) {
          Tier0VectorDeltaBuffer.FlushBatch<String> batch = buffer.beginFlush();
          if (batch != null) {
            buffer.abortFlush(batch.getEpoch());
          }
        }
      });

      writer.get();
      hydrator.get();
      flusher.get();
    } finally {
      pool.shutdownNow();
    }

    Tier0VectorDeltaBuffer.Snapshot<String> snap = buffer.snapshot();
    // Every key written via live put MUST remain live (not clobbered by deleteIfAbsent)
    for (String liveKey : livePutKeys) {
      assertThat(snap.getLiveEntries()).containsKey(liveKey);
      assertThat(snap.isTombstoned(liveKey)).isFalse();
    }
    // Odd keys not written by live put MUST be tombstoned
    for (int i = 1; i < numKeys; i += 2) {
      assertThat(snap.isTombstoned("race-" + i)).isTrue();
    }
  }

  @Test
  public void snapshotSeqlockFencesOutStaleFlushingTombstoneAfterConcurrentEviction() {
    Tier0VectorDeltaBuffer<String> buffer = new Tier0VectorDeltaBuffer<>(14);
    float[] vec1 = new float[] {1f, 0f, 0f};
    float[] vec2 = new float[] {0.9f, 0.1f, 0f};

    long put1Seq = buffer.put("recreated", vec1, Map.of("v", 1));
    Tier0VectorDeltaBuffer.FlushBatch<String> initBatch = buffer.beginFlush();
    buffer.completeFlush(initBatch.getEpoch());
    buffer.evictCommitted(Collections.singletonMap("recreated", put1Seq));

    // Tombstone e1 enters E_flushing; live recreation e2 > e1 enters E_active.
    buffer.destroy("recreated");
    Tier0VectorDeltaBuffer.FlushBatch<String> tombBatch = buffer.beginFlush();
    assertThat(tombBatch).isNotNull();
    long recreateSeq = buffer.put("recreated", vec2, Map.of("v", 2));

    // Deterministically simulate Path-A committing e2 to Lucene and evicting both epochs right
    // after snapshot() scans stale tombstone e1 in E_flushing and before scanning E_active.
    AtomicInteger hookInvocations = new AtomicInteger(0);
    long beforeVersion = buffer.getEvictionVersion();
    assertThat(beforeVersion % 2L).isEqualTo(0L);

    Tier0VectorDeltaBuffer.Snapshot<String> snap = buffer.snapshot(() -> {
      hookInvocations.incrementAndGet();
      buffer.evictCommitted(Collections.singletonMap("recreated", recreateSeq));
    });

    assertThat(hookInvocations.get()).isEqualTo(1);
    assertThat(buffer.getEvictionVersion()).isGreaterThan(beforeVersion);
    assertThat(buffer.getEvictionVersion() % 2L).isEqualTo(0L);
    // Seqlock fence must have retried after eviction and never exposed stale tombstone e1 in T_mask
    assertThat(snap.getTombstonedKeys()).doesNotContain("recreated");
    assertThat(snap.getMaskedKeys()).doesNotContain("recreated");
    assertThat(snap.isEmpty()).isTrue();
  }

  @Test
  public void snapshotSeqlockFencesOutCompleteFlushMidSnapshot() {
    Tier0VectorDeltaBuffer<String> buffer = new Tier0VectorDeltaBuffer<>(15);
    buffer.put("k1", new float[] {1f, 0f});
    buffer.destroy("k1");
    Tier0VectorDeltaBuffer.FlushBatch<String> tombBatch = buffer.beginFlush();
    long seq2 = buffer.put("k1", new float[] {0.5f, 0.5f});

    Tier0VectorDeltaBuffer.Snapshot<String> snap = buffer.snapshot(() -> {
      buffer.completeFlush(tombBatch.getEpoch());
      buffer.evictCommitted(Collections.singletonMap("k1", seq2));
    });

    assertThat(snap.getTombstonedKeys()).doesNotContain("k1");
    assertThat(snap.getMaskedKeys()).doesNotContain("k1");
  }

  @Test
  public void snapshotUnderConcurrentEvictAndRecreateChurnNeverExposesStaleTombstone()
      throws Exception {
    Tier0VectorDeltaBuffer<String> buffer = new Tier0VectorDeltaBuffer<>(16);
    final int cycles = 1500;
    final AtomicBoolean done = new AtomicBoolean(false);
    final AtomicInteger recreatedUpTo = new AtomicInteger(-1);
    final AtomicInteger snapshotsChecked = new AtomicInteger(0);
    final AtomicInteger violationCycle = new AtomicInteger(-1);

    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      Future<?> reader = pool.submit(() -> {
        while (!done.get() && violationCycle.get() < 0) {
          // Read volatile bound BEFORE snapshot() so any key <= bound was already recreated live
          // before snapshot() started; stale E_flushing tombstones must never appear.
          int mustNeverTombstoneUpTo = recreatedUpTo.get();
          Tier0VectorDeltaBuffer.Snapshot<String> snap = buffer.snapshot();
          snapshotsChecked.incrementAndGet();
          for (String tombKey : snap.getTombstonedKeys()) {
            int cycle = Integer.parseInt(tombKey.substring(2));
            if (cycle <= mustNeverTombstoneUpTo) {
              violationCycle.compareAndSet(-1, cycle);
            }
          }
        }
      });

      for (int i = 0; i < cycles; i++) {
        String key = "k-" + i;
        buffer.destroy(key);
        Tier0VectorDeltaBuffer.FlushBatch<String> batch = buffer.beginFlush();
        long liveSeq = buffer.put(key, new float[] {0f, 1f}, Map.of("cycle", i));
        recreatedUpTo.set(i);
        if (i % 2 == 0) {
          buffer.evictCommitted(Collections.singletonMap(key, liveSeq));
          if (batch != null) {
            buffer.completeFlush(batch.getEpoch());
          }
        } else {
          if (batch != null) {
            buffer.completeFlush(batch.getEpoch());
          }
          buffer.evictCommitted(Collections.singletonMap(key, liveSeq));
        }
      }
      done.set(true);
      reader.get(15, TimeUnit.SECONDS);
    } finally {
      pool.shutdownNow();
    }

    assertThat(violationCycle.get()).isEqualTo(-1);
    assertThat(snapshotsChecked.get()).isGreaterThan(0);
    assertThat(buffer.getEvictionVersion() % 2L).isEqualTo(0L);
  }

  @Test
  public void snapshotNoCommitPreventsTwoKeyTornCut() throws Exception {
    Tier0VectorDeltaBuffer<String> buffer = new Tier0VectorDeltaBuffer<>(17);
    buffer.put("x", new float[] {1f, 0f}, Map.of("seq", 0));
    buffer.put("y", new float[] {1f, 0f}, Map.of("seq", 0));

    long evictionBefore = buffer.getEvictionVersion();
    long mutationBefore = buffer.getMutationVersion();
    AtomicInteger hookInvocations = new AtomicInteger(0);
    ExecutorService writerPool = Executors.newSingleThreadExecutor();
    try {
      Tier0VectorDeltaBuffer.Snapshot<String> snap = buffer.snapshot(null, () -> {
        hookInvocations.incrementAndGet();
        try {
          writerPool.submit(() -> {
            buffer.put("x", new float[] {0f, 1f}, Map.of("seq", 1));
            buffer.put("y", new float[] {0f, 1f}, Map.of("seq", 1));
          }).get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
          throw new AssertionError(e);
        }
      });

      assertThat(hookInvocations.get()).isEqualTo(1);
      assertThat(buffer.getEvictionVersion()).isEqualTo(evictionBefore);
      assertThat(buffer.getMutationVersion()).isEqualTo(mutationBefore + 4L);
      assertThat(snap.getLiveEntries()).containsKeys("x", "y");
      assertThat(snap.getLiveEntries().get("x").getMetadata().get("seq")).isEqualTo(1);
      assertThat(snap.getLiveEntries().get("y").getMetadata().get("seq")).isEqualTo(1);
    } finally {
      writerPool.shutdownNow();
    }
  }

  @Test
  public void snapshotResolvesOlderFlushingPutAgainstNewerActiveDelete() {
    Tier0VectorDeltaBuffer<String> buffer = new Tier0VectorDeltaBuffer<>(18);
    buffer.put("k1", new float[] {1f, 0f}, Map.of("v", 1));
    Tier0VectorDeltaBuffer.FlushBatch<String> flush1 = buffer.beginFlush();
    assertThat(flush1).isNotNull();
    buffer.destroy("k1");

    Tier0VectorDeltaBuffer.Snapshot<String> staticSnap = buffer.snapshot();
    assertThat(staticSnap.getLiveEntries()).doesNotContainKey("k1");
    assertThat(staticSnap.getTombstonedKeys()).containsExactly("k1");
    assertThat(staticSnap.getMaskedKeys()).containsExactly("k1");
    buffer.completeFlush(flush1.getEpoch());
    buffer.clear();

    // Simulate concurrent destroy in E_active after scanning E_flushing put without any commit
    buffer.put("k2", new float[] {0f, 1f}, Map.of("v", 1));
    Tier0VectorDeltaBuffer.FlushBatch<String> flush2 = buffer.beginFlush();
    assertThat(flush2).isNotNull();
    AtomicInteger hookRuns = new AtomicInteger(0);
    Tier0VectorDeltaBuffer.Snapshot<String> racedSnap = buffer.snapshot(() -> {
      hookRuns.incrementAndGet();
      buffer.destroy("k2");
    });

    assertThat(hookRuns.get()).isEqualTo(1);
    assertThat(racedSnap.getLiveEntries()).doesNotContainKey("k2");
    assertThat(racedSnap.getTombstonedKeys()).containsExactly("k2");
    assertThat(racedSnap.getMaskedKeys()).containsExactly("k2");
    buffer.completeFlush(flush2.getEpoch());
  }

  @Test
  public void snapshotNoCommitConcurrentTwoKeyScheduleNeverExposesOldXNewY() throws Exception {
    Tier0VectorDeltaBuffer<String> buffer = new Tier0VectorDeltaBuffer<>(19);
    buffer.put("x", new float[] {1f, 0f}, Map.of("seq", 0));
    buffer.put("y", new float[] {1f, 0f}, Map.of("seq", 0));

    final int iterations = 5000;
    final AtomicBoolean done = new AtomicBoolean(false);
    final AtomicInteger snapshotsObserved = new AtomicInteger(0);
    final AtomicInteger tornX = new AtomicInteger(-1);
    final AtomicInteger tornY = new AtomicInteger(-1);

    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      Future<?> reader = pool.submit(() -> {
        while (!done.get() && tornX.get() < 0) {
          Tier0VectorDeltaBuffer.Snapshot<String> snap = buffer.snapshot();
          snapshotsObserved.incrementAndGet();
          Tier0VectorDeltaBuffer.DeltaEntry<String> ex = snap.getLiveEntries().get("x");
          Tier0VectorDeltaBuffer.DeltaEntry<String> ey = snap.getLiveEntries().get("y");
          if (ex == null || ey == null) {
            tornX.compareAndSet(-1, -2);
            break;
          }
          int sx = (Integer) ex.getMetadata().get("seq");
          int sy = (Integer) ey.getMetadata().get("seq");
          if (sx < sy) {
            tornX.compareAndSet(-1, sx);
            tornY.compareAndSet(-1, sy);
          }
        }
      });

      for (int i = 1; i <= iterations; i++) {
        buffer.put("x", new float[] {1f, i * 0.0001f}, Map.of("seq", i));
        buffer.put("y", new float[] {1f, i * 0.0001f}, Map.of("seq", i));
      }
      done.set(true);
      reader.get(15, TimeUnit.SECONDS);
    } finally {
      pool.shutdownNow();
    }

    assertThat(tornX.get()).as("Found torn (old x=%d, new y=%d)", tornX.get(), tornY.get())
        .isEqualTo(-1);
    assertThat(snapshotsObserved.get()).isGreaterThan(0);
  }

  @Test
  public void snapshotNoCommitPrePinnedTwoWritersNeverExposesOldXNewY() throws Exception {
    Tier0VectorDeltaBuffer<String> buffer = new Tier0VectorDeltaBuffer<>(20);
    buffer.put("x", new float[] {1f, 0f}, Map.of("seq", 0));
    buffer.put("y", new float[] {1f, 0f}, Map.of("seq", 0));

    long evictionBefore = buffer.getEvictionVersion();
    long mutationBefore = buffer.getMutationVersion();
    ExecutorService writersPool = Executors.newFixedThreadPool(2);
    try {
      // Exact R1 F1 schedule: after snapshot observes writerState == 0, two distinct-key writers
      // both pin active.writerState and execute their pre-increment BEFORE snapshot reads m1.
      // After snapshot reads the first key (seq=0), both writers publish seq=1, post-increment,
      // and unpin BEFORE snapshot reads the second key and validates. Post-increment ensures
      // m2 != m1 rejects the torn {0, 1} cut on attempt 0 and returns atomic {1, 1}.
      CountDownLatch bothPreIncremented = new CountDownLatch(2);
      CountDownLatch allowPublish = new CountDownLatch(1);
      AtomicInteger writerCheckHooks = new AtomicInteger(0);
      AtomicInteger midScanHooks = new AtomicInteger(0);
      List<Future<Long>> writerFutures = new ArrayList<>(2);

      Tier0VectorDeltaBuffer.Snapshot<String> snap = buffer.snapshot(
          () -> {
            writerCheckHooks.incrementAndGet();
            writerFutures.add(writersPool.submit(() -> buffer.putWithPreMutateHookForTest(
                "x", new float[] {0f, 1f}, Map.of("seq", 1), () -> {
                  bothPreIncremented.countDown();
                  try {
                    assertThat(allowPublish.await(5, TimeUnit.SECONDS)).isTrue();
                  } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                  }
                })));
            writerFutures.add(writersPool.submit(() -> buffer.putWithPreMutateHookForTest(
                "y", new float[] {0f, 1f}, Map.of("seq", 1), () -> {
                  bothPreIncremented.countDown();
                  try {
                    assertThat(allowPublish.await(5, TimeUnit.SECONDS)).isTrue();
                  } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                  }
                })));
            try {
              assertThat(bothPreIncremented.await(5, TimeUnit.SECONDS)).isTrue();
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
              throw new AssertionError(ie);
            }
          },
          null,
          () -> {
            midScanHooks.incrementAndGet();
            allowPublish.countDown();
            for (Future<Long> f : writerFutures) {
              try {
                f.get(5, TimeUnit.SECONDS);
              } catch (Exception e) {
                throw new AssertionError(e);
              }
            }
          });

      assertThat(writerCheckHooks.get()).isEqualTo(1);
      assertThat(midScanHooks.get()).isEqualTo(1);
      assertThat(buffer.getEvictionVersion()).isEqualTo(evictionBefore);
      assertThat(buffer.getMutationVersion()).isEqualTo(mutationBefore + 4L);
      assertThat(snap.getLiveEntries().get("x").getMetadata().get("seq")).isEqualTo(1);
      assertThat(snap.getLiveEntries().get("y").getMetadata().get("seq")).isEqualTo(1);
    } finally {
      writersPool.shutdownNow();
    }
  }

  @Test
  public void putIfSequenceBeforeAndDeleteIfSequenceBeforeOverwriteStaleWhilePreservingPostFenceWrites() {
    Tier0VectorDeltaBuffer<String> buffer = new Tier0VectorDeltaBuffer<>(21);
    float[] v1 = new float[] {1f, 0f};
    float[] v2 = new float[] {0f, 1f};
    float[] v3 = new float[] {0.6f, 0.8f};

    long sStalePut = buffer.put("stale-put", v1, Map.of("v", 1));
    long sStaleLiveToTomb = buffer.put("stale-live-to-tomb", v1, Map.of("v", 1));
    long sStaleTomb = buffer.destroy("stale-tomb");
    long sProvisionalPut = buffer.putProvisionalIfAbsent("prov-put", v1, Map.of("v", 1));
    long sProvisionalTomb = buffer.deleteProvisionalIfAbsent("prov-tomb");
    assertThat(sProvisionalPut).isEqualTo(Tier0VectorDeltaBuffer.PROVISIONAL_HYDRATION_SEQUENCE);
    assertThat(sProvisionalTomb).isEqualTo(Tier0VectorDeltaBuffer.PROVISIONAL_HYDRATION_SEQUENCE);

    long promotionStartSeq = buffer.nextSequence();
    assertThat(sStalePut).isGreaterThanOrEqualTo(Tier0VectorDeltaBuffer.LIVE_SEQUENCE_START);
    assertThat(sStalePut).isLessThan(promotionStartSeq);
    assertThat(sStaleLiveToTomb).isLessThan(promotionStartSeq);
    assertThat(sStaleTomb).isLessThan(promotionStartSeq);
    assertThat(sProvisionalPut).isLessThan(Tier0VectorDeltaBuffer.LIVE_SEQUENCE_START);
    assertThat(sProvisionalTomb).isLessThan(Tier0VectorDeltaBuffer.LIVE_SEQUENCE_START);

    long sPostPut = buffer.put("post-put", v3, Map.of("v", 3));
    long sPostDestroy = buffer.destroy("post-destroy");
    long sPostFlushing = buffer.put("post-flushing", v3, Map.of("v", 3));
    assertThat(sPostPut).isGreaterThanOrEqualTo(promotionStartSeq);
    assertThat(sPostDestroy).isGreaterThanOrEqualTo(promotionStartSeq);
    assertThat(sPostFlushing).isGreaterThanOrEqualTo(promotionStartSeq);

    // Pre-promotion & provisional entries MUST be overwritten by authoritative bucket state
    long mutBefore = buffer.getMutationVersion();
    assertThat(buffer.putIfSequenceBefore("stale-put", v2, Map.of("v", 2), promotionStartSeq))
        .isGreaterThanOrEqualTo(promotionStartSeq);
    assertThat(buffer.deleteIfSequenceBefore("stale-live-to-tomb", promotionStartSeq))
        .isGreaterThanOrEqualTo(promotionStartSeq);
    assertThat(buffer.putIfSequenceBefore("stale-tomb", v2, Map.of("v", 2), promotionStartSeq))
        .isGreaterThanOrEqualTo(promotionStartSeq);
    assertThat(buffer.deleteIfSequenceBefore("prov-put", promotionStartSeq))
        .isGreaterThanOrEqualTo(promotionStartSeq);
    assertThat(buffer.putIfSequenceBefore("prov-tomb", v2, Map.of("v", 2), promotionStartSeq))
        .isGreaterThanOrEqualTo(promotionStartSeq);
    assertThat(buffer.getMutationVersion()).isEqualTo(mutBefore + 10L);

    // Concurrent live post-promotion writes in E_active MUST NEVER be overwritten
    long mutLiveCheck = buffer.getMutationVersion();
    assertThat(buffer.putIfSequenceBefore("post-put", v1, Map.of("v", 1), promotionStartSeq))
        .isEqualTo(-1L);
    assertThat(buffer.deleteIfSequenceBefore("post-put", promotionStartSeq)).isEqualTo(-1L);
    assertThat(buffer.putIfSequenceBefore("post-destroy", v1, Map.of("v", 1), promotionStartSeq))
        .isEqualTo(-1L);
    assertThat(buffer.getMutationVersion()).isEqualTo(mutLiveCheck);

    // Rotate post-flushing into E_flushing and verify fencing also guards E_flushing
    Tier0VectorDeltaBuffer.FlushBatch<String> batch = buffer.beginFlush();
    assertThat(batch).isNotNull();
    try {
      assertThat(
          buffer.putIfSequenceBefore("post-flushing", v1, Map.of("v", 1), promotionStartSeq))
              .isEqualTo(-1L);
      assertThat(buffer.deleteIfSequenceBefore("post-flushing", promotionStartSeq))
          .isEqualTo(-1L);

      Tier0VectorDeltaBuffer.Snapshot<String> snap = buffer.snapshot();
      assertThat(snap.getLiveEntries().get("stale-put").getVector()).containsExactly(v2);
      assertThat(snap.getLiveEntries().get("stale-tomb").getVector()).containsExactly(v2);
      assertThat(snap.getLiveEntries().get("prov-tomb").getVector()).containsExactly(v2);
      assertThat(snap.getLiveEntries().get("post-put").getVector()).containsExactly(v3);
      assertThat(snap.getLiveEntries().get("post-flushing").getVector()).containsExactly(v3);
      assertThat(snap.getTombstonedKeys())
          .containsExactlyInAnyOrder("stale-live-to-tomb", "prov-put", "post-destroy");
    } finally {
      buffer.completeFlush(batch.getEpoch());
    }
  }

  private static void addVectorDoc(IndexWriter writer, String key, float[] vec,
      VectorSimilarityFunction similarity) throws IOException {
    Document doc = new Document();
    SerializerUtil.addKey(key, doc);
    doc.add(new KnnFloatVectorField(VECTOR_FIELD, vec, similarity));
    doc.add(new StringField("status", "active", Field.Store.NO));
    writer.addDocument(doc);
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
}
