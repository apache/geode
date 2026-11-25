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

package org.apache.geode.cache.lucene.internal.repository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntSupplier;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.Bits;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.cache.lucene.internal.LuceneIndexStats;
import org.apache.geode.cache.lucene.internal.directory.RegionDirectory;
import org.apache.geode.cache.lucene.internal.distributed.EntryScore;
import org.apache.geode.cache.lucene.internal.filesystem.FileSystemStats;
import org.apache.geode.cache.lucene.internal.partition.BucketTargetingMap;
import org.apache.geode.cache.lucene.internal.repository.serializer.SerializerUtil;
import org.apache.geode.cache.lucene.internal.vector.VectorSimilarityScorer;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.LockNotHeldException;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * A repository that writes to a single lucene index writer
 */
public class IndexRepositoryImpl implements IndexRepository {

  private static final boolean APPLY_ALL_DELETES = System
      .getProperty(GeodeGlossary.GEMFIRE_PREFIX + "IndexRepository.APPLY_ALL_DELETES", "true")
      .equalsIgnoreCase("true");

  private final IndexWriter writer;
  private final LuceneSerializer serializer;
  private final SearcherManager searcherManager;
  private final Region<?, ?> region;
  private final Region<?, ?> userRegion;
  private final LuceneIndexStats stats;
  private final DocumentCountSupplier documentCountSupplier;
  private final DistributedLockService lockService;
  private final String lockName;
  private final LuceneIndex index;
  private final AtomicLong searcherEpoch = new AtomicLong(0L);

  private static final Logger logger = LogService.getLogger();

  public IndexRepositoryImpl(Region<?, ?> region, IndexWriter writer, LuceneSerializer serializer,
      LuceneIndexStats stats, Region<?, ?> userRegion, DistributedLockService lockService,
      String lockName, LuceneIndex index) throws IOException {
    this.region = region;
    this.userRegion = userRegion;
    this.writer = writer;
    searcherManager = createSearchManager();
    this.serializer = serializer;
    this.stats = stats;
    documentCountSupplier = new DocumentCountSupplier();
    stats.addDocumentsSupplier(documentCountSupplier);
    this.lockService = lockService;
    this.lockName = lockName;
    this.index = index;
  }

  protected SearcherManager createSearchManager() throws IOException {
    return new SearcherManager(writer, APPLY_ALL_DELETES, true, null);
  }

  @Override
  public void create(Object key, Object value) throws IOException {
    long start = stats.startUpdate();
    Collection<Document> docs = Collections.emptyList();
    boolean exceptionHappened = false;
    try {
      try {
        docs = serializer.toDocuments(index, value);
      } catch (Exception e) {
        exceptionHappened = true;
        stats.incFailedEntries();
        logger.info("Failed to add index for " + value + " due to " + e.getMessage());
      }
      if (!exceptionHappened) {
        docs.forEach(doc -> SerializerUtil.addKey(key, doc));
        writer.addDocuments(docs);
      }
    } finally {
      stats.endUpdate(start);
    }
  }

  @Override
  public void update(Object key, Object value) throws IOException {
    long start = stats.startUpdate();
    Collection<Document> docs = Collections.emptyList();
    boolean exceptionHappened = false;
    try {
      try {
        docs = serializer.toDocuments(index, value);
      } catch (Exception e) {
        exceptionHappened = true;
        stats.incFailedEntries();
        logger.info("Failed to update index for " + value + " due to " + e.getMessage());
      }
      if (!exceptionHappened) {
        docs.forEach(doc -> SerializerUtil.addKey(key, doc));
        Term keyTerm = SerializerUtil.toKeyTerm(key);
        writer.updateDocuments(keyTerm, docs);
      }
    } finally {
      stats.endUpdate(start);
    }
  }

  @Override
  public void delete(Object key) throws IOException {
    long start = stats.startUpdate();
    try {
      Term keyTerm = SerializerUtil.toKeyTerm(key);
      writer.deleteDocuments(keyTerm);
    } finally {
      stats.endUpdate(start);
    }
  }

  @Override
  public long getSearcherEpoch() {
    return searcherEpoch.get();
  }

  @Override
  public void query(Query query, int limit, IndexResultCollector collector) throws IOException {
    query(query, limit, collector, null);
  }

  @Override
  public void query(Query query, int limit, IndexResultCollector collector,
      Runnable onSearcherAcquired) throws IOException {
    executeQuery(false, 0L, query, limit, collector, onSearcherAcquired);
  }

  @Override
  public boolean tryQueryAtEpoch(long expectedEpoch, Query query, int limit,
      IndexResultCollector collector, Runnable onSearcherAcquired) throws IOException {
    return executeQuery(true, expectedEpoch, query, limit, collector, onSearcherAcquired);
  }

  private boolean executeQuery(boolean validateEpoch, long expectedEpoch, Query query, int limit,
      IndexResultCollector collector, Runnable onSearcherAcquired) throws IOException {
    if (validateEpoch && ((expectedEpoch & 1L) != 0L || searcherEpoch.get() != expectedEpoch)) {
      return false;
    }
    long start = stats.startRepositoryQuery();
    int totalHits = 0;
    IndexSearcher searcher = searcherManager.acquire();
    try {
      if (validateEpoch && searcherEpoch.get() != expectedEpoch) {
        return false;
      }
      if (onSearcherAcquired != null) {
        onSearcherAcquired.run();
      }
      TopDocs docs = searcher.search(query, limit);
      // Lucene 9.x: totalHits is now a TotalHits object with value field
      totalHits = (int) docs.totalHits.value;
      for (ScoreDoc scoreDoc : docs.scoreDocs) {
        Document doc = searcher.doc(scoreDoc.doc);
        Object key = SerializerUtil.getKey(doc);
        if (logger.isDebugEnabled()) {
          logger.debug("query found doc:" + doc + ":" + scoreDoc);
        }
        collector.collect(key, scoreDoc.score);
      }
      return true;
    } finally {
      searcherManager.release(searcher);
      stats.endRepositoryQuery(start, totalHits);
    }
  }

  @Override
  public void queryWithFp32Rerank(Query query, int k, int oversampleK, String vectorField,
      float[] queryVector, VectorSimilarityFunction similarity, IndexResultCollector collector)
      throws IOException {
    queryWithFp32Rerank(
        query, k, oversampleK, vectorField, queryVector, similarity, collector, null);
  }

  @Override
  public void queryWithFp32Rerank(Query query, int k, int oversampleK, String vectorField,
      float[] queryVector, VectorSimilarityFunction similarity, IndexResultCollector collector,
      Runnable onSearcherAcquired) throws IOException {
    executeQueryWithFp32Rerank(false, 0L, query, k, oversampleK, vectorField, queryVector,
        similarity, collector, onSearcherAcquired);
  }

  @Override
  public boolean tryQueryWithFp32RerankAtEpoch(long expectedEpoch, Query query, int k,
      int oversampleK, String vectorField, float[] queryVector,
      VectorSimilarityFunction similarity, IndexResultCollector collector,
      Runnable onSearcherAcquired) throws IOException {
    return executeQueryWithFp32Rerank(true, expectedEpoch, query, k, oversampleK, vectorField,
        queryVector, similarity, collector, onSearcherAcquired);
  }

  private boolean executeQueryWithFp32Rerank(boolean validateEpoch, long expectedEpoch,
      Query query, int k, int oversampleK, String vectorField, float[] queryVector,
      VectorSimilarityFunction similarity, IndexResultCollector collector,
      Runnable onSearcherAcquired) throws IOException {
    if (validateEpoch && ((expectedEpoch & 1L) != 0L || searcherEpoch.get() != expectedEpoch)) {
      return false;
    }
    if (k <= 0 || oversampleK <= 0) {
      if (onSearcherAcquired != null) {
        onSearcherAcquired.run();
      }
      return true;
    }
    if (vectorField == null || queryVector == null || similarity == null) {
      return executeQuery(validateEpoch, expectedEpoch, query, k, collector, onSearcherAcquired);
    }
    int effectiveOversampleK = Math.max(k, oversampleK);
    long start = stats.startRepositoryQuery();
    int totalHits = 0;
    IndexSearcher searcher = searcherManager.acquire();
    try {
      if (validateEpoch && searcherEpoch.get() != expectedEpoch) {
        return false;
      }
      if (onSearcherAcquired != null) {
        onSearcherAcquired.run();
      }
      TopDocs docs = searcher.search(query, effectiveOversampleK);
      totalHits = (int) docs.totalHits.value;
      if (docs.scoreDocs.length == 0) {
        return true;
      }

      List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
      Map<Integer, List<ScoreDoc>> scoreDocsByLeaf = new HashMap<>();
      for (ScoreDoc sd : docs.scoreDocs) {
        int leafIdx = ReaderUtil.subIndex(sd.doc, leaves);
        scoreDocsByLeaf.computeIfAbsent(leafIdx, idx -> new ArrayList<>()).add(sd);
      }

      StoredFields storedFields = searcher.storedFields();
      List<EntryScore<Object>> reranked = new ArrayList<>(docs.scoreDocs.length);

      for (Map.Entry<Integer, List<ScoreDoc>> leafEntry : scoreDocsByLeaf.entrySet()) {
        LeafReaderContext leaf = leaves.get(leafEntry.getKey());
        List<ScoreDoc> leafScoreDocs = leafEntry.getValue();
        // FloatVectorValues extends DocIdSetIterator: advance() MUST move strictly monotonically
        // per leaf (target > docID()). Sort ascending by doc, highest score first on ties.
        leafScoreDocs.sort((a, b) -> {
          int cmp = Integer.compare(a.doc, b.doc);
          return cmp != 0 ? cmp : Float.compare(b.score, a.score);
        });

        FloatVectorValues vectorValues = leaf.reader().getFloatVectorValues(vectorField);
        int lastDocInLeaf = -1;
        for (ScoreDoc sd : leafScoreDocs) {
          int docInLeaf = sd.doc - leaf.docBase;
          if (docInLeaf == lastDocInLeaf) {
            continue;
          }
          lastDocInLeaf = docInLeaf;
          float score = sd.score;
          if (vectorValues != null) {
            int currentDoc = vectorValues.docID();
            if (currentDoc < docInLeaf) {
              currentDoc = vectorValues.advance(docInLeaf);
            }
            if (currentDoc == docInLeaf) {
              float[] stored = vectorValues.vectorValue();
              if (stored != null && stored.length == queryVector.length) {
                score = VectorSimilarityScorer.score(queryVector, stored, similarity);
              }
            }
          }
          Document doc = storedFields.document(sd.doc);
          Object key = SerializerUtil.getKey(doc);
          if (logger.isDebugEnabled()) {
            logger.debug("queryWithFp32Rerank found doc:" + doc + ":" + sd + " reranked=" + score);
          }
          reranked.add(new EntryScore<>(key, score));
        }
      }

      reranked.sort((a, b) -> {
        int cmp = Float.compare(b.getScore(), a.getScore());
        if (cmp != 0) {
          return cmp;
        }
        return String.valueOf(a.getKey()).compareTo(String.valueOf(b.getKey()));
      });

      int limit = Math.min(k, reranked.size());
      for (int i = 0; i < limit; i++) {
        EntryScore<Object> hit = reranked.get(i);
        collector.collect(hit.getKey(), hit.getScore());
      }
      return true;
    } finally {
      searcherManager.release(searcher);
      stats.endRepositoryQuery(start, totalHits);
    }
  }

  @Override
  public Set<Object> collectIndexedKeys() throws IOException {
    if (isClosed()) {
      return Collections.emptySet();
    }
    IndexSearcher searcher = searcherManager.acquire();
    try {
      return collectIndexedKeys(searcher.getIndexReader());
    } finally {
      searcherManager.release(searcher);
    }
  }

  /**
   * Collects all live document keys from a local {@code .files} bucket read-only via
   * {@link DirectoryReader}, without requiring primary ownership, distributed locks, or an
   * {@link IndexWriter}. Safe to invoke while a colocated bucket's {@code BucketAdvisor} is still
   * transitioning to primary.
   */
  public static Set<Object> collectIndexedKeys(BucketRegion fileBucket, int bucketId,
      FileSystemStats stats) throws IOException {
    if (fileBucket == null || fileBucket.isDestroyed() || fileBucket.isEmpty()) {
      return Collections.emptySet();
    }
    RegionDirectory dir =
        new RegionDirectory(new BucketTargetingMap<>(fileBucket, bucketId), stats);
    try {
      if (!DirectoryReader.indexExists(dir)) {
        return Collections.emptySet();
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        return collectIndexedKeys(reader);
      }
    } finally {
      dir.close();
    }
  }

  /**
   * Collects all live user-region keys stored in an open Lucene {@link IndexReader}.
   */
  public static Set<Object> collectIndexedKeys(IndexReader indexReader) throws IOException {
    Set<Object> indexedKeys = new HashSet<>();
    StoredFields storedFields = indexReader.storedFields();
    for (LeafReaderContext leaf : indexReader.leaves()) {
      LeafReader reader = leaf.reader();
      Bits liveDocs = reader.getLiveDocs();
      int maxDoc = reader.maxDoc();
      for (int docId = 0; docId < maxDoc; docId++) {
        if (liveDocs != null && !liveDocs.get(docId)) {
          continue;
        }
        Document doc = storedFields.document(leaf.docBase + docId);
        Object key = SerializerUtil.getKey(doc);
        if (key != null) {
          indexedKeys.add(key);
        }
      }
    }
    return indexedKeys;
  }

  @Override
  public synchronized void commit() throws IOException {
    long start = stats.startCommit();
    searcherEpoch.incrementAndGet();
    try {
      writer.commit();
      searcherManager.maybeRefresh();
    } finally {
      searcherEpoch.incrementAndGet();
      stats.endCommit(start);
    }
  }

  @Override
  public synchronized void commitAndRefreshBlocking() throws IOException {
    long start = stats.startCommit();
    searcherEpoch.incrementAndGet();
    try {
      writer.commit();
      searcherManager.maybeRefreshBlocking();
    } finally {
      searcherEpoch.incrementAndGet();
      stats.endCommit(start);
    }
  }

  @Override
  public IndexWriter getWriter() {
    return writer;
  }

  @Override
  public Region<?, ?> getRegion() {
    return region;
  }

  public LuceneSerializer getSerializer() {
    return serializer;
  }

  @Override
  public boolean isClosed() {
    return userRegion.isDestroyed() || !writer.isOpen();
  }

  @Override
  public void cleanup() {
    try {
      stats.removeDocumentsSupplier(documentCountSupplier);
      try {
        writer.close();
      } catch (Exception e) {
        logger.debug("Unable to clean up index repository", e);
      }
    } finally {
      try {
        if (lockService != null) {
          lockService.unlock(lockName);
        }
      } catch (LockNotHeldException e) {
        logger.debug("Tried to unlock file region lock(" + lockName + ") that we did not hold", e);
      }
    }
  }

  private class DocumentCountSupplier implements IntSupplier {
    @Override
    public int getAsInt() {
      if (isClosed() || !((BucketRegion) userRegion).getBucketAdvisor().isPrimary()) {
        stats.removeDocumentsSupplier(this);
        return 0;
      }
      try {
        // Lucene 9.x: IndexWriter.numDocs() removed - use searcherManager to get reader
        IndexSearcher searcher = searcherManager.acquire();
        try {
          return searcher.getIndexReader().numDocs();
        } finally {
          searcherManager.release(searcher);
        }
      } catch (AlreadyClosedException | IOException e) {
        // ignore
        return 0;
      }
    }
  }
}
