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
import java.util.Collection;
import java.util.Collections;
import java.util.function.IntSupplier;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.BytesRef;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.cache.lucene.internal.LuceneIndexStats;
import org.apache.geode.cache.lucene.internal.repository.serializer.SerializerUtil;
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
  private Region<?, ?> region;
  private Region<?, ?> userRegion;
  private LuceneIndexStats stats;
  private DocumentCountSupplier documentCountSupplier;
  private final DistributedLockService lockService;
  private String lockName;
  private LuceneIndex index;

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
  public void query(Query query, int limit, IndexResultCollector collector) throws IOException {
    long start = stats.startRepositoryQuery();
    long totalHits = 0;
    IndexSearcher searcher = searcherManager.acquire();
    searcher.setSimilarity(new Similarity());
    try {
      TopDocs docs = searcher.search(query, limit);
      totalHits = docs.totalHits;
      for (ScoreDoc scoreDoc : docs.scoreDocs) {
        Document doc = searcher.doc(scoreDoc.doc);
        Object key = SerializerUtil.getKey(doc);
        if (logger.isDebugEnabled()) {
          logger.debug("query found doc:" + doc + ":" + scoreDoc);
        }
        collector.collect(key, scoreDoc.score);
      }
    } finally {
      searcherManager.release(searcher);
      stats.endRepositoryQuery(start, totalHits);
    }
  }

  @Override
  public synchronized void commit() throws IOException {
    long start = stats.startCommit();
    try {
      writer.commit();
      searcherManager.maybeRefresh();
    } finally {
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
        return writer.numDocs();
      } catch (AlreadyClosedException e) {
        // ignore
        return 0;
      }
    }
  }

  private static class Similarity extends TFIDFSimilarity {
    @Override
    public float tf(float freq) {
      return (float) Math.sqrt(freq);
    }

    @Override
    public float idf(long docFreq, long docCount) {
      return (float) Math.sqrt((docCount) / (1 + docFreq) + 1);
    }

    @Override
    public float lengthNorm(int length) {
      return (float) (1 / Math.sqrt(length));
    }

    @Override
    public float sloppyFreq(int distance) {
      return 0;
    }

    @Override
    public float scorePayload(int doc, int start, int end, BytesRef payload) {
      return 1;
    }
  }
}
