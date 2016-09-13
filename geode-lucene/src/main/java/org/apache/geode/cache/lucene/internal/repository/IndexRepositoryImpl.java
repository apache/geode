/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.geode.cache.lucene.internal.repository;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.lucene.internal.LuceneIndexStats;
import org.apache.geode.cache.lucene.internal.repository.serializer.LuceneSerializer;
import org.apache.geode.cache.lucene.internal.repository.serializer.SerializerUtil;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.store.AlreadyClosedException;

import java.io.IOException;
import java.util.function.IntSupplier;

/**
 * A repository that writes to a single lucene index writer
 */
public class IndexRepositoryImpl implements IndexRepository {
  
  private static final boolean APPLY_ALL_DELETES = System
      .getProperty(DistributionConfig.GEMFIRE_PREFIX + "IndexRepository.APPLY_ALL_DELETES", "true")
      .equalsIgnoreCase("true");
  
  private final IndexWriter writer;
  private final LuceneSerializer serializer;
  private final SearcherManager searcherManager;
  private Region<?,?> region;
  private Region<?,?> userRegion;
  private LuceneIndexStats stats;
  private DocumentCountSupplier documentCountSupplier;

  private static final Logger logger = LogService.getLogger();
  
  public IndexRepositoryImpl(Region<?,?> region, IndexWriter writer, LuceneSerializer serializer, LuceneIndexStats stats, Region<?, ?> userRegion) throws IOException {
    this.region = region;
    this.userRegion = userRegion;
    this.writer = writer;
    searcherManager = new SearcherManager(writer, APPLY_ALL_DELETES, true, null);
    this.serializer = serializer;
    this.stats = stats;
    documentCountSupplier = new DocumentCountSupplier();
    stats.addDocumentsSupplier(documentCountSupplier);
  }

  @Override
  public void create(Object key, Object value) throws IOException {
    long start = stats.startUpdate();
    try {
      Document doc = new Document();
      SerializerUtil.addKey(key, doc);
      serializer.toDocument(value, doc);
      writer.addDocument(doc);
    } finally {
      stats.endUpdate(start);
    }
  }

  @Override
  public void update(Object key, Object value) throws IOException {
    long start = stats.startUpdate();
    try {
      Document doc = new Document();
      SerializerUtil.addKey(key, doc);
      serializer.toDocument(value, doc);
      writer.updateDocument(SerializerUtil.getKeyTerm(doc), doc);
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
    long start = stats.startQuery();
    int totalHits = 0;
    IndexSearcher searcher = searcherManager.acquire();
    try {
      TopDocs docs = searcher.search(query, limit);
      totalHits = docs.totalHits;
      for(ScoreDoc scoreDoc : docs.scoreDocs) {
        Document doc = searcher.doc(scoreDoc.doc);
        Object key = SerializerUtil.getKey(doc);
        if (logger.isDebugEnabled()) {
          logger.debug("query found doc:"+doc+":"+scoreDoc);
        }
        collector.collect(key, scoreDoc.score);
      }
    } finally {
      searcherManager.release(searcher);
      stats.endQuery(start, totalHits);
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
    return userRegion.isDestroyed();
  }

  @Override
  public void cleanup() {
    stats.removeDocumentsSupplier(documentCountSupplier);
    try {
      writer.close();
    }
    catch (IOException e) {
      logger.warn("Unable to clean up index repository", e);
    }
  }

  private class DocumentCountSupplier implements IntSupplier {
    @Override
    public int getAsInt() {
      if(isClosed()) {
        stats.removeDocumentsSupplier(this);
        return 0;
      }
      try {
        return writer.numDocs();
      } catch(AlreadyClosedException e) {
        //ignore
        return 0;
      }
    }
  }
}
