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

package com.gemstone.gemfire.cache.lucene.internal.repository;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TopDocs;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.lucene.internal.repository.serializer.LuceneSerializer;
import com.gemstone.gemfire.cache.lucene.internal.repository.serializer.SerializerUtil;

/**
 * A repository that writes to a single lucene index writer
 */
public class IndexRepositoryImpl implements IndexRepository {
  
  private static final boolean APPLY_ALL_DELETES = System
      .getProperty("gemfire.IndexRepository.APPLY_ALL_DELETES", "true")
      .equalsIgnoreCase("true");
  
  private final IndexWriter writer;
  private final LuceneSerializer serializer;
  private final SearcherManager searcherManager;
  private Region<?,?> region;
  
  public IndexRepositoryImpl(Region<?,?> region, IndexWriter writer, LuceneSerializer serializer) throws IOException {
    this.region = region;
    this.writer = writer;
    searcherManager = new SearcherManager(writer, APPLY_ALL_DELETES, null);
    this.serializer = serializer;
  }

  @Override
  public void create(Object key, Object value) throws IOException {
      Document doc = new Document();
      SerializerUtil.addKey(key, doc);
      serializer.toDocument(value, doc);
      writer.addDocument(doc);
  }

  @Override
  public void update(Object key, Object value) throws IOException {
    Document doc = new Document();
    SerializerUtil.addKey(key, doc);
    serializer.toDocument(value, doc);
    writer.updateDocument(SerializerUtil.getKeyTerm(doc), doc);
  }

  @Override
  public void delete(Object key) throws IOException {
    Term keyTerm = SerializerUtil.toKeyTerm(key);
    writer.deleteDocuments(keyTerm);
  }

  @Override
  public void query(Query query, int limit, IndexResultCollector collector) throws IOException {
    IndexSearcher searcher = searcherManager.acquire();
    try {
      TopDocs docs = searcher.search(query, limit);
      for(ScoreDoc scoreDoc : docs.scoreDocs) {
        Document doc = searcher.doc(scoreDoc.doc);
        Object key = SerializerUtil.getKey(doc);
        collector.collect(key, scoreDoc.score);
      }
    } finally {
      searcherManager.release(searcher);
    }
  }

  @Override
  public synchronized void commit() throws IOException {
    writer.commit();
    searcherManager.maybeRefresh();
  }

  public IndexWriter getWriter() {
    return writer;
  }

  public LuceneSerializer getSerializer() {
    return serializer;
  }

  @Override
  public boolean isClosed() {
    return region.isDestroyed();
  }
}
