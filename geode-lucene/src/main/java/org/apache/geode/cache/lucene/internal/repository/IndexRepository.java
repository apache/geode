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

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.Query;

import org.apache.geode.cache.Region;

/**
 * An Repository interface for the writing data to lucene.
 */
public interface IndexRepository {

  /**
   * Create a new entry in the lucene index
   *
   */
  void create(Object key, Object value) throws IOException;

  /**
   * Update the entries in the lucene index
   *
   */
  void update(Object key, Object value) throws IOException;

  /**
   * Delete the entries in the lucene index
   *
   */
  void delete(Object key) throws IOException;

  /**
   * Query the index index repository, passing the results to the collector Only the documents with
   * the top scores, up to the limit, will be passed to the collector, in order of score.
   *
   * @param limit the maximum number of hits to return
   * @param collector the class to aggregate the hits
   *
   */
  void query(Query query, int limit, IndexResultCollector collector) throws IOException;

  /**
   * Executes {@code query} and invokes {@code onSearcherAcquired} immediately after acquiring the
   * consistent Lucene {@code IndexSearcher} cut (before scanning hits).
   */
  default void query(Query query, int limit, IndexResultCollector collector,
      Runnable onSearcherAcquired) throws IOException {
    try {
      query(query, limit, collector);
    } finally {
      if (onSearcherAcquired != null) {
        onSearcherAcquired.run();
      }
    }
  }

  /**
   * Executes {@code query} fetching up to {@code oversampleK} candidates from Lucene HNSW and
   * rescores them using exact stored FP32 {@code .vec} vectors from each leaf's
   * {@code FloatVectorValues} before passing the top {@code k} hits in descending score order to
   * {@code collector}.
   */
  default void queryWithFp32Rerank(Query query, int k, int oversampleK, String vectorField,
      float[] queryVector, VectorSimilarityFunction similarity, IndexResultCollector collector)
      throws IOException {
    query(query, k, collector);
  }

  /**
   * Executes {@link #queryWithFp32Rerank} and invokes {@code onSearcherAcquired} immediately after
   * acquiring the consistent Lucene {@code IndexSearcher} cut (before HNSW/rerank traversal).
   */
  default void queryWithFp32Rerank(Query query, int k, int oversampleK, String vectorField,
      float[] queryVector, VectorSimilarityFunction similarity, IndexResultCollector collector,
      Runnable onSearcherAcquired) throws IOException {
    try {
      queryWithFp32Rerank(query, k, oversampleK, vectorField, queryVector, similarity, collector);
    } finally {
      if (onSearcherAcquired != null) {
        onSearcherAcquired.run();
      }
    }
  }

  /**
   * Commit the changes to all lucene index
   *
   */
  void commit() throws IOException;

  /**
   * Commit the changes to the Lucene index and perform a blocking searcher refresh so all
   * committed mutations are guaranteed visible to subsequent {@code IndexSearcher} acquisitions
   * before returning.
   */
  default void commitAndRefreshBlocking() throws IOException {
    commit();
  }

  Region<?, ?> getRegion();

  /**
   * Check to see if this repository is closed due to underlying resources being closed or destroyed
   *
   * @return true if this repository is closed.
   */
  boolean isClosed();

  /**
   * For debugging purposes, return the underlying IndexWriter
   */
  IndexWriter getWriter();

  /**
   * Clean up any resources associated with this index repository.
   */
  void cleanup();
}
