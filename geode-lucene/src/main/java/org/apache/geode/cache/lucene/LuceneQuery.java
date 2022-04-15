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
package org.apache.geode.cache.lucene;

import java.util.Collection;
import java.util.List;

import org.apache.geode.CancelException;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.lucene.internal.LuceneQueryImpl;
import org.apache.geode.cache.persistence.PartitionOfflineException;

/**
 * <p>
 * A query on a Lucene index. Instances of this interface are created using
 * {@link LuceneQueryFactory#create}. Once this query is constructed, use one of the find methods to
 * find region entries that match this query.
 * <p>
 * Instances obtained from {@link LuceneQueryFactory} are immutable, so they are safe for reuse and
 * can be shared by multiple threads.
 * <p>
 * Because Lucene indexes are maintained asynchronously, results returned from the find methods may
 * not reflect the most recent updates to the region.
 * <p>
 * Results are returned in order of their score with respect to this query.
 */
public interface LuceneQuery<K, V> {
  /**
   * Execute the query and return the region keys that match this query, up to the limit specified
   * by {@link #getLimit()}.
   *
   * @return Collection of Apache Geode region keys that satisfy the Lucene query.
   * @throws LuceneQueryException if the query could not be parsed or executed.
   * @throws CacheClosedException if the cache was closed while the Lucene query was being executed.
   * @throws FunctionException if the function execution mechanism encounters an error while
   *         executing the Lucene query.
   * @throws PartitionOfflineException if the node containing the buckets required to execute the
   *         Lucene query goes offline.
   * @throws CancelException if a cancel is in progress while the Lucene query was being executed.
   */
  Collection<K> findKeys() throws LuceneQueryException;

  /**
   * Execute the query and return the region values that match this query, up to the limit specified
   * by {@link #getLimit()}
   *
   * @return a Collection of Apache Geode region values that satisfy the Lucene query.
   * @throws LuceneQueryException if the query could not be parsed or executed.
   * @throws CacheClosedException if the cache was closed while the Lucene query was being executed.
   * @throws FunctionException if the function execution mechanism encounters an error while
   *         executing the Lucene query.
   * @throws PartitionOfflineException if the node containing the buckets required to execute the
   *         Lucene query goes offline.
   * @throws CancelException if a cancel is in progress while the Lucene query was being executed.
   */
  Collection<V> findValues() throws LuceneQueryException;

  /**
   * Execute the query and return a list of {@link LuceneResultStruct}s that match this query, up to
   * the limit specified by {@link #getLimit()} A {@link LuceneResultStruct} contains the region
   * key, value, and a score for that entry.
   *
   * @return a List of LuceneResultStruct that match the Lucene query
   * @throws LuceneQueryException if the query could not be parsed or executed.
   * @throws CacheClosedException if the cache was closed while the Lucene query was being executed.
   * @throws FunctionException if the function execution mechanism encounters an error while
   *         executing the Lucene query.
   * @throws PartitionOfflineException if the node containing the buckets required to execute the
   *         Lucene query goes offline.
   * @throws CancelException if a cancel is in progress while the Lucene query was being executed.
   */
  List<LuceneResultStruct<K, V>> findResults() throws LuceneQueryException;

  /**
   * Execute the query and get a {@link PageableLuceneQueryResults}. The
   * {@link PageableLuceneQueryResults} provides the ability to fetch a page of results at a time,
   * as specified by {@link #getPageSize()}
   *
   * @return a PageableLuceneQuery that can be used to fetch one page of result at a time.
   * @throws LuceneQueryException if the query could not be parsed or executed.
   * @throws CacheClosedException if the cache was closed while the Lucene query was being executed.
   * @throws FunctionException if the function execution mechanism encounters an error while
   *         executing the Lucene query.
   * @throws PartitionOfflineException if the node containing the buckets required to execute the
   *         Lucene query goes offline.
   * @throws CancelException if a cancel is in progress while the Lucene query was being executed.
   */
  PageableLuceneQueryResults<K, V> findPages() throws LuceneQueryException;

  /**
   * Gets the page size setting of current query. This page size is set while creating
   * {@link LuceneQueryImpl} object
   *
   * @return int value representing the page size of the current query
   */
  int getPageSize();

  /**
   * Get limit size setting of current query. This value is the maximum number of results that can
   * be returned by the Lucene query. This value is set while creating the {@link LuceneQueryImpl}
   * object
   *
   * @return int value representing the limit of the current query
   */
  int getLimit();

}
