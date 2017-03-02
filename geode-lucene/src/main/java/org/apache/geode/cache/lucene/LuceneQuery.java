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

import org.apache.geode.annotations.Experimental;

/**
 * <p>
 * A query on a Lucene index. Instances of this interface are created using
 * {@link LuceneQueryFactory#create}. Once this query is constructed, use one of the find methods to
 * find region entries that match this query.
 * </p>
 * <p>
 * Instances obtained from {@link LuceneQueryFactory} are immutable, so they are safe for reuse and
 * can be shared by multiple threads.
 * </p>
 * <p>
 * Because Lucene indexes are maintained asynchronously, results returned from the find methods may
 * not reflect the most recent updates to the region.
 * </p>
 * <p>
 * Results are returned in order of their score with respect to this query. See
 * {@link org.apache.lucene.search} for details on how Lucene scores entries.
 * </p>
 */
@Experimental
public interface LuceneQuery<K, V> {
  /**
   * Execute the query and return the region keys that match this query, up to the limit specified
   * by {@link #getLimit()}.
   * 
   * @throws LuceneQueryException if the query could not be parsed or executed.
   */
  public Collection<K> findKeys() throws LuceneQueryException;

  /**
   * Execute the query and return the region values that match this query, up to the limit specified
   * by {@link #getLimit()}
   * 
   * @throws LuceneQueryException if the query could not be parsed or executed.
   */
  public Collection<V> findValues() throws LuceneQueryException;

  /**
   * Execute the query and return a list of {@link LuceneResultStruct}s that match this query, up to
   * the limit specified by {@link #getLimit()} A {@link LuceneResultStruct} contains the region
   * key, value, and a score for that entry.
   *
   * @throws LuceneQueryException if the query could not be parsed or executed.
   */
  public List<LuceneResultStruct<K, V>> findResults() throws LuceneQueryException;

  /**
   * Execute the query and get a {@link PageableLuceneQueryResults}. The
   * {@link PageableLuceneQueryResults} provides the ability to fetch a page of results at a time,
   * as specified by {@link #getPageSize()}
   *
   * @throws LuceneQueryException if the query could not be parsed or executed.
   */
  public PageableLuceneQueryResults<K, V> findPages() throws LuceneQueryException;

  /**
   * Get page size setting of current query.
   */
  public int getPageSize();

  /**
   * Get limit size setting of current query.
   */
  public int getLimit();

}
