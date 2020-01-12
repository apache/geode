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
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.lucene.internal.LuceneIndexCreationInProgressException;

/**
 *
 * The LuceneService provides the capability to create Lucene indexes and execute lucene queries on
 * data stored in Geode regions. The Lucene indexes are automatically maintained by Geode whenever
 * entries are updated in the associated regions.
 *
 * <p>
 * To obtain an instance of LuceneService, use
 * {@link LuceneServiceProvider#get(GemFireCache cache)}.
 * </p>
 * <p>
 * Lucene indexes can be created using gfsh, xml, or the java API. Below is an example of creating a
 * Lucene index with the java API. The Lucene index should be created on each member that has the
 * region that is being indexed.
 * </p>
 *
 * <pre>
 * {
 *   &#64;code
 *       luceneService.createIndexFactory()
 *        .addField("name")
 *        .addField("zipcode")
 *        .addField("email", new KeywordAnalyzer())
 *        .create(indexName, regionName);
 * }
 * </pre>
 * <p>
 * You can also specify what {@link Analyzer} to use for each field. In the example above, email is
 * being tokenized with the KeywordAnalyzer so it is treated as a single word. The default analyzer
 * if none is specified is the {@link StandardAnalyzer}.
 * </p>
 *
 *
 * Indexes should be created on all peers that host the region being indexed. Clients do not need to
 * define the index, they can directly execute queries using this service.
 *
 * <p>
 * Queries on this service can return either the region keys, values, or both that match a Lucene
 * query expression. To execute a query, start with the {@link #createLuceneQueryFactory()} method.
 * </p>
 *
 * <pre>
 * {
 *   &#64;code
 *   LuceneQuery query = luceneService.createLuceneQueryFactory().setLimit(200).create(indexName,
 *       regionName, "name:John AND zipcode:97006", defaultField);
 *   Collection results = query.findValues();
 * }
 * </pre>
 *
 * <p>
 * The Lucene index data is colocated with the region that is indexed. This means that the index
 * data will be partitioned, copied, or persisted using the same configuration options you provide
 * for the region that is indexed. Queries will automatically be distributed in parallel to cover
 * all partitions of a partitioned region.
 * </p>
 * <p>
 * Indexes are maintained asynchronously, so changes to regions may not be immediately reflected in
 * the index. This means that queries executed using this service may return stale results. Use the
 * {@link #waitUntilFlushed(String, String, long, TimeUnit)} method if you need to wait for your
 * changes to be indexed before executing a query, however this method should be used sparingly
 * because it is an expensive operation.
 * </p>
 *
 * <p>
 * Currently, only partitioned regions are supported. Creating an index on a region with
 * {@link DataPolicy#REPLICATE} will fail.
 * </p>
 *
 */
public interface LuceneService {

  /**
   * A special field name that indicates that the entire region value should be indexed. This will
   * only work if the region value is a String or Number, in which case a Lucene document will be
   * created with a single field with this name.
   */
  String REGION_VALUE_FIELD = "__REGION_VALUE_FIELD";

  /**
   * Get a factory for creating a Lucene index on this member.
   */
  LuceneIndexFactory createIndexFactory();

  /**
   * Destroy the Lucene index
   *
   * @param indexName the name of the index to destroy
   * @param regionPath the path of the region whose index to destroy
   */
  void destroyIndex(String indexName, String regionPath);

  /**
   * Destroy all the Lucene indexes for the region
   *
   * @param regionPath The path of the region on which to destroy the indexes
   */
  void destroyIndexes(String regionPath);

  /**
   * Get the Lucene index object specified by region name and index name
   *
   * @param indexName index name
   * @param regionPath region name
   * @return LuceneIndex object
   */
  LuceneIndex getIndex(String indexName, String regionPath);

  /**
   * get all the Lucene indexes.
   *
   * @return all index objects in a Collection
   */
  Collection<LuceneIndex> getAllIndexes();

  /**
   * Create a factory for building a Lucene query.
   */
  LuceneQueryFactory createLuceneQueryFactory();

  /**
   * returns the cache to which the LuceneService belongs
   *
   */
  Cache getCache();


  /**
   * Wait until the current entries in cache are indexed.
   *
   * Lucene indexes are maintained asynchronously. This means that updates to the region will not be
   * immediately reflected in the Lucene index. This method will either timeout or wait until any
   * data put into the region before this method call is flushed to the lucene index.
   *
   * This method is an expensive operation, so using it before every query is highly discouraged.
   *
   * @param indexName index name
   *
   * @param regionPath region name
   *
   * @param timeout max wait time
   *
   * @param unit Time unit associated with the max wait time
   *
   * @return true if entries are flushed within timeout, false if the timeout has elapsed
   */
  boolean waitUntilFlushed(String indexName, String regionPath, long timeout, TimeUnit unit)
      throws InterruptedException;

  /**
   * Returns if the indexing process is in progress
   *
   * Before executing a lucene query, it can be checked if the indexing operation is in progress.
   * Queries executed during the indexing process will get a
   * {@link LuceneIndexCreationInProgressException}
   *
   * @param indexName index name
   *
   * @param regionPath region name
   *
   * @return true if the indexing operation is in progress otherwise false.
   */
  boolean isIndexingInProgress(String indexName, String regionPath);
}
