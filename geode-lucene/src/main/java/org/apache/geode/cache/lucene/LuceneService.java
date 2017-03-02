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
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.geode.cache.DataPolicy;
import org.apache.lucene.analysis.Analyzer;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.lucene.internal.LuceneIndexCreationProfile;

/**
 *
 * The LuceneService provides the capability to create Lucene indexes and execute lucene queries on
 * data stored in Geode regions. The Lucene indexes are automatically maintained by Geode whenever
 * entries are updated in the associated regions.
 *
 * <p>
 * To obtain an instance of LuceneService, use {@link LuceneServiceProvider#get(GemFireCache)}.
 * </p>
 * <p>
 * Lucene indexes can be created using gfsh, xml, or the java API. Below is an example of creating a
 * Lucene index with the java API. The Lucene index created on each member that will host data for
 * the region.
 * </p>
 * 
 * <pre>
 * {
 *   &#64;code
 *   LuceneIndex index =
 *       luceneService.createIndex(indexName, regionName, "field1", "field2", "field3");
 * }
 * </pre>
 * <p>
 * You can also specify what {@link Analyzer} to use for each field.
 * </p>
 * 
 * <pre>
 * {
 *   &#64;code
 *   LuceneIndex index = luceneService.createIndex(indexName, regionName, analyzerPerField);
 * }
 * </pre>
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
 *   Collection<Object> results = query.findValues();
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
@Experimental
public interface LuceneService {

  /**
   * A special field name that indicates that the entire region value should be indexed. This will
   * only work if the region value is a String or Number, in which case a Lucene document will be
   * created with a single field with this name.
   */
  public String REGION_VALUE_FIELD = "__REGION_VALUE_FIELD";

  /**
   * Create a Lucene index using default analyzer.
   * 
   * @param fields The fields of the object to index. Only fields listed here will be stored in the
   *        index. Fields should map to PDX fieldNames if the object is serialized with PDX, or to
   *        java fields on the object otherwise. The special field name {@link #REGION_VALUE_FIELD}
   *        indicates that the entire value should be stored as a single field in the index.
   */
  public void createIndex(String indexName, String regionPath, String... fields);

  /**
   * Create a Lucene index using specified {@link Analyzer} per field. Analyzers are used by Lucene
   * to tokenize your field into individual words.
   * 
   * @param indexName index name
   * @param regionPath region name
   * @param analyzerPerField A map of fields to analyzers. See
   *        {@link #createIndex(String, String, String...)} for details on valid values for fields.
   *        Each field will be tokenized using the provided Analyzer.
   */
  public void createIndex(String indexName, String regionPath,
      Map<String, Analyzer> analyzerPerField);

  /**
   * Destroy the Lucene index
   *
   * @param indexName the name of the index to destroy
   * @param regionPath the path of the region whose index to destroy
   */
  public void destroyIndex(String indexName, String regionPath);

  /**
   * Destroy all the Lucene indexes for the region
   *
   * @param regionPath The path of the region on which to destroy the indexes
   */
  public void destroyIndexes(String regionPath);

  /**
   * Get the Lucene index object specified by region name and index name
   * 
   * @param indexName index name
   * @param regionPath region name
   * @return LuceneIndex object
   */
  public LuceneIndex getIndex(String indexName, String regionPath);

  /**
   * get all the Lucene indexes.
   * 
   * @return all index objects in a Collection
   */
  public Collection<LuceneIndex> getAllIndexes();

  /**
   * Create a factory for building a Lucene query.
   */
  public LuceneQueryFactory createLuceneQueryFactory();

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
   * @param regionPath region name
   * @param timeout max wait time
   * @param unit Time unit associated with the max wait time
   * @return true if entries are flushed within timeout, false if the timeout has elapsed
   */
  public boolean waitUntilFlushed(String indexName, String regionPath, long timeout, TimeUnit unit)
      throws InterruptedException;
}
