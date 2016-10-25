/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.cache.lucene;

import java.util.Collection;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.lucene.internal.LuceneIndexCreationProfile;

/**
 * LuceneService instance is a singleton for each cache.
 * 
 * It provides handle for managing the {@link LuceneIndex} and create the {@link LuceneQuery}
 * via {@link LuceneQueryFactory}
 * 
 * </p>
 * Example: <br>
 * 
 * <pre>
 * At client and server JVM, initializing cache will create the LuceneServiceImpl object, 
 * which is a singleton at each JVM. 
 * 
 * At each server JVM, for data region to create index, create the index on fields with default analyzer:
 * LuceneIndex index = luceneService.createIndex(indexName, regionName, "field1", "field2", "field3"); 
 * or create index on fields with specified analyzer:
 * LuceneIndex index = luceneService.createIndex(indexName, regionName, analyzerPerField);
 * 
 * We can also create index via cache.xml or gfsh.
 * 
 * At client side, create query and run the search:
 * 
 * LuceneQuery query = luceneService.createLuceneQueryFactory().setLimit(200).setPageSize(20)
 * .setResultTypes(SCORE, VALUE, KEY).setFieldProjection("field1", "field2")
 * .create(indexName, regionName, querystring, analyzer);
 * 
 * The querystring is using lucene's queryparser syntax, such as "field1:zhou* AND field2:gzhou@pivotal.io"
 *  
 * PageableLuceneQueryResults results = query.search();
 * 
 * If pagination is not specified:
 * List list = results.getNextPage(); // return all results in one getNextPage() call
 * or if paging is specified:
 * if (results.hasNextPage()) {
 *   List page = results.nextPage(); // return resules page by page
 * }
 * 
 * The item of the list is either the domain object or instance of {@link LuceneResultStruct}
 * </pre>
 * 
 *
 */
@Experimental
public interface LuceneService {

  /**
   * A special field name that indicates that the entire region value should
   * be indexed. This will only work if the region value is a String or Number, in
   * which case a lucene document will be created with a single field with this name.
   */
  String REGION_VALUE_FIELD = "__REGION_VALUE_FIELD";

  /**
   * Create a lucene index using default analyzer.
   * @param fields The fields of the object to index. Only fields listed here will be stored
   * in the index. Fields should map to PDX fieldNames if the object is serialized with PDX, or
   * to java fields on the object otherwise. The special field name {{@link #REGION_VALUE_FIELD}}
   * indicates that the entire value should be stored as a single field in the index.
   */
  public void createIndex(String indexName, String regionPath, String... fields);
  
  /**
   * Create a lucene index using specified analyzer per field
   * 
   * @param indexName index name
   * @param regionPath region name
   * @param analyzerPerField A map of fields to analyzers. See {{@link #createIndex(String, String, String...)}}
   * for details on valid values for fields. Each field will be tokenized using the provided Analyzer.
   */
  public void createIndex(String indexName, String regionPath,
      Map<String, Analyzer> analyzerPerField);

  /**
   * Destroy the lucene index
   * 
   * @param index index object
   * @deprecated TODO This feature is not yet implemented
   */
  @Deprecated
  public void destroyIndex(LuceneIndex index);
  
  /**
   * Get the lucene index object specified by region name and index name
   * @param indexName index name
   * @param regionPath region name
   * @return LuceneIndex object
   */
  public LuceneIndex getIndex(String indexName, String regionPath);
  
  /**
   * get all the lucene indexes.
   * @return all index objects in a Collection
   */
  public Collection<LuceneIndex> getAllIndexes();

  /**
   * create LuceneQueryFactory
   * @return LuceneQueryFactory object
   */
  public LuceneQueryFactory createLuceneQueryFactory();

 }
