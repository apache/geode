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

import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;

import org.apache.geode.cache.query.Query;

/**
 * Factory for configuring a Lucene query. Use this factory to set parameters of the query such as
 * page size, result limit, and query expression. To get an instance of this factory call
 * {@link LuceneService#createLuceneQueryFactory}.
 * <P>
 * To use this factory configure it with the <code>set</code> methods and then call one of the
 * create methods on this class. {@link #create(String, String, String, String)} creates a query by
 * parsing a query string. {@link #create(String, String, LuceneQueryProvider)} creates a query
 * based on a custom Lucene {@link Query} object.
 *
 */
public interface LuceneQueryFactory {

  /**
   * Default query result limit is 100
   */
  int DEFAULT_LIMIT = 100;

  /**
   * Default page size of result is 0, which means no pagination
   */
  int DEFAULT_PAGESIZE = 0;

  /**
   * Set page size for a query result. The default page size is 0 which means no pagination.
   *
   * @throws IllegalArgumentException if the value is less than 0
   */
  LuceneQueryFactory setPageSize(int pageSize);

  /**
   * Set maximum number of results for a query. By default, the limit is set to
   * {@link #DEFAULT_LIMIT} which is 100.
   *
   * @throws IllegalArgumentException if the value is less than or equal to zero.
   */
  LuceneQueryFactory setLimit(int limit);

  /**
   * Creates a query based on a query string which is parsed by Lucene's
   * {@link StandardQueryParser}. See the javadocs for {@link StandardQueryParser} for details on
   * the syntax of the query string. The query string and default field as passed as is to
   * {@link StandardQueryParser#parse(String, String)}
   *
   * @param regionName region name
   * @param indexName index name
   * @param queryString Query string parsed by Lucene's StandardQueryParser
   * @param defaultField default field used by the Lucene's StandardQueryParser
   * @param <K> the key type in the query results
   * @param <V> the value type in the query results
   * @return LuceneQuery object
   */
  <K, V> LuceneQuery<K, V> create(String indexName, String regionName, String queryString,
      String defaultField);

  /**
   * <p>
   * Create a query based on a programmatically constructed Lucene {@link Query}. This can be used
   * for queries that are not covered by {@link StandardQueryParser}, such as range queries.
   * </p>
   * <p>
   * Because Geode may execute the Lucene query on multiple nodes in parallel and {@link Query} is
   * not serializable, this method requires a serializable {@link LuceneQueryProvider} that can
   * create a {@link Query} on the nodes hosting the Lucene index.
   * </p>
   * <p>
   * Here's an example of using this method to create a range query on an integer field called
   * "age."
   * </p>
   *
   * <pre>
   * {@code
   *   LuceneQuery query = factory.create("index", "region", index -> IntPoint.newRangeQuery("age", 20, 30))
   * }
   * </pre>
   *
   * @param indexName index name
   * @param regionName region name
   * @param provider constructs and provides a Lucene {@link Query}.
   * @param <K> the key type in the query results
   * @param <V> the value type in the query results
   * @return LuceneQuery object
   */
  <K, V> LuceneQuery<K, V> create(String indexName, String regionName,
      LuceneQueryProvider provider);
}
