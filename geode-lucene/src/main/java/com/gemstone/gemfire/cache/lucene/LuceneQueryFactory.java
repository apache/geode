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
package com.gemstone.gemfire.cache.lucene;

import org.apache.lucene.queryparser.classic.ParseException;

import com.gemstone.gemfire.annotations.Experimental;

/**
 * Factory for creating instances of {@link LuceneQuery}.
 * To get an instance of this factory call {@link LuceneService#createLuceneQueryFactory}.
 * <P>
 * To use this factory configure it with the <code>set</code> methods and then
 * call {@link #create} to produce a {@link LuceneQuery} instance.
 * 
 * @author Xiaojian Zhou
 */
@Experimental
public interface LuceneQueryFactory {
  
  /**
   * Default query result limit is 100
   */
  public static final int DEFAULT_LIMIT = 100;
  
  /**
   *  Default page size of result is 0, which means no pagination
   */
  public static final int DEFAULT_PAGESIZE = 0;
  
  /**
   * Set page size for a query result. The default page size is 0 which means no pagination.
   * If specified negative value, throw IllegalArgumentException
   * @param pageSize
   * @return itself
   */
  LuceneQueryFactory setPageSize(int pageSize);
  
  /**
   * Set max limit of result for a query
   * If specified limit is less or equal to zero, throw IllegalArgumentException
   * @param limit
   * @return itself
   */
  LuceneQueryFactory setResultLimit(int limit);
  
  /**
   * Set a list of fields for result projection.
   * 
   * @param fieldNames
   * @return itself
   * 
   * @deprecated TODO This feature is not yet implemented
   */
  @Deprecated
  LuceneQueryFactory setProjectionFields(String... fieldNames);
  
  /**
   * Create wrapper object for lucene's QueryParser object using default standard analyzer.
   * The queryString is using lucene QueryParser's syntax. QueryParser is for easy-to-use 
   * with human understandable syntax. 
   *  
   * @param regionName region name
   * @param indexName index name
   * @param queryString query string in lucene QueryParser's syntax
   * @param <K> the key type in the query results
   * @param <V> the value type in the query results
   * @return LuceneQuery object
   * @throws ParseException
   */
  public <K, V> LuceneQuery<K, V> create(String indexName, String regionName, String queryString) 
      throws ParseException;

  /**
   * Creates a wrapper object for Lucene's Query object. This {@link LuceneQuery} builder method could be used in
   * advanced cases, such as cases where Lucene's Query object construction needs Lucene's API over query string.
   * 
   * @param indexName index name
   * @param regionName region name
   * @param provider constructs and provides a Lucene Query object
   * @param <K> the key type in the query results
   * @param <V> the value type in the query results
   * @return LuceneQuery object
   */
  public <K, V> LuceneQuery<K, V> create(String indexName, String regionName, LuceneQueryProvider provider);
}
