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

import com.gemstone.gemfire.annotations.Experimental;

/**
 * Provides wrapper object of Lucene's Query object and execute the search. 
 * <p>Instances of this interface are created using
 * {@link LuceneQueryFactory#create}.
 * 
 */
@Experimental
public interface LuceneQuery<K, V> {
  /**
   * Execute the search and get results. 
   */
  public LuceneQueryResults<K, V> search();
  
  /**
   * Get page size setting of current query. 
   */
  public int getPageSize();
  
  /**
   * Get limit size setting of current query. 
   */
  public int getLimit();

  /**
   * Get projected fields setting of current query. 
   */
  public String[] getProjectedFieldNames();
}
