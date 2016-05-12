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

package com.gemstone.gemfire.cache.lucene.internal;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.lucene.LuceneQuery;
import com.gemstone.gemfire.cache.lucene.LuceneQueryFactory;
import com.gemstone.gemfire.cache.lucene.LuceneQueryProvider;

public class LuceneQueryFactoryImpl implements LuceneQueryFactory {
  private int limit = DEFAULT_LIMIT;
  private int pageSize = DEFAULT_PAGESIZE;
  private String[] projectionFields = null;
  private Cache cache;
  
  LuceneQueryFactoryImpl(Cache cache) {
    this.cache = cache;
  }
  
  @Override
  public LuceneQueryFactory setPageSize(int pageSize) {
    this.pageSize = pageSize;
    return this;
  }

  @Override
  public LuceneQueryFactory setResultLimit(int limit) {
    this.limit = limit;
    return this;
  }

  @Override
  public <K, V> LuceneQuery<K, V> create(String indexName, String regionName, String queryString) {
    return create(indexName, regionName, new StringQueryProvider(queryString));
  }
  
  public <K, V> LuceneQuery<K, V> create(String indexName, String regionName, LuceneQueryProvider provider) {
    Region<K, V> region = cache.getRegion(regionName);
    if(region == null) {
      throw new IllegalArgumentException("Region not found: " + regionName);
    }
    LuceneQueryImpl<K, V> luceneQuery = new LuceneQueryImpl<K, V>(indexName, region, provider, projectionFields, limit, pageSize);
    return luceneQuery;
  }
  
  @Override
  public LuceneQueryFactory setProjectionFields(String... fieldNames) {
    projectionFields = fieldNames.clone();
    return this;
  }

}
