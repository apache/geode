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

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.cache.lucene.LuceneQuery;
import com.gemstone.gemfire.cache.lucene.LuceneQueryFactory;
import com.gemstone.gemfire.cache.lucene.LuceneQueryProvider;
import com.gemstone.gemfire.cache.lucene.LuceneQueryResults;
import com.gemstone.gemfire.cache.lucene.internal.distributed.LuceneFunction;
import com.gemstone.gemfire.cache.lucene.internal.distributed.LuceneFunctionContext;
import com.gemstone.gemfire.cache.lucene.internal.distributed.TopEntries;
import com.gemstone.gemfire.cache.lucene.internal.distributed.TopEntriesCollector;
import com.gemstone.gemfire.cache.lucene.internal.distributed.TopEntriesCollectorManager;
import com.gemstone.gemfire.cache.lucene.internal.distributed.TopEntriesFunctionCollector;

public class LuceneQueryImpl<K, V> implements LuceneQuery<K, V> {
  private int limit = LuceneQueryFactory.DEFAULT_LIMIT;
  private int pageSize = LuceneQueryFactory.DEFAULT_PAGESIZE;
  private String indexName;
  // The projected fields are local to a specific index per Query object. 
  private String[] projectedFieldNames;
  /* the lucene Query object to be wrapped here */
  private LuceneQueryProvider query;
  private Region<K, V> region;
  
  public LuceneQueryImpl(String indexName, Region<K, V> region, LuceneQueryProvider provider, String[] projectionFields, 
      int limit, int pageSize) {
    this.indexName = indexName;
    this.region = region;
    this.limit = limit;
    this.pageSize = pageSize;
    this.projectedFieldNames = projectionFields;
    this.query = provider;
  }

  @Override
  public LuceneQueryResults<K, V> search() {
    TopEntriesCollectorManager manager = new TopEntriesCollectorManager(null, limit);
    LuceneFunctionContext<TopEntriesCollector> context = new LuceneFunctionContext<>(query, indexName, manager, limit);
    TopEntriesFunctionCollector collector = new TopEntriesFunctionCollector(context);

    ResultCollector<TopEntriesCollector, TopEntries> rc = (ResultCollector<TopEntriesCollector, TopEntries>) FunctionService.onRegion(region)
        .withArgs(context)
        .withCollector(collector)
        .execute(LuceneFunction.ID);
    
    //TODO provide a timeout to the user?
    TopEntries entries = rc.getResult();
    
    return new LuceneQueryResultsImpl<K, V>(entries.getHits(), region, pageSize);
  }

  @Override
  public int getPageSize() {
    return this.pageSize;
  }

  @Override
  public int getLimit() {
    return this.limit;
  }

  @Override
  public String[] getProjectedFieldNames() {
    return this.projectedFieldNames;
  }
}
