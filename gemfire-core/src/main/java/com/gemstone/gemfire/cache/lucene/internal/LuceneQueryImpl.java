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
package com.gemstone.gemfire.cache.lucene.internal;

import java.util.Set;

import org.apache.lucene.search.Query;

import com.gemstone.gemfire.cache.lucene.LuceneQuery;
import com.gemstone.gemfire.cache.lucene.LuceneQueryFactory;
import com.gemstone.gemfire.cache.lucene.LuceneQueryFactory.ResultType;
import com.gemstone.gemfire.cache.lucene.LuceneQueryResults;

public class LuceneQueryImpl implements LuceneQuery {
  private int limit = LuceneQueryFactory.DEFAULT_LIMIT;
  private int pageSize = LuceneQueryFactory.DEFAULT_PAGESIZE;
  private String indexName;
  private String regionName;
  private Set<ResultType> resultTypes;
  
  // The projected fields are local to a specific index per Query object. 
  private Set<String> projectedFieldNames;
  
  /* the lucene Query object to be wrapped here */
  private Query query;
  
  LuceneQueryImpl(String indexName, String regionName, int limit, int pageSize, Set<ResultType> resultTypes, 
      Set<String> projectionFieldNames, Query query) {
    this.indexName = indexName;
    this.regionName = regionName;
    this.limit = limit;
    this.pageSize = pageSize;
    this.resultTypes = resultTypes;
    this.projectedFieldNames = projectionFieldNames;
    this.query = query;
  }

  @Override
  public LuceneQueryResults<?> search() {
    // TODO Auto-generated method stub
    return null;
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
  public ResultType[] getResultTypes() {
    return (ResultType[])this.resultTypes.toArray();
  }

  @Override
  public String[] getProjectedFieldNames() {
    return (String[])this.projectedFieldNames.toArray();
  }
  
}
