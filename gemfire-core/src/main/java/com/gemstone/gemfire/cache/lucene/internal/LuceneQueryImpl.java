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
