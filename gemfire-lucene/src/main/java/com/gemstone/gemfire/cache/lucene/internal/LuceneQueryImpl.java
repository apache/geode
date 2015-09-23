package com.gemstone.gemfire.cache.lucene.internal;

import java.util.Set;

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
    LuceneFunctionContext<TopEntriesCollector> context = new LuceneFunctionContext<>(query, indexName,
        new TopEntriesCollectorManager());
    TopEntriesFunctionCollector collector = new TopEntriesFunctionCollector();

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
