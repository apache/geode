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
    LuceneQueryImpl<K, V> luceneQuery = new LuceneQueryImpl<K, V>(indexName, region, provider, projectionFields, limit, pageSize);
    return luceneQuery;
  }
  
  @Override
  public LuceneQueryFactory setProjectionFields(String... fieldNames) {
    projectionFields = fieldNames.clone();
    return this;
  }

}
