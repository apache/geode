package com.gemstone.gemfire.cache.lucene.internal;

import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;

import com.gemstone.gemfire.cache.lucene.LuceneIndex;
import com.gemstone.gemfire.cache.lucene.LuceneQuery;
import com.gemstone.gemfire.cache.lucene.LuceneQueryFactory;
import com.gemstone.gemfire.cache.lucene.LuceneQueryProvider;

public class LuceneQueryFactoryImpl implements LuceneQueryFactory {
  private int limit = DEFAULT_LIMIT;
  private int pageSize = DEFAULT_PAGESIZE;
  private Set<String> projectionFields = new HashSet<String>();
  
  /* reference to the index. One index could have multiple Queries, but one Query must belong
   * to one index
   */
  private LuceneIndex relatedIndex;

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
  public LuceneQuery create(String indexName, String regionName,
      String queryString) throws ParseException {
    return create(indexName, regionName, new StringQueryProvider(queryString));
  }
  
  public LuceneQuery create(String indexName, String regionName, LuceneQueryProvider provider) {
    LuceneQueryImpl luceneQuery = new LuceneQueryImpl(indexName, regionName, provider, projectionFields, limit, pageSize);
    return luceneQuery;
  }
  

  public LuceneIndex getRelatedIndex() {
    return this.relatedIndex;
  }

  @Override
  public LuceneQueryFactory setProjectionFields(String... fieldNames) {
    if (fieldNames != null) {
      for (String fieldName:fieldNames) {
        this.projectionFields.add(fieldName);
      }
    }
    return this;
  }

}
