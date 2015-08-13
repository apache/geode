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

public class LuceneQueryFactoryImpl implements LuceneQueryFactory {
  private int limit_attr = DEFAULT_LIMIT;
  private int pageSize_attr = DEFAULT_PAGESIZE;
  private Set<ResultType> resultType_attr = new HashSet<ResultType>();
  private Set<String> projection_fields_attr = new HashSet<String>();
  
  /* reference to the index. One index could have multiple Queries, but one Query must belong
   * to one index
   */
  private LuceneIndex relatedIndex;

  @Override
  public LuceneQueryFactory setPageSize(int pageSize) {
    this.pageSize_attr = pageSize;
    return this;
  }

  @Override
  public LuceneQueryFactory setResultLimit(int limit) {
    this.limit_attr = limit;
    return this;
  }

  @Override
  public LuceneQueryFactory setResultTypes(ResultType... resultTypes) {
    if (resultTypes != null) {
      for (ResultType resultType:resultTypes) {
        this.resultType_attr.add(resultType);
      }
    }
    return this;
  }

  @Override
  public LuceneQuery create(String indexName, String regionName,
      String queryString, Analyzer analyzer) throws ParseException {
    QueryParser parser = new QueryParser(null, analyzer);
    Query query = parser.parse(queryString);
    LuceneQueryImpl luceneQuery = new LuceneQueryImpl(indexName, regionName, limit_attr, pageSize_attr, 
        resultType_attr, projection_fields_attr, query);
    return luceneQuery;
  }

  @Override
  public LuceneQuery create(String indexName, String regionName,
      String queryString) throws ParseException {
    StandardAnalyzer analyzer = new StandardAnalyzer();
    return create(indexName, regionName, queryString, analyzer);
  }
  
  @Override
  public LuceneQuery create(String indexName, String regionName,
      Query query) {
    LuceneQueryImpl luceneQuery = new LuceneQueryImpl(indexName, regionName, limit_attr, pageSize_attr, 
        resultType_attr, projection_fields_attr, query);
    return luceneQuery;
  }

  public LuceneIndex getRelatedIndex() {
    return this.relatedIndex;
  }

  @Override
  public LuceneQueryFactory setProjectionFields(String... fieldNames) {
    if (fieldNames != null) {
      for (String fieldName:fieldNames) {
        this.projection_fields_attr.add(fieldName);
      }
    }
    return this;
  }

}
