package com.gemstone.gemfire.cache.lucene;

import com.gemstone.gemfire.cache.lucene.LuceneQueryFactory.ResultType;

/**
 * Provides wrapper object of Lucene's Query object and execute the search. 
 * <p>Instances of this interface are created using
 * {@link LuceneQueryFactory#create}.
 * 
 */
public interface LuceneQuery {
  /**
   * Execute the search and get results. 
   */
  public LuceneQueryResults search();
  
  /**
   * Get page size setting of current query. 
   */
  public int getPageSize();
  
  /**
   * Get limit size setting of current query. 
   */
  public int getLimit();

  /**
   * Get result types setting of current query. 
   */
  public ResultType[] getResultTypes();
  
  /**
   * Get projected fields setting of current query. 
   */
  public String[] getProjectedFieldNames();
}
