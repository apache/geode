package com.gemstone.gemfire.cache.lucene;

import org.apache.lucene.queryparser.classic.ParseException;

/**
 * Factory for creating instances of {@link LuceneQuery}.
 * To get an instance of this factory call {@link LuceneService#createLuceneQueryFactory}.
 * <P>
 * To use this factory configure it with the <code>set</code> methods and then
 * call {@link #create} to produce a {@link LuceneQuery} instance.
 * 
 * @author Xiaojian Zhou
 * @since 8.5
 */
public interface LuceneQueryFactory {
  
  /**
   * Default query result limit is 100
   */
  public static final int DEFAULT_LIMIT = 100;
  
  /**
   *  Default page size of result is 0, which means no pagination
   */
  public static final int DEFAULT_PAGESIZE = 0;
  
  /**
   * Set page size for a query result. The default page size is 0 which means no pagination.
   * If specified negative value, throw IllegalArgumentException
   * @param pageSize
   * @return itself
   */
  LuceneQueryFactory setPageSize(int pageSize);
  
  /**
   * Set max limit of result for a query
   * If specified limit is less or equal to zero, throw IllegalArgumentException
   * @param limit
   * @return itself
   */
  LuceneQueryFactory setResultLimit(int limit);
  
  /**
   * Set a list of fields for result projection.
   * 
   * @param fieldNames
   * @return itself
   */
  LuceneQueryFactory setProjectionFields(String... fieldNames);
  
  /**
   * Create wrapper object for lucene's QueryParser object using default standard analyzer.
   * The queryString is using lucene QueryParser's syntax. QueryParser is for easy-to-use 
   * with human understandable syntax. 
   *  
   * @param regionName region name
   * @param indexName index name
   * @param queryString query string in lucene QueryParser's syntax
   * @return LuceneQuery object
   * @throws ParseException
   */
  public LuceneQuery create(String indexName, String regionName, String queryString) 
      throws ParseException;

  /**
   * Creates a wrapper object for Lucene's Query object. This {@link LuceneQuery} builder method could be used in
   * advanced cases, such as cases where Lucene's Query object construction needs Lucene's API over query string. The
   * {@link QueryDeserializer} will be used to re-construct the Lucene Query object on remote hosts.
   * 
   * @param indexName index name
   * @param regionName region name
   * @param provider constructs and provides a Lucene Query object
   * @return LuceneQuery object
   */
  public LuceneQuery create(String indexName, String regionName, LuceneQueryProvider provider);
}
