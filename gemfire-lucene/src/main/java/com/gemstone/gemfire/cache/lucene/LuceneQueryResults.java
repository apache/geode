package com.gemstone.gemfire.cache.lucene;

import java.util.List;

/**
 * <p>
 * Defines the interface for a container of lucene query result collected from function execution.<br>
 * 
 * @author Xiaojian Zhou
 * @since 8.5
 */

public interface LuceneQueryResults {
  /**
   * @return query result identifier. The identifier can be used to trace the origin of this result
   */
  public Object getID();

  /**
   * @return total number of hits for this query
   */
  public int size();

  /**
   * Returns the list of the top hits for the query.
   */
  public List<LuceneResultStruct> getHits();

  /**
   * Returns the maximum score value encountered. Note that in case scores are not tracked, this returns {@link Float#NaN}.
   */
  public float getMaxScore();

  /*
   * get next page of result if pagesize is specified in query, otherwise, return null
   */
  public List<LuceneResultStruct> getNextPage();

  /* Is next page of result available */
  public boolean hasNextPage();
}
