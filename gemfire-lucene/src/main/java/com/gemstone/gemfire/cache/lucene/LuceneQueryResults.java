package com.gemstone.gemfire.cache.lucene;

import java.util.List;

import com.gemstone.gemfire.annotations.Experimental;

/**
 * <p>
 * Defines the interface for a container of lucene query result collected from function execution.<br>
 * 
 * @author Xiaojian Zhou
 * 
 * @param <K> The type of the key
 * @param <V> The type of the value
 */
@Experimental
public interface LuceneQueryResults<K, V> {
  /**
   * @return total number of hits for this query
   */
  public int size();

  /**
   * Returns the maximum score value encountered. Note that in case scores are not tracked, this returns {@link Float#NaN}.
   */
  public float getMaxScore();

  /**
   * Get the next page of results.
   * 
   * @return a page of results, or null if there are no more pages
   */
  public List<LuceneResultStruct<K, V>> getNextPage();

  /**
   *  True if there another page of results. 
   */
  public boolean hasNextPage();
}
