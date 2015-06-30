package com.gemstone.gemfire.cache.lucene;

import java.util.Collection;
import java.util.List;

import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.distributed.DistributedMember;

/**
 * <p>
 * Defines the interface for a container of lucene query result collected from function
 * execution.<br>
 * 
 * @author Xiaojian Zhou
 * @since 8.5
 */

public interface LuceneQueryResults<E> {

  /* get next page of result if pagesize is specified in query, otherwise, return null */
  public List<E> getNextPage();
  
  /* Is next page of result available */
  public boolean hasNextPage();
  
  /* total number of items */
  public int size();

}
