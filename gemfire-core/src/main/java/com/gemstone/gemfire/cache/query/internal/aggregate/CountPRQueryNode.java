package com.gemstone.gemfire.cache.query.internal.aggregate;

import com.gemstone.gemfire.cache.query.Aggregator;

/**
 * Computes the count of the rows on the PR query node
 * 
 * @author ashahid
 *
 */
public class CountPRQueryNode implements Aggregator {
  private int count = 0;

  /**
   * Recieves the input of the individual counts from the bucket nodes.
   */
  @Override
  public void accumulate(Object value) {
    this.count += ((Integer) value).intValue();
  }

  @Override
  public void init() {

  }

  @Override
  public Object terminate() {
    return Integer.valueOf(count);
  }

}
