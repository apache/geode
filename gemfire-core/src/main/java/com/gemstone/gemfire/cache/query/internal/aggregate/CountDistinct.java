package com.gemstone.gemfire.cache.query.internal.aggregate;

/**
 * 
 * Computes the count of the distinct rows for replicated region based queries.
 * 
 * @author ashahid
 */

public class CountDistinct extends DistinctAggregator {

  @Override
  public Object terminate() {
    return Integer.valueOf(this.distinct.size());
  }

}
