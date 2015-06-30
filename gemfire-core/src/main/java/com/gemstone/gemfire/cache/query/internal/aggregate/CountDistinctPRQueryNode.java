package com.gemstone.gemfire.cache.query.internal.aggregate;

import java.util.Set;

/**
 * Computes the count of the distinct rows on the PR query node.
 * 
 * @author ashahid
 *
 */
public class CountDistinctPRQueryNode extends DistinctAggregator {

  /**
   * The input data is the Set containing distinct values from each of the
   * bucket nodes.
   */
  @Override
  public void accumulate(Object value) {
    this.distinct.addAll((Set) value);

  }

  @Override
  public Object terminate() {
    return Integer.valueOf(this.distinct.size());
  }

}
