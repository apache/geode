package com.gemstone.gemfire.cache.query.internal.aggregate;

import java.util.Set;

/**
 * Computes the sum of distinct values on the PR query node.
 * 
 * @author ashahid
 *
 */
public class SumDistinctPRQueryNode extends DistinctAggregator {

  /**
   * The input data is the Set of values(distinct) receieved from each of the
   * bucket nodes.
   */
  @Override
  public void accumulate(Object value) {
    this.distinct.addAll((Set) value);
  }

  @Override
  public Object terminate() {
    double sum = 0;
    for (Object o : this.distinct) {
      sum += ((Number) o).doubleValue();
    }
    return downCast(sum);
  }
}
