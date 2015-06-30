package com.gemstone.gemfire.cache.query.internal.aggregate;

/**
 * Computes the sum of distinct values for replicated region based queries.
 * @author ashahid
 *
 */
public class SumDistinct extends DistinctAggregator {

  @Override
  public Object terminate() {
    double sum = 0;
    for (Object o : this.distinct) {
      sum += ((Number) o).doubleValue();
    }
    return downCast(sum);
  }

}
