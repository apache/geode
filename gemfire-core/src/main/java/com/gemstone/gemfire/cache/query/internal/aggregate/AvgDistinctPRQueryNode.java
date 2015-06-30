package com.gemstone.gemfire.cache.query.internal.aggregate;

/**
 * Computes the final average of distinct values for the partitioned region
 * based queries. This aggregator is initialized on the PR query node & acts on
 * the results obtained from bucket nodes.
 * 
 * @author ashahid
 *
 */
public class AvgDistinctPRQueryNode extends SumDistinctPRQueryNode {

  @Override
  public Object terminate() {
    double sum = ((Number) super.terminate()).doubleValue();
    double result = sum / this.distinct.size();
    return downCast(result);
  }
}
