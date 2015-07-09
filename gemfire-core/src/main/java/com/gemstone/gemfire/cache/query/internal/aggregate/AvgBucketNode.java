package com.gemstone.gemfire.cache.query.internal.aggregate;

import com.gemstone.gemfire.cache.query.QueryService;

/**
 * The aggregator for compuing average which is used on the bucket node for
 * partitioned region based queries.
 * 
 * @author ashahid
 *
 */
public class AvgBucketNode extends Sum {

  private int count = 0;

  @Override
  public void accumulate(Object value) {
    if (value != null && value != QueryService.UNDEFINED) {
      super.accumulate(value);
      ++count;
    }
  }

  /**
   * Returns a two element array of the total number of values & the computed
   * sum of the values.
   */
  @Override
  public Object terminate() {
    return new Object[] { Integer.valueOf(count), super.terminate() };
  }

}
