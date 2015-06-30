package com.gemstone.gemfire.cache.query.internal.aggregate;

import com.gemstone.gemfire.cache.query.QueryService;

/**
 * Computes the average of distinct values for replicated region based queries.
 * 
 * @author ashahid
 *
 */
public class AvgDistinct extends SumDistinct {

  @Override
  public void accumulate(Object value) {
    if (value != null && value != QueryService.UNDEFINED) {
      super.accumulate(value);
    }
  }

  @Override
  public Object terminate() {
    double sum = ((Number) super.terminate()).doubleValue();
    double result = sum / this.distinct.size();
    return downCast(result);
  }

}
