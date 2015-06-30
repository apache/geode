package com.gemstone.gemfire.cache.query.internal.aggregate;

import com.gemstone.gemfire.cache.query.QueryService;

/**
 * Computes the sum for replicated & PR based queries.
 * 
 * @author ashahid
 *
 */
public class Sum extends AbstractAggregator {

  private double result = 0;

  @Override
  public void accumulate(Object value) {
    if (value != null && value != QueryService.UNDEFINED) {
      Number number = (Number) value;
      result += number.doubleValue();
    }
  }

  @Override
  public void init() {

  }

  @Override
  public Object terminate() {
    return downCast(result);
  }
}
