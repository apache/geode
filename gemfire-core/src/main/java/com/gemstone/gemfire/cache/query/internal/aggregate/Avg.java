package com.gemstone.gemfire.cache.query.internal.aggregate;

import com.gemstone.gemfire.cache.query.QueryService;

/**
 * Computes the non distinct average for replicated region based queries
 * 
 * @author ashahid
 *
 */
public class Avg extends Sum {
  private int num = 0;

  @Override
  public void accumulate(Object value) {
    if (value != null && value != QueryService.UNDEFINED) {
      super.accumulate(value);
      ++num;
    }
  }

  @Override
  public void init() {

  }

  @Override
  public Object terminate() {
    double sum = ((Number) super.terminate()).doubleValue();
    double result = sum / num;
    return downCast(result);
  }

}
