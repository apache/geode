package com.gemstone.gemfire.cache.query.internal.aggregate;

import com.gemstone.gemfire.cache.query.Aggregator;
import com.gemstone.gemfire.cache.query.QueryService;

/**
 * Computes the count of the non distinct rows for replicated & PR based
 * queries.
 * 
 * @author ashahid
 *
 */
public class Count implements Aggregator {
  private int count = 0;

  @Override
  public void accumulate(Object value) {
    if (value != null && value != QueryService.UNDEFINED) {
      ++this.count;
    }
  }

  @Override
  public void init() {

  }

  @Override
  public Object terminate() {
    return Integer.valueOf(count);
  }

}
