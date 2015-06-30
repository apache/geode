package com.gemstone.gemfire.cache.query.internal.aggregate;

import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.cache.query.QueryService;

/**
 * The class used to hold the distinct values. This will get instantiated on the
 * bucket node as part of distinct queries for sum, count, average.
 * 
 * @author ashahid
 *
 */
public class DistinctAggregator extends AbstractAggregator {
  protected final Set<Object> distinct;

  public DistinctAggregator() {
    this.distinct = new HashSet<Object>();
  }

  @Override
  public void accumulate(Object value) {
    if (value != null && value != QueryService.UNDEFINED) {
      this.distinct.add(value);
    }
  }

  @Override
  public void init() {
    // TODO Auto-generated method stub

  }

  @Override
  public Object terminate() {
    return this.distinct;
  }

}
