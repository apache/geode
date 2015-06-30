package com.gemstone.gemfire.cache.query.internal.aggregate;

import com.gemstone.gemfire.cache.query.Aggregator;
import com.gemstone.gemfire.cache.query.QueryService;

/**
 * Computes the Max or Min
 * 
 * @author ashahid
 *
 */

public class MaxMin implements Aggregator {
  private final boolean findMax;
  private Comparable currentOptima;

  public MaxMin(boolean findMax) {
    this.findMax = findMax;
  }

  @Override
  public void accumulate(Object value) {
    if (value == null || value == QueryService.UNDEFINED) {
      return;
    }
    Comparable comparable = (Comparable) value;
    
    if (currentOptima == null) {
      currentOptima = comparable;
    } else {
      int compare = currentOptima.compareTo(comparable);
      if (findMax) {
        currentOptima = compare < 0 ? comparable : currentOptima;
      } else {
        currentOptima = compare > 0 ? comparable : currentOptima;
      }
    }

  }

  @Override
  public void init() {
    // TODO Auto-generated method stub

  }

  @Override
  public Object terminate() {
    return currentOptima;
  }

}
