package com.gemstone.gemfire.cache.query.internal.aggregate;

import com.gemstone.gemfire.cache.query.Aggregator;

/**
 * Abstract Aggregator class providing support for downcasting the result
 * 
 * @author ashahid
 *
 */
public abstract class AbstractAggregator implements Aggregator {

  public static Number downCast(double value) {
    Number retVal;
    if (value % 1 == 0) {
      long longValue = (long) value;
      if (longValue <= Integer.MAX_VALUE && longValue >= Integer.MIN_VALUE) {
        retVal = Integer.valueOf((int) longValue);
      } else {
        retVal = Long.valueOf(longValue);
      }
    } else {
      if (value <= Float.MAX_VALUE && value >= Float.MIN_VALUE) {
        retVal = Float.valueOf((float) value);
      } else {
        retVal = Double.valueOf(value);
      }
    }
    return retVal;
  }
}
