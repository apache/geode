package com.gemstone.gemfire.cache.query;

/**
 * Behavior of a user-defined aggregator. Aggregates values and returns a
 * result. In addition to the methods in the interface, implementing classes
 * must have a 0-arg public constructor.
 * 
 * @author ashahid
 *
 */
public interface Aggregator {

  /**
   * Accumulate the next scalar value
   * 
   * @param value
   */
  public void accumulate(Object value);

  /**
   * Initialize the Aggregator
   */
  public void init();

  /**
   * 
   * @return Return the result scalar value
   */
  public Object terminate();
}
