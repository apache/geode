package com.gemstone.gemfire.internal.offheap;

/**
 * Instances that implement this interface must have release called on them
 * before the instance becomes garbage.
 * 
 * @author darrel
 * @since 9.0
 */
public interface Releasable {
  /**
   * Release any off-heap data owned by this instance.
   */
  public void release();
}
