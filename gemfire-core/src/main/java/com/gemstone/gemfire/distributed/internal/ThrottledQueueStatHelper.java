/*=========================================================================
 * Copyright (c) 2003-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.distributed.internal;

/**
 * Used to implement statistics on a throttled queue.
 * The implementation will call these methods at to proper time.
 *
 * @author Darrel Schneider
 *
 * @since 3.5
 */
public interface ThrottledQueueStatHelper extends QueueStatHelper {

  /**
   * Called each time a thread was delayed by the throttle.
   */
  public void incThrottleCount();
  /**
   * Called after a throttled operation has completed.
   * @param nanos the amount of time, in nanoseconds, the throttle caused us to wait.
   */
  public void throttleTime(long nanos);
}
