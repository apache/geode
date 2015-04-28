/*=========================================================================
 * Copyright (c) 2003-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.distributed.internal;

/**
 * Used to implement statistics on a queue.
 * The implementation will call these methods at to proper time.
 *
 * @author Darrel Schneider
 *
 * @since 3.5
 */
public interface QueueStatHelper {

  /**
   * Called when an item is added to the queue.
   */
  public void add();
  /**
   * Called when an item is removed from the queue.
   */
  public void remove();
  /**
   * Called when count items are removed from the queue.
   */
  public void remove(int count);
}
