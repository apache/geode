/*=========================================================================
 * Copyright (c) 2003-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.distributed.internal;

/**
 * Used to implement statistics on a pool.
 * The implementation will call these methods at to proper time.
 *
 * @author Darrel Schneider
 *
 * @since 3.5
 */
public interface PoolStatHelper {

  /**
   * Called each time the pool starts working on a job.
   */
  public void startJob();
  /**
   * Called each time the pool finishes a job it started.
   */
  public void endJob();
}
