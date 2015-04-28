/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.control;

import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Operation for rebalancing resources used by the {@link 
 * com.gemstone.gemfire.cache GemFire Cache}.
 * 
 * @since 6.0
 */
public interface RebalanceOperation {
  
  // NOTE: cancelled is the spelling used in java.util.concurrent.Future
  
  /**
   * Returns true if this operation was cancelled before it completed.
   */
  public boolean isCancelled();

  /**
   * Returns true if this operation completed.
   */
  public boolean isDone();

  /**
   * Cancels this rebalance operation. The rebalance operation will find a
   * safe point and then stop.
   *
   * @return false if this operation could not be cancelled, typically because
   * it has already completed; true otherwise
   */
  public boolean cancel();

  /**
   * Wait for this operation to complete and return the results.
   *
   * @return the rebalance results
   * @throws CancellationException if the operation was cancelled
   * @throws InterruptedException if the wait was interrupted
   */
  public RebalanceResults getResults() 
  throws CancellationException, InterruptedException;

  /**
   * Wait for this operation to complete and return the results.
   *
   * @param timeout the maximum time to wait
   * @param unit the time unit of the timeout argument
   * @return the rebalance results
   * @throws CancellationException if the operation was cancelled
   * @throws TimeoutException if the wait timed out
   * @throws InterruptedException if the wait was interrupted 
   */
  public RebalanceResults getResults(long timeout, TimeUnit unit)
  throws CancellationException, TimeoutException, InterruptedException;
}
