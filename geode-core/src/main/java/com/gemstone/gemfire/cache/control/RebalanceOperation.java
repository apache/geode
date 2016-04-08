/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gemstone.gemfire.cache.control;

import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Operation for rebalancing resources used by the {@link 
 * com.gemstone.gemfire.cache.Cache}.
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
