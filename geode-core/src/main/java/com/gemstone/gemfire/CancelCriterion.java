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
package com.gemstone.gemfire;

/**
 * Abstract cancellation proxy for cancelling an operation, esp. a thread.
 * 
 * Creators of services or threads should implement a subclass of CancelCriterion,
 * and implement the two methods - cancelInProgress, and generateCancelledException(e).
 * 
 * Code inside the service can check to see if the service is cancelled by calling
 * {@link #checkCancelInProgress(Throwable)}. Generally the pattern is to check
 * before performing an operation, check if the service is canceled before propgrating 
 * an exception futher up the stack, and check for cancelation inside a long loop.
 * Eg.
 * 
 * <code>
 * while(true) {
 *   c.checkCancelInProgress(null);
 *   try {
 *      dispatchEvents();
 *   } catch(IOException e) {
 *     c.checkCancelInProgress(e);
 *     throw e;
 *   }
 * }
 * </code>
 * 
 * @see CancelException
 * @since GemFire 5.1
 */
public abstract class CancelCriterion
{
  /**
   * Indicate if the service is in the progress of being cancelled.  The
   * typical use of this is to indicate, in the case of an {@link InterruptedException},
   * that the current operation should be cancelled.
   * @return null if the service is not shutting down, or a message that can be used to
   * construct an exception indicating the service is shut down.
   */
  public abstract String cancelInProgress();
//import com.gemstone.gemfire.distributed.internal.DistributionManager;
//    * <p>
//    * In particular, a {@link DistributionManager} returns a non-null result if
//    * message distribution has been terminated.
  
  /**
   * Use this utility  function in your implementation of cancelInProgress()
   * and cancelled() to indicate a system failure
   * 
   * @return failure string if system failure has occurred
   */
  protected final String checkFailure() {
    Throwable tilt = SystemFailure.getFailure();
    if (tilt != null) {
      // Allocate no objects here!
      return SystemFailure.JVM_CORRUPTION;
    }
    return null;
  }

  /**
   * See if the current operation is being cancelled.  If so, it either
   * throws a {@link RuntimeException} (usually a {@link CancelException}).
   * 
   * @param e an underlying exception or null if there is no exception 
   * that triggered this check
   * @see #cancelInProgress()
   */
  public final void checkCancelInProgress(Throwable e) {
    SystemFailure.checkFailure();
    String reason = cancelInProgress();
    if (reason == null) {
      return;
    }
    throw generateCancelledException(e);
  }

  /**
   * Template factory method for generating the exception to be thrown by
   * {@link #checkCancelInProgress(Throwable)}. Override this to specify
   * different exception for checkCancelInProgress() to throw.
   * 
   * This method should wrap the exception in a service specific 
   * CancelationException (eg CacheClosedException). 
   * or return null if the service is not being canceled.
   * 
   * @param e
   *          an underlying exception, if any
   * @return RuntimeException to be thrown by checkCancelInProgress(), null if
   *         the receiver has not been cancelled.
   */
  abstract public RuntimeException generateCancelledException(Throwable e);
}
