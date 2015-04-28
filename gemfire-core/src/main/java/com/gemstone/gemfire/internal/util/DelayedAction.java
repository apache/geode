/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.util;

import java.util.concurrent.CountDownLatch;

/**
 * Coordinates thread scheduling to simplify multi-threaded testing.  Ensures that
 * two threads have both reached expected code before proceeding.  Unlike a
 * barrier pattern, the threads can be allowed to proceed independently.
 *  
 * @author bakera
 */
public class DelayedAction implements Runnable {
  private final CountDownLatch hit = new CountDownLatch(1);
  private final CountDownLatch run = new CountDownLatch(1);
  
  private final Runnable delayedAction;
  
  public DelayedAction(Runnable action) {
    delayedAction = action;
  }
  
  @Override
  public void run() {
    hit.countDown();
    try {
      run.await();
      if (delayedAction != null) {
        delayedAction.run();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
  
  /**
   * Blocks until the delayed action is ready to be executed.
   * @throws InterruptedException interrupted while waiting
   */
  public void waitForArrival() throws InterruptedException {
    hit.await();
  }
  
  /**
   * Allows the delayed action to proceed when ready.
   */
  public void allowToProceed() {
    run.countDown();
  }
}
