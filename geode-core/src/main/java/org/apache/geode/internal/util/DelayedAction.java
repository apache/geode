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
package com.gemstone.gemfire.internal.util;

import java.util.concurrent.CountDownLatch;

/**
 * Coordinates thread scheduling to simplify multi-threaded testing.  Ensures that
 * two threads have both reached expected code before proceeding.  Unlike a
 * barrier pattern, the threads can be allowed to proceed independently.
 *  
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
