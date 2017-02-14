/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.util.concurrent;

import org.apache.geode.CancelCriterion;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.Assert;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * This class is a "stoppable" cover for {@link CountDownLatch}.
 */
public class StoppableCountDownLatch {

  /**
   * This is how often waiters will wake up to check for cancellation
   */
  static final long RETRY_TIME = Long
      .getLong(DistributionConfig.GEMFIRE_PREFIX + "stoppable-retry-interval", 2000).longValue();


  /**
   * The underlying latch
   */
  private final CountDownLatch latch;

  /**
   * The cancellation criterion
   */
  private final CancelCriterion stopper;

  /**
   * @param count the number of times {@link #countDown} must be invoked before threads can pass
   *        through {@link #await()}
   *
   * @throws IllegalArgumentException if {@code count} is negative
   */
  public StoppableCountDownLatch(CancelCriterion stopper, int count) {
    Assert.assertTrue(stopper != null);
    this.latch = new CountDownLatch(count);
    this.stopper = stopper;
  }

  /**
   * @throws InterruptedException
   */
  public void await() throws InterruptedException {
    for (;;) {
      stopper.checkCancelInProgress(null);
      if (latch.await(RETRY_TIME, TimeUnit.MILLISECONDS)) {
        break;
      }
    }
  }

  /**
   * @param msTimeout how long to wait in milliseconds
   *
   * @return true if it was unlatched
   */
  public boolean await(long msTimeout) throws InterruptedException {
    stopper.checkCancelInProgress(null);
    return latch.await(msTimeout, TimeUnit.MILLISECONDS);
  }

  public boolean await(final long timeout, final TimeUnit unit) throws InterruptedException {
    stopper.checkCancelInProgress(null);
    return latch.await(timeout, unit);
  }

  public synchronized void countDown() {
    latch.countDown();
  }

  /**
   * @return the current count
   */
  public long getCount() {
    return latch.getCount();
  }

  /**
   * @return a string identifying this latch, as well as its state
   */
  @Override
  public String toString() {
    return "(Stoppable) " + latch.toString();
  }
}
