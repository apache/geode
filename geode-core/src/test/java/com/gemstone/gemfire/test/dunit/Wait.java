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
package com.gemstone.gemfire.test.dunit;

import static org.junit.Assert.fail;
import static com.gemstone.gemfire.test.dunit.Jitter.*;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * <code>Wait</code> provides static utility methods to wait for some
 * asynchronous action with intermittent polling.
 * 
 * These methods can be used directly: <code>Wait.waitForCriterion(...)</code>, 
 * however, they are intended to be referenced through static import:
 *
 * <pre>
 * import static com.gemstone.gemfire.test.dunit.Wait.*;
 *    ...
 *    waitForCriterion(...);
 * </pre>
 *
 * Extracted from DistributedTestCase.
 * 
 * @deprecated Use {@link com.jayway.awaitility.Awaitility} instead.
 */
@Deprecated
public class Wait {
  
  private static final Logger logger = LogService.getLogger();

  protected Wait() {
  }
  
  /**
   * Pause for a default interval (250 milliseconds).
   *  
   * @deprecated Please use {@link com.jayway.awaitility.Awaitility} instead.
   */
  public static void pause() {
    pause(250);
  }

  /**
   * Pause for the specified milliseconds. Make sure system clock has advanced
   * by the specified number of millis before returning.
   * 
   * @deprecated Please use {@link com.jayway.awaitility.Awaitility} instead.
   */
  public static final void pause(final int milliseconds) {
    if (milliseconds >= 1000 || logger.isDebugEnabled()) { // check for debug but log at info
      logger.info("Pausing for {} ms...", milliseconds);
    }
    final long target = System.currentTimeMillis() + milliseconds;
    try {
      for (;;) {
        long msLeft = target - System.currentTimeMillis();
        if (msLeft <= 0) {
          break;
        }
        Thread.sleep(msLeft);
      }
    }
    catch (InterruptedException e) {
      Assert.fail("interrupted", e);
    }
  }
  
  /**
   * Wait until given criterion is met
   * 
   * @param waitCriterion criterion to wait on
   * @param timeoutMillis total time to wait, in milliseconds
   * @param pollingInterval pause interval between waits
   * @param throwOnTimeout if false, don't generate an error
   * @deprecated Please use {@link com.jayway.awaitility.Awaitility} instead.
   */
  @Deprecated
  public static void waitForCriterion(final WaitCriterion waitCriterion, final long timeoutMillis, final long pollingInterval, final boolean throwOnTimeout) {
    long waitThisTime = jitterInterval(pollingInterval);
    final long tilt = System.currentTimeMillis() + timeoutMillis;
    for (;;) {
      if (waitCriterion.done()) {
        return; // success
      }
      if (waitCriterion instanceof StoppableWaitCriterion) {
        StoppableWaitCriterion ev2 = (StoppableWaitCriterion)waitCriterion;
        if (ev2.stopWaiting()) {
          if (throwOnTimeout) {
            fail("stopWaiting returned true: " + waitCriterion.description());
          }
          return;
        }
      }

      // Calculate time left
      long timeLeft = tilt - System.currentTimeMillis();
      if (timeLeft <= 0) {
        if (!throwOnTimeout) {
          return; // not an error, but we're done
        }
        fail("Event never occurred after " + timeoutMillis + " ms: " + waitCriterion.description());
      }
      
      if (waitThisTime > timeLeft) {
        waitThisTime = timeLeft;
      }
      
      // Wait a little bit
      Thread.yield();
      try {
        Thread.sleep(waitThisTime);
      } catch (InterruptedException e) {
        fail("interrupted");
      }
    }
  }

  /**
   * Blocks until the clock used for expiration moves forward.
   * 
   * @param cacheTimeMillisSource region that provides cacheTimeMillis
   * @return the last time stamp observed
   * @deprecated Please use {@link com.jayway.awaitility.Awaitility} instead.
   */
  public static final long waitForExpiryClockToChange(final LocalRegion cacheTimeMillisSource) {
    return waitForExpiryClockToChange(cacheTimeMillisSource, cacheTimeMillisSource.cacheTimeMillis());
  }

  /**
   * Blocks until the clock used for expiration moves forward.
   * 
   * @param cacheTimeMillisSource region that provides cacheTimeMillis
   * @param baseTime the timestamp that the clock must exceed
   * @return the last time stamp observed
   * @deprecated Please use {@link com.jayway.awaitility.Awaitility} instead.
   */
  public static final long waitForExpiryClockToChange(final LocalRegion cacheTimeMillisSource, final long baseTime) {
    long nowTime;
    do {
      Thread.yield();
      nowTime = cacheTimeMillisSource.cacheTimeMillis();
    } while ((nowTime - baseTime) <= 0L);
    return nowTime;
  }

  /**
   * Wait on a mutex.  This is done in a loop in order to address the 
   * "spurious wakeup" "feature" in Java.
   * 
   * @param waitCriterion condition to test
   * @param mutex object to lock and wait on
   * @param milliseconds total amount of time to wait
   * @param pollingInterval interval to pause for the wait
   * @param throwOnTimeout if false, no error is thrown.
   * @deprecated Please use {@link com.jayway.awaitility.Awaitility} instead.
   */
  public static void waitMutex(final WaitCriterion waitCriterion, final Object mutex, final long milliseconds, final long pollingInterval, final boolean throwOnTimeout) {
    final long tilt = System.currentTimeMillis() + milliseconds;
    long waitThisTime = jitterInterval(pollingInterval);
    synchronized (mutex) {
      for (;;) {
        if (waitCriterion.done()) {
          break;
        }
        
        long timeLeft = tilt - System.currentTimeMillis();
        if (timeLeft <= 0) {
          if (!throwOnTimeout) {
            return; // not an error, but we're done
          }
          fail("Event never occurred after " + milliseconds + " ms: " + waitCriterion.description());
        }
        
        if (waitThisTime > timeLeft) {
          waitThisTime = timeLeft;
        }
        
        try {
          mutex.wait(waitThisTime);
        } catch (InterruptedException e) {
          fail("interrupted");
        }
      } // for
    } // synchronized
  }
}
