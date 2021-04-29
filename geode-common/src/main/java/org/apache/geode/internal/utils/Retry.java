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
package org.apache.geode.internal.utils;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.geode.annotations.VisibleForTesting;

/**
 * Utility class for retrying operations.
 */
public class Retry {

  interface Timer {
    long nanoTime();

    void sleep(long sleepTimeInNano) throws InterruptedException;
  }

  static class SteadyTimer implements Timer {
    @Override
    public long nanoTime() {
      return System.nanoTime();
    }

    @Override
    public void sleep(long sleepTimeInNano) throws InterruptedException {
      long millis = NANOSECONDS.toMillis(sleepTimeInNano);
      // avoid throwing IllegalArgumentException
      if (millis > 0) {
        Thread.sleep(millis);
      }
    }
  }

  private static final SteadyTimer steadyClock = new SteadyTimer();

  /**
   * Try the supplier function until the predicate is true or timeout occurs.
   *
   * @param timeout to retry for
   * @param timeoutUnit the unit for timeout
   * @param interval time between each try
   * @param intervalUnit the unit for interval
   * @param supplier to execute until predicate is true or times out
   * @param predicate to test for retry
   * @param <T> type of return value
   * @return value from supplier after it passes predicate or times out.
   */
  public static <T> T tryFor(long timeout, TimeUnit timeoutUnit,
      long interval, TimeUnit intervalUnit,
      Supplier<T> supplier,
      Predicate<T> predicate) throws TimeoutException, InterruptedException {
    return tryFor(timeout, timeoutUnit, interval, intervalUnit, supplier, predicate, steadyClock);
  }

  @VisibleForTesting
  static <T> T tryFor(long timeout, TimeUnit timeoutUnit,
      long interval, TimeUnit intervalUnit,
      Supplier<T> supplier,
      Predicate<T> predicate,
      Timer timer) throws TimeoutException, InterruptedException {
    long until = timer.nanoTime() + NANOSECONDS.convert(timeout, timeoutUnit);
    long intervalNano = NANOSECONDS.convert(interval, intervalUnit);

    T value;
    for (;;) {
      value = supplier.get();
      if (predicate.test(value)) {
        return value;
      } else {
        // if there is still more time left after we sleep for interval period, then sleep and retry
        // otherwise break out and throw TimeoutException
        if ((timer.nanoTime() + intervalNano) < until) {
          timer.sleep(intervalNano);
        } else {
          break;
        }
      }
    }

    throw new TimeoutException();
  }
}
