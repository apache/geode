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
package org.apache.geode.internal;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Utility class for retrying operations.
 */
public class Retry {

  interface Clock {
    long nanoTime();
  }

  private static class SteadyClock implements Clock {
    @Override
    public long nanoTime() {
      return System.nanoTime();
    }
  }

  private static final SteadyClock steadyClock = new SteadyClock();

  /**
   * Try the supplier function until the predicate is true or timeout occurs.
   *
   * @param supplier to execute until predicate is true or times out
   * @param predicate to test for retry
   * @param timeout to retry for
   * @param timeUnit to retry for
   * @param <T> type of return value
   * @return value from supplier after it passes predicate or times out.
   */
  public static <T> T tryFor(final Supplier<T> supplier, final Predicate<T> predicate,
      final long timeout, final TimeUnit timeUnit) throws TimeoutException {
    return tryFor(supplier, predicate, timeout, timeUnit, steadyClock);
  }

  static <T> T tryFor(final Supplier<T> supplier, final Predicate<T> predicate,
      final long timeout, final TimeUnit timeUnit, final Clock clock)
      throws TimeoutException {
    long until = clock.nanoTime() + NANOSECONDS.convert(timeout, timeUnit);

    T value;
    do {
      value = supplier.get();
      if (predicate.test(value)) {
        return value;
      }
    } while (clock.nanoTime() < until);

    throw new TimeoutException();
  }

}
