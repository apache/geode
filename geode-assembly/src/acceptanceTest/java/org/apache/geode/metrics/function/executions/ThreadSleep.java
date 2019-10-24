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
package org.apache.geode.metrics.function.executions;

import static java.lang.Math.max;
import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.time.Duration;

class ThreadSleep {
  private static final Duration MIN_TIME_TO_SLEEP = Duration.ofMillis(10);

  /**
   * Replacement for {@link Thread#sleep(long)} that sleeps at least as long as the given duration
   *
   * @param duration duration to sleep
   */
  static void sleepForAtLeast(Duration duration) {
    long sleepTimeNanos = duration.toNanos();
    long startTimeNanos = nanoTime();

    while (true) {
      long elapsedTimeNanos = nanoTime() - startTimeNanos;
      if (elapsedTimeNanos >= sleepTimeNanos) {
        break;
      }

      long remainingTimeNanos = sleepTimeNanos - elapsedTimeNanos;
      try {
        Thread.sleep(max(MIN_TIME_TO_SLEEP.toMillis(), NANOSECONDS.toMillis(remainingTimeNanos)));
      } catch (InterruptedException ignored) {
      }
    }
  }
}
