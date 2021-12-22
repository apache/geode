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

import org.apache.geode.annotations.Immutable;

/**
 * A timer class that reports current or elapsed time in nanonseconds. The static method
 * {@link #getTime} reports the current time. The instance methods support basic stop-watch-style
 * functions that are convenient for simple performance measurements. For example:
 *
 * <pre>
 * class Example {
 *   void example() {
 *     NanoTimer timer = new NanoTimer();
 *     for (int i = 0; i < n; ++i) {
 *       someComputationThatYouAreMeasuring();
 *       long duration = timer.reset();
 *       System.out.println("Duration: " + duration);
 *       // To avoid contaminating timing with printing times,
 *       // you could call reset again here.
 *     }
 *     long average = timer.getTimeSinceConstruction() / n;
 *     System.out.println("Average: " + average);
 *   }
 * }
 * </pre>
 *
 */
public class NanoTimer {

  public static final long NANOS_PER_MILLISECOND = 1000000;

  /**
   * The timestamp taken when this timer was constructed.
   */
  private final long constructionTime;

  /**
   * The timestamp taken when this timer was last reset or constructed.
   */
  private long lastResetTime;

  private final TimeService timeService;

  @Immutable
  private static final TimeService systemTimeService = () -> System.nanoTime();

  /**
   * Create a NanoTimer.
   */
  public NanoTimer() {
    timeService = systemTimeService;
    lastResetTime = systemTimeService.getTime();
    constructionTime = lastResetTime;
  }

  /**
   * For unit testing
   */
  protected NanoTimer(TimeService ts) {
    timeService = ts;
    lastResetTime = ts.getTime();
    constructionTime = lastResetTime;
  }

  /**
   * Converts nanoseconds to milliseconds by dividing nanos by {@link #NANOS_PER_MILLISECOND}.
   *
   * @param nanos value in nanoseconds
   * @return value converted to milliseconds
   */
  public static long nanosToMillis(long nanos) {
    return nanos / NANOS_PER_MILLISECOND;
  }

  /**
   * Converts milliseconds to nanoseconds by multiplying millis by {@link #NANOS_PER_MILLISECOND}.
   *
   * @param millis value in milliseconds
   * @return value converted to nanoseconds
   */
  public static long millisToNanos(long millis) {
    return millis * NANOS_PER_MILLISECOND;
  }

  /**
   * Return the time in nanoseconds since some arbitrary time in the past. The time rolls over to
   * zero every 2^64 nanosecs (approx 584 years). Interval computations spanning periods longer than
   * this will be wrong.
   */
  public static long getTime() {
    return java.lang.System.nanoTime();
  }

  /**
   * Return the construction time in nanoseconds since some arbitrary time in the past.
   *
   * @return timestamp in nanoseconds since construction.
   */
  public long getConstructionTime() {
    return constructionTime;
  }

  /**
   * Return the last reset time in naonseconds since some arbitrary time in the past.
   * <p/>
   * The time rolls over to zero every 2^64 nanosecs (approx 584 years). Interval computations
   * spanning periods longer than this will be wrong. If the timer has not yet been reset then the
   * construction time is returned.
   *
   * @return timestamp in nanoseconds of construction or the last reset.
   */
  public long getLastResetTime() {
    return lastResetTime;
  }

  /**
   * Compute and return the time in nanoseconds since the last reset or construction of this timer,
   * and reset the timer to the current {@link #getTime}.
   *
   * @return time in nanoseconds since construction or last reset.
   */
  public long reset() {
    long save = lastResetTime;
    lastResetTime = timeService.getTime();
    return lastResetTime - save;
  }

  /**
   * Compute and return the time in nanoseconds since the last reset or construction of this Timer,
   * but does not reset this timer.
   *
   * @return time in nanoseconds since construction or last reset.
   */
  public long getTimeSinceReset() {
    return timeService.getTime() - lastResetTime;
  }

  /**
   * Compute and return the time in nanoseconds since this timer was constructed.
   *
   * @return time in nanoseconds since construction.
   */
  public long getTimeSinceConstruction() {
    return timeService.getTime() - constructionTime;
  }

  /**
   * Allows unit tests to insert a deterministic clock for testing.
   */
  public interface TimeService {
    /**
     * Returns the current time.
     */
    long getTime();
  }
}
