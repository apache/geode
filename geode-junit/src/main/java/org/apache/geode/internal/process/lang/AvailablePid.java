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
package org.apache.geode.internal.process.lang;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.geode.internal.process.ProcessUtils.isProcessAlive;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import com.google.common.base.Stopwatch;

/**
 * Finds unused pids for use in tests.
 */
public class AvailablePid {

  static final int DEFAULT_LOWER_BOUND = 1;
  static final int DEFAULT_UPPER_BOUND = 64000;
  static final int DEFAULT_TIMEOUT_MILLIS = 60 * 1000;

  private final int lowerBound;
  private final int upperBound;
  private final Random random;
  private final int timeoutMillis;

  /**
   * Construct with:
   * <ul>
   * <li>default {@link Bounds} of {@link #DEFAULT_LOWER_BOUND} (inclusive) and
   * {@link #DEFAULT_UPPER_BOUND} (inclusive)
   * <li>Random with no see
   * <li>default timeout of 1 minute.
   * </ul>
   */
  public AvailablePid() {
    this(new Bounds(DEFAULT_LOWER_BOUND, DEFAULT_UPPER_BOUND), new Random(),
        DEFAULT_TIMEOUT_MILLIS);
  }

  /**
   * Construct with:
   * <ul>
   * <li>default {@link Bounds} of {@link #DEFAULT_LOWER_BOUND} (inclusive) and
   * {@link #DEFAULT_UPPER_BOUND} (inclusive)
   * <li>Random with specified seed
   * <li>default timeout of 1 minute
   * </ul>
   */
  public AvailablePid(final long seed) {
    this(new Bounds(DEFAULT_LOWER_BOUND, DEFAULT_UPPER_BOUND), new Random(seed),
        DEFAULT_TIMEOUT_MILLIS);
  }

  /**
   * Construct with:
   * <ul>
   * <li>default {@link Bounds} of {@link #DEFAULT_LOWER_BOUND} (inclusive) and
   * {@link #DEFAULT_UPPER_BOUND} (inclusive)
   * <li>specified Random instance
   * <li>default timeout of 1 minute
   * </ul>
   */
  public AvailablePid(final Random random) {
    this(new Bounds(DEFAULT_LOWER_BOUND, DEFAULT_UPPER_BOUND), random, DEFAULT_TIMEOUT_MILLIS);
  }

  /**
   * Construct with:
   * <ul>
   * <li>specified {@link Bounds} of {@code lowerBound} (inclusive) and {@code upperBound}
   * (inclusive)
   * <li>Random with no seed
   * <li>default timeout of 1 minute
   * </ul>
   */
  public AvailablePid(final Bounds bounds) {
    this(bounds, new Random(), DEFAULT_TIMEOUT_MILLIS);
  }

  /**
   * Construct with:
   * <ul>
   * <li>specified {@link Bounds} of {@code lowerBound} (inclusive) and {@code upperBound}
   * (inclusive)
   * <li>specified Random instance
   * <li>specified default timeout millis
   * </ul>
   */
  public AvailablePid(final Bounds bounds, final Random random, final int timeoutMillis) {
    this.lowerBound = bounds.lowerBound;
    this.upperBound = bounds.upperBound;
    this.random = random;
    this.timeoutMillis = timeoutMillis;
  }

  /**
   * Returns specified pid if it's unused. Else returns randomly unused pid between
   * {@code lowerBound} (inclusive) and {@code upperBound} (inclusive).
   */
  public int findAvailablePid(final int pid) throws TimeoutException {
    if (isProcessAlive(pid)) {
      return pid;
    }
    return findAvailablePid();
  }

  /**
   * Returns randomly unused pid between {@code lowerBound} (inclusive) and {@code upperBound}
   * (inclusive).
   */
  public int findAvailablePid() throws TimeoutException {
    Stopwatch stopwatch = Stopwatch.createStarted();
    int pid = random();
    while (isProcessAlive(pid)) {
      if (stopwatch.elapsed(MILLISECONDS) > timeoutMillis) {
        throw new TimeoutException(
            "Failed to find available pid within " + timeoutMillis + " millis.");
      }
      pid = random();
    }
    return pid;
  }

  /**
   * Returns specified number of unique, randomly unused pids between {@code lowerBound} (inclusive)
   * and {@code upperBound} (inclusive).
   */
  public int[] findAvailablePids(final int number) throws TimeoutException {
    Set<Integer> pids = new HashSet<>();
    while (pids.size() < number) {
      int pid = new AvailablePid().findAvailablePid();
      if (!pids.contains(pid)) {
        pids.add(pid);
      }
    }
    return Arrays.stream(pids.toArray(new Integer[0])).mapToInt(Integer::intValue).toArray();
  }

  /**
   * Returns a value between {@code lowerBound} (inclusive) and {@code upperBound} (inclusive)
   */
  int random() {
    return random.nextInt(upperBound + 1 - lowerBound) + lowerBound;
  }

  /**
   * Lower and upper bounds for desired PIDs. Both are inclusive -- if you specify
   * {@code new Bounds(1, 100)} then {@code AvailablePid} will return values of 1 through 100.
   *
   * <ul>
   * <li>{@code lowerBound} must be an integer greater than zero.
   * <li>{@code upperBound} must be an integer greater than {@code lowerBound}.
   * </ul>
   */
  static class Bounds {
    final int lowerBound;
    final int upperBound;

    Bounds(final int lowerBound, final int upperBound) {
      if (lowerBound < 1) {
        throw new IllegalArgumentException("lowerBound must be greater than '0'");
      }
      if (upperBound <= lowerBound) {
        throw new IllegalArgumentException(
            "upperBound must be greater than lowerBound '" + lowerBound + "'");
      }
      this.lowerBound = lowerBound;
      this.upperBound = upperBound;
    }
  }

}
