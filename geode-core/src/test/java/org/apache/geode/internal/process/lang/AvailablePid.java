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

import static org.apache.geode.internal.process.ProcessUtils.isProcessAlive;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.apache.geode.internal.util.StopWatch;

/**
 * Finds unused pids for use in tests.
 */
public class AvailablePid {

  static final int LOWER_BOUND = 1;
  static final int UPPER_BOUND = 64000;
  static final int DEFAULT_TIMEOUT_MILLIS = 60 * 1000;

  private final Random random;
  private final int timeoutMillis;

  /**
   * Construct with no seed and default timeout of 1 minute.
   */
  public AvailablePid() {
    this(new Random(), DEFAULT_TIMEOUT_MILLIS);
  }

  /**
   * Construct with specified seed and timeout.
   */
  public AvailablePid(final long seed, final int timeoutMillis) {
    this(new Random(seed), timeoutMillis);
  }

  /**
   * Construct with specified Random implementation.
   */
  public AvailablePid(final Random random, final int timeoutMillis) {
    this.random = random;
    this.timeoutMillis = timeoutMillis;
  }

  /**
   * Returns specified pid if it's unused. Else returns randomly unused pid.
   */
  public int findAvailablePid(final int pid) throws TimeoutException {
    if (isProcessAlive(pid)) {
      return pid;
    }
    return findAvailablePid();
  }

  /**
   * Returns randomly unused pid.
   */
  public int findAvailablePid() throws TimeoutException {
    StopWatch stopWatch = new StopWatch(true);
    int pid = random();
    while (isProcessAlive(pid)) {
      if (stopWatch.elapsedTimeMillis() > timeoutMillis) {
        throw new TimeoutException(
            "Failed to find available pid within " + timeoutMillis + " millis.");
      }
      pid = random();
    }
    return pid;
  }

  /**
   * Returns specified number of randomly unused pids.
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

  private int random() {
    return random.nextInt(UPPER_BOUND - LOWER_BOUND);
  }

}
