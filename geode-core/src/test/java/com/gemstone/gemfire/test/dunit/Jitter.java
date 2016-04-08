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

import java.util.Random;

/**
 * Extracted from DistributedTestCase
 */
class Jitter {

  /**
   * If true, we randomize the amount of time we wait before polling a
   * {@link WaitCriterion}.
   */
  private static final boolean USE_JITTER = true;
  
  private static final Random jitter = new Random();

  protected Jitter() {
  }

  /**
   * Returns an adjusted interval from <code>minimum()</code to 
   * <code>intervalMillis</code> milliseconds. If jittering is disabled then 
   * the value returned will be equal to intervalMillis.
   * 
   * @param intervalMillis
   * @return adjust milliseconds to use as interval for WaitCriteria polling
   */
  static long jitterInterval(long intervalMillis) {
    if (USE_JITTER) {
      return adjustIntervalIfJitterIsEnabled(intervalMillis);
    } else {
      return intervalMillis;
    }
  }
  
  static int minimum() {
    return 10;
  }
  
  static int maximum() {
    return 5000;
  }
  
  /**
   * If jittering is enabled then returns a jittered interval up to a maximum
   * of <code>intervalMillis</code> milliseconds, inclusive.
   * 
   * If jittering is disabled then returns <code>intervalMillis</code>.
   * 
   * The result is bounded by 50 ms as a minimum and 5000 ms as a maximum.
   * 
   * @param ms total amount of time to wait
   * @return randomized interval we should wait
   */
  private static int adjustIntervalIfJitterIsEnabled(final long intervalMillis) {
    final int minLegal = minimum();
    final int maxLegal = maximum();
    
    if (intervalMillis <= minLegal) {
      return (int)intervalMillis; // Don't ever jitter anything below this.
    }

    int maxValue = maxLegal;
    if (intervalMillis < maxLegal) {
      maxValue = (int)intervalMillis;
    }

    return minLegal + jitter.nextInt(maxValue - minLegal + 1);
  }
}
