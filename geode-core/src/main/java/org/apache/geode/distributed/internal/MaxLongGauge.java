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
package org.apache.geode.distributed.internal;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.geode.Statistics;

/**
 * This class holds the max value of a stat inside an AtomicLong. Every time a higher value is found
 * the class will forward the delta between the old max and the new max to the statistics class.
 * This pattern is a lock-less alternative to calling Statistics.getLong and then updating the max.
 */
class MaxLongGauge {
  private final int statId;
  private final Statistics stats;
  private final AtomicLong max;

  public MaxLongGauge(int statId, Statistics stats) {
    this.statId = statId;
    this.stats = stats;
    max = new AtomicLong();
  }

  public void recordMax(long currentValue) {
    boolean done = false;
    while (!done) {
      long maxValue = max.get();
      if (currentValue <= maxValue) {
        done = true;
      } else {
        done = max.compareAndSet(maxValue, currentValue);
        if (done) {
          stats.incLong(statId, currentValue - maxValue);
        }
      }
    }
  }
}
