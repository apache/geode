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
package org.apache.geode.management.internal.beans.stats;

import org.apache.geode.management.internal.beans.MetricsCalculator;

public class StatsAverageLatency {

  private final String numberKey;
  private final String timeKey;
  private final StatType numKeyType;

  private final MBeanStatsMonitor monitor;

  public StatsAverageLatency(String numberKey, StatType numKeyType, String timeKey,
      MBeanStatsMonitor monitor) {
    this.numberKey = numberKey;
    this.numKeyType = numKeyType;
    this.timeKey = timeKey;
    this.monitor = monitor;
  }

  public long getAverageLatency() {
    if (numKeyType.equals(StatType.INT_TYPE)) {
      int numberCounter = monitor.getStatistic(numberKey).intValue();
      long timeCounter = monitor.getStatistic(timeKey).longValue();
      return MetricsCalculator.getAverageLatency(numberCounter, timeCounter);
    } else {
      long numberCounter = monitor.getStatistic(numberKey).longValue();
      long timeCounter = monitor.getStatistic(timeKey).longValue();
      return MetricsCalculator.getAverageLatency(numberCounter, timeCounter);
    }

  }

}
