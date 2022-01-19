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


public class StatsRate {

  private long prevLongCounter = 0;

  private int prevIntCounter = 0;

  private final MBeanStatsMonitor monitor;

  private final String[] statsKeys;

  private final StatType type;



  public StatsRate(String statsKey, StatType type, MBeanStatsMonitor monitor) {
    statsKeys = new String[] {statsKey};
    this.monitor = monitor;
    this.type = type;
  }

  public StatsRate(String[] statsKeys, StatType type, MBeanStatsMonitor monitor) {
    this.statsKeys = statsKeys;
    this.monitor = monitor;
    this.type = type;
  }

  public float getRate() {
    long currentTime = System.currentTimeMillis();
    return getRate(currentTime);
  }

  public float getRate(long pollTime) {
    float rate = 0;
    switch (type) {
      case INT_TYPE:
        int currentIntCounter = getCurrentIntCounter();
        rate = currentIntCounter - prevIntCounter;
        prevIntCounter = currentIntCounter;
        return rate;
      case LONG_TYPE:
        long currentLongCounter = getCurrentLongCounter();
        rate = currentLongCounter - prevLongCounter;
        prevLongCounter = currentLongCounter;
        return rate;
      default:
        return rate;
    }
  }

  private int getCurrentIntCounter() {
    int currentCounter = 0;
    for (String statKey : statsKeys) {
      currentCounter += monitor.getStatistic(statKey).intValue();
    }
    return currentCounter;
  }

  private long getCurrentLongCounter() {
    long currentCounter = 0;
    for (String statKey : statsKeys) {
      currentCounter += monitor.getStatistic(statKey).longValue();
    }

    return currentCounter;
  }

}
