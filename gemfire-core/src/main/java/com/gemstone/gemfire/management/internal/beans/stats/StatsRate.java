/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.beans.stats;

import com.gemstone.gemfire.management.internal.beans.MetricsCalculator;

/**
 * 
 * @author rishim
 *
 */
public class StatsRate {
  
  private long prevLongCounter = 0;
  
  private int prevIntCounter = 0;

  private MBeanStatsMonitor monitor;
  
  private String[] statsKeys;
  
  private StatType type;

  
  
  public StatsRate(String statsKey, StatType type, MBeanStatsMonitor monitor) {
    this.statsKeys = new String[] { statsKey };
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
