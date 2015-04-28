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
public class StatsAverageLatency {

  private String numberKey;
  private String timeKey;
  private StatType numKeyType;

  private MBeanStatsMonitor monitor;

  public StatsAverageLatency(String numberKey, StatType numKeyType,
      String timeKey, MBeanStatsMonitor monitor) {
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
