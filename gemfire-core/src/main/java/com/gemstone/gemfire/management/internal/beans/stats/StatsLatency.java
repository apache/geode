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
public class StatsLatency {

  private int prevIntNumberCounter = 0;
  private long prevLongNumberCounter = 0;

  private long prevTimeCounter = 0;

  private String numberKey;
  private String timeKey;
  private StatType numKeyType;

  private MBeanStatsMonitor monitor;

  public StatsLatency(String numberKey, StatType numKeyType, String timeKey,
      MBeanStatsMonitor monitor) {
    this.numberKey = numberKey;
    this.numKeyType = numKeyType;
    this.timeKey = timeKey;
    this.monitor = monitor;
  }

  public long getLatency() {
    if (numKeyType.equals(StatType.INT_TYPE)) {
      int latestNumberCounter = monitor.getStatistic(numberKey).intValue();

      long latestTimeCounter = monitor.getStatistic(timeKey).longValue();

      long latency = MetricsCalculator.getLatency(prevIntNumberCounter,
          latestNumberCounter, prevTimeCounter, latestTimeCounter);
      prevTimeCounter = latestTimeCounter;
      prevIntNumberCounter = latestNumberCounter;
      return latency;
    } else {
      long latestNumberCounter = monitor.getStatistic(numberKey).longValue();
      long latestTimeCounter = monitor.getStatistic(timeKey).longValue();
      long latency = MetricsCalculator.getLatency(prevLongNumberCounter,
          latestNumberCounter, prevTimeCounter, latestTimeCounter);
      prevLongNumberCounter = latestNumberCounter;
      prevTimeCounter = latestTimeCounter;
      return latency;
    }

  }

}
