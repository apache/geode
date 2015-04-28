/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.statistics;

/**
 * @author Kirk Lund
 * @since 7.0
 */
public final class GaugeMonitor extends StatisticsMonitor {

  private final Number lowThreshold;
  private final Number highThreshold;
  
  public GaugeMonitor(Number lowThreshold, Number highThreshold) {
    super();
    this.lowThreshold = lowThreshold;
    this.highThreshold = highThreshold;
  }

  @Override
  public GaugeMonitor addStatistic(StatisticId statId) {
    super.addStatistic(statId);
    return this;
  }

  @Override
  public GaugeMonitor removeStatistic(StatisticId statId) {
    super.removeStatistic(statId);
    return this;
  }
  
  @Override
  protected StringBuilder appendToString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("lowThreshold=").append(this.lowThreshold);
    sb.append(", highThreshold=").append(this.highThreshold);
    return sb;
  }
}
