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
public final class CounterMonitor extends StatisticsMonitor {
  
  public static enum Type {
    GREATER_THAN, LESS_THAN
  }

  private volatile Number threshold;
  
  public CounterMonitor(Number threshold) {
    super();
    this.threshold = threshold;
  }

  @Override
  public CounterMonitor addStatistic(StatisticId statId) {
    super.addStatistic(statId);
    return this;
  }
  
  @Override
  public CounterMonitor removeStatistic(StatisticId statId) {
    super.removeStatistic(statId);
    return this;
  }

  public CounterMonitor greaterThan(Number threshold) {
    return this;
  }
  
  @Override
  protected StringBuilder appendToString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("threshold=").append(this.threshold);
    return sb;
  }
}
