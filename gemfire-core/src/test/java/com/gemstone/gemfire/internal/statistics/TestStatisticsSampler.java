/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.statistics;

import java.util.List;

import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.internal.StatisticsManager;

/**
 * @author Kirk Lund
 * @since 7.0
 */
public class TestStatisticsSampler implements StatisticsSampler {
  
  private final StatisticsManager manager;
  
  public TestStatisticsSampler(StatisticsManager manager) {
    this.manager = manager;
  }
  
  @Override
  public int getStatisticsModCount() {
    return this.manager.getStatListModCount();
  }
  
  @Override
  public Statistics[] getStatistics() {
    @SuppressWarnings("unchecked")
    List<Statistics> statsList = (List<Statistics>)this.manager.getStatsList();
    synchronized (statsList) {
      return (Statistics[])statsList.toArray(new Statistics[statsList.size()]);
    }
  }

  @Override
  public boolean waitForSample(long timeout) throws InterruptedException {
    return false;
  }
  
  @Override
  public SampleCollector waitForSampleCollector(long timeout) throws InterruptedException {
    return null;
  }
}
