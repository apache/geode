/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management.internal.beans.stats;

import java.util.HashMap;
import java.util.Map;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.statistics.StatisticId;
import com.gemstone.gemfire.internal.statistics.StatisticNotFoundException;
import com.gemstone.gemfire.internal.statistics.StatisticsListener;
import com.gemstone.gemfire.internal.statistics.StatisticsNotification;
import com.gemstone.gemfire.internal.statistics.ValueMonitor;

/**
 * Class to get mappings of stats name to their values
 * 
 * @author rishim
 * 
 */
public class MBeanStatsMonitor implements StatisticsListener {

  protected ValueMonitor monitor;

  /**
   * Map which contains statistics with their name and value
   */
  protected DefaultHashMap statsMap;

  protected String monitorName;
  
  private LogWriterI18n logger;

  public MBeanStatsMonitor(String name) {
    this.monitorName = name;
    this.monitor = new ValueMonitor();
    this.statsMap = new DefaultHashMap();
    this.logger = InternalDistributedSystem.getLoggerI18n();

  }

  public void addStatisticsToMonitor(Statistics stats) {
    monitor.addListener(this);// if already listener is added this will be a no-op
    monitor.addStatistics(stats);
  }

  public void removeStatisticsFromMonitor(Statistics stats) {
    statsMap.clear();
  }

  public void stopListener() {
    monitor.removeListener(this);
  }

  public Number getStatistic(String statName) {
    return statsMap.get(statName) != null ? statsMap.get(statName) : 0;
  }

  @Override
  public void handleNotification(StatisticsNotification notification) {

    for (StatisticId statId : notification) {
      StatisticDescriptor descriptor = statId.getStatisticDescriptor();
      String name = descriptor.getName();
      Number value;
      try {
        value = notification.getValue(statId);
      } catch (StatisticNotFoundException e) {
        value = 0;
      }
      log(name,value);
      statsMap.put(name, value);

    }
  }
  
  protected void log(String name, Number value){

    if (logger != null && logger.finestEnabled()) {
      logger.finest("Monitor = " + monitorName + " descriptor = " + name + " And Value = " + value);
    }
  }

  public static class DefaultHashMap {
    private Map<String, Number> internalMap = new HashMap<String, Number>();

    public DefaultHashMap() {
    }

    public Number get(String key) {
      return internalMap.get(key) != null ? internalMap.get(key) : 0;
    }

    public void put(String key, Number value) {
      internalMap.put(key, value);
    }

    public void clear() {
      internalMap.clear();
    }
  }

}
