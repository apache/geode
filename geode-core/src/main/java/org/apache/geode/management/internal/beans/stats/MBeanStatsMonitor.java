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

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.statistics.StatisticId;
import org.apache.geode.internal.statistics.StatisticNotFoundException;
import org.apache.geode.internal.statistics.StatisticsListener;
import org.apache.geode.internal.statistics.StatisticsNotification;
import org.apache.geode.internal.statistics.ValueMonitor;

/**
 * Class to get mappings of stats name to their values
 */
public class MBeanStatsMonitor implements StatisticsListener {

  private static final Logger logger = LogService.getLogger();

  protected ValueMonitor monitor;

  /**
   * Map which contains statistics with their name and value
   */
  protected Map<String, Number> statsMap;

  private String monitorName;

  public MBeanStatsMonitor(final String name) {
    this(name, new ValueMonitor());
  }

  MBeanStatsMonitor(final String name, final ValueMonitor monitor) {
    this.monitorName = name;
    this.monitor = monitor;
    this.statsMap = new HashMap<>();
  }

  public void addStatisticsToMonitor(final Statistics stats) {
    monitor.addListener(this);// if already listener is added this will be a no-op
    // Initialize the stats with the current values.
    StatisticsType type = stats.getType();
    StatisticDescriptor[] descriptors = type.getStatistics();
    for (StatisticDescriptor d : descriptors) {
      statsMap.put(d.getName(), stats.get(d));
    }

    monitor.addStatistics(stats);
  }

  public void removeStatisticsFromMonitor(final Statistics stats) {
    statsMap.clear();
  }

  public void stopListener() {
    monitor.removeListener(this);
  }

  public Number getStatistic(final String statName) {
    Number value = statsMap.getOrDefault(statName, 0);
    return value != null ? value : 0;
  }

  @Override
  public void handleNotification(final StatisticsNotification notification) {
    for (StatisticId statId : notification) {
      StatisticDescriptor descriptor = statId.getStatisticDescriptor();
      String name = descriptor.getName();
      Number value;
      try {
        value = notification.getValue(statId);
      } catch (StatisticNotFoundException e) {
        value = 0;
      }
      log(name, value);
      statsMap.put(name, value);
    }
  }

  protected void log(final String name, final Number value) {
    if (logger.isTraceEnabled()) {
      logger.trace("Monitor = {} descriptor = {} And value = {}", monitorName, name, value);
    }
  }
}
