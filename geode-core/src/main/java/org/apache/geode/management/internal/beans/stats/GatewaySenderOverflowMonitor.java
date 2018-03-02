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

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.internal.statistics.StatisticId;
import org.apache.geode.internal.statistics.StatisticNotFoundException;
import org.apache.geode.internal.statistics.StatisticsListener;
import org.apache.geode.internal.statistics.StatisticsNotification;
import org.apache.geode.internal.statistics.ValueMonitor;

public class GatewaySenderOverflowMonitor extends MBeanStatsMonitor {

  private volatile long entriesOverflowedToDisk = 0;

  private volatile long bytesOverflowedToDisk = 0;

  private volatile long lruEvictions = 0;

  private Map<Statistics, ValueMonitor> monitors;

  private Map<Statistics, StatisticsListener> listeners;

  public GatewaySenderOverflowMonitor(String name) {
    super(name);
    monitors = new HashMap<>();
    listeners = new HashMap<>();
  }

  @Override
  public void addStatisticsToMonitor(Statistics stats) {
    ValueMonitor overflowMonitor = new ValueMonitor();
    StatisticsListener listener = new GatewaySenderOverflowStatisticsListener();
    overflowMonitor.addListener(listener);
    overflowMonitor.addStatistics(stats);
    monitors.put(stats, overflowMonitor);
    listeners.put(stats, listener);
  }

  @Override
  public void removeStatisticsFromMonitor(Statistics stats) {}

  @Override
  public void stopListener() {
    for (Statistics stat : listeners.keySet()) {
      ValueMonitor monitor = monitors.get(stat);
      monitor.removeListener(listeners.get(stat));
      monitor.removeStatistics(stat);
    }
    listeners.clear();
    monitors.clear();
  }

  @Override
  public Number getStatistic(String name) {
    if (name.equals(StatsKey.GATEWAYSENDER_LRU_EVICTIONS)) {
      return getLruEvictions();
    }
    if (name.equals(StatsKey.GATEWAYSENDER_ENTRIES_OVERFLOWED_TO_DISK)) {
      return getEntriesOverflowedToDisk();
    }
    if (name.equals(StatsKey.GATEWAYSENDER_BYTES_OVERFLOWED_TO_DISK)) {
      return getBytesOverflowedToDisk();
    }
    return 0;
  }


  private class GatewaySenderOverflowStatisticsListener implements StatisticsListener {

    DefaultHashMap statsMap = new DefaultHashMap();

    @Override
    public void handleNotification(StatisticsNotification notification) {
      synchronized (statsMap) {
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

          Number deltaValue = computeDelta(statsMap, name, value);
          statsMap.put(name, value);
          increaseStats(name, deltaValue);
        }
      }
    }
  };

  private Number computeDelta(DefaultHashMap statsMap, String name, Number currentValue) {
    if (name.equals(StatsKey.GATEWAYSENDER_LRU_EVICTIONS)) {
      Number prevValue = statsMap.get(StatsKey.GATEWAYSENDER_LRU_EVICTIONS).longValue();
      Number deltaValue = currentValue.longValue() - prevValue.longValue();
      return deltaValue;
    }
    if (name.equals(StatsKey.GATEWAYSENDER_ENTRIES_OVERFLOWED_TO_DISK)) {
      Number prevValue =
          statsMap.get(StatsKey.GATEWAYSENDER_ENTRIES_OVERFLOWED_TO_DISK).longValue();
      Number deltaValue = currentValue.longValue() - prevValue.longValue();
      return deltaValue;
    }
    if (name.equals(StatsKey.GATEWAYSENDER_BYTES_OVERFLOWED_TO_DISK)) {
      Number prevValue = statsMap.get(StatsKey.GATEWAYSENDER_BYTES_OVERFLOWED_TO_DISK).longValue();
      Number deltaValue = currentValue.longValue() - prevValue.longValue();
      return deltaValue;
    }
    return 0;
  }

  private void increaseStats(String name, Number value) {
    if (name.equals(StatsKey.GATEWAYSENDER_LRU_EVICTIONS)) {
      lruEvictions += value.longValue();
      return;
    }
    if (name.equals(StatsKey.GATEWAYSENDER_ENTRIES_OVERFLOWED_TO_DISK)) {
      entriesOverflowedToDisk += value.longValue();
      return;
    }
    if (name.equals(StatsKey.GATEWAYSENDER_BYTES_OVERFLOWED_TO_DISK)) {
      bytesOverflowedToDisk += value.longValue();
      return;
    }
  }

  public long getLruEvictions() {
    return lruEvictions;
  }

  public long getEntriesOverflowedToDisk() {
    return entriesOverflowedToDisk;
  }

  public long getBytesOverflowedToDisk() {
    return bytesOverflowedToDisk;
  }
}
