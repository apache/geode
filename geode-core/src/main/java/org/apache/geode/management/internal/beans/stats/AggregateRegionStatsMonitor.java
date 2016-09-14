/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import org.apache.geode.management.internal.beans.stats.MBeanStatsMonitor.DefaultHashMap;


/**
 * 
 *
 */
public class AggregateRegionStatsMonitor extends MBeanStatsMonitor{


  private volatile int primaryBucketCount = 0;

  private volatile int bucketCount = 0;

  private volatile int totalBucketSize = 0;

  private volatile long lruDestroys = 0;
  
  private volatile long lruEvictions = 0;
  
  private volatile long diskSpace = 0;
  
  
  
  private Map<Statistics, ValueMonitor> monitors;
  
  private Map<Statistics ,MemberLevelRegionStatisticsListener > listeners;

  public AggregateRegionStatsMonitor(String name) {
    super(name);
    monitors = new HashMap<Statistics, ValueMonitor>();
    listeners = new HashMap<Statistics, MemberLevelRegionStatisticsListener>();
  }

  @Override
  public void addStatisticsToMonitor(Statistics stats) {
    ValueMonitor regionMonitor = new ValueMonitor();
    MemberLevelRegionStatisticsListener listener = new MemberLevelRegionStatisticsListener();
    regionMonitor.addListener(listener);
    regionMonitor.addStatistics(stats);
    monitors.put(stats, regionMonitor);
    listeners.put(stats, listener);
  }
  

  public void removePartitionStatistics(Statistics stats) {
    MemberLevelRegionStatisticsListener listener = removeListener(stats);
    if (listener != null) {
      listener.decreaseParStats(stats);
    }
  }
  
  public void removeLRUStatistics(Statistics stats) {
    removeListener(stats);
  }

  public void removeDirectoryStatistics(Statistics stats){
    removeListener(stats);
  }
  
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
  
  private MemberLevelRegionStatisticsListener removeListener(Statistics stats){
    ValueMonitor monitor = monitors.remove(stats);
    if (monitor != null) {
      monitor.removeStatistics(stats);
    }
    
    MemberLevelRegionStatisticsListener listener = listeners.remove(stats);
    if (listener != null) {
      monitor.removeListener(listener);
    }
    return listener;
  }

  @Override
  public Number getStatistic(String name) {
    if (name.equals(StatsKey.LRU_EVICTIONS)) {
      return getLruEvictions();
    }
    if (name.equals(StatsKey.LRU_DESTROYS)) {
      return getLruDestroys();
    }
    if (name.equals(StatsKey.PRIMARY_BUCKET_COUNT)) {
      return getTotalPrimaryBucketCount();
    }
    if (name.equals(StatsKey.BUCKET_COUNT)) {
      return getTotalBucketCount();
    }
    if (name.equals(StatsKey.TOTAL_BUCKET_SIZE)) {
      return getTotalBucketSize();
    }
    if (name.equals(StatsKey.DISK_SPACE)) {
      return getDiskSpace();
    }
    return 0;
  }

  private class MemberLevelRegionStatisticsListener implements
      StatisticsListener {
    DefaultHashMap statsMap = new DefaultHashMap();
    
    private boolean removed = false;

    @Override
    public void handleNotification(StatisticsNotification notification) {
      synchronized (statsMap) {
        if(removed){
          return;
        }
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
          Number deltaValue = computeDelta(statsMap, name, value);
          statsMap.put(name, value);
          increaseStats(name, deltaValue);
          // fix for bug 46604
        }
      }

    }
    
    
    /**
     * Only decrease those values which can both increase and decrease and not
     * values which can only increase like read/writes
     * 
     * Remove last sample value from the aggregate. Last Sampled value can be
     * obtained from the DefaultHashMap for the disk
     * 
     * @param stats
     */
    public void decreaseParStats(Statistics stats) {
      synchronized (statsMap) {
        primaryBucketCount -= statsMap.get(StatsKey.PRIMARY_BUCKET_COUNT).intValue();
        bucketCount -= statsMap.get(StatsKey.BUCKET_COUNT).intValue();
        totalBucketSize -= statsMap.get(StatsKey.TOTAL_BUCKET_SIZE).intValue();
        removed = true;
      }

    }
    

 
  };
  

  
  private Number computeDelta(DefaultHashMap statsMap, String name,
      Number currentValue) {
    if (name.equals(StatsKey.PRIMARY_BUCKET_COUNT)) {
      Number prevValue = statsMap.get(StatsKey.PRIMARY_BUCKET_COUNT).intValue();
      Number deltaValue = currentValue.intValue() - prevValue.intValue();
      return deltaValue;
    }

    if (name.equals(StatsKey.BUCKET_COUNT)) {
      Number prevValue = statsMap.get(StatsKey.BUCKET_COUNT).intValue();
      Number deltaValue = currentValue.intValue() - prevValue.intValue();
      return deltaValue;
    }

    if (name.equals(StatsKey.TOTAL_BUCKET_SIZE)) {
      Number prevValue = statsMap.get(StatsKey.TOTAL_BUCKET_SIZE).intValue();
      Number deltaValue = currentValue.intValue() - prevValue.intValue();
      return deltaValue;
    }

    if (name.equals(StatsKey.LRU_EVICTIONS)) {
      Number prevValue = statsMap.get(StatsKey.LRU_EVICTIONS).longValue();
      Number deltaValue = currentValue.longValue() - prevValue.longValue();
      return deltaValue;
    }

    if (name.equals(StatsKey.LRU_DESTROYS)) {
      Number prevValue = statsMap.get(StatsKey.LRU_DESTROYS)
          .longValue();
      Number deltaValue = currentValue.longValue() - prevValue.longValue();
      return deltaValue;
    }

    if (name.equals(StatsKey.DISK_SPACE)) {
      Number prevValue = statsMap.get(StatsKey.DISK_SPACE)
          .longValue();
      Number deltaValue = currentValue.longValue() - prevValue.longValue();
      return deltaValue;
    }
    return 0;
  }

  private void increaseStats(String name, Number value) {
    if (name.equals(StatsKey.PRIMARY_BUCKET_COUNT)) {
      primaryBucketCount += value.intValue();
      return;
    }
    if (name.equals(StatsKey.BUCKET_COUNT)) {
      bucketCount += value.intValue();
      return;
    }
    if (name.equals(StatsKey.TOTAL_BUCKET_SIZE)) {
      totalBucketSize += value.intValue();
      return;
    }
    
    if (name.equals(StatsKey.LRU_EVICTIONS)) {
      lruEvictions += value.longValue();
      return;
    }
    
    if (name.equals(StatsKey.LRU_DESTROYS)) {
      lruDestroys += value.longValue();
      return;
    }
    
    if (name.equals(StatsKey.DISK_SPACE)) {
      diskSpace += value.longValue();
      return;
    }
    
  }
 
  public int getTotalPrimaryBucketCount() {
    return primaryBucketCount;
  }

  public int getTotalBucketCount() {
    return bucketCount;
  }

  public int getTotalBucketSize() {
    return totalBucketSize;
  }

  public long getLruDestroys() {
    return lruDestroys;
  }

  public long getLruEvictions() {
    return lruEvictions;
  }
  
  public long getDiskSpace(){
    return diskSpace;
  }

  @Override
  public void removeStatisticsFromMonitor(Statistics stats) {
    // TODO Auto-generated method stub
    
  }
  
 
}
