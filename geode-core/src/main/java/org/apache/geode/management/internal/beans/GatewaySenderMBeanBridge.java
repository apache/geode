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
package org.apache.geode.management.internal.beans;

import java.util.List;

import org.apache.geode.Statistics;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.AbstractGatewaySenderEventProcessor;
import org.apache.geode.internal.cache.wan.GatewaySenderEventDispatcher;
import org.apache.geode.internal.cache.wan.GatewaySenderStats;
import org.apache.geode.management.internal.beans.stats.GatewaySenderOverflowMonitor;
import org.apache.geode.management.internal.beans.stats.MBeanStatsMonitor;
import org.apache.geode.management.internal.beans.stats.StatType;
import org.apache.geode.management.internal.beans.stats.StatsAverageLatency;
import org.apache.geode.management.internal.beans.stats.StatsKey;
import org.apache.geode.management.internal.beans.stats.StatsRate;

public class GatewaySenderMBeanBridge {

  private GatewaySender sender;

  private MBeanStatsMonitor monitor;

  private GatewaySenderOverflowMonitor overflowMonitor;

  private StatsRate eventsQueuedRate;

  private StatsRate eventsReceivedRate;

  private StatsRate batchesDispatchedRate;

  private StatsRate lruEvictionsRate;

  private StatsAverageLatency batchDistributionAvgLatency;

  private GatewaySenderEventDispatcher dispatcher;

  private AbstractGatewaySender abstractSender;

  public GatewaySenderMBeanBridge(GatewaySender sender) {
    this.sender = sender;
    this.monitor =
        new MBeanStatsMonitor("GatewaySenderMXBeanMonitor");

    this.overflowMonitor = new GatewaySenderOverflowMonitor("GatewaySenderMXBeanOverflowMonitor");

    this.abstractSender = ((AbstractGatewaySender) this.sender);
    GatewaySenderStats stats = abstractSender.getStatistics();

    addGatewaySenderStats(stats);

    initializeStats();
  }

  public void setDispatcher() {
    AbstractGatewaySenderEventProcessor eventProcessor = abstractSender.getEventProcessor();
    if (eventProcessor != null) {
      this.dispatcher = abstractSender.getEventProcessor().getDispatcher();
    }
  }

  public void addGatewaySenderStats(GatewaySenderStats gatewaySenderStats) {
    monitor.addStatisticsToMonitor(gatewaySenderStats.getStats());
  }

  public void addOverflowStatistics(Statistics statistics) {
    if (statistics != null) {
      overflowMonitor.addStatisticsToMonitor(statistics);
    }
  }

  public void stopMonitor() {
    monitor.stopListener();
  }

  private void initializeStats() {
    eventsQueuedRate =
        new StatsRate(StatsKey.GATEWAYSENDER_EVENTS_QUEUED, StatType.INT_TYPE, monitor);
    eventsReceivedRate =
        new StatsRate(StatsKey.GATEWAYSENDER_EVENTS_RECEIVED, StatType.INT_TYPE, monitor);
    batchesDispatchedRate =
        new StatsRate(StatsKey.GATEWAYSENDER_BATCHES_DISTRIBUTED, StatType.INT_TYPE, monitor);
    batchDistributionAvgLatency =
        new StatsAverageLatency(StatsKey.GATEWAYSENDER_BATCHES_DISTRIBUTED, StatType.INT_TYPE,
            StatsKey.GATEWAYSENDER_BATCHES_DISTRIBUTE_TIME, monitor);
    lruEvictionsRate =
        new StatsRate(StatsKey.GATEWAYSENDER_LRU_EVICTIONS, StatType.LONG_TYPE, overflowMonitor);
  }

  public int getAlertThreshold() {
    return sender.getAlertThreshold();
  }

  public int getBatchSize() {
    return sender.getBatchSize();
  }

  public long getBatchTimeInterval() {
    return sender.getBatchTimeInterval();
  }

  public String getOverflowDiskStoreName() {
    return sender.getDiskStoreName();
  }

  public String[] getGatewayEventFilters() {
    List<GatewayEventFilter> filters = sender.getGatewayEventFilters();
    String[] filtersStr = null;
    if (filters != null && filters.size() > 0) {
      filtersStr = new String[filters.size()];
    } else {
      return filtersStr;
    }
    int j = 0;
    for (GatewayEventFilter filter : filters) {
      filtersStr[j] = filter.toString();
      j++;
    }
    return filtersStr;
  }

  public String[] getGatewayTransportFilters() {
    List<GatewayTransportFilter> transportFilters = sender.getGatewayTransportFilters();

    String[] transportFiltersStr = null;
    if (transportFilters != null && transportFilters.size() > 0) {
      transportFiltersStr = new String[transportFilters.size()];
    } else {
      return transportFiltersStr;
    }
    int j = 0;
    for (GatewayTransportFilter listener : transportFilters) {
      transportFiltersStr[j] = listener.getClass().getCanonicalName();
      j++;
    }
    return transportFiltersStr;
  }

  public int getMaximumQueueMemory() {
    return sender.getMaximumQueueMemory();

  }

  public int getRemoteDSId() {
    return sender.getRemoteDSId();
  }

  public String getSenderId() {
    return sender.getId();
  }

  public int getSocketBufferSize() {
    return sender.getSocketBufferSize();
  }

  public long getSocketReadTimeout() {
    return sender.getSocketReadTimeout();
  }

  public boolean isBatchConflationEnabled() {
    return sender.isBatchConflationEnabled();
  }

  public boolean isManualStart() {
    return sender.isManualStart();
  }

  public boolean isPaused() {
    return sender.isPaused();
  }

  public boolean isPersistenceEnabled() {
    return sender.isPersistenceEnabled();
  }

  public boolean isRunning() {
    return sender.isRunning();
  }

  public void pause() {
    sender.pause();
  }

  public void resume() {
    sender.resume();
  }

  public void start() {
    sender.start();
  }

  public void stop() {
    sender.stop();
  }

  public void rebalance() {
    sender.rebalance();
  }

  public boolean isPrimary() {
    return ((AbstractGatewaySender) sender).isPrimary();
  }

  public int getDispatcherThreads() {
    return sender.getDispatcherThreads();
  }

  public String getOrderPolicy() {
    return sender.getOrderPolicy() != null ? sender.getOrderPolicy().name() : null;
  }

  public boolean isDiskSynchronous() {
    return sender.isDiskSynchronous();
  }

  public boolean isParallel() {
    return sender.isParallel();
  }

  /** Statistics Related Attributes **/


  public int getTotalBatchesRedistributed() {
    return getStatistic(StatsKey.GATEWAYSENDER_TOTAL_BATCHES_REDISTRIBUTED).intValue();
  }

  public int getTotalEventsConflated() {
    return getStatistic(StatsKey.GATEWAYSENDER_EVENTS_QUEUED_CONFLATED).intValue();
  }

  public int getEventQueueSize() {
    return abstractSender.getEventQueueSize();
  }

  public float getEventsQueuedRate() {
    return eventsQueuedRate.getRate();
  }

  public float getEventsReceivedRate() {
    return eventsReceivedRate.getRate();
  }

  public float getBatchesDispatchedRate() {
    return batchesDispatchedRate.getRate();
  }

  public long getAverageDistributionTimePerBatch() {
    return batchDistributionAvgLatency.getAverageLatency();
  }

  public float getLRUEvictionsRate() {
    return lruEvictionsRate.getRate();
  }

  public long getEntriesOverflowedToDisk() {
    return overflowMonitor.getStatistic(StatsKey.GATEWAYSENDER_ENTRIES_OVERFLOWED_TO_DISK)
        .longValue();
  }

  public long getBytesOverflowedToDisk() {
    return overflowMonitor.getStatistic(StatsKey.GATEWAYSENDER_BYTES_OVERFLOWED_TO_DISK)
        .longValue();
  }

  private Number getStatistic(String statName) {
    if (monitor != null) {
      return monitor.getStatistic(statName);
    } else {
      return 0;
    }
  }

  public String getGatewayReceiver() {
    return ((AbstractGatewaySender) this.sender).getServerLocation().toString();
  }

  public boolean isConnected() {
    if (this.dispatcher != null && this.dispatcher.isConnectedToRemote()) {
      return true;
    } else {
      return false;
    }
  }

  public int getEventsExceedingAlertThreshold() {
    return getStatistic(StatsKey.GATEWAYSENDER_EVENTS_EXCEEDING_ALERT_THRESHOLD).intValue();
  }
}
