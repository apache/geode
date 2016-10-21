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

import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueStats;
import org.apache.geode.management.internal.ManagementStrings;
import org.apache.geode.management.internal.beans.stats.MBeanStatsMonitor;
import org.apache.geode.management.internal.beans.stats.StatsKey;

/**
 * 
 * 
 */
public class AsyncEventQueueMBeanBridge {

  private AsyncEventQueueImpl queueImpl;

  private MBeanStatsMonitor monitor;

  public AsyncEventQueueMBeanBridge(AsyncEventQueue queue) {
    this.queueImpl = (AsyncEventQueueImpl) queue;
    this.monitor =
        new MBeanStatsMonitor(ManagementStrings.ASYNC_EVENT_QUEUE_MONITOR.toLocalizedString());

    AsyncEventQueueStats stats = queueImpl.getStatistics();
    addAsyncEventQueueStats(stats);
  }

  public AsyncEventQueueMBeanBridge() {
    this.monitor =
        new MBeanStatsMonitor(ManagementStrings.ASYNC_EVENT_QUEUE_MONITOR.toLocalizedString());
  }

  public void addAsyncEventQueueStats(AsyncEventQueueStats asyncEventQueueStats) {
    monitor.addStatisticsToMonitor(asyncEventQueueStats.getStats());
  }


  public String getAsyncEventListener() {
    return queueImpl.getAsyncEventListener().getClass().getCanonicalName();
  }

  public int getBatchSize() {
    return queueImpl.getBatchSize();
  }

  public int getBatchTimeInteval() {
    return queueImpl.getBatchTimeInterval();
  }

  public boolean isBatchConflationEnabled() {
    return queueImpl.isBatchConflationEnabled();
  }

  public String getId() {
    return queueImpl.getId();
  }

  public int getMaximumQueueMemory() {
    return queueImpl.getMaximumQueueMemory();
  }

  public String getOverflowDiskStoreName() {
    return queueImpl.getDiskStoreName();
  }

  public boolean isPersistent() {
    return queueImpl.isPersistent();
  }

  public boolean isParallel() {
    return queueImpl.isParallel();
  }

  public boolean isPrimary() {
    return queueImpl.isPrimary();
  }

  public int getDispatcherThreads() {
    return queueImpl.getDispatcherThreads();
  }

  public String getOrderPolicy() {
    return queueImpl.getOrderPolicy() != null ? queueImpl.getOrderPolicy().name() : null;
  }

  public boolean isDiskSynchronous() {
    return queueImpl.isDiskSynchronous();
  }

  public int getEventQueueSize() {
    return getStatistic(StatsKey.ASYNCEVENTQUEUE_EVENTS_QUEUE_SIZE).intValue();
  }

  private Number getStatistic(String statName) {
    if (monitor != null) {
      return monitor.getStatistic(statName);
    } else {
      return 0;
    }
  }

}
