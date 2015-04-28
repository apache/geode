/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management.internal.beans;

import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueImpl;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueStats;
import com.gemstone.gemfire.management.internal.ManagementStrings;
import com.gemstone.gemfire.management.internal.beans.stats.MBeanStatsMonitor;
import com.gemstone.gemfire.management.internal.beans.stats.StatsKey;

/**
 * 
 * @author rishim
 * 
 */
public class AsyncEventQueueMBeanBridge {

  private AsyncEventQueueImpl queueImpl;
  
  private MBeanStatsMonitor monitor;

  public AsyncEventQueueMBeanBridge(AsyncEventQueue queue) {
    this.queueImpl = (AsyncEventQueueImpl)queue;
    this.monitor = new MBeanStatsMonitor(
        ManagementStrings.ASYNC_EVENT_QUEUE_MONITOR.toLocalizedString());
    
    AsyncEventQueueStats stats = queueImpl.getStatistics();
    addAsyncEventQueueStats(stats);
  }
  
  public AsyncEventQueueMBeanBridge() {
    this.monitor = new MBeanStatsMonitor(
        ManagementStrings.ASYNC_EVENT_QUEUE_MONITOR.toLocalizedString());
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
    return queueImpl.getOrderPolicy() != null ? queueImpl.getOrderPolicy().name()
        : null;
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
