/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.control;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.StatisticsTypeFactory;
import com.gemstone.gemfire.distributed.internal.PoolStatHelper;
import com.gemstone.gemfire.distributed.internal.QueueStatHelper;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.StatisticsTypeFactoryImpl;

/**
 * Contains methods for manipulating resource manager statistics.
 * @author dsmith
 *
 */
public class ResourceManagerStats {
  // static fields 
  private static final StatisticsType type;
  
  private static final int rebalancesInProgressId;
  private static final int rebalancesCompletedId;
  private static final int rebalanceTimeId;
  private static final int rebalanceBucketCreatesInProgressId;
  private static final int rebalanceBucketCreatesCompletedId;
  private static final int rebalanceBucketCreatesFailedId;
  private static final int rebalanceBucketCreateTimeId;
  private static final int rebalanceBucketCreateBytesId;
  private static final int rebalanceBucketRemovesInProgressId;
  private static final int rebalanceBucketRemovesCompletedId;
  private static final int rebalanceBucketRemovesFailedId;
  private static final int rebalanceBucketRemovesTimeId;
  private static final int rebalanceBucketRemovesBytesId;
  private static final int rebalanceBucketTransfersInProgressId;
  private static final int rebalanceBucketTransfersCompletedId;
  private static final int rebalanceBucketTransfersFailedId;
  private static final int rebalanceBucketTransfersTimeId;
  private static final int rebalanceBucketTransfersBytesId;
  private static final int rebalancePrimaryTransfersInProgressId;
  private static final int rebalancePrimaryTransfersCompletedId;
  private static final int rebalancePrimaryTransfersFailedId;
  private static final int rebalancePrimaryTransferTimeId;
  private static final int rebalanceMembershipChanges;
  private static final int heapCriticalEventsId;
  private static final int heapSafeEventsId;
  private static final int evictionStartEventsId;
  private static final int evictMoreEventsId;
  private static final int evictionStopEventsId;
  private static final int criticalThresholdId;
  private static final int evictionThresholdId;
  private static final int tenuredHeapUsageId;
  private static final int resourceEventsDeliveredId;
  private static final int resourceEventQueueSizeId;
  private static final int thresholdEventProcessorThreadJobsId;
  


  static {
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();
    type = f.createType(
      "ResourceManagerStats", 
      "Statistics about resource management",
      new StatisticDescriptor[] {
          f.createIntGauge(
              "rebalancesInProgress",
              "Current number of cache rebalance operations being directed by this process.",
              "operations"),
            f.createIntCounter(
              "rebalancesCompleted",
              "Total number of cache rebalance operations directed by this process.",
              "operations"),
            f.createLongCounter(
              "rebalanceTime",
              "Total time spent directing cache rebalance operations.",
              "nanoseconds", false),

            f.createIntGauge(
              "rebalanceBucketCreatesInProgress",
              "Current number of bucket create operations being directed for rebalancing.",
              "operations"),
            f.createIntCounter(
              "rebalanceBucketCreatesCompleted",
              "Total number of bucket create operations directed for rebalancing.",
              "operations"),
            f.createIntCounter(
                  "rebalanceBucketCreatesFailed",
                  "Total number of bucket create operations directed for rebalancing that failed.",
                  "operations"),
            f.createLongCounter(
              "rebalanceBucketCreateTime",
              "Total time spent directing bucket create operations for rebalancing.",
              "nanoseconds", false),
            f.createLongCounter(
              "rebalanceBucketCreateBytes",
              "Total bytes created while directing bucket create operations for rebalancing.",
              "bytes", false),
              
              f.createIntGauge(
                  "rebalanceBucketRemovesInProgress",
                  "Current number of bucket remove operations being directed for rebalancing.",
                  "operations"),
                f.createIntCounter(
                  "rebalanceBucketRemovesCompleted",
                  "Total number of bucket remove operations directed for rebalancing.",
                  "operations"),
                f.createIntCounter(
                      "rebalanceBucketRemovesFailed",
                      "Total number of bucket remove operations directed for rebalancing that failed.",
                      "operations"),
                f.createLongCounter(
                  "rebalanceBucketRemovesTime",
                  "Total time spent directing bucket remove operations for rebalancing.",
                  "nanoseconds", false),
                f.createLongCounter(
                  "rebalanceBucketRemovesBytes",
                  "Total bytes removed while directing bucket remove operations for rebalancing.",
                  "bytes", false),


            f.createIntGauge(
              "rebalanceBucketTransfersInProgress",
              "Current number of bucket transfer operations being directed for rebalancing.",
              "operations"),
            f.createIntCounter(
              "rebalanceBucketTransfersCompleted",
              "Total number of bucket transfer operations directed for rebalancing.",
              "operations"),
            f.createIntCounter(
                  "rebalanceBucketTransfersFailed",
                  "Total number of bucket transfer operations directed for rebalancing that failed.",
                  "operations"),
            f.createLongCounter(
              "rebalanceBucketTransfersTime",
              "Total time spent directing bucket transfer operations for rebalancing.",
              "nanoseconds", false),
            f.createLongCounter(
              "rebalanceBucketTransfersBytes",
              "Total bytes transfered while directing bucket transfer operations for rebalancing.",
              "bytes", false),

            f.createIntGauge(
              "rebalancePrimaryTransfersInProgress",
              "Current number of primary transfer operations being directed for rebalancing.",
              "operations"),
            f.createIntCounter(
              "rebalancePrimaryTransfersCompleted",
              "Total number of primary transfer operations directed for rebalancing.",
              "operations"),
            f.createIntCounter(
                  "rebalancePrimaryTransfersFailed",
                  "Total number of primary transfer operations directed for rebalancing that failed.",
                  "operations"),
            f.createLongCounter(
              "rebalancePrimaryTransferTime",
              "Total time spent directing primary transfer operations for rebalancing.",
              "nanoseconds", false),
            f.createIntCounter(
              "rebalanceMembershipChanges",
              "The number of times that membership has changed during a rebalance",
              "events"),
              
            f.createIntGauge(
                "heapCriticalEvents",
                "Total number of times the heap usage went over critical threshold.",
                "events"),
            f.createIntGauge(
                "heapSafeEvents", 
                "Total number of times the heap usage fell below critical threshold.",
                "events"),
            f.createIntGauge(
                "evictionStartEvents",
                "Total number of times heap usage went over eviction threshold.",
                "events"),
            f.createIntCounter(
                "evictMoreEvents",
                "Total number of times evict more event was delivered",
                "events"),
            f.createIntGauge(
                "evictionStopEvents",
                "Total number of times heap usage fell below eviction threshold.",
                "events"),
            f.createLongGauge(
                "criticalThreshold",
                "The currently set critical threshold value in bytes",
                "bytes"),
            f.createLongGauge(
                "evictionThreshold", 
                "The currently set eviction threshold value in bytes",
                "bytes"),
            f.createLongGauge(
                "tenuredHeapUsed",
                "Total memory used in the tenured/old space",
                "bytes"),
            f.createIntCounter(
                "resourceEventsDelivered",
                "Total number of resource events delivered to listeners",
                "events"),
            f.createIntGauge(
                "resourceEventQueueSize",
                "Pending events for thresholdEventProcessor thread",
                "events"),
            f.createIntGauge(
                "thresholdEventProcessorThreadJobs",
                "Number of jobs currently being processed by the thresholdEventProcessorThread",
                "jobs")
      });
    
    rebalancesInProgressId = type.nameToId("rebalancesInProgress");
    rebalancesCompletedId = type.nameToId("rebalancesCompleted");
    rebalanceTimeId = type.nameToId("rebalanceTime");
    rebalanceBucketCreatesInProgressId = type.nameToId("rebalanceBucketCreatesInProgress");
    rebalanceBucketCreatesCompletedId = type.nameToId("rebalanceBucketCreatesCompleted");
    rebalanceBucketCreatesFailedId = type.nameToId("rebalanceBucketCreatesFailed");
    rebalanceBucketCreateTimeId = type.nameToId("rebalanceBucketCreateTime");
    rebalanceBucketCreateBytesId = type.nameToId("rebalanceBucketCreateBytes");
    rebalanceBucketRemovesInProgressId = type.nameToId("rebalanceBucketRemovesInProgress");
    rebalanceBucketRemovesCompletedId = type.nameToId("rebalanceBucketRemovesCompleted");
    rebalanceBucketRemovesFailedId = type.nameToId("rebalanceBucketRemovesFailed");
    rebalanceBucketRemovesTimeId = type.nameToId("rebalanceBucketRemovesTime");
    rebalanceBucketRemovesBytesId = type.nameToId("rebalanceBucketRemovesBytes");
    rebalanceBucketTransfersInProgressId = type.nameToId("rebalanceBucketTransfersInProgress");
    rebalanceBucketTransfersCompletedId = type.nameToId("rebalanceBucketTransfersCompleted");
    rebalanceBucketTransfersFailedId = type.nameToId("rebalanceBucketTransfersFailed");
    rebalanceBucketTransfersTimeId = type.nameToId("rebalanceBucketTransfersTime");
    rebalanceBucketTransfersBytesId = type.nameToId("rebalanceBucketTransfersBytes");
    rebalancePrimaryTransfersInProgressId = type.nameToId("rebalancePrimaryTransfersInProgress");
    rebalancePrimaryTransfersCompletedId = type.nameToId("rebalancePrimaryTransfersCompleted");
    rebalancePrimaryTransfersFailedId = type.nameToId("rebalancePrimaryTransfersFailed");
    rebalancePrimaryTransferTimeId = type.nameToId("rebalancePrimaryTransferTime");
    rebalanceMembershipChanges = type.nameToId("rebalanceMembershipChanges");
    heapCriticalEventsId = type.nameToId("heapCriticalEvents");
    heapSafeEventsId = type.nameToId("heapSafeEvents");
    evictionStartEventsId = type.nameToId("evictionStartEvents");
    evictMoreEventsId = type.nameToId("evictMoreEvents");
    evictionStopEventsId = type.nameToId("evictionStopEvents");
    criticalThresholdId = type.nameToId("criticalThreshold");
    evictionThresholdId = type.nameToId("evictionThreshold");
    tenuredHeapUsageId = type.nameToId("tenuredHeapUsed");
    resourceEventsDeliveredId = type.nameToId("resourceEventsDelivered");
    resourceEventQueueSizeId = type.nameToId("resourceEventQueueSize");
    thresholdEventProcessorThreadJobsId = type.nameToId("thresholdEventProcessorThreadJobs");
  }
  
  private final Statistics stats;
  
  public ResourceManagerStats(StatisticsFactory factory) {
    this.stats = factory.createAtomicStatistics(type, "ResourceManagerStats");
  }

  public void close() {
    this.stats.close();
  }
  
  public long startRebalance() {
    this.stats.incInt(rebalancesInProgressId, 1);
    return System.nanoTime();
  }
  
  public void endRebalance(long start) {
    long elapsed = System.nanoTime() - start;
    this.stats.incInt(rebalancesInProgressId, -1);
    this.stats.incInt(rebalancesCompletedId, 1);
    this.stats.incLong(rebalanceTimeId, elapsed);
  }
  
  public void startBucketCreate(int regions) {
    this.stats.incInt(rebalanceBucketCreatesInProgressId, regions);
  }
  
  public void endBucketCreate(int regions, boolean success, long bytes, long elapsed) {
    this.stats.incInt(rebalanceBucketCreatesInProgressId, -regions);
    this.stats.incLong(rebalanceBucketCreateTimeId, elapsed);
    if(success) {
      this.stats.incInt(rebalanceBucketCreatesCompletedId, regions);
      this.stats.incLong(rebalanceBucketCreateBytesId, bytes);
    } else {
      this.stats.incInt(rebalanceBucketCreatesFailedId, regions);
    }
  }
  
  public void startBucketRemove(int regions) {
    this.stats.incInt(rebalanceBucketRemovesInProgressId, regions);
  }
  
  public void endBucketRemove(int regions, boolean success, long bytes, long elapsed) {
    this.stats.incInt(rebalanceBucketRemovesInProgressId, -regions);
    this.stats.incLong(rebalanceBucketRemovesTimeId, elapsed);
    if(success) {
      this.stats.incInt(rebalanceBucketRemovesCompletedId, regions);
      this.stats.incLong(rebalanceBucketRemovesBytesId, bytes);
    } else {
      this.stats.incInt(rebalanceBucketRemovesFailedId, regions);
    }
  }
  
  public void startBucketTransfer(int regions) {
    this.stats.incInt(rebalanceBucketTransfersInProgressId, regions);
  }
  
  public void endBucketTransfer(int regions, boolean success, long bytes, long elapsed) {
    this.stats.incInt(rebalanceBucketTransfersInProgressId, -regions);
    this.stats.incLong(rebalanceBucketTransfersTimeId, elapsed);
    if(success) {
      this.stats.incInt(rebalanceBucketTransfersCompletedId, regions);
      this.stats.incLong(rebalanceBucketTransfersBytesId, bytes);
    } else {
      this.stats.incInt(rebalanceBucketTransfersFailedId, regions);
    }
  }
  
  public void startPrimaryTransfer(int regions) {
    this.stats.incInt(rebalancePrimaryTransfersInProgressId, regions);
  }
  
  public void endPrimaryTransfer(int regions, boolean success, long elapsed) {
    this.stats.incInt(rebalancePrimaryTransfersInProgressId, -regions);
    this.stats.incLong(rebalancePrimaryTransferTimeId, elapsed);
    if(success) {
      this.stats.incInt(rebalancePrimaryTransfersCompletedId, regions);
    } else {
      this.stats.incInt(rebalancePrimaryTransfersFailedId, regions);
    }
  }
  
  public void incRebalanceMembershipChanges(int delta) {
    this.stats.incInt(rebalanceMembershipChanges, 1);
  }
  
  public int getRebalanceMembershipChanges() {
    return this.stats.getInt(rebalanceMembershipChanges);
  }
  
  public int getRebalancesInProgress() {
    return this.stats.getInt(rebalancesInProgressId);
  }
  public int getRebalancesCompleted() {
    return this.stats.getInt(rebalancesCompletedId);
  }
  public long getRebalanceTime() {
    return this.stats.getLong(rebalanceTimeId);
  }
  public int getRebalanceBucketCreatesInProgress() {
    return this.stats.getInt(rebalanceBucketCreatesInProgressId);
  }
  public int getRebalanceBucketCreatesCompleted() {
    return this.stats.getInt(rebalanceBucketCreatesCompletedId);
  }
  public int getRebalanceBucketCreatesFailed() {
    return this.stats.getInt(rebalanceBucketCreatesFailedId);
  }
  public long getRebalanceBucketCreateTime() {
    return this.stats.getLong(rebalanceBucketCreateTimeId);
  }
  public long getRebalanceBucketCreateBytes() {
    return this.stats.getLong(rebalanceBucketCreateBytesId);
  }
  public int getRebalanceBucketTransfersInProgress() {
    return this.stats.getInt(rebalanceBucketTransfersInProgressId);
  }
  public int getRebalanceBucketTransfersCompleted() {
    return this.stats.getInt(rebalanceBucketTransfersCompletedId);
  }
  public int getRebalanceBucketTransfersFailed() {
    return this.stats.getInt(rebalanceBucketTransfersFailedId);
  }
  public long getRebalanceBucketTransfersTime() {
    return this.stats.getLong(rebalanceBucketTransfersTimeId);
  }
  public long getRebalanceBucketTransfersBytes() {
    return this.stats.getLong(rebalanceBucketTransfersBytesId);
  }
  public int getRebalancePrimaryTransfersInProgress() {
    return this.stats.getInt(rebalancePrimaryTransfersInProgressId);
  }
  public int getRebalancePrimaryTransfersCompleted() {
    return this.stats.getInt(rebalancePrimaryTransfersCompletedId);
  }
  public int getRebalancePrimaryTransfersFailed() {
    return this.stats.getInt(rebalancePrimaryTransfersFailedId);
  }
  public long getRebalancePrimaryTransferTime() {
    return this.stats.getLong(rebalancePrimaryTransferTimeId);
  }
  
  public void incResourceEventsDelivered() {
    this.stats.incInt(resourceEventsDeliveredId, 1);
  }

  public int getResourceEventsDelivered() {
    return this.stats.getInt(resourceEventsDeliveredId);
  }
  
  public void incHeapCriticalEvents() {
    this.stats.incInt(heapCriticalEventsId, 1);
  }

  public int getHeapCriticalEvents() {
    return this.stats.getInt(heapCriticalEventsId);
  }
  
  public void incHeapSafeEvents() {
    this.stats.incInt(heapSafeEventsId, 1);
  }
  
  public int getHeapSafeEvents() {
    return this.stats.getInt(heapSafeEventsId);
  }
  
  public void incEvictionStartEvents() {
    this.stats.incInt(evictionStartEventsId, 1);
  }
  
  public int getEvictionStartEvents() {
    return this.stats.getInt(evictionStartEventsId);
  }

  public void incEvictMoreEvents() {
    this.stats.incInt(evictMoreEventsId, 1);
  }

  public int getEvictMoreEvents() {
    return this.stats.getInt(evictMoreEventsId);
  }

  public void incEvictionStopEvents() {
    this.stats.incInt(evictionStopEventsId, 1);
  }
  
  public int getEvictionStopEvents() {
    return this.stats.getInt(evictionStopEventsId);
  }
  
  public void changeCriticalThreshold(long newValue) {
    this.stats.setLong(criticalThresholdId, newValue);
  }
  
  public long getCriticalThreshold() {
    return this.stats.getLong(criticalThresholdId);    
  }
  
  public void changeEvictionThreshold(long newValue) {
    this.stats.setLong(evictionThresholdId, newValue);
  }
  
  public long getEvictionThreshold() {
    return this.stats.getLong(evictionThresholdId);
  }
  
  public void changeTenuredHeapUsed(long newValue) {
    this.stats.setLong(tenuredHeapUsageId, newValue);
  }
  
  public long getTenuredHeapUsed() {
    return this.stats.getLong(tenuredHeapUsageId);
  }

  public void incResourceEventQueueSize(int delta) {
    this.stats.incInt(resourceEventQueueSizeId, delta);
  }

  public int getResourceEventQueueSize() {
    return this.stats.getInt(resourceEventQueueSizeId);
  }

  public void incThresholdEventProcessorThreadJobs(int delta) {
    this.stats.incInt(thresholdEventProcessorThreadJobsId, delta);
  }

  public int getThresholdEventProcessorThreadJobs() {
    return this.stats.getInt(thresholdEventProcessorThreadJobsId);
  }

  /**
   * @return a {@link QueueStatHelper} so that we can record number of events
   * in the thresholdEventProcessor queue.
   */
  public QueueStatHelper getResourceEventQueueStatHelper() {
    return new QueueStatHelper() {
      public void add() {
        incResourceEventQueueSize(1);
      }

      public void remove() {
        incResourceEventQueueSize(-1);
      }

      public void remove(int count) {
        incResourceEventQueueSize(-1 * count);
      }
    };
  }
  
  public PoolStatHelper getResourceEventPoolStatHelper() {
    return new PoolStatHelper() {
      public void endJob() {
        incThresholdEventProcessorThreadJobs(-1);
      }
      public void startJob() {
        incThresholdEventProcessorThreadJobs(1);
      }
    };
  }
}
