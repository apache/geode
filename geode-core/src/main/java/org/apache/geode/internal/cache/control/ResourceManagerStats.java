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
package org.apache.geode.internal.cache.control;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.distributed.internal.PoolStatHelper;
import org.apache.geode.distributed.internal.QueueStatHelper;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;

/**
 * Contains methods for manipulating resource manager statistics.
 *
 */
public class ResourceManagerStats {
  // static fields 
  private static final StatisticsType type;
  
  private static final int rebalancesInProgressId;
  private static final int rebalancesCompletedId;
  private static final int autoRebalanceAttemptsId;
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
  private static final int offHeapCriticalEventsId;
  private static final int heapSafeEventsId;
  private static final int offHeapSafeEventsId;
  private static final int evictionStartEventsId;
  private static final int offHeapEvictionStartEventsId;
  private static final int evictionStopEventsId;
  private static final int offHeapEvictionStopEventsId;
  private static final int criticalThresholdId;
  private static final int offHeapCriticalThresholdId;
  private static final int evictionThresholdId;
  private static final int offHeapEvictionThresholdId;
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
            f.createIntCounter(
                "autoRebalanceAttempts",
                "Total number of cache auto-rebalance attempts.",
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
                "offHeapCriticalEvents",
                "Total number of times off-heap usage went over critical threshold.",
                "events"),
            f.createIntGauge(
                "heapSafeEvents", 
                "Total number of times the heap usage fell below critical threshold.",
                "events"),
            f.createIntGauge(
                "offHeapSafeEvents", 
                "Total number of times off-heap usage fell below critical threshold.",
                "events"),
            f.createIntGauge(
                "evictionStartEvents",
                "Total number of times heap usage went over eviction threshold.",
                "events"),
            f.createIntGauge(
                "offHeapEvictionStartEvents",
                "Total number of times off-heap usage went over eviction threshold.",
                "events"),
            f.createIntGauge(
                "evictionStopEvents",
                "Total number of times heap usage fell below eviction threshold.",
                "events"),
            f.createIntGauge(
                "offHeapEvictionStopEvents",
                "Total number of times off-heap usage fell below eviction threshold.",
                "events"),
            f.createLongGauge(
                "criticalThreshold",
                "The currently set heap critical threshold value in bytes",
                "bytes"),
            f.createLongGauge(
                "offHeapCriticalThreshold",
                "The currently set off-heap critical threshold value in bytes",
                "bytes"),
            f.createLongGauge(
                "evictionThreshold", 
                "The currently set heap eviction threshold value in bytes",
                "bytes"),
            f.createLongGauge(
                "offHeapEvictionThreshold", 
                "The currently set off-heap eviction threshold value in bytes",
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
    autoRebalanceAttemptsId = type.nameToId("autoRebalanceAttempts");
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
    offHeapCriticalEventsId = type.nameToId("offHeapCriticalEvents");
    heapSafeEventsId = type.nameToId("heapSafeEvents");
    offHeapSafeEventsId = type.nameToId("offHeapSafeEvents");
    evictionStartEventsId = type.nameToId("evictionStartEvents");
    offHeapEvictionStartEventsId = type.nameToId("offHeapEvictionStartEvents");
    evictionStopEventsId = type.nameToId("evictionStopEvents");
    offHeapEvictionStopEventsId = type.nameToId("offHeapEvictionStopEvents");
    criticalThresholdId = type.nameToId("criticalThreshold");
    offHeapCriticalThresholdId = type.nameToId("offHeapCriticalThreshold");
    evictionThresholdId = type.nameToId("evictionThreshold");
    offHeapEvictionThresholdId = type.nameToId("offHeapEvictionThreshold");
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
  
  public void incAutoRebalanceAttempts() {
    this.stats.incInt(autoRebalanceAttemptsId, 1);
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
  public int getAutoRebalanceAttempts() {
    return this.stats.getInt(autoRebalanceAttemptsId);
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
  
  public void incOffHeapCriticalEvents() {
    this.stats.incInt(offHeapCriticalEventsId, 1);
  }
  
  public int getOffHeapCriticalEvents() {
    return this.stats.getInt(offHeapCriticalEventsId);
  }
  
  public void incHeapSafeEvents() {
    this.stats.incInt(heapSafeEventsId, 1);
  }
  
  public int getHeapSafeEvents() {
    return this.stats.getInt(heapSafeEventsId);
  }
  
  public void incOffHeapSafeEvents() {
    this.stats.incInt(offHeapSafeEventsId, 1);
  }
  
  public int getOffHeapSafeEvents() {
    return this.stats.getInt(offHeapSafeEventsId);
  }
  
  public void incEvictionStartEvents() {
    this.stats.incInt(evictionStartEventsId, 1);
  }
  
  public int getEvictionStartEvents() {
    return this.stats.getInt(evictionStartEventsId);
  }
  
  public void incOffHeapEvictionStartEvents() {
    this.stats.incInt(offHeapEvictionStartEventsId, 1);
  }
  
  public int getOffHeapEvictionStartEvents() {
    return this.stats.getInt(offHeapEvictionStartEventsId);
  }
  
  public void incEvictionStopEvents() {
    this.stats.incInt(evictionStopEventsId, 1);
  }
  
  public int getEvictionStopEvents() {
    return this.stats.getInt(evictionStopEventsId);
  }
  
  public void incOffHeapEvictionStopEvents() {
    this.stats.incInt(offHeapEvictionStopEventsId, 1);
  }
  
  public int getOffHeapEvictionStopEvents() {
    return this.stats.getInt(offHeapEvictionStopEventsId);
  }
  
  public void changeCriticalThreshold(long newValue) {
    this.stats.setLong(criticalThresholdId, newValue);
  }
  
  public long getCriticalThreshold() {
    return this.stats.getLong(criticalThresholdId);    
  }
  
  public void changeOffHeapCriticalThreshold(long newValue) {
    this.stats.setLong(offHeapCriticalThresholdId, newValue);
  }
  
  public long getOffHeapCriticalThreshold() {
    return this.stats.getLong(offHeapCriticalThresholdId);    
  }
  
  public void changeEvictionThreshold(long newValue) {
    this.stats.setLong(evictionThresholdId, newValue);
  }
  
  public long getEvictionThreshold() {
    return this.stats.getLong(evictionThresholdId);
  }
  
  public void changeOffHeapEvictionThreshold(long newValue) {
    this.stats.setLong(offHeapEvictionThresholdId, newValue);
  }
  
  public long getOffHeapEvictionThreshold() {
    return this.stats.getLong(offHeapEvictionThresholdId);
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
      @Override
      public void add() {
        incResourceEventQueueSize(1);
      }

      @Override
      public void remove() {
        incResourceEventQueueSize(-1);
      }

      @Override
      public void remove(int count) {
        incResourceEventQueueSize(-1 * count);
      }
    };
  }
  
  public PoolStatHelper getResourceEventPoolStatHelper() {
    return new PoolStatHelper() {
      @Override
      public void endJob() {
        incThresholdEventProcessorThreadJobs(-1);
      }
      @Override
      public void startJob() {
        incThresholdEventProcessorThreadJobs(1);
      }
    };
  }
}
