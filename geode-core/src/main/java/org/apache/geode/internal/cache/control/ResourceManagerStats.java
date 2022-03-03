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
package org.apache.geode.internal.cache.control;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.distributed.internal.PoolStatHelper;
import org.apache.geode.distributed.internal.QueueStatHelper;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;

/**
 * Contains methods for manipulating resource manager statistics.
 *
 */
public class ResourceManagerStats {
  // static fields
  @Immutable
  private static final StatisticsType type;

  private static final int rebalancesInProgressId;
  private static final int rebalancesCompletedId;
  private static final int autoRebalanceAttemptsId;
  private static final int rebalanceTimeId;
  private static final int restoreRedundanciesInProgressId;
  private static final int restoreRedundanciesCompletedId;
  private static final int restoreRedundancyTimeId;
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
  private static final int numThreadsStuckId;



  static {
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();
    type = f.createType("ResourceManagerStats", "Statistics about resource management",
        new StatisticDescriptor[] {
            f.createLongGauge("rebalancesInProgress",
                "Current number of cache rebalance operations being directed by this process.",
                "operations"),
            f.createLongCounter("rebalancesCompleted",
                "Total number of cache rebalance operations directed by this process.",
                "operations"),
            f.createLongCounter("autoRebalanceAttempts",
                "Total number of cache auto-rebalance attempts.", "operations"),
            f.createLongCounter("rebalanceTime",
                "Total time spent directing cache rebalance operations.", "nanoseconds", false),

            f.createLongCounter("restoreRedundanciesInProgress",
                "Current number of cache restore redundancy operations being directed by this process.",
                "operations"),
            f.createLongCounter("restoreRedundanciesCompleted",
                "Total number of cache restore redundancy operations directed by this process.",
                "operations"),
            f.createLongCounter("restoreRedundancyTime",
                "Total time spent directing cache restore redundancy operations.", "nanoseconds",
                false),

            f.createLongGauge("rebalanceBucketCreatesInProgress",
                "Current number of bucket create operations being directed for rebalancing.",
                "operations"),
            f.createLongCounter("rebalanceBucketCreatesCompleted",
                "Total number of bucket create operations directed for rebalancing.", "operations"),
            f.createLongCounter("rebalanceBucketCreatesFailed",
                "Total number of bucket create operations directed for rebalancing that failed.",
                "operations"),
            f.createLongCounter("rebalanceBucketCreateTime",
                "Total time spent directing bucket create operations for rebalancing.",
                "nanoseconds", false),
            f.createLongCounter("rebalanceBucketCreateBytes",
                "Total bytes created while directing bucket create operations for rebalancing.",
                "bytes", false),

            f.createLongGauge("rebalanceBucketRemovesInProgress",
                "Current number of bucket remove operations being directed for rebalancing.",
                "operations"),
            f.createLongCounter("rebalanceBucketRemovesCompleted",
                "Total number of bucket remove operations directed for rebalancing.", "operations"),
            f.createLongCounter("rebalanceBucketRemovesFailed",
                "Total number of bucket remove operations directed for rebalancing that failed.",
                "operations"),
            f.createLongCounter("rebalanceBucketRemovesTime",
                "Total time spent directing bucket remove operations for rebalancing.",
                "nanoseconds", false),
            f.createLongCounter("rebalanceBucketRemovesBytes",
                "Total bytes removed while directing bucket remove operations for rebalancing.",
                "bytes", false),


            f.createLongGauge("rebalanceBucketTransfersInProgress",
                "Current number of bucket transfer operations being directed for rebalancing.",
                "operations"),
            f.createLongCounter("rebalanceBucketTransfersCompleted",
                "Total number of bucket transfer operations directed for rebalancing.",
                "operations"),
            f.createLongCounter("rebalanceBucketTransfersFailed",
                "Total number of bucket transfer operations directed for rebalancing that failed.",
                "operations"),
            f.createLongCounter("rebalanceBucketTransfersTime",
                "Total time spent directing bucket transfer operations for rebalancing.",
                "nanoseconds", false),
            f.createLongCounter("rebalanceBucketTransfersBytes",
                "Total bytes transfered while directing bucket transfer operations for rebalancing.",
                "bytes", false),

            f.createLongGauge("rebalancePrimaryTransfersInProgress",
                "Current number of primary transfer operations being directed for rebalancing.",
                "operations"),
            f.createLongCounter("rebalancePrimaryTransfersCompleted",
                "Total number of primary transfer operations directed for rebalancing.",
                "operations"),
            f.createLongCounter("rebalancePrimaryTransfersFailed",
                "Total number of primary transfer operations directed for rebalancing that failed.",
                "operations"),
            f.createLongCounter("rebalancePrimaryTransferTime",
                "Total time spent directing primary transfer operations for rebalancing.",
                "nanoseconds", false),
            f.createLongCounter("rebalanceMembershipChanges",
                "The number of times that membership has changed during a rebalance", "events"),

            f.createLongGauge("heapCriticalEvents",
                "Total number of times the heap usage went over critical threshold.", "events"),
            f.createLongGauge("offHeapCriticalEvents",
                "Total number of times off-heap usage went over critical threshold.", "events"),
            f.createLongGauge("heapSafeEvents",
                "Total number of times the heap usage fell below critical threshold.", "events"),
            f.createLongGauge("offHeapSafeEvents",
                "Total number of times off-heap usage fell below critical threshold.", "events"),
            f.createLongGauge("evictionStartEvents",
                "Total number of times heap usage went over eviction threshold.", "events"),
            f.createLongGauge("offHeapEvictionStartEvents",
                "Total number of times off-heap usage went over eviction threshold.", "events"),
            f.createLongGauge("evictionStopEvents",
                "Total number of times heap usage fell below eviction threshold.", "events"),
            f.createLongGauge("offHeapEvictionStopEvents",
                "Total number of times off-heap usage fell below eviction threshold.", "events"),
            f.createLongGauge("criticalThreshold",
                "The currently set heap critical threshold value in bytes", "bytes"),
            f.createLongGauge("offHeapCriticalThreshold",
                "The currently set off-heap critical threshold value in bytes", "bytes"),
            f.createLongGauge("evictionThreshold",
                "The currently set heap eviction threshold value in bytes", "bytes"),
            f.createLongGauge("offHeapEvictionThreshold",
                "The currently set off-heap eviction threshold value in bytes", "bytes"),
            f.createLongGauge("tenuredHeapUsed", "Total memory used in the tenured/old space",
                "bytes"),
            f.createLongCounter("resourceEventsDelivered",
                "Total number of resource events delivered to listeners", "events"),
            f.createLongGauge("resourceEventQueueSize",
                "Pending events for thresholdEventProcessor thread", "events"),
            f.createLongGauge("thresholdEventProcessorThreadJobs",
                "Number of jobs currently being processed by the thresholdEventProcessorThread",
                "jobs"),
            f.createLongGauge("numThreadsStuck",
                "Number of running threads that have not changed state within the thread-monitor-time-limit-ms interval.",
                "stuck Threads")});

    rebalancesInProgressId = type.nameToId("rebalancesInProgress");
    rebalancesCompletedId = type.nameToId("rebalancesCompleted");
    autoRebalanceAttemptsId = type.nameToId("autoRebalanceAttempts");
    rebalanceTimeId = type.nameToId("rebalanceTime");
    restoreRedundanciesInProgressId = type.nameToId("restoreRedundanciesInProgress");
    restoreRedundanciesCompletedId = type.nameToId("restoreRedundanciesCompleted");
    restoreRedundancyTimeId = type.nameToId("restoreRedundancyTime");
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
    numThreadsStuckId = type.nameToId("numThreadsStuck");
  }

  private final Statistics stats;

  public ResourceManagerStats(StatisticsFactory factory) {
    stats = factory.createAtomicStatistics(type, "ResourceManagerStats");
  }

  public void close() {
    stats.close();
  }

  public long startRebalance() {
    stats.incLong(rebalancesInProgressId, 1L);
    return System.nanoTime();
  }

  public void incAutoRebalanceAttempts() {
    stats.incLong(autoRebalanceAttemptsId, 1L);
  }

  public void endRebalance(long start) {
    long elapsed = System.nanoTime() - start;
    stats.incLong(rebalancesInProgressId, -1L);
    stats.incLong(rebalancesCompletedId, 1L);
    stats.incLong(rebalanceTimeId, elapsed);
  }

  public long startRestoreRedundancy() {
    stats.incLong(restoreRedundanciesInProgressId, 1L);
    return System.nanoTime();
  }

  public void endRestoreRedundancy(long start) {
    long elapsed = System.nanoTime() - start;
    stats.incLong(restoreRedundanciesInProgressId, -1L);
    stats.incLong(restoreRedundanciesCompletedId, 1L);
    stats.incLong(restoreRedundancyTimeId, elapsed);
  }

  public void startBucketCreate(int regions) {
    stats.incLong(rebalanceBucketCreatesInProgressId, regions);
  }

  public void endBucketCreate(int regions, boolean success, long bytes, long elapsed) {
    stats.incLong(rebalanceBucketCreatesInProgressId, -regions);
    stats.incLong(rebalanceBucketCreateTimeId, elapsed);
    if (success) {
      stats.incLong(rebalanceBucketCreatesCompletedId, regions);
      stats.incLong(rebalanceBucketCreateBytesId, bytes);
    } else {
      stats.incLong(rebalanceBucketCreatesFailedId, regions);
    }
  }

  public void startBucketRemove(int regions) {
    stats.incLong(rebalanceBucketRemovesInProgressId, regions);
  }

  public void endBucketRemove(int regions, boolean success, long bytes, long elapsed) {
    stats.incLong(rebalanceBucketRemovesInProgressId, -regions);
    stats.incLong(rebalanceBucketRemovesTimeId, elapsed);
    if (success) {
      stats.incLong(rebalanceBucketRemovesCompletedId, regions);
      stats.incLong(rebalanceBucketRemovesBytesId, bytes);
    } else {
      stats.incLong(rebalanceBucketRemovesFailedId, regions);
    }
  }

  public void startBucketTransfer(int regions) {
    stats.incLong(rebalanceBucketTransfersInProgressId, regions);
  }

  public void endBucketTransfer(int regions, boolean success, long bytes, long elapsed) {
    stats.incLong(rebalanceBucketTransfersInProgressId, -regions);
    stats.incLong(rebalanceBucketTransfersTimeId, elapsed);
    if (success) {
      stats.incLong(rebalanceBucketTransfersCompletedId, regions);
      stats.incLong(rebalanceBucketTransfersBytesId, bytes);
    } else {
      stats.incLong(rebalanceBucketTransfersFailedId, regions);
    }
  }

  public void startPrimaryTransfer(int regions) {
    stats.incLong(rebalancePrimaryTransfersInProgressId, regions);
  }

  public void endPrimaryTransfer(int regions, boolean success, long elapsed) {
    stats.incLong(rebalancePrimaryTransfersInProgressId, -regions);
    stats.incLong(rebalancePrimaryTransferTimeId, elapsed);
    if (success) {
      stats.incLong(rebalancePrimaryTransfersCompletedId, regions);
    } else {
      stats.incLong(rebalancePrimaryTransfersFailedId, regions);
    }
  }

  public void incRebalanceMembershipChanges(long delta) {
    stats.incLong(rebalanceMembershipChanges, 1L);
  }

  public long getRebalanceMembershipChanges() {
    return stats.getLong(rebalanceMembershipChanges);
  }

  public long getRebalancesInProgress() {
    return stats.getLong(rebalancesInProgressId);
  }

  public long getRebalancesCompleted() {
    return stats.getLong(rebalancesCompletedId);
  }

  public long getAutoRebalanceAttempts() {
    return stats.getLong(autoRebalanceAttemptsId);
  }

  public long getRebalanceTime() {
    return stats.getLong(rebalanceTimeId);
  }

  public long getRestoreRedundanciesInProgress() {
    return stats.getLong(restoreRedundanciesInProgressId);
  }

  public long getRestoreRedundanciesCompleted() {
    return stats.getLong(restoreRedundanciesCompletedId);
  }

  public long getRestoreRedundancyTime() {
    return stats.getLong(restoreRedundancyTimeId);
  }

  public long getRebalanceBucketCreatesInProgress() {
    return stats.getLong(rebalanceBucketCreatesInProgressId);
  }

  public long getRebalanceBucketCreatesCompleted() {
    return stats.getLong(rebalanceBucketCreatesCompletedId);
  }

  public long getRebalanceBucketCreatesFailed() {
    return stats.getLong(rebalanceBucketCreatesFailedId);
  }

  public long getRebalanceBucketCreateTime() {
    return stats.getLong(rebalanceBucketCreateTimeId);
  }

  public long getRebalanceBucketCreateBytes() {
    return stats.getLong(rebalanceBucketCreateBytesId);
  }

  public long getRebalanceBucketTransfersInProgress() {
    return stats.getLong(rebalanceBucketTransfersInProgressId);
  }

  public long getRebalanceBucketTransfersCompleted() {
    return stats.getLong(rebalanceBucketTransfersCompletedId);
  }

  public long getRebalanceBucketTransfersFailed() {
    return stats.getLong(rebalanceBucketTransfersFailedId);
  }

  public long getRebalanceBucketTransfersTime() {
    return stats.getLong(rebalanceBucketTransfersTimeId);
  }

  public long getRebalanceBucketTransfersBytes() {
    return stats.getLong(rebalanceBucketTransfersBytesId);
  }

  public long getRebalancePrimaryTransfersInProgress() {
    return stats.getLong(rebalancePrimaryTransfersInProgressId);
  }

  public long getRebalancePrimaryTransfersCompleted() {
    return stats.getLong(rebalancePrimaryTransfersCompletedId);
  }

  public long getRebalancePrimaryTransfersFailed() {
    return stats.getLong(rebalancePrimaryTransfersFailedId);
  }

  public long getRebalancePrimaryTransferTime() {
    return stats.getLong(rebalancePrimaryTransferTimeId);
  }

  public void incResourceEventsDelivered() {
    stats.incLong(resourceEventsDeliveredId, 1L);
  }

  public long getResourceEventsDelivered() {
    return stats.getLong(resourceEventsDeliveredId);
  }

  public void incHeapCriticalEvents() {
    stats.incLong(heapCriticalEventsId, 1L);
  }

  public long getHeapCriticalEvents() {
    return stats.getLong(heapCriticalEventsId);
  }

  public void incOffHeapCriticalEvents() {
    stats.incLong(offHeapCriticalEventsId, 1L);
  }

  public long getOffHeapCriticalEvents() {
    return stats.getLong(offHeapCriticalEventsId);
  }

  public void incHeapSafeEvents() {
    stats.incLong(heapSafeEventsId, 1L);
  }

  public long getHeapSafeEvents() {
    return stats.getLong(heapSafeEventsId);
  }

  public void incOffHeapSafeEvents() {
    stats.incLong(offHeapSafeEventsId, 1L);
  }

  public long getOffHeapSafeEvents() {
    return stats.getLong(offHeapSafeEventsId);
  }

  public void incEvictionStartEvents() {
    stats.incLong(evictionStartEventsId, 1L);
  }

  public long getEvictionStartEvents() {
    return stats.getLong(evictionStartEventsId);
  }

  public void incOffHeapEvictionStartEvents() {
    stats.incLong(offHeapEvictionStartEventsId, 1L);
  }

  public long getOffHeapEvictionStartEvents() {
    return stats.getLong(offHeapEvictionStartEventsId);
  }

  public void incEvictionStopEvents() {
    stats.incLong(evictionStopEventsId, 1L);
  }

  public long getEvictionStopEvents() {
    return stats.getLong(evictionStopEventsId);
  }

  public void incOffHeapEvictionStopEvents() {
    stats.incLong(offHeapEvictionStopEventsId, 1L);
  }

  public long getOffHeapEvictionStopEvents() {
    return stats.getLong(offHeapEvictionStopEventsId);
  }

  public void changeCriticalThreshold(long newValue) {
    stats.setLong(criticalThresholdId, newValue);
  }

  public long getCriticalThreshold() {
    return stats.getLong(criticalThresholdId);
  }

  public void changeOffHeapCriticalThreshold(long newValue) {
    stats.setLong(offHeapCriticalThresholdId, newValue);
  }

  public long getOffHeapCriticalThreshold() {
    return stats.getLong(offHeapCriticalThresholdId);
  }

  public void changeEvictionThreshold(long newValue) {
    stats.setLong(evictionThresholdId, newValue);
  }

  public long getEvictionThreshold() {
    return stats.getLong(evictionThresholdId);
  }

  public void changeOffHeapEvictionThreshold(long newValue) {
    stats.setLong(offHeapEvictionThresholdId, newValue);
  }

  public long getOffHeapEvictionThreshold() {
    return stats.getLong(offHeapEvictionThresholdId);
  }

  public void changeTenuredHeapUsed(long newValue) {
    stats.setLong(tenuredHeapUsageId, newValue);
  }

  public long getTenuredHeapUsed() {
    return stats.getLong(tenuredHeapUsageId);
  }

  public void incResourceEventQueueSize(long delta) {
    stats.incLong(resourceEventQueueSizeId, delta);
  }

  public long getResourceEventQueueSize() {
    return stats.getLong(resourceEventQueueSizeId);
  }

  public void incThresholdEventProcessorThreadJobs(long delta) {
    stats.incLong(thresholdEventProcessorThreadJobsId, delta);
  }

  public long getThresholdEventProcessorThreadJobs() {
    return stats.getLong(thresholdEventProcessorThreadJobsId);
  }

  /**
   * @return a {@link QueueStatHelper} so that we can record number of events in the
   *         thresholdEventProcessor queue.
   */
  public QueueStatHelper getResourceEventQueueStatHelper() {
    return new QueueStatHelper() {
      @Override
      public void add() {
        incResourceEventQueueSize(1L);
      }

      @Override
      public void remove() {
        incResourceEventQueueSize(-1L);
      }

      @Override
      public void remove(long count) {
        incResourceEventQueueSize(-1 * count);
      }
    };
  }

  public PoolStatHelper getResourceEventPoolStatHelper() {
    return new PoolStatHelper() {
      @Override
      public void endJob() {
        incThresholdEventProcessorThreadJobs(-1L);
      }

      @Override
      public void startJob() {
        incThresholdEventProcessorThreadJobs(1L);
      }
    };
  }

  /**
   * Returns the value of ThreadStuck (how many (if at all) stuck threads are in the system)
   */
  public long getNumThreadStuck() {
    return stats.getLong(numThreadsStuckId);
  }

  /**
   * Sets the value of Thread Stuck
   */
  public void setNumThreadStuck(long value) {
    stats.setLong(numThreadsStuckId, value);
  }
}
