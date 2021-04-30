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
package org.apache.geode.internal.cache;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;

/**
 * Represents a statistics type that can be archived to vsd. Loading of this class automatically
 * triggers statistics archival.
 * <p>
 *
 * A singleton instance can be requested with the initSingleton(...) and getSingleton() methods.
 * <p>
 *
 * Individual instances can be created with the constructor.
 * <p>
 *
 * To manipulate the statistic values, use (inc|dec|set|get)&lt;fieldName&gt; methods.
 *
 * @since GemFire 5.0
 */
public class PartitionedRegionStats {

  @Immutable
  private static final StatisticsType type;

  private static final int dataStoreEntryCountId;
  private static final int dataStoreBytesInUseId;
  private static final int bucketCountId;

  private static final int putsCompletedId;
  private static final int putOpsRetriedId;
  private static final int putRetriesId;

  private static final int createsCompletedId;
  private static final int createOpsRetriedId;
  private static final int createRetriesId;

  private static final int preferredReadLocalId;
  private static final int preferredReadRemoteId;

  private static final int getsCompletedId;
  private static final int getOpsRetriedId;
  private static final int getRetriesId;

  private static final int destroysCompletedId;
  private static final int destroyOpsRetriedId;
  private static final int destroyRetriesId;

  private static final int invalidatesCompletedId;
  private static final int invalidateOpsRetriedId;
  private static final int invalidateRetriesId;

  private static final int containsKeyCompletedId;
  private static final int containsKeyOpsRetriedId;
  private static final int containsKeyRetriesId;

  private static final int containsValueForKeyCompletedId;

  private static final int partitionMessagesSentId;
  private static final int partitionMessagesReceivedId;
  private static final int partitionMessagesProcessedId;

  private static final int putTimeId;
  private static final int createTimeId;
  private static final int getTimeId;
  private static final int destroyTimeId;
  private static final int invalidateTimeId;
  private static final int containsKeyTimeId;
  private static final int containsValueForKeyTimeId;
  private static final int partitionMessagesProcessingTimeId;

  private static final String PUTALLS_COMPLETED = "putAllsCompleted";
  private static final String PUTALL_MSGS_RETRIED = "putAllMsgsRetried";
  private static final String PUTALL_RETRIES = "putAllRetries";
  private static final String PUTALL_TIME = "putAllTime";

  private static final int fieldId_PUTALLS_COMPLETED;
  private static final int fieldId_PUTALL_MSGS_RETRIED;
  private static final int fieldId_PUTALL_RETRIES;
  private static final int fieldId_PUTALL_TIME;

  private static final String REMOVE_ALLS_COMPLETED = "removeAllsCompleted";
  private static final String REMOVE_ALL_MSGS_RETRIED = "removeAllMsgsRetried";
  private static final String REMOVE_ALL_RETRIES = "removeAllRetries";
  private static final String REMOVE_ALL_TIME = "removeAllTime";

  private static final int fieldId_REMOVE_ALLS_COMPLETED;
  private static final int fieldId_REMOVE_ALL_MSGS_RETRIED;
  private static final int fieldId_REMOVE_ALL_RETRIES;
  private static final int fieldId_REMOVE_ALL_TIME;

  private static final int volunteeringInProgressId; // count of volunteering in progress
  private static final int volunteeringBecamePrimaryId; // ended as primary
  private static final int volunteeringBecamePrimaryTimeId; // time spent that ended as primary
  private static final int volunteeringOtherPrimaryId; // ended as not primary
  private static final int volunteeringOtherPrimaryTimeId; // time spent that ended as not primary
  private static final int volunteeringClosedId; // ended as closed
  private static final int volunteeringClosedTimeId; // time spent that ended as closed

  private static final int applyReplicationCompletedId;
  private static final int applyReplicationInProgressId;
  private static final int applyReplicationTimeId;
  private static final int sendReplicationCompletedId;
  private static final int sendReplicationInProgressId;
  private static final int sendReplicationTimeId;
  private static final int putRemoteCompletedId;
  private static final int putRemoteInProgressId;
  private static final int putRemoteTimeId;
  private static final int putLocalCompletedId;
  private static final int putLocalInProgressId;
  private static final int putLocalTimeId;

  private static final int totalNumBucketsId; // total number of buckets
  private static final int primaryBucketCountId; // number of hosted primary buckets
  private static final int volunteeringThreadsId; // number of threads actively volunteering
  private static final int lowRedundancyBucketCountId; // number of buckets currently without full
                                                       // redundancy
  private static final int noCopiesBucketCountId; // number of buckets currently without any
                                                  // redundancy

  private static final int configuredRedundantCopiesId;
  private static final int actualRedundantCopiesId;

  private static final int getEntriesCompletedId;
  private static final int getEntryTimeId;

  private static final int recoveriesInProgressId;
  private static final int recoveriesCompletedId;
  private static final int recoveriesTimeId;
  private static final int bucketCreatesInProgressId;
  private static final int bucketCreatesCompletedId;
  private static final int bucketCreatesFailedId;
  private static final int bucketCreateTimeId;

  private static final int rebalanceBucketCreatesInProgressId;
  private static final int rebalanceBucketCreatesCompletedId;
  private static final int rebalanceBucketCreatesFailedId;
  private static final int rebalanceBucketCreateTimeId;

  private static final int primaryTransfersInProgressId;
  private static final int primaryTransfersCompletedId;
  private static final int primaryTransfersFailedId;
  private static final int primaryTransferTimeId;

  private static final int rebalancePrimaryTransfersInProgressId;
  private static final int rebalancePrimaryTransfersCompletedId;
  private static final int rebalancePrimaryTransfersFailedId;
  private static final int rebalancePrimaryTransferTimeId;

  private static final int prMetaDataSentCountId;

  private static final int localMaxMemoryId;

  static {

    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();
    type = f.createType("PartitionedRegionStats",
        "Statistics for operations and connections in the Partitioned Region",
        new StatisticDescriptor[] {

            f.createLongGauge("bucketCount", "Number of buckets in this node.", "buckets"),
            f.createLongCounter("putsCompleted", "Number of puts completed.", "operations",
                true),
            f.createLongCounter("putOpsRetried",
                "Number of put operations which had to be retried due to failures.", "operations",
                false),
            f.createLongCounter("putRetries",
                "Total number of times put operations had to be retried.", "retry attempts", false),
            f.createLongCounter("createsCompleted", "Number of creates completed.", "operations",
                true),
            f.createLongCounter("createOpsRetried",
                "Number of create operations which had to be retried due to failures.",
                "operations", false),
            f.createLongCounter("createRetries",
                "Total number of times put operations had to be retried.", "retry attempts", false),
            f.createLongCounter("preferredReadLocal", "Number of reads satisfied from local store",
                "operations", true),
            f.createLongCounter(PUTALLS_COMPLETED, "Number of putAlls completed.", "operations",
                true),
            f.createLongCounter(PUTALL_MSGS_RETRIED,
                "Number of putAll messages which had to be retried due to failures.", "operations",
                false),
            f.createLongCounter(PUTALL_RETRIES,
                "Total number of times putAll messages had to be retried.", "retry attempts",
                false),
            f.createLongCounter(PUTALL_TIME, "Total time spent doing putAlls.", "nanoseconds",
                false),
            f.createLongCounter(REMOVE_ALLS_COMPLETED, "Number of removeAlls completed.",
                "operations", true),
            f.createLongCounter(REMOVE_ALL_MSGS_RETRIED,
                "Number of removeAll messages which had to be retried due to failures.",
                "operations", false),
            f.createLongCounter(REMOVE_ALL_RETRIES,
                "Total number of times removeAll messages had to be retried.", "retry attempts",
                false),
            f.createLongCounter(REMOVE_ALL_TIME, "Total time spent doing removeAlls.",
                "nanoseconds", false),
            f.createLongCounter("preferredReadRemote",
                "Number of reads satisfied from remote store",
                "operations", false),
            f.createLongCounter("getsCompleted", "Number of gets completed.", "operations",
                true),
            f.createLongCounter("getOpsRetried",
                "Number of get operations which had to be retried due to failures.", "operations",
                false),
            f.createLongCounter("getRetries",
                "Total number of times get operations had to be retried.", "retry attempts", false),
            f.createLongCounter("destroysCompleted", "Number of destroys completed.", "operations",
                true),
            f.createLongCounter("destroyOpsRetried",
                "Number of destroy operations which had to be retried due to failures.",
                "operations", false),
            f.createLongCounter("destroyRetries",
                "Total number of times destroy operations had to be retried.", "retry attempts",
                false),
            f.createLongCounter("invalidatesCompleted", "Number of invalidates completed.",
                "operations", true),

            f.createLongCounter("invalidateOpsRetried",
                "Number of invalidate operations which had to be retried due to failures.",
                "operations", false),
            f.createLongCounter("invalidateRetries",
                "Total number of times invalidate operations had to be retried.", "retry attempts",
                false),
            f.createLongCounter("containsKeyCompleted", "Number of containsKeys completed.",
                "operations", true),

            f.createLongCounter("containsKeyOpsRetried",
                "Number of containsKey or containsValueForKey operations which had to be retried due to failures.",
                "operations", false),
            f.createLongCounter("containsKeyRetries",
                "Total number of times containsKey or containsValueForKey operations had to be retried.",
                "operations", false),
            f.createLongCounter("containsValueForKeyCompleted",
                "Number of containsValueForKeys completed.", "operations", true),
            f.createLongCounter("PartitionMessagesSent", "Number of PartitionMessages Sent.",
                "operations", true),
            f.createLongCounter("PartitionMessagesReceived",
                "Number of PartitionMessages Received.",
                "operations", true),
            f.createLongCounter("PartitionMessagesProcessed",
                "Number of PartitionMessages Processed.", "operations", true),
            f.createLongCounter("putTime", "Total time spent doing puts.", "nanoseconds", false),
            f.createLongCounter("createTime", "Total time spent doing create operations.",
                "nanoseconds", false),
            f.createLongCounter("getTime", "Total time spent performing get operations.",
                "nanoseconds", false),
            f.createLongCounter("destroyTime", "Total time spent doing destroys.", "nanoseconds",
                false),
            f.createLongCounter("invalidateTime", "Total time spent doing invalidates.",
                "nanoseconds", false),
            f.createLongCounter("containsKeyTime",
                "Total time spent performing containsKey operations.", "nanoseconds", false),
            f.createLongCounter("containsValueForKeyTime",
                "Total time spent performing containsValueForKey operations.", "nanoseconds",
                false),
            f.createLongCounter("partitionMessagesProcessingTime",
                "Total time spent on PartitionMessages processing.", "nanoseconds", false),
            f.createLongGauge("dataStoreEntryCount",
                "The number of entries stored in this Cache for the named Partitioned Region. This does not include entries which are tombstones. See CachePerfStats.tombstoneCount.",
                "entries"),
            f.createLongGauge("dataStoreBytesInUse",
                "The current number of bytes stored in this Cache for the named Partitioned Region",
                "bytes"),
            f.createLongGauge("volunteeringInProgress",
                "Current number of attempts to volunteer for primary of a bucket.", "operations"),
            f.createLongCounter("volunteeringBecamePrimary",
                "Total number of attempts to volunteer that ended when this member became primary.",
                "operations"),
            f.createLongCounter("volunteeringBecamePrimaryTime",
                "Total time spent volunteering that ended when this member became primary.",
                "nanoseconds", false),
            f.createLongCounter("volunteeringOtherPrimary",
                "Total number of attempts to volunteer that ended when this member discovered other primary.",
                "operations"),
            f.createLongCounter("volunteeringOtherPrimaryTime",
                "Total time spent volunteering that ended when this member discovered other primary.",
                "nanoseconds", false),
            f.createLongCounter("volunteeringClosed",
                "Total number of attempts to volunteer that ended when this member's bucket closed.",
                "operations"),
            f.createLongCounter("volunteeringClosedTime",
                "Total time spent volunteering that ended when this member's bucket closed.",
                "nanoseconds", false),
            f.createLongGauge("totalNumBuckets", "The total number of buckets.", "buckets"),
            f.createLongGauge("primaryBucketCount",
                "Current number of primary buckets hosted locally.", "buckets"),
            f.createLongGauge("volunteeringThreads",
                "Current number of threads volunteering for primary.", "threads"),
            f.createLongGauge("lowRedundancyBucketCount",
                "Current number of buckets without full redundancy.", "buckets"),
            f.createLongGauge("noCopiesBucketCount",
                "Current number of buckets without any copies remaining.", "buckets"),
            f.createLongGauge("configuredRedundantCopies",
                "Configured number of redundant copies for this partitioned region.", "copies"),
            f.createLongGauge("actualRedundantCopies",
                "Actual number of redundant copies for this partitioned region.", "copies"),
            f.createLongCounter("getEntryCompleted", "Number of getEntry operations completed.",
                "operations", true),
            f.createLongCounter("getEntryTime", "Total time spent performing getEntry operations.",
                "nanoseconds", false),

            f.createLongGauge("recoveriesInProgress",
                "Current number of redundancy recovery operations in progress for this region.",
                "operations"),
            f.createLongCounter("recoveriesCompleted",
                "Total number of redundancy recovery operations performed on this region.",
                "operations"),
            f.createLongCounter("recoveryTime", "Total number time spent recovering redundancy.",
                "operations"),
            f.createLongGauge("bucketCreatesInProgress",
                "Current number of bucket create operations being performed for rebalancing.",
                "operations"),
            f.createLongCounter("bucketCreatesCompleted",
                "Total number of bucket create operations performed for rebalancing.",
                "operations"),
            f.createLongCounter("bucketCreatesFailed",
                "Total number of bucket create operations performed for rebalancing that failed.",
                "operations"),
            f.createLongCounter("bucketCreateTime",
                "Total time spent performing bucket create operations for rebalancing.",
                "nanoseconds", false),
            f.createLongGauge("primaryTransfersInProgress",
                "Current number of primary transfer operations being performed for rebalancing.",
                "operations"),
            f.createLongCounter("primaryTransfersCompleted",
                "Total number of primary transfer operations performed for rebalancing.",
                "operations"),
            f.createLongCounter("primaryTransfersFailed",
                "Total number of primary transfer operations performed for rebalancing that failed.",
                "operations"),
            f.createLongCounter("primaryTransferTime",
                "Total time spent performing primary transfer operations for rebalancing.",
                "nanoseconds", false),

            f.createLongCounter("applyReplicationCompleted",
                "Total number of replicated values sent from a primary to this redundant data store.",
                "operations", true),
            f.createLongGauge("applyReplicationInProgress",
                "Current number of replication operations in progress on this redundant data store.",
                "operations", false),
            f.createLongCounter("applyReplicationTime",
                "Total time spent storing replicated values on this redundant data store.",
                "nanoseconds", false),
            f.createLongCounter("sendReplicationCompleted",
                "Total number of replicated values sent from this primary to a redundant data store.",
                "operations", true),
            f.createLongGauge("sendReplicationInProgress",
                "Current number of replication operations in progress from this primary.",
                "operations", false),
            f.createLongCounter("sendReplicationTime",
                "Total time spent replicating values from this primary to a redundant data store.",
                "nanoseconds", false),
            f.createLongCounter("putRemoteCompleted",
                "Total number of completed puts that did not originate in the primary. These puts require an extra network hop to the primary.",
                "operations", true),
            f.createLongGauge("putRemoteInProgress",
                "Current number of puts in progress that did not originate in the primary.",
                "operations", false),
            f.createLongCounter("putRemoteTime",
                "Total time spent doing puts that did not originate in the primary.", "nanoseconds",
                false),
            f.createLongCounter("putLocalCompleted",
                "Total number of completed puts that did originate in the primary. These puts are optimal.",
                "operations", true),
            f.createLongGauge("putLocalInProgress",
                "Current number of puts in progress that did originate in the primary.",
                "operations", false),
            f.createLongCounter("putLocalTime",
                "Total time spent doing puts that did originate in the primary.", "nanoseconds",
                false),

            f.createLongGauge("rebalanceBucketCreatesInProgress",
                "Current number of bucket create operations being performed for rebalancing.",
                "operations"),
            f.createLongCounter("rebalanceBucketCreatesCompleted",
                "Total number of bucket create operations performed for rebalancing.",
                "operations"),
            f.createLongCounter("rebalanceBucketCreatesFailed",
                "Total number of bucket create operations performed for rebalancing that failed.",
                "operations"),
            f.createLongCounter("rebalanceBucketCreateTime",
                "Total time spent performing bucket create operations for rebalancing.",
                "nanoseconds", false),
            f.createLongGauge("rebalancePrimaryTransfersInProgress",
                "Current number of primary transfer operations being performed for rebalancing.",
                "operations"),
            f.createLongCounter("rebalancePrimaryTransfersCompleted",
                "Total number of primary transfer operations performed for rebalancing.",
                "operations"),
            f.createLongCounter("rebalancePrimaryTransfersFailed",
                "Total number of primary transfer operations performed for rebalancing that failed.",
                "operations"),
            f.createLongCounter("rebalancePrimaryTransferTime",
                "Total time spent performing primary transfer operations for rebalancing.",
                "nanoseconds", false),
            f.createLongCounter("prMetaDataSentCount",
                "total number of times meta data refreshed sent on client's request.", "operation",
                false),

            f.createLongGauge("localMaxMemory",
                "local max memory in bytes for this region on this member", "bytes"),
        });

    bucketCountId = type.nameToId("bucketCount");

    putsCompletedId = type.nameToId("putsCompleted");
    putOpsRetriedId = type.nameToId("putOpsRetried");
    putRetriesId = type.nameToId("putRetries");
    createsCompletedId = type.nameToId("createsCompleted");
    createOpsRetriedId = type.nameToId("createOpsRetried");
    createRetriesId = type.nameToId("createRetries");
    getsCompletedId = type.nameToId("getsCompleted");
    preferredReadLocalId = type.nameToId("preferredReadLocal");
    preferredReadRemoteId = type.nameToId("preferredReadRemote");
    getOpsRetriedId = type.nameToId("getOpsRetried");
    getRetriesId = type.nameToId("getRetries");
    destroysCompletedId = type.nameToId("destroysCompleted");
    destroyOpsRetriedId = type.nameToId("destroyOpsRetried");
    destroyRetriesId = type.nameToId("destroyRetries");
    invalidatesCompletedId = type.nameToId("invalidatesCompleted");
    invalidateOpsRetriedId = type.nameToId("invalidateOpsRetried");
    invalidateRetriesId = type.nameToId("invalidateRetries");
    containsKeyCompletedId = type.nameToId("containsKeyCompleted");
    containsKeyOpsRetriedId = type.nameToId("containsKeyOpsRetried");
    containsKeyRetriesId = type.nameToId("containsKeyRetries");
    containsValueForKeyCompletedId = type.nameToId("containsValueForKeyCompleted");
    partitionMessagesSentId = type.nameToId("PartitionMessagesSent");
    partitionMessagesReceivedId = type.nameToId("PartitionMessagesReceived");
    partitionMessagesProcessedId = type.nameToId("PartitionMessagesProcessed");
    fieldId_PUTALLS_COMPLETED = type.nameToId(PUTALLS_COMPLETED);
    fieldId_PUTALL_MSGS_RETRIED = type.nameToId(PUTALL_MSGS_RETRIED);
    fieldId_PUTALL_RETRIES = type.nameToId(PUTALL_RETRIES);
    fieldId_PUTALL_TIME = type.nameToId(PUTALL_TIME);
    fieldId_REMOVE_ALLS_COMPLETED = type.nameToId(REMOVE_ALLS_COMPLETED);
    fieldId_REMOVE_ALL_MSGS_RETRIED = type.nameToId(REMOVE_ALL_MSGS_RETRIED);
    fieldId_REMOVE_ALL_RETRIES = type.nameToId(REMOVE_ALL_RETRIES);
    fieldId_REMOVE_ALL_TIME = type.nameToId(REMOVE_ALL_TIME);
    putTimeId = type.nameToId("putTime");
    createTimeId = type.nameToId("createTime");
    getTimeId = type.nameToId("getTime");
    destroyTimeId = type.nameToId("destroyTime");
    invalidateTimeId = type.nameToId("invalidateTime");
    containsKeyTimeId = type.nameToId("containsKeyTime");
    containsValueForKeyTimeId = type.nameToId("containsValueForKeyTime");
    partitionMessagesProcessingTimeId = type.nameToId("partitionMessagesProcessingTime");
    dataStoreEntryCountId = type.nameToId("dataStoreEntryCount");
    dataStoreBytesInUseId = type.nameToId("dataStoreBytesInUse");

    volunteeringInProgressId = type.nameToId("volunteeringInProgress");
    volunteeringBecamePrimaryId = type.nameToId("volunteeringBecamePrimary");
    volunteeringBecamePrimaryTimeId = type.nameToId("volunteeringBecamePrimaryTime");
    volunteeringOtherPrimaryId = type.nameToId("volunteeringOtherPrimary");
    volunteeringOtherPrimaryTimeId = type.nameToId("volunteeringOtherPrimaryTime");
    volunteeringClosedId = type.nameToId("volunteeringClosed");
    volunteeringClosedTimeId = type.nameToId("volunteeringClosedTime");

    totalNumBucketsId = type.nameToId("totalNumBuckets");
    primaryBucketCountId = type.nameToId("primaryBucketCount");
    volunteeringThreadsId = type.nameToId("volunteeringThreads");
    lowRedundancyBucketCountId = type.nameToId("lowRedundancyBucketCount");
    noCopiesBucketCountId = type.nameToId("noCopiesBucketCount");

    getEntriesCompletedId = type.nameToId("getEntryCompleted");
    getEntryTimeId = type.nameToId("getEntryTime");

    configuredRedundantCopiesId = type.nameToId("configuredRedundantCopies");
    actualRedundantCopiesId = type.nameToId("actualRedundantCopies");

    recoveriesCompletedId = type.nameToId("recoveriesCompleted");
    recoveriesInProgressId = type.nameToId("recoveriesInProgress");
    recoveriesTimeId = type.nameToId("recoveryTime");
    bucketCreatesInProgressId = type.nameToId("bucketCreatesInProgress");
    bucketCreatesCompletedId = type.nameToId("bucketCreatesCompleted");
    bucketCreatesFailedId = type.nameToId("bucketCreatesFailed");
    bucketCreateTimeId = type.nameToId("bucketCreateTime");
    primaryTransfersInProgressId = type.nameToId("primaryTransfersInProgress");
    primaryTransfersCompletedId = type.nameToId("primaryTransfersCompleted");
    primaryTransfersFailedId = type.nameToId("primaryTransfersFailed");
    primaryTransferTimeId = type.nameToId("primaryTransferTime");

    rebalanceBucketCreatesInProgressId = type.nameToId("rebalanceBucketCreatesInProgress");
    rebalanceBucketCreatesCompletedId = type.nameToId("rebalanceBucketCreatesCompleted");
    rebalanceBucketCreatesFailedId = type.nameToId("rebalanceBucketCreatesFailed");
    rebalanceBucketCreateTimeId = type.nameToId("rebalanceBucketCreateTime");
    rebalancePrimaryTransfersInProgressId = type.nameToId("rebalancePrimaryTransfersInProgress");
    rebalancePrimaryTransfersCompletedId = type.nameToId("rebalancePrimaryTransfersCompleted");
    rebalancePrimaryTransfersFailedId = type.nameToId("rebalancePrimaryTransfersFailed");
    rebalancePrimaryTransferTimeId = type.nameToId("rebalancePrimaryTransferTime");

    applyReplicationCompletedId = type.nameToId("applyReplicationCompleted");
    applyReplicationInProgressId = type.nameToId("applyReplicationInProgress");
    applyReplicationTimeId = type.nameToId("applyReplicationTime");
    sendReplicationCompletedId = type.nameToId("sendReplicationCompleted");
    sendReplicationInProgressId = type.nameToId("sendReplicationInProgress");
    sendReplicationTimeId = type.nameToId("sendReplicationTime");
    putRemoteCompletedId = type.nameToId("putRemoteCompleted");
    putRemoteInProgressId = type.nameToId("putRemoteInProgress");
    putRemoteTimeId = type.nameToId("putRemoteTime");
    putLocalCompletedId = type.nameToId("putLocalCompleted");
    putLocalInProgressId = type.nameToId("putLocalInProgress");
    putLocalTimeId = type.nameToId("putLocalTime");

    prMetaDataSentCountId = type.nameToId("prMetaDataSentCount");

    localMaxMemoryId = type.nameToId("localMaxMemory");
  }

  private final Statistics stats;

  private final StatisticsClock clock;

  /**
   * Utility map for temporarily holding stat start times.
   * <p>
   * This was originally added to avoid having to add a long volunteeringStarted variable to every
   * instance of BucketAdvisor. Majority of BucketAdvisors never volunteer and an instance of
   * BucketAdvisor exists for every bucket defined in a PartitionedRegion which could result in a
   * lot of unused longs. Volunteering is a rare event and thus the performance implications of a
   * HashMap lookup is small and preferrable to so many longs. Key: BucketAdvisor, Value: Long
   */
  private final Map<Object, Long> startTimeMap;

  public PartitionedRegionStats(StatisticsFactory factory, String name, StatisticsClock clock) {
    stats = factory.createAtomicStatistics(type, name);

    if (clock.isEnabled()) {
      startTimeMap = new ConcurrentHashMap<>();
    } else {
      startTimeMap = Collections.emptyMap();
    }

    this.clock = clock;
  }

  public void close() {
    this.stats.close();
  }

  public Statistics getStats() {
    return this.stats;
  }

  public long getTime() {
    return clock.getTime();
  }

  // ------------------------------------------------------------------------
  // region op stats
  // ------------------------------------------------------------------------

  public void endPut(long start) {
    endPut(start, 1);
  }

  /**
   * This method sets the end time for putAll and updates the counters
   *
   */
  public void endPutAll(long start) {
    endPutAll(start, 1);
  }

  public void endRemoveAll(long start) {
    endRemoveAll(start, 1);
  }

  public void endCreate(long start) {
    endCreate(start, 1);
  }

  public void endGet(long start) {
    endGet(start, 1);
  }

  public void endContainsKey(long start) {
    endContainsKey(start, 1);
  }

  public void endContainsValueForKey(long start) {
    endContainsValueForKey(start, 1);
  }

  public void endPut(long start, int numInc) {
    if (clock.isEnabled()) {
      long delta = clock.getTime() - start;
      this.stats.incLong(putTimeId, delta);
    }
    this.stats.incLong(putsCompletedId, numInc);
  }

  /**
   * This method sets the end time for putAll and updates the counters
   *
   */
  public void endPutAll(long start, int numInc) {
    if (clock.isEnabled()) {
      long delta = clock.getTime() - start;
      this.stats.incLong(fieldId_PUTALL_TIME, delta);
      // this.putStatsHistogram.endOp(delta);

    }
    this.stats.incLong(fieldId_PUTALLS_COMPLETED, numInc);
  }

  public void endRemoveAll(long start, int numInc) {
    if (clock.isEnabled()) {
      long delta = clock.getTime() - start;
      this.stats.incLong(fieldId_REMOVE_ALL_TIME, delta);
    }
    this.stats.incLong(fieldId_REMOVE_ALLS_COMPLETED, numInc);
  }

  public void endCreate(long start, int numInc) {
    if (clock.isEnabled()) {
      this.stats.incLong(createTimeId, clock.getTime() - start);
    }
    this.stats.incLong(createsCompletedId, numInc);
  }

  public void endGet(long start, int numInc) {
    if (clock.isEnabled()) {
      final long delta = clock.getTime() - start;
      this.stats.incLong(getTimeId, delta);
    }
    this.stats.incLong(getsCompletedId, numInc);
  }

  public void endDestroy(long start) {
    if (clock.isEnabled()) {
      this.stats.incLong(destroyTimeId, clock.getTime() - start);
    }
    this.stats.incLong(destroysCompletedId, 1);
  }

  public void endInvalidate(long start) {
    if (clock.isEnabled()) {
      this.stats.incLong(invalidateTimeId, clock.getTime() - start);
    }
    this.stats.incLong(invalidatesCompletedId, 1);
  }

  public void endContainsKey(long start, int numInc) {
    if (clock.isEnabled()) {
      this.stats.incLong(containsKeyTimeId, clock.getTime() - start);
    }
    this.stats.incLong(containsKeyCompletedId, numInc);
  }

  public void endContainsValueForKey(long start, int numInc) {
    if (clock.isEnabled()) {
      this.stats.incLong(containsValueForKeyTimeId, clock.getTime() - start);
    }
    this.stats.incLong(containsValueForKeyCompletedId, numInc);
  }

  public void incContainsKeyValueRetries() {
    this.stats.incLong(containsKeyRetriesId, 1);
  }

  public void incContainsKeyValueOpsRetried() {
    this.stats.incLong(containsKeyOpsRetriedId, 1);
  }

  public void incInvalidateRetries() {
    this.stats.incLong(invalidateRetriesId, 1);
  }

  public void incInvalidateOpsRetried() {
    this.stats.incLong(invalidateOpsRetriedId, 1);
  }

  public void incDestroyRetries() {
    this.stats.incLong(destroyRetriesId, 1);
  }

  public void incDestroyOpsRetried() {
    this.stats.incLong(destroyOpsRetriedId, 1);
  }

  public void incPutRetries() {
    this.stats.incLong(putRetriesId, 1);
  }

  public void incPutOpsRetried() {
    this.stats.incLong(putOpsRetriedId, 1);
  }

  public void incGetOpsRetried() {
    this.stats.incLong(getOpsRetriedId, 1);
  }

  public void incGetRetries() {
    this.stats.incLong(getRetriesId, 1);
  }

  @VisibleForTesting
  public long getGetRetries() {
    return this.stats.getLong(getRetriesId);
  }

  public void incCreateOpsRetried() {
    this.stats.incLong(createOpsRetriedId, 1);
  }

  public void incCreateRetries() {
    this.stats.incLong(createRetriesId, 1);
  }

  // ------------------------------------------------------------------------
  // preferred read stats
  // ------------------------------------------------------------------------

  public void incPreferredReadLocal() {
    this.stats.incLong(preferredReadLocalId, 1);
  }

  public void incPreferredReadRemote() {
    this.stats.incLong(preferredReadRemoteId, 1);
  }

  // ------------------------------------------------------------------------
  // messaging stats
  // ------------------------------------------------------------------------

  public long startPartitionMessageProcessing() {
    this.stats.incLong(partitionMessagesReceivedId, 1);
    return getTime();
  }

  public void endPartitionMessagesProcessing(long start) {
    if (clock.isEnabled()) {
      long delta = clock.getTime() - start;
      this.stats.incLong(partitionMessagesProcessingTimeId, delta);
    }
    this.stats.incLong(partitionMessagesProcessedId, 1);
  }

  public void incPartitionMessagesSent() {
    this.stats.incLong(partitionMessagesSentId, 1);
  }

  // ------------------------------------------------------------------------
  // datastore stats
  // ------------------------------------------------------------------------

  public void incBucketCount(int delta) {
    this.stats.incLong(bucketCountId, delta);
  }

  public void setBucketCount(int i) {
    this.stats.setLong(bucketCountId, i);
  }

  public void incDataStoreEntryCount(int amt) {
    this.stats.incLong(dataStoreEntryCountId, amt);
  }

  public long getDataStoreEntryCount() {
    return this.stats.getLong(dataStoreEntryCountId);
  }

  public void incBytesInUse(long delta) {
    this.stats.incLong(dataStoreBytesInUseId, delta);
  }

  public long getDataStoreBytesInUse() {
    return this.stats.getLong(dataStoreBytesInUseId);
  }

  public long getTotalBucketCount() {
    return this.stats.getLong(bucketCountId);
  }

  public void incPutAllRetries() {
    this.stats.incLong(fieldId_PUTALL_RETRIES, 1);
  }

  public void incPutAllMsgsRetried() {
    this.stats.incLong(fieldId_PUTALL_MSGS_RETRIED, 1);
  }

  public void incRemoveAllRetries() {
    this.stats.incLong(fieldId_REMOVE_ALL_RETRIES, 1);
  }

  public void incRemoveAllMsgsRetried() {
    this.stats.incLong(fieldId_REMOVE_ALL_MSGS_RETRIED, 1);
  }

  // ------------------------------------------------------------------------
  // stats for volunteering/discovering/becoming primary
  // ------------------------------------------------------------------------

  public long getVolunteeringInProgress() {
    return this.stats.getLong(volunteeringInProgressId);
  }

  public long getVolunteeringBecamePrimary() {
    return this.stats.getLong(volunteeringBecamePrimaryId);
  }

  public long getVolunteeringBecamePrimaryTime() {
    return this.stats.getLong(volunteeringBecamePrimaryTimeId);
  }

  public long getVolunteeringOtherPrimary() {
    return this.stats.getLong(volunteeringOtherPrimaryId);
  }

  public long getVolunteeringOtherPrimaryTime() {
    return this.stats.getLong(volunteeringOtherPrimaryTimeId);
  }

  public long getVolunteeringClosed() {
    return this.stats.getLong(volunteeringClosedId);
  }

  public long getVolunteeringClosedTime() {
    return this.stats.getLong(volunteeringClosedTimeId);
  }

  public long startVolunteering() {
    this.stats.incLong(volunteeringInProgressId, 1);
    return clock.getTime();
  }

  public void endVolunteeringBecamePrimary(long start) {
    long ts = clock.getTime();
    this.stats.incLong(volunteeringInProgressId, -1);
    this.stats.incLong(volunteeringBecamePrimaryId, 1);
    if (clock.isEnabled()) {
      long time = ts - start;
      this.stats.incLong(volunteeringBecamePrimaryTimeId, time);
    }
  }

  public void endVolunteeringOtherPrimary(long start) {
    long ts = clock.getTime();
    this.stats.incLong(volunteeringInProgressId, -1);
    this.stats.incLong(volunteeringOtherPrimaryId, 1);
    if (clock.isEnabled()) {
      long time = ts - start;
      this.stats.incLong(volunteeringOtherPrimaryTimeId, time);
    }
  }

  public void endVolunteeringClosed(long start) {
    long ts = clock.getTime();
    this.stats.incLong(volunteeringInProgressId, -1);
    this.stats.incLong(volunteeringClosedId, 1);
    if (clock.isEnabled()) {
      long time = ts - start;
      this.stats.incLong(volunteeringClosedTimeId, time);
    }
  }

  public long getTotalNumBuckets() {
    return this.stats.getLong(totalNumBucketsId);
  }

  public void incTotalNumBuckets(int val) {
    this.stats.incLong(totalNumBucketsId, val);
  }

  public long getPrimaryBucketCount() {
    return this.stats.getLong(primaryBucketCountId);
  }

  public void incPrimaryBucketCount(int val) {
    this.stats.incLong(primaryBucketCountId, val);
  }

  public long getVolunteeringThreads() {
    return this.stats.getLong(volunteeringThreadsId);
  }

  public void incVolunteeringThreads(int val) {
    this.stats.incLong(volunteeringThreadsId, val);
  }

  public long getLowRedundancyBucketCount() {
    return this.stats.getLong(lowRedundancyBucketCountId);
  }

  public long getNoCopiesBucketCount() {
    return this.stats.getLong(noCopiesBucketCountId);
  }

  public void incLowRedundancyBucketCount(int val) {
    this.stats.incLong(lowRedundancyBucketCountId, val);
  }

  public void incNoCopiesBucketCount(int val) {
    this.stats.incLong(noCopiesBucketCountId, val);
  }

  public long getConfiguredRedundantCopies() {
    return this.stats.getLong(configuredRedundantCopiesId);
  }

  public void setConfiguredRedundantCopies(int val) {
    this.stats.setLong(configuredRedundantCopiesId, val);
  }

  public void setLocalMaxMemory(long l) {
    this.stats.setLong(localMaxMemoryId, l);
  }

  public void setActualRedundantCopies(int val) {
    this.stats.setLong(actualRedundantCopiesId, val);
  }

  // ------------------------------------------------------------------------
  // startTimeMap methods
  // ------------------------------------------------------------------------

  /** Put stat start time in holding map for later removal and use by caller */
  public void putStartTime(Object key, long startTime) {
    if (clock.isEnabled()) {
      this.startTimeMap.put(key, startTime);
    }
  }

  /** Remove stat start time from holding map to complete a clock stat */
  public long removeStartTime(Object key) {
    Long startTime = (Long) this.startTimeMap.remove(key);
    return startTime == null ? 0 : startTime;
  }

  /**
   * Statistic to track the {@link Region#getEntry(Object)} call
   *
   * @param startTime the time the getEntry operation started
   */
  public void endGetEntry(long startTime) {
    endGetEntry(startTime, 1);
  }

  /**
   * This method sets the end time for update and updates the counters
   *
   */
  public void endGetEntry(long start, int numInc) {
    if (clock.isEnabled()) {
      this.stats.incLong(getEntryTimeId, clock.getTime() - start);
    }
    this.stats.incLong(getEntriesCompletedId, numInc);
  }

  // ------------------------------------------------------------------------
  // bucket creation, primary transfer stats (see also rebalancing stats below)
  // ------------------------------------------------------------------------
  public long startRecovery() {
    this.stats.incLong(recoveriesInProgressId, 1);
    return clock.getTime();
  }

  public void endRecovery(long start) {
    long ts = clock.getTime();
    this.stats.incLong(recoveriesInProgressId, -1);
    if (clock.isEnabled()) {
      this.stats.incLong(recoveriesTimeId, ts - start);
    }
    this.stats.incLong(recoveriesCompletedId, 1);
  }

  public long getRecoveriesInProgress() {
    return this.stats.getLong(recoveriesInProgressId);
  }

  public long startBucketCreate(boolean isRebalance) {
    this.stats.incLong(bucketCreatesInProgressId, 1);
    if (isRebalance) {
      startRebalanceBucketCreate();
    }
    return clock.getTime();
  }

  public void endBucketCreate(long start, boolean success, boolean isRebalance) {
    long ts = clock.getTime();
    this.stats.incLong(bucketCreatesInProgressId, -1);
    if (clock.isEnabled()) {
      this.stats.incLong(bucketCreateTimeId, ts - start);
    }
    if (success) {
      this.stats.incLong(bucketCreatesCompletedId, 1);
    } else {
      this.stats.incLong(bucketCreatesFailedId, 1);
    }
    if (isRebalance) {
      endRebalanceBucketCreate(start, ts, success);
    }
  }

  public long startPrimaryTransfer(boolean isRebalance) {
    this.stats.incLong(primaryTransfersInProgressId, 1);
    if (isRebalance) {
      startRebalancePrimaryTransfer();
    }
    return clock.getTime();
  }

  public void endPrimaryTransfer(long start, boolean success, boolean isRebalance) {
    long ts = clock.getTime();
    this.stats.incLong(primaryTransfersInProgressId, -1);
    if (clock.isEnabled()) {
      this.stats.incLong(primaryTransferTimeId, ts - start);
    }
    if (success) {
      this.stats.incLong(primaryTransfersCompletedId, 1);
    } else {
      this.stats.incLong(primaryTransfersFailedId, 1);
    }
    if (isRebalance) {
      endRebalancePrimaryTransfer(start, ts, success);
    }
  }

  public long getBucketCreatesInProgress() {
    return this.stats.getLong(bucketCreatesInProgressId);
  }

  public long getBucketCreatesCompleted() {
    return this.stats.getLong(bucketCreatesCompletedId);
  }

  public long getBucketCreatesFailed() {
    return this.stats.getLong(bucketCreatesFailedId);
  }

  public long getBucketCreateTime() {
    return this.stats.getLong(bucketCreateTimeId);
  }

  public long getPrimaryTransfersInProgress() {
    return this.stats.getLong(primaryTransfersInProgressId);
  }

  public long getPrimaryTransfersCompleted() {
    return this.stats.getLong(primaryTransfersCompletedId);
  }

  public long getPrimaryTransfersFailed() {
    return this.stats.getLong(primaryTransfersFailedId);
  }

  public long getPrimaryTransferTime() {
    return this.stats.getLong(primaryTransferTimeId);
  }

  // ------------------------------------------------------------------------
  // rebalancing stats
  // ------------------------------------------------------------------------

  private void startRebalanceBucketCreate() {
    this.stats.incLong(rebalanceBucketCreatesInProgressId, 1);
  }

  private void endRebalanceBucketCreate(long start, long end, boolean success) {
    this.stats.incLong(rebalanceBucketCreatesInProgressId, -1);
    if (clock.isEnabled()) {
      this.stats.incLong(rebalanceBucketCreateTimeId, end - start);
    }
    if (success) {
      this.stats.incLong(rebalanceBucketCreatesCompletedId, 1);
    } else {
      this.stats.incLong(rebalanceBucketCreatesFailedId, 1);
    }
  }

  private void startRebalancePrimaryTransfer() {
    this.stats.incLong(rebalancePrimaryTransfersInProgressId, 1);
  }

  private void endRebalancePrimaryTransfer(long start, long end, boolean success) {
    this.stats.incLong(rebalancePrimaryTransfersInProgressId, -1);
    if (clock.isEnabled()) {
      this.stats.incLong(rebalancePrimaryTransferTimeId, end - start);
    }
    if (success) {
      this.stats.incLong(rebalancePrimaryTransfersCompletedId, 1);
    } else {
      this.stats.incLong(rebalancePrimaryTransfersFailedId, 1);
    }
  }

  public long getRebalanceBucketCreatesInProgress() {
    return this.stats.getLong(rebalanceBucketCreatesInProgressId);
  }

  public long getRebalanceBucketCreatesCompleted() {
    return this.stats.getLong(rebalanceBucketCreatesCompletedId);
  }

  public long getRebalanceBucketCreatesFailed() {
    return this.stats.getLong(rebalanceBucketCreatesFailedId);
  }

  public long getRebalanceBucketCreateTime() {
    return this.stats.getLong(rebalanceBucketCreateTimeId);
  }

  public long getRebalancePrimaryTransfersInProgress() {
    return this.stats.getLong(rebalancePrimaryTransfersInProgressId);
  }

  public long getRebalancePrimaryTransfersCompleted() {
    return this.stats.getLong(rebalancePrimaryTransfersCompletedId);
  }

  public long getRebalancePrimaryTransfersFailed() {
    return this.stats.getLong(rebalancePrimaryTransfersFailedId);
  }

  public long getRebalancePrimaryTransferTime() {
    return this.stats.getLong(rebalancePrimaryTransferTimeId);
  }

  public long startApplyReplication() {
    stats.incLong(applyReplicationInProgressId, 1);
    return clock.getTime();
  }

  public void endApplyReplication(long start) {
    long delta = clock.getTime() - start;
    stats.incLong(applyReplicationInProgressId, -1);
    stats.incLong(applyReplicationCompletedId, 1);
    stats.incLong(applyReplicationTimeId, delta);
  }

  public long startSendReplication() {
    stats.incLong(sendReplicationInProgressId, 1);
    return clock.getTime();
  }

  public void endSendReplication(long start) {
    long delta = clock.getTime() - start;
    stats.incLong(sendReplicationInProgressId, -1);
    stats.incLong(sendReplicationCompletedId, 1);
    stats.incLong(sendReplicationTimeId, delta);
  }

  public long startPutRemote() {
    stats.incLong(putRemoteInProgressId, 1);
    return clock.getTime();
  }

  public void endPutRemote(long start) {
    long delta = clock.getTime() - start;
    stats.incLong(putRemoteInProgressId, -1);
    stats.incLong(putRemoteCompletedId, 1);
    stats.incLong(putRemoteTimeId, delta);
  }

  public long startPutLocal() {
    stats.incLong(putLocalInProgressId, 1);
    return clock.getTime();
  }

  public void endPutLocal(long start) {
    long delta = clock.getTime() - start;
    stats.incLong(putLocalInProgressId, -1);
    stats.incLong(putLocalCompletedId, 1);
    stats.incLong(putLocalTimeId, delta);
  }

  public void incPRMetaDataSentCount() {
    this.stats.incLong(prMetaDataSentCountId, 1);
  }

  public long getPRMetaDataSentCount() {
    return this.stats.getLong(prMetaDataSentCountId);
  }
}
