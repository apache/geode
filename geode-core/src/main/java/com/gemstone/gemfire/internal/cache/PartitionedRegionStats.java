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

package com.gemstone.gemfire.internal.cache;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.StatisticsTypeFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.StatisticsTypeFactoryImpl;

/**
 * Represents a statistics type that can be archived to vsd. Loading of this
 * class automatically triggers statistics archival.
 * <p>
 * 
 * A singleton instance can be requested with the initSingleton(...) and
 * getSingleton() methods.
 * <p>
 * 
 * Individual instances can be created with the constructor.
 * <p>
 * 
 * To manipulate the statistic values, use (inc|dec|set|get)&lt;fieldName&gt;
 * methods.
 * 
 * @since GemFire 5.0
 */
public class PartitionedRegionStats {

  private static final StatisticsType type;
  
  private final static int dataStoreEntryCountId;
  private final static int dataStoreBytesInUseId;
  private final static int bucketCountId;

  private final static int putsCompletedId;
  private final static int putOpsRetriedId;
  private final static int putRetriesId;

  private final static int createsCompletedId;
  private final static int createOpsRetriedId;
  private final static int createRetriesId;

  private final static int preferredReadLocalId;
  private final static int preferredReadRemoteId;

  private final static int getsCompletedId;
  private final static int getOpsRetriedId;
  private final static int getRetriesId;

  private final static int destroysCompletedId;
  private final static int destroyOpsRetriedId;
  private final static int destroyRetriesId;

  private final static int invalidatesCompletedId;
  private final static int invalidateOpsRetriedId;
  private final static int invalidateRetriesId;

  private final static int containsKeyCompletedId;
  private final static int containsKeyOpsRetriedId;
  private final static int containsKeyRetriesId;

  private final static int containsValueForKeyCompletedId;

  private final static int partitionMessagesSentId;
  private final static int partitionMessagesReceivedId;
  private final static int partitionMessagesProcessedId;

  private final static int putTimeId;
  private final static int createTimeId;
  private final static int getTimeId;
  private final static int destroyTimeId;
  private final static int invalidateTimeId;
  private final static int containsKeyTimeId;
  private final static int containsValueForKeyTimeId;
  private final static int partitionMessagesProcessingTimeId;

  private final static String PUTALLS_COMPLETED = "putAllsCompleted";
  private final static String PUTALL_MSGS_RETRIED = "putAllMsgsRetried";
  private final static String PUTALL_RETRIES = "putAllRetries";
  private final static String PUTALL_TIME = "putAllTime";
  
  private final static int fieldId_PUTALLS_COMPLETED;
  private final static int fieldId_PUTALL_MSGS_RETRIED;
  private final static int fieldId_PUTALL_RETRIES;
  private final static int fieldId_PUTALL_TIME;
  
  private final static String REMOVE_ALLS_COMPLETED = "removeAllsCompleted";
  private final static String REMOVE_ALL_MSGS_RETRIED = "removeAllMsgsRetried";
  private final static String REMOVE_ALL_RETRIES = "removeAllRetries";
  private final static String REMOVE_ALL_TIME = "removeAllTime";
  
  private final static int fieldId_REMOVE_ALLS_COMPLETED;
  private final static int fieldId_REMOVE_ALL_MSGS_RETRIED;
  private final static int fieldId_REMOVE_ALL_RETRIES;
  private final static int fieldId_REMOVE_ALL_TIME;
  
  private final static int volunteeringInProgressId; // count of volunteering in progress
  private final static int volunteeringBecamePrimaryId; // ended as primary
  private final static int volunteeringBecamePrimaryTimeId; // time spent that ended as primary
  private final static int volunteeringOtherPrimaryId; // ended as not primary
  private final static int volunteeringOtherPrimaryTimeId; // time spent that ended as not primary
  private final static int volunteeringClosedId; // ended as closed
  private final static int volunteeringClosedTimeId; // time spent that ended as closed

  private final static int applyReplicationCompletedId;
  private final static int applyReplicationInProgressId;
  private final static int applyReplicationTimeId;
  private final static int sendReplicationCompletedId;
  private final static int sendReplicationInProgressId;
  private final static int sendReplicationTimeId;
  private final static int putRemoteCompletedId;
  private final static int putRemoteInProgressId;
  private final static int putRemoteTimeId;
  private final static int putLocalCompletedId;
  private final static int putLocalInProgressId;
  private final static int putLocalTimeId;

  private final static int totalNumBucketsId; // total number of buckets  
  private final static int primaryBucketCountId; // number of hosted primary buckets
  private final static int volunteeringThreadsId; // number of threads actively volunteering
  private final static int lowRedundancyBucketCountId; // number of buckets currently without full redundancy
  
  private final static int configuredRedundantCopiesId;
  private final static int actualRedundantCopiesId;
  
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
    final boolean largerIsBetter = true;
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();
    type = f.createType(
      "PartitionedRegionStats", 
      "Statistics for operations and connections in the Partitioned Region",
      new StatisticDescriptor[] {

        f.createIntGauge(
            "bucketCount", 
            "Number of buckets in this node.",
            "buckets"),
        f.createIntCounter(
            "putsCompleted", 
            "Number of puts completed.",
            "operations", 
            largerIsBetter),         
        f.createIntCounter(
            "putOpsRetried", 
            "Number of put operations which had to be retried due to failures.",
            "operations", 
            false),          
        f.createIntCounter(
            "putRetries", 
            "Total number of times put operations had to be retried.",
            "retry attempts", 
            false),
        f.createIntCounter(
            "createsCompleted", 
            "Number of creates completed.",
            "operations", 
            largerIsBetter),
        f.createIntCounter(
            "createOpsRetried", 
            "Number of create operations which had to be retried due to failures.",
            "operations", 
            false),
        f.createIntCounter(
            "createRetries", 
            "Total number of times put operations had to be retried.",
            "retry attempts", 
            false),
        f.createIntCounter(
            "preferredReadLocal", 
            "Number of reads satisfied from local store",
            "operations", 
            largerIsBetter),
        f.createIntCounter(PUTALLS_COMPLETED, "Number of putAlls completed.",
            "operations", largerIsBetter),         
        f.createIntCounter(PUTALL_MSGS_RETRIED, "Number of putAll messages which had to be retried due to failures.",
            "operations", false),          
        f.createIntCounter(PUTALL_RETRIES, "Total number of times putAll messages had to be retried.",
            "retry attempts", false),
        f.createLongCounter(PUTALL_TIME, "Total time spent doing putAlls.",
            "nanoseconds", !largerIsBetter),
        f.createIntCounter(REMOVE_ALLS_COMPLETED, "Number of removeAlls completed.",
            "operations", largerIsBetter),         
        f.createIntCounter(REMOVE_ALL_MSGS_RETRIED, "Number of removeAll messages which had to be retried due to failures.",
            "operations", false),          
        f.createIntCounter(REMOVE_ALL_RETRIES, "Total number of times removeAll messages had to be retried.",
            "retry attempts", false),
        f.createLongCounter(REMOVE_ALL_TIME, "Total time spent doing removeAlls.",
            "nanoseconds", !largerIsBetter),
       f.createIntCounter(
            "preferredReadRemote", 
            "Number of reads satisfied from remote store",
            "operations", 
            false),
        f.createIntCounter(
            "getsCompleted", 
            "Number of gets completed.",
            "operations", 
            largerIsBetter),
        f.createIntCounter(
            "getOpsRetried", 
            "Number of get operations which had to be retried due to failures.",
            "operations", 
            false),
        f.createIntCounter(
            "getRetries", 
            "Total number of times get operations had to be retried.",
            "retry attempts", 
            false),
        f.createIntCounter(
            "destroysCompleted",
            "Number of destroys completed.", 
            "operations", 
            largerIsBetter),
        f.createIntCounter(
            "destroyOpsRetried",
            "Number of destroy operations which had to be retried due to failures.", 
            "operations", 
            false),
        f.createIntCounter(
            "destroyRetries",
            "Total number of times destroy operations had to be retried.", 
            "retry attempts", 
            false),
        f.createIntCounter(
            "invalidatesCompleted",
            "Number of invalidates completed.", 
            "operations", 
            largerIsBetter),

        f.createIntCounter(
            "invalidateOpsRetried",
            "Number of invalidate operations which had to be retried due to failures.", 
            "operations", 
            false),
        f.createIntCounter(
            "invalidateRetries",
            "Total number of times invalidate operations had to be retried.", 
            "retry attempts", 
            false),
        f.createIntCounter(
            "containsKeyCompleted",
            "Number of containsKeys completed.", 
            "operations", 
            largerIsBetter),

        f.createIntCounter(
            "containsKeyOpsRetried",
            "Number of containsKey or containsValueForKey operations which had to be retried due to failures.", 
            "operations", 
            false),
        f.createIntCounter(
            "containsKeyRetries",
            "Total number of times containsKey or containsValueForKey operations had to be retried.", 
            "operations", 
            false),
        f.createIntCounter(
            "containsValueForKeyCompleted",
            "Number of containsValueForKeys completed.", 
            "operations",
            largerIsBetter),
        f.createIntCounter(
            "PartitionMessagesSent",
            "Number of PartitionMessages Sent.", 
            "operations",
            largerIsBetter),
        f.createIntCounter(
            "PartitionMessagesReceived",
            "Number of PartitionMessages Received.", 
            "operations",
            largerIsBetter),
        f.createIntCounter(
            "PartitionMessagesProcessed",
            "Number of PartitionMessages Processed.", 
            "operations",
            largerIsBetter),
        f.createLongCounter(
            "putTime", 
            "Total time spent doing puts.",
            "nanoseconds", 
            false),
        f.createLongCounter(
            "createTime",
            "Total time spent doing create operations.", 
            "nanoseconds",
            false),
        f.createLongCounter(
            "getTime",
            "Total time spent performing get operations.", 
            "nanoseconds",
            false),
        f.createLongCounter(
            "destroyTime", 
            "Total time spent doing destroys.",
            "nanoseconds", 
            false),
        f.createLongCounter(
            "invalidateTime",
            "Total time spent doing invalidates.", 
            "nanoseconds",
            false),
        f.createLongCounter(
            "containsKeyTime",
            "Total time spent performing containsKey operations.",
            "nanoseconds", 
            false),
        f.createLongCounter(
            "containsValueForKeyTime",
            "Total time spent performing containsValueForKey operations.",
            "nanoseconds", 
            false),
        f.createLongCounter(
            "partitionMessagesProcessingTime",
            "Total time spent on PartitionMessages processing.",
            "nanoseconds", 
            false),
        f.createIntGauge(
            "dataStoreEntryCount", 
            "The number of entries stored in this Cache for the named Partitioned Region. This does not include entries which are tombstones. See CachePerfStats.tombstoneCount.",
            "entries"),
        f.createLongGauge(
            "dataStoreBytesInUse", 
            "The current number of bytes stored in this Cache for the named Partitioned Region",
            "bytes"),
        f.createIntGauge(
            "volunteeringInProgress", 
            "Current number of attempts to volunteer for primary of a bucket.", 
            "operations"), 
        f.createIntCounter(
            "volunteeringBecamePrimary", 
            "Total number of attempts to volunteer that ended when this member became primary.", 
            "operations"), 
        f.createLongCounter(
            "volunteeringBecamePrimaryTime", 
            "Total time spent volunteering that ended when this member became primary.", 
            "nanoseconds", 
            false),
        f.createIntCounter(
            "volunteeringOtherPrimary", 
            "Total number of attempts to volunteer that ended when this member discovered other primary.", 
            "operations"), 
        f.createLongCounter(
            "volunteeringOtherPrimaryTime", 
            "Total time spent volunteering that ended when this member discovered other primary.", 
            "nanoseconds", 
            false),
        f.createIntCounter(
            "volunteeringClosed", 
            "Total number of attempts to volunteer that ended when this member's bucket closed.", 
            "operations"), 
        f.createLongCounter(
            "volunteeringClosedTime", 
            "Total time spent volunteering that ended when this member's bucket closed.", 
            "nanoseconds", 
            false),
        f.createIntGauge(
            "totalNumBuckets", 
            "The total number of buckets.", 
            "buckets"), 
        f.createIntGauge(
            "primaryBucketCount", 
            "Current number of primary buckets hosted locally.", 
            "buckets"), 
        f.createIntGauge(
            "volunteeringThreads", 
            "Current number of threads volunteering for primary.", 
            "threads"),
        f.createIntGauge(
            "lowRedundancyBucketCount", 
            "Current number of buckets without full redundancy.", 
            "buckets"),
        f.createIntGauge(
            "configuredRedundantCopies", 
            "Configured number of redundant copies for this partitioned region.", 
            "copies"),
        f.createIntGauge(
            "actualRedundantCopies", 
            "Actual number of redundant copies for this partitioned region.", 
            "copies"),
        f.createIntCounter(
            "getEntryCompleted", 
            "Number of getEntry operations completed.",
            "operations", 
            largerIsBetter),
        f.createLongCounter(
            "getEntryTime",
            "Total time spent performing getEntry operations.", 
            "nanoseconds", 
            false),
            
        f.createIntGauge(
            "recoveriesInProgress",
            "Current number of redundancy recovery operations in progress for this region.",
            "operations"),
        f.createIntCounter(
            "recoveriesCompleted",
            "Total number of redundancy recovery operations performed on this region.",
            "operations"),
        f.createLongCounter(
            "recoveryTime",
            "Total number time spent recovering redundancy.",
            "operations"),
        f.createIntGauge(
            "bucketCreatesInProgress",
            "Current number of bucket create operations being performed for rebalancing.",
            "operations"),
        f.createIntCounter(
            "bucketCreatesCompleted",
            "Total number of bucket create operations performed for rebalancing.",
            "operations"),
        f.createIntCounter(
            "bucketCreatesFailed",
            "Total number of bucket create operations performed for rebalancing that failed.",
            "operations"),
        f.createLongCounter(
            "bucketCreateTime",
            "Total time spent performing bucket create operations for rebalancing.",
            "nanoseconds", 
            false),
        f.createIntGauge(
            "primaryTransfersInProgress",
            "Current number of primary transfer operations being performed for rebalancing.",
            "operations"),
        f.createIntCounter(
            "primaryTransfersCompleted",
            "Total number of primary transfer operations performed for rebalancing.",
            "operations"),
        f.createIntCounter(
            "primaryTransfersFailed",
            "Total number of primary transfer operations performed for rebalancing that failed.",
            "operations"),
        f.createLongCounter(
            "primaryTransferTime",
            "Total time spent performing primary transfer operations for rebalancing.",
            "nanoseconds", false),

        f.createIntCounter("applyReplicationCompleted",
            "Total number of replicated values sent from a primary to this redundant data store.",
            "operations", largerIsBetter),
        f.createIntGauge("applyReplicationInProgress",
            "Current number of replication operations in progress on this redundant data store.",
            "operations", !largerIsBetter),
        f.createLongCounter("applyReplicationTime",
            "Total time spent storing replicated values on this redundant data store.",
            "nanoseconds", !largerIsBetter),
        f.createIntCounter("sendReplicationCompleted",
            "Total number of replicated values sent from this primary to a redundant data store.",
            "operations", largerIsBetter),
        f.createIntGauge("sendReplicationInProgress",
            "Current number of replication operations in progress from this primary.",
            "operations", !largerIsBetter),
        f.createLongCounter("sendReplicationTime",
            "Total time spent replicating values from this primary to a redundant data store.",
            "nanoseconds", !largerIsBetter),
        f.createIntCounter("putRemoteCompleted",
            "Total number of completed puts that did not originate in the primary. These puts require an extra network hop to the primary.",
            "operations", largerIsBetter),
        f.createIntGauge("putRemoteInProgress",
            "Current number of puts in progress that did not originate in the primary.",
            "operations", !largerIsBetter),
        f.createLongCounter("putRemoteTime",
            "Total time spent doing puts that did not originate in the primary.",
            "nanoseconds", !largerIsBetter),
        f.createIntCounter("putLocalCompleted",
            "Total number of completed puts that did originate in the primary. These puts are optimal.",
            "operations", largerIsBetter),
        f.createIntGauge("putLocalInProgress",
            "Current number of puts in progress that did originate in the primary.",
            "operations", !largerIsBetter),
        f.createLongCounter("putLocalTime",
            "Total time spent doing puts that did originate in the primary.",
            "nanoseconds", !largerIsBetter),
            
        f.createIntGauge(
            "rebalanceBucketCreatesInProgress",
            "Current number of bucket create operations being performed for rebalancing.",
            "operations"),
        f.createIntCounter(
            "rebalanceBucketCreatesCompleted",
            "Total number of bucket create operations performed for rebalancing.",
            "operations"),
        f.createIntCounter(
            "rebalanceBucketCreatesFailed",
            "Total number of bucket create operations performed for rebalancing that failed.",
            "operations"),
        f.createLongCounter(
            "rebalanceBucketCreateTime",
            "Total time spent performing bucket create operations for rebalancing.",
            "nanoseconds", 
            false),
        f.createIntGauge(
            "rebalancePrimaryTransfersInProgress",
            "Current number of primary transfer operations being performed for rebalancing.",
            "operations"),
        f.createIntCounter(
            "rebalancePrimaryTransfersCompleted",
            "Total number of primary transfer operations performed for rebalancing.",
            "operations"),
        f.createIntCounter(
            "rebalancePrimaryTransfersFailed",
            "Total number of primary transfer operations performed for rebalancing that failed.",
            "operations"),
        f.createLongCounter(
            "rebalancePrimaryTransferTime",
            "Total time spent performing primary transfer operations for rebalancing.",
            "nanoseconds", false),
        f.createLongCounter(
                "prMetaDataSentCount",
                "total number of times meta data refreshed sent on client's request.",
                "operation", false),    
                
        f.createLongGauge("localMaxMemory", 
            "local max memory in bytes for this region on this member", 
            "bytes")
            
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
    containsKeyOpsRetriedId= type.nameToId("containsKeyOpsRetried");
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

  /** 
   * Utility map for temporarily holding stat start times.
   * <p>
   * This was originally added to avoid having to add a long 
   * volunteeringStarted variable to every instance of BucketAdvisor. Majority
   * of BucketAdvisors never volunteer and an instance of BucketAdvisor exists
   * for every bucket defined in a PartitionedRegion which could result in a
   * lot of unused longs. Volunteering is a rare event and thus the performance
   * implications of a HashMap lookup is small and preferrable to so many
   * longs. Key: BucketAdvisor, Value: Long
   */
  private final Map startTimeMap;

  public static long startTime() {
    return CachePerfStats.getStatTime();
  }
  public static long getStatTime() {
    return CachePerfStats.getStatTime();
  }

  public PartitionedRegionStats(StatisticsFactory factory, String name) {
    this.stats = factory.createAtomicStatistics(
type, name /* fixes bug 42343 */);
    
    if (CachePerfStats.enableClockStats) {
      this.startTimeMap = new ConcurrentHashMap();
    }
    else {
      this.startTimeMap = Collections.EMPTY_MAP;
    }
  }
  
  public void close() {
    this.stats.close();
  }

  public Statistics getStats() {
    return this.stats;
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
   * @param start
   */
  public void endPutAll(long start)
  {
    endPutAll(start, 1);
  }
  public void endRemoveAll(long start)
  {
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
    if (CachePerfStats.enableClockStats) {
      long delta = CachePerfStats.getStatTime() - start;
      this.stats.incLong(putTimeId, delta);
    }
    this.stats.incInt(putsCompletedId, numInc);
  }
  /**
   * This method sets the end time for putAll and updates the counters
   * 
   * @param start
   * @param numInc
   */
  public void endPutAll(long start, int numInc)
  {
    if (CachePerfStats.enableClockStats) {
      long delta = CachePerfStats.getStatTime() - start;
      this.stats.incLong(fieldId_PUTALL_TIME, delta);
//      this.putStatsHistogram.endOp(delta);
      
    }
    this.stats.incInt(fieldId_PUTALLS_COMPLETED, numInc);
  }
  public void endRemoveAll(long start, int numInc)
  {
    if (CachePerfStats.enableClockStats) {
      long delta = CachePerfStats.getStatTime() - start;
      this.stats.incLong(fieldId_REMOVE_ALL_TIME, delta);
    }
    this.stats.incInt(fieldId_REMOVE_ALLS_COMPLETED, numInc);
  }
  public void endCreate(long start, int numInc) {
    if (CachePerfStats.enableClockStats) {
      this.stats.incLong(createTimeId, CachePerfStats.getStatTime() - start);
    }
    this.stats.incInt(createsCompletedId, numInc);
  }
  public void endGet(long start, int numInc) {
    if (CachePerfStats.enableClockStats) {
      final long delta = CachePerfStats.getStatTime() - start; 
      this.stats.incLong(getTimeId, delta);
    }
    this.stats.incInt(getsCompletedId, numInc);
  }
  public void endDestroy(long start) {
    if (CachePerfStats.enableClockStats) {
      this.stats.incLong(destroyTimeId, CachePerfStats.getStatTime() - start);
    }
    this.stats.incInt(destroysCompletedId, 1);
  }
  public void endInvalidate(long start) {
    if (CachePerfStats.enableClockStats) {
      this.stats.incLong(invalidateTimeId, CachePerfStats.getStatTime() - start);
    }
    this.stats.incInt(invalidatesCompletedId, 1);
  }
  public void endContainsKey(long start, int numInc) {
    if (CachePerfStats.enableClockStats) {
      this.stats.incLong(containsKeyTimeId, CachePerfStats.getStatTime() - start);
    }
    this.stats.incInt(containsKeyCompletedId, numInc);
  }
  public void endContainsValueForKey(long start, int numInc) {
    if (CachePerfStats.enableClockStats) {
      this.stats.incLong(containsValueForKeyTimeId, CachePerfStats.getStatTime() - start);
    }
    this.stats.incInt(containsValueForKeyCompletedId, numInc);
  }
  public void incContainsKeyValueRetries() {
    this.stats.incInt(containsKeyRetriesId, 1); 
  }
  public void incContainsKeyValueOpsRetried() {
    this.stats.incInt(containsKeyOpsRetriedId, 1); 
  }
  public void incInvalidateRetries() {
    this.stats.incInt(invalidateRetriesId, 1);
  }
  public void incInvalidateOpsRetried() {
    this.stats.incInt(invalidateOpsRetriedId, 1);
  }
  public void incDestroyRetries() {
    this.stats.incInt(destroyRetriesId, 1);
  }
  public void incDestroyOpsRetried() {
    this.stats.incInt(destroyOpsRetriedId, 1);
  }
  public void incPutRetries() {
    this.stats.incInt(putRetriesId, 1);
  }
  public void incPutOpsRetried() {
    this.stats.incInt(putOpsRetriedId, 1);
  }
  public void incGetOpsRetried() {
    this.stats.incInt(getOpsRetriedId, 1);
  }
  public void incGetRetries() {
    this.stats.incInt(getRetriesId, 1);
  }
  public void incCreateOpsRetried() {
    this.stats.incInt(createOpsRetriedId, 1);
  }
  public void incCreateRetries() {
    this.stats.incInt(createRetriesId, 1);
  }

  // ------------------------------------------------------------------------
  // preferred read stats
  // ------------------------------------------------------------------------
  
  public void incPreferredReadLocal() {
    this.stats.incInt(preferredReadLocalId, 1);
  }
  public void incPreferredReadRemote() {
    this.stats.incInt(preferredReadRemoteId, 1);
  }
  
  // ------------------------------------------------------------------------
  // messaging stats
  // ------------------------------------------------------------------------
  
  public final long startPartitionMessageProcessing() {
    this.stats.incInt(partitionMessagesReceivedId, 1);
    return startTime();
  }
  public void endPartitionMessagesProcessing(long start) {
    if (CachePerfStats.enableClockStats) {
      long delta = CachePerfStats.getStatTime() - start;
      this.stats.incLong(partitionMessagesProcessingTimeId, delta);
    }
    this.stats.incInt(partitionMessagesProcessedId, 1);
  }
  public void incPartitionMessagesSent() {
    this.stats.incInt(partitionMessagesSentId, 1);
  }
  
  // ------------------------------------------------------------------------
  // datastore stats
  // ------------------------------------------------------------------------
  
  public void incBucketCount(int delta) {
    this.stats.incInt(bucketCountId, delta);
  }

  public void setBucketCount(int i) {
    this.stats.setInt(bucketCountId, i);
  }
  public void incDataStoreEntryCount(int amt) {
    this.stats.incInt(dataStoreEntryCountId, amt);
  }
  public int getDataStoreEntryCount() {
    return this.stats.getInt(dataStoreEntryCountId);
  }
  public void incBytesInUse(long delta) {
    this.stats.incLong(dataStoreBytesInUseId, delta);
  }
  public long getDataStoreBytesInUse() {
    return this.stats.getLong(dataStoreBytesInUseId);
  }
  public int getTotalBucketCount() {
    int bucketCount = this.stats.getInt(bucketCountId);
    return bucketCount;
  }

  public void incPutAllRetries()
  {
    this.stats.incInt(fieldId_PUTALL_RETRIES, 1);
  }

  public void incPutAllMsgsRetried()
  {
    this.stats.incInt(fieldId_PUTALL_MSGS_RETRIED, 1);
  }
  public void incRemoveAllRetries() {
    this.stats.incInt(fieldId_REMOVE_ALL_RETRIES, 1);
  }

  public void incRemoveAllMsgsRetried() {
    this.stats.incInt(fieldId_REMOVE_ALL_MSGS_RETRIED, 1);
  }
  
  // ------------------------------------------------------------------------
  // stats for volunteering/discovering/becoming primary
  // ------------------------------------------------------------------------
  
  public int getVolunteeringInProgress() {
    return this.stats.getInt(volunteeringInProgressId);
  }
  public int getVolunteeringBecamePrimary() {
    return this.stats.getInt(volunteeringBecamePrimaryId);
  }
  public long getVolunteeringBecamePrimaryTime() {
    return this.stats.getLong(volunteeringBecamePrimaryTimeId);
  }
  public int getVolunteeringOtherPrimary() {
    return this.stats.getInt(volunteeringOtherPrimaryId);
  }
  public long getVolunteeringOtherPrimaryTime() {
    return this.stats.getLong(volunteeringOtherPrimaryTimeId);
  }
  public int getVolunteeringClosed() {
    return this.stats.getInt(volunteeringClosedId);
  }
  public long getVolunteeringClosedTime() {
    return this.stats.getLong(volunteeringClosedTimeId);
  }

  public long startVolunteering() {
    this.stats.incInt(volunteeringInProgressId, 1);
    return CachePerfStats.getStatTime(); 
  }
  public void endVolunteeringBecamePrimary(long start) {
    long ts = CachePerfStats.getStatTime();
    this.stats.incInt(volunteeringInProgressId, -1);
    this.stats.incInt(volunteeringBecamePrimaryId, 1);
    if (CachePerfStats.enableClockStats) {
      long time = ts-start;
      this.stats.incLong(volunteeringBecamePrimaryTimeId, time);
    }
  }
  public void endVolunteeringOtherPrimary(long start) {
    long ts = CachePerfStats.getStatTime();
    this.stats.incInt(volunteeringInProgressId, -1);
    this.stats.incInt(volunteeringOtherPrimaryId, 1);
    if (CachePerfStats.enableClockStats) {
      long time = ts-start;
      this.stats.incLong(volunteeringOtherPrimaryTimeId, time);
    }
  }
  public void endVolunteeringClosed(long start) {
    long ts = CachePerfStats.getStatTime();
    this.stats.incInt(volunteeringInProgressId, -1);
    this.stats.incInt(volunteeringClosedId, 1);
    if (CachePerfStats.enableClockStats) {
      long time = ts-start;
      this.stats.incLong(volunteeringClosedTimeId, time);
    }
  }

  public int getTotalNumBuckets() {
    return this.stats.getInt(totalNumBucketsId);
  }
  public void incTotalNumBuckets(int val) {
    this.stats.incInt(totalNumBucketsId, val);
  }
  
  public int getPrimaryBucketCount() {
    return this.stats.getInt(primaryBucketCountId);
  }
  public void incPrimaryBucketCount(int val) {
    this.stats.incInt(primaryBucketCountId, val);
  }
  
  public int getVolunteeringThreads() {
    return this.stats.getInt(volunteeringThreadsId);
  }
  public void incVolunteeringThreads(int val) {
    this.stats.incInt(volunteeringThreadsId, val);
  }
  
  public int getLowRedundancyBucketCount() {
    return this.stats.getInt(lowRedundancyBucketCountId);
  }
  public void incLowRedundancyBucketCount(int val) {
    this.stats.incInt(lowRedundancyBucketCountId, val);
  }
  
  public int getConfiguredRedundantCopies() {
    return this.stats.getInt(configuredRedundantCopiesId);
  }
  public void setConfiguredRedundantCopies(int val) {
    this.stats.setInt(configuredRedundantCopiesId, val);
  }
  public void setLocalMaxMemory(long l) {
    this.stats.setLong(localMaxMemoryId, l);
  }
  public int getActualRedundantCopies() {
    return this.stats.getInt(actualRedundantCopiesId);
  }
  public void setActualRedundantCopies(int val) {
    this.stats.setInt(actualRedundantCopiesId, val);
  }
  
  // ------------------------------------------------------------------------
  // startTimeMap methods
  // ------------------------------------------------------------------------
  
  /** Put stat start time in holding map for later removal and use by caller */
  public void putStartTime(Object key, long startTime) {
    if (CachePerfStats.enableClockStats) {
      this.startTimeMap.put(key, Long.valueOf(startTime));
    }
  }
  /** Remove stat start time from holding map to complete a clock stat */
  public long removeStartTime(Object key) {
    Long startTime = (Long)this.startTimeMap.remove(key);
    return startTime == null ? 0 : startTime.longValue();
  }

  /**
   * Statistic to track the {@link  Region#getEntry(Object)} call
   * @param startTime the time the getEntry operation started
   */
  public void endGetEntry(long startTime) {
    endGetEntry(startTime, 1);
  }
  
  /**
   * This method sets the end time for update and updates the counters
   * 
   * @param start
   * @param numInc
   */
  public void endGetEntry(long start, int numInc) {
    if (CachePerfStats.enableClockStats) {
      this.stats.incLong(getEntryTimeId, CachePerfStats.getStatTime() - start);
    }
    this.stats.incInt(getEntriesCompletedId, numInc);
  }

  // ------------------------------------------------------------------------
  // bucket creation, primary transfer stats (see also rebalancing stats below)
  // ------------------------------------------------------------------------
  public long startRecovery() {
    this.stats.incInt(recoveriesInProgressId, 1);
    return PartitionedRegionStats.getStatTime();
  }
  
  public void endRecovery(long start) {
    long ts = PartitionedRegionStats.getStatTime();
    this.stats.incInt(recoveriesInProgressId, -1);
    if (CachePerfStats.enableClockStats) {
      this.stats.incLong(recoveriesTimeId, ts-start);
    }
    this.stats.incInt(recoveriesCompletedId, 1);
  }
  
  public long startBucketCreate(boolean isRebalance) {
    this.stats.incInt(bucketCreatesInProgressId, 1);
    if (isRebalance) {
      startRebalanceBucketCreate();
    }
    return PartitionedRegionStats.getStatTime();
  }
  public void endBucketCreate(long start, boolean success, boolean isRebalance) {
    long ts = PartitionedRegionStats.getStatTime();
    this.stats.incInt(bucketCreatesInProgressId, -1);
    if (CachePerfStats.enableClockStats) {
      this.stats.incLong(bucketCreateTimeId, ts-start);
    }
    if (success) {
      this.stats.incInt(bucketCreatesCompletedId, 1);
    } else {
      this.stats.incInt(bucketCreatesFailedId, 1);
    }
    if (isRebalance) {
      endRebalanceBucketCreate(start, ts, success);
    }
  }
  
  public long startPrimaryTransfer(boolean isRebalance) {
    this.stats.incInt(primaryTransfersInProgressId, 1);
    if (isRebalance) {
      startRebalancePrimaryTransfer();
    }
    return PartitionedRegionStats.getStatTime();
  }
  public void endPrimaryTransfer(long start, boolean success, boolean isRebalance) {
    long ts = PartitionedRegionStats.getStatTime();
    this.stats.incInt(primaryTransfersInProgressId, -1);
    if (CachePerfStats.enableClockStats) {
      this.stats.incLong(primaryTransferTimeId, ts-start);
    }
    if (success) {
      this.stats.incInt(primaryTransfersCompletedId, 1);
    } else {
      this.stats.incInt(primaryTransfersFailedId, 1);
    }
    if (isRebalance) {
      endRebalancePrimaryTransfer(start, ts, success);
    }
  }
  
  public int getBucketCreatesInProgress() {
    return this.stats.getInt(bucketCreatesInProgressId);
  }
  public int getBucketCreatesCompleted() {
    return this.stats.getInt(bucketCreatesCompletedId);
  }
  public int getBucketCreatesFailed() {
    return this.stats.getInt(bucketCreatesFailedId);
  }
  public long getBucketCreateTime() {
    return this.stats.getLong(bucketCreateTimeId);
  }
  public int getPrimaryTransfersInProgress() {
    return this.stats.getInt(primaryTransfersInProgressId);
  }
  public int getPrimaryTransfersCompleted() {
    return this.stats.getInt(primaryTransfersCompletedId);
  }
  public int getPrimaryTransfersFailed() {
    return this.stats.getInt(primaryTransfersFailedId);
  }
  public long getPrimaryTransferTime() {
    return this.stats.getLong(primaryTransferTimeId);
  }
  
  // ------------------------------------------------------------------------
  // rebalancing stats
  // ------------------------------------------------------------------------
  
  private void startRebalanceBucketCreate() {
    this.stats.incInt(rebalanceBucketCreatesInProgressId, 1);
  }
  private void endRebalanceBucketCreate(long start, long end, boolean success) {
    this.stats.incInt(rebalanceBucketCreatesInProgressId, -1);
    if (CachePerfStats.enableClockStats) {
      this.stats.incLong(rebalanceBucketCreateTimeId, end-start);
    }
    if (success) {
      this.stats.incInt(rebalanceBucketCreatesCompletedId, 1);
    } else {
      this.stats.incInt(rebalanceBucketCreatesFailedId, 1);
    }
  }
  
  private void startRebalancePrimaryTransfer() {
    this.stats.incInt(rebalancePrimaryTransfersInProgressId, 1);
  }
  private void endRebalancePrimaryTransfer(long start, long end, boolean success) {
    this.stats.incInt(rebalancePrimaryTransfersInProgressId, -1);
    if (CachePerfStats.enableClockStats) {
      this.stats.incLong(rebalancePrimaryTransferTimeId, end-start);
    }
    if (success) {
      this.stats.incInt(rebalancePrimaryTransfersCompletedId, 1);
    } else {
      this.stats.incInt(rebalancePrimaryTransfersFailedId, 1);
    }
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

  public long startApplyReplication() {
    stats.incInt(applyReplicationInProgressId, 1);
    return CachePerfStats.getStatTime();
  }
  public void endApplyReplication(long start) {
    long delta = CachePerfStats.getStatTime() - start;
    stats.incInt(applyReplicationInProgressId, -1);
    stats.incInt(applyReplicationCompletedId, 1);
    stats.incLong(applyReplicationTimeId, delta);
  }

  public long startSendReplication() {
    stats.incInt(sendReplicationInProgressId, 1);
    return CachePerfStats.getStatTime();
  }

  public void endSendReplication(long start) {
    long delta = CachePerfStats.getStatTime() - start;
    stats.incInt(sendReplicationInProgressId, -1);
    stats.incInt(sendReplicationCompletedId, 1);
    stats.incLong(sendReplicationTimeId, delta);
  }

  public long startPutRemote() {
    stats.incInt(putRemoteInProgressId, 1);
    return CachePerfStats.getStatTime();
  }

  public void endPutRemote(long start) {
    long delta = CachePerfStats.getStatTime() - start;
    stats.incInt(putRemoteInProgressId, -1);
    stats.incInt(putRemoteCompletedId, 1);
    stats.incLong(putRemoteTimeId, delta);
  }

  public long startPutLocal() {
    stats.incInt(putLocalInProgressId, 1);
    return CachePerfStats.getStatTime();
  }

  public void endPutLocal(long start) {
    long delta = CachePerfStats.getStatTime() - start;
    stats.incInt(putLocalInProgressId, -1);
    stats.incInt(putLocalCompletedId, 1);
    stats.incLong(putLocalTimeId, delta);
  }
  
  public void incPRMetaDataSentCount(){
    this.stats.incLong(prMetaDataSentCountId, 1);
  }
  
  public long getPRMetaDataSentCount(){
    return this.stats.getLong(prMetaDataSentCountId);
  }
}
