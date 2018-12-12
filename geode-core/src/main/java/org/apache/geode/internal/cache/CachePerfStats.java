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

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.distributed.internal.PoolStatHelper;
import org.apache.geode.distributed.internal.QueueStatHelper;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;

/**
 * CachePerfStats tracks statistics about GemFire cache performance.
 */
public class CachePerfStats {
  public static boolean enableClockStats = false;

  ////////////////// Static fields ///////////////////////////

  private static final StatisticsType type;

  protected static final int loadsInProgressId;
  protected static final int loadsCompletedId;
  protected static final int loadTimeId;
  protected static final int netloadsInProgressId;
  protected static final int netloadsCompletedId;
  protected static final int netloadTimeId;
  protected static final int netsearchesInProgressId;
  protected static final int netsearchesCompletedId;
  protected static final int netsearchTimeId;
  protected static final int cacheWriterCallsInProgressId;
  protected static final int cacheWriterCallsCompletedId;
  protected static final int cacheWriterCallTimeId;
  protected static final int cacheListenerCallsInProgressId;
  protected static final int cacheListenerCallsCompletedId;
  protected static final int cacheListenerCallTimeId;
  protected static final int getInitialImagesInProgressId;
  protected static final int getInitialImagesCompletedId;
  protected static final int deltaGetInitialImagesCompletedId;
  protected static final int getInitialImageTimeId;
  protected static final int getInitialImageKeysReceivedId;
  protected static final int regionsId;
  protected static final int partitionedRegionsId;
  protected static final int destroysId;
  protected static final int createsId;
  protected static final int putsId;
  protected static final int putTimeId;
  protected static final int putallsId;
  protected static final int putallTimeId;
  protected static final int removeAllsId;
  protected static final int removeAllTimeId;
  protected static final int updatesId;
  protected static final int updateTimeId;
  protected static final int invalidatesId;
  protected static final int getsId;
  protected static final int getTimeId;
  protected static final int eventQueueSizeId;
  protected static final int eventQueueThrottleTimeId;
  protected static final int eventQueueThrottleCountId;
  protected static final int eventThreadsId;
  protected static final int missesId;
  protected static final int queryExecutionsId;
  protected static final int queryExecutionTimeId;
  protected static final int queryResultsHashCollisionsId;
  protected static final int queryResultsHashCollisionProbeTimeId;
  protected static final int partitionedRegionQueryRetriesId;

  protected static final int txSuccessLifeTimeId;
  protected static final int txFailedLifeTimeId;
  protected static final int txRollbackLifeTimeId;
  protected static final int txCommitsId;
  protected static final int txFailuresId;
  protected static final int txRollbacksId;
  protected static final int txCommitTimeId;
  protected static final int txFailureTimeId;
  protected static final int txRollbackTimeId;
  protected static final int txCommitChangesId;
  protected static final int txFailureChangesId;
  protected static final int txRollbackChangesId;
  protected static final int txConflictCheckTimeId;

  protected static final int reliableQueuedOpsId;
  protected static final int reliableQueueSizeId;
  protected static final int reliableQueueMaxId;
  protected static final int reliableRegionsId;
  protected static final int reliableRegionsMissingId;
  protected static final int reliableRegionsQueuingId;
  protected static final int reliableRegionsMissingFullAccessId;
  protected static final int reliableRegionsMissingLimitedAccessId;
  protected static final int reliableRegionsMissingNoAccessId;
  protected static final int entryCountId;
  protected static final int eventsQueuedId;
  protected static final int retriesId;

  protected static final int diskTasksWaitingId;
  protected static final int evictorJobsStartedId;
  protected static final int evictorJobsCompletedId;
  protected static final int evictorQueueSizeId;

  protected static final int evictWorkTimeId;


  protected static final int indexUpdateInProgressId;
  protected static final int indexUpdateCompletedId;
  protected static final int indexUpdateTimeId;
  protected static final int clearsId;
  protected static final int indexInitializationInProgressId;
  protected static final int indexInitializationCompletedId;
  protected static final int indexInitializationTimeId;

  /** Id of the meta data refresh statistic */
  protected static final int metaDataRefreshCountId;

  protected static final int conflatedEventsId;
  protected static final int tombstoneCountId;
  protected static final int tombstoneGCCountId;
  protected static final int tombstoneOverhead1Id;
  protected static final int tombstoneOverhead2Id;
  protected static final int clearTimeoutsId;

  protected static final int deltaUpdatesId;
  protected static final int deltaUpdatesTimeId;
  protected static final int deltaFailedUpdatesId;

  protected static final int deltasPreparedId;
  protected static final int deltasPreparedTimeId;
  protected static final int deltasSentId;

  protected static final int deltaFullValuesSentId;
  protected static final int deltaFullValuesRequestedId;

  protected static final int importedEntriesCountId;
  protected static final int importTimeId;
  protected static final int exportedEntriesCountId;
  protected static final int exportTimeId;

  protected static final int compressionCompressTimeId;
  protected static final int compressionDecompressTimeId;
  protected static final int compressionCompressionsId;
  protected static final int compressionDecompressionsId;
  protected static final int compressionPreCompressedBytesId;
  protected static final int compressionPostCompressedBytesId;

  /** The Statistics object that we delegate most behavior to */
  protected final Statistics stats;

  //////////////////////// Static methods ////////////////////////

  static {
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();

    final String loadsInProgressDesc =
        "Current number of threads in this cache doing a cache load.";
    final String loadsCompletedDesc =
        "Total number of times a load on this cache has completed (as a result of either a local get() or a remote netload).";
    final String loadTimeDesc = "Total time spent invoking loaders on this cache.";
    final String netloadsInProgressDesc =
        "Current number of threads doing a network load initiated by a get() in this cache.";
    final String netloadsCompletedDesc =
        "Total number of times a network load initiated on this cache has completed.";
    final String netloadTimeDesc = "Total time spent doing network loads on this cache.";
    final String netsearchesInProgressDesc =
        "Current number of threads doing a network search initiated by a get() in this cache.";
    final String netsearchesCompletedDesc =
        "Total number of times network searches initiated by this cache have completed.";
    final String netsearchTimeDesc = "Total time spent doing network searches for cache values.";
    final String cacheWriterCallsInProgressDesc =
        "Current number of threads doing a cache writer call.";
    final String cacheWriterCallsCompletedDesc =
        "Total number of times a cache writer call has completed.";
    final String cacheWriterCallTimeDesc = "Total time spent doing cache writer calls.";
    final String cacheListenerCallsInProgressDesc =
        "Current number of threads doing a cache listener call.";
    final String cacheListenerCallsCompletedDesc =
        "Total number of times a cache listener call has completed.";
    final String cacheListenerCallTimeDesc = "Total time spent doing cache listener calls.";
    final String getInitialImagesInProgressDesc =
        "Current number of getInitialImage operations currently in progress.";
    final String getInitialImagesCompletedDesc =
        "Total number of times getInitialImages (both delta and full GII) initiated by this cache have completed.";
    final String deltaGetInitialImagesCompletedDesc =
        "Total number of times delta getInitialImages initiated by this cache have completed.";
    final String getInitialImageTimeDesc =
        "Total time spent doing getInitialImages for region creation.";
    final String getInitialImageKeysReceivedDesc =
        "Total number of keys received while doing getInitialImage operations.";
    final String regionsDesc = "The current number of regions in the cache.";
    final String partitionedRegionsDesc = "The current number of partitioned regions in the cache.";
    final String destroysDesc =
        "The total number of times a cache object entry has been destroyed in this cache.";
    final String updatesDesc =
        "The total number of updates originating remotely that have been applied to this cache.";
    final String updateTimeDesc = "Total time spent performing an update.";
    final String invalidatesDesc =
        "The total number of times an existing cache object entry value in this cache has been invalidated";
    final String getsDesc =
        "The total number of times a successful get has been done on this cache.";
    final String createsDesc = "The total number of times an entry is added to this cache.";
    final String putsDesc =
        "The total number of times an entry is added or replaced in this cache as a result of a local operation (put(), create(), or get() which results in load, netsearch, or netloading a value). Note that this only counts puts done explicitly on this cache. It does not count updates pushed from other caches.";
    final String putTimeDesc =
        "Total time spent adding or replacing an entry in this cache as a result of a local operation.  This includes synchronizing on the map, invoking cache callbacks, sending messages to other caches and waiting for responses (if required).";
    final String putallsDesc =
        "The total number of times a map is added or replaced in this cache as a result of a local operation. Note that this only counts putAlls done explicitly on this cache. It does not count updates pushed from other caches.";
    final String putallTimeDesc =
        "Total time spent replacing a map in this cache as a result of a local operation.  This includes synchronizing on the map, invoking cache callbacks, sending messages to other caches and waiting for responses (if required).";
    final String removeAllsDesc =
        "The total number of removeAll operations that originated in this cache. Note that this only counts removeAlls done explicitly on this cache. It does not count removes pushed from other caches.";
    final String removeAllTimeDesc =
        "Total time spent performing removeAlls that originated in this cache. This includes time spent waiting for the removeAll to be done in remote caches (if required).";
    final String getTimeDesc =
        "Total time spent doing get operations from this cache (including netsearch and netload)";
    final String eventQueueSizeDesc = "The number of cache events waiting to be processed.";
    final String eventQueueThrottleTimeDesc =
        "The total amount of time, in nanoseconds, spent delayed by the event queue throttle.";
    final String eventQueueThrottleCountDesc =
        "The total number of times a thread was delayed in adding an event to the event queue.";
    final String eventThreadsDesc = "The number of threads currently processing events.";
    final String missesDesc =
        "Total number of times a get on the cache did not find a value already in local memory. The number of hits (i.e. gets that did not miss) can be calculated by subtracting misses from gets.";
    final String queryExecutionsDesc = "Total number of times some query has been executed";
    final String queryExecutionTimeDesc = "Total time spent executing queries";
    final String queryResultsHashCollisionsDesc =
        "Total number of times an hash code collision occurred when inserting an object into an OQL result set or rehashing it";
    final String queryResultsHashCollisionProbeTimeDesc =
        "Total time spent probing the hashtable in an OQL result set due to hash code collisions, includes reads, writes, and rehashes";
    final String partitionedRegionOQLQueryRetriesDesc =
        "Total number of times an OQL Query on a Partitioned Region had to be retried";
    final String txSuccessLifeTimeDesc =
        "The total amount of time, in nanoseconds, spent in a transaction before a successful commit. The time measured starts at transaction begin and ends when commit is called.";
    final String txFailedLifeTimeDesc =
        "The total amount of time, in nanoseconds, spent in a transaction before a failed commit. The time measured starts at transaction begin and ends when commit is called.";
    final String txRollbackLifeTimeDesc =
        "The total amount of time, in nanoseconds, spent in a transaction before an explicit rollback. The time measured starts at transaction begin and ends when rollback is called.";
    final String txCommitsDesc = "Total number times a transaction commit has succeeded.";
    final String txFailuresDesc = "Total number times a transaction commit has failed.";
    final String txRollbacksDesc =
        "Total number times a transaction has been explicitly rolled back.";
    final String txCommitTimeDesc =
        "The total amount of time, in nanoseconds, spent doing successful transaction commits.";
    final String txFailureTimeDesc =
        "The total amount of time, in nanoseconds, spent doing failed transaction commits.";
    final String txRollbackTimeDesc =
        "The total amount of time, in nanoseconds, spent doing explicit transaction rollbacks.";
    final String txCommitChangesDesc = "Total number of changes made by committed transactions.";
    final String txFailureChangesDesc = "Total number of changes lost by failed transactions.";
    final String txRollbackChangesDesc =
        "Total number of changes lost by explicit transaction rollbacks.";
    final String txConflictCheckTimeDesc =
        "The total amount of time, in nanoseconds, spent doing conflict checks during transaction commit";
    final String reliableQueuedOpsDesc =
        "Current number of cache operations queued for distribution to required roles.";
    final String reliableQueueSizeDesc =
        "Current size in megabytes of disk used to queue for distribution to required roles.";
    final String reliableQueueMaxDesc =
        "Maximum size in megabytes allotted for disk usage to queue for distribution to required roles.";
    final String reliableRegionsDesc = "Current number of regions configured for reliability.";
    final String reliableRegionsMissingDesc =
        "Current number regions configured for reliability that are missing required roles.";
    final String reliableRegionsQueuingDesc =
        "Current number regions configured for reliability that are queuing for required roles.";
    final String reliableRegionsMissingFullAccessDesc =
        "Current number of regions configured for reliablity that are missing require roles with full access";
    final String reliableRegionsMissingLimitedAccessDesc =
        "Current number of regions configured for reliablity that are missing required roles with Limited access";
    final String reliableRegionsMissingNoAccessDesc =
        "Current number of regions configured for reliablity that are missing required roles with No access";
    final String clearsDesc = "The total number of times a clear has been done on this cache.";
    final String metaDataRefreshCountDesc =
        "Total number of times the meta data is refreshed due to hopping observed.";
    final String conflatedEventsDesc =
        "Number of events not delivered due to conflation.  Typically this means that the event arrived after a later event was already applied to the cache.";
    final String tombstoneCountDesc =
        "Number of destroyed entries that are retained for concurrent modification detection";
    final String tombstoneGCCountDesc =
        "Number of garbage-collections performed on destroyed entries";
    final String tombstoneOverhead1Desc =
        "Amount of memory consumed by destroyed entries in replicated or partitioned regions";
    final String tombstoneOverhead2Desc =
        "Amount of memory consumed by destroyed entries in non-replicated regions";
    final String clearTimeoutsDesc =
        "Number of timeouts waiting for events concurrent to a clear() operation to be received and applied before performing the clear()";
    final String deltaUpdatesDesc =
        "The total number of times entries in this cache are updated through delta bytes.";
    final String deltaUpdatesTimeDesc =
        "Total time spent applying the received delta bytes to entries in this cache.";
    final String deltaFailedUpdatesDesc =
        "The total number of times entries in this cache failed to be updated through delta bytes.";
    final String deltasPreparedDesc = "The total number of times delta was prepared in this cache.";
    final String deltasPreparedTimeDesc = "Total time spent preparing delta bytes in this cache.";
    final String deltasSentDesc =
        "The total number of times delta was sent to remote caches. This excludes deltas sent from server to client.";
    final String deltaFullValuesSentDesc =
        "The total number of times a full value was sent to a remote cache.";
    final String deltaFullValuesRequestedDesc =
        "The total number of times a full value was requested by this cache.";
    final String importedEntriesCountDesc =
        "The total number of entries imported from a snapshot file.";
    final String importTimeDesc = "The total time spent importing entries from a snapshot file.";
    final String exportedEntriesCountDesc =
        "The total number of entries exported into a snapshot file.";
    final String exportTimeDesc = "The total time spent exporting entries into a snapshot file.";
    final String compressionCompressTimeDesc = "The total time spent compressing data.";
    final String compressionDecompressTimeDesc = "The total time spent decompressing data.";
    final String compressionCompressionsDesc = "The total number of compression operations.";
    final String compressionDecompressionsDesc = "The total number of decompression operations.";
    final String compressionPreCompresssedBytesDesc =
        "The total number of bytes before compressing.";
    final String compressionPostCompressedBytesDesc =
        "The total number of bytes after compressing.";
    final String evictByCriteria_evictionsDesc = "The total number of entries evicted";// total
                                                                                       // actual
                                                                                       // evictions
                                                                                       // (entries
                                                                                       // evicted)
    final String evictByCriteria_evictionTimeDesc = "Time taken for eviction process";// total
                                                                                      // eviction
                                                                                      // time
                                                                                      // including
                                                                                      // product +
                                                                                      // user expr.
    final String evictByCriteria_evictionsInProgressDesc = "Total number of evictions in progress";
    final String evictByCriteria_evaluationsDesc = "Total number of evaluations for eviction";// total
                                                                                              // eviction
                                                                                              // attempts
    final String evictByCriteria_evaluationTimeDesc =
        "Total time taken for evaluation of user expression during eviction";// time taken to
                                                                             // evaluate user
                                                                             // expression.

    type = f.createType("CachePerfStats", "Statistics about GemFire cache performance",
        new StatisticDescriptor[] {
            f.createIntGauge("loadsInProgress", loadsInProgressDesc, "operations"),
            f.createIntCounter("loadsCompleted", loadsCompletedDesc, "operations"),
            f.createLongCounter("loadTime", loadTimeDesc, "nanoseconds", false),
            f.createIntGauge("netloadsInProgress", netloadsInProgressDesc, "operations"),
            f.createIntCounter("netloadsCompleted", netloadsCompletedDesc, "operations"),
            f.createLongCounter("netloadTime", netloadTimeDesc, "nanoseconds", false),
            f.createIntGauge("netsearchesInProgress", netsearchesInProgressDesc, "operations"),
            f.createIntCounter("netsearchesCompleted", netsearchesCompletedDesc, "operations"),
            f.createLongCounter("netsearchTime", netsearchTimeDesc, "nanoseconds"),
            f.createIntGauge("cacheWriterCallsInProgress", cacheWriterCallsInProgressDesc,
                "operations"),
            f.createIntCounter("cacheWriterCallsCompleted", cacheWriterCallsCompletedDesc,
                "operations"),
            f.createLongCounter("cacheWriterCallTime", cacheWriterCallTimeDesc, "nanoseconds"),
            f.createIntGauge("cacheListenerCallsInProgress", cacheListenerCallsInProgressDesc,
                "operations"),
            f.createIntCounter("cacheListenerCallsCompleted", cacheListenerCallsCompletedDesc,
                "operations"),
            f.createLongCounter("cacheListenerCallTime", cacheListenerCallTimeDesc, "nanoseconds"),
            f.createIntGauge("indexUpdateInProgress", "Current number of ops in progress",
                "operations"),
            f.createIntCounter("indexUpdateCompleted", "Total number of ops that have completed",
                "operations"),
            f.createLongCounter("indexUpdateTime", "Total amount of time spent doing this op",
                "nanoseconds"),
            f.createIntGauge("indexInitializationInProgress",
                "Current number of index initializations in progress", "operations"),
            f.createIntCounter("indexInitializationCompleted",
                "Total number of index initializations that have completed", "operations"),
            f.createLongCounter("indexInitializationTime",
                "Total amount of time spent initializing indexes", "nanoseconds"),

            f.createIntGauge("getInitialImagesInProgress", getInitialImagesInProgressDesc,
                "operations"),
            f.createIntCounter("getInitialImagesCompleted", getInitialImagesCompletedDesc,
                "operations"),
            f.createIntCounter("deltaGetInitialImagesCompleted", deltaGetInitialImagesCompletedDesc,
                "operations"),
            f.createLongCounter("getInitialImageTime", getInitialImageTimeDesc, "nanoseconds"),
            f.createIntCounter("getInitialImageKeysReceived", getInitialImageKeysReceivedDesc,
                "keys"),
            f.createIntGauge("regions", regionsDesc, "regions"),
            f.createIntGauge("partitionedRegions", partitionedRegionsDesc, "partitionedRegions"),
            f.createIntCounter("destroys", destroysDesc, "operations"),
            f.createIntCounter("updates", updatesDesc, "operations"),
            f.createLongCounter("updateTime", updateTimeDesc, "nanoseconds"),
            f.createIntCounter("invalidates", invalidatesDesc, "operations"),
            f.createIntCounter("gets", getsDesc, "operations"),
            f.createIntCounter("misses", missesDesc, "operations"),
            f.createIntCounter("creates", createsDesc, "operations"),
            f.createIntCounter("puts", putsDesc, "operations"),
            f.createLongCounter("putTime", putTimeDesc, "nanoseconds", false),
            f.createIntCounter("putalls", putallsDesc, "operations"),
            f.createLongCounter("putallTime", putallTimeDesc, "nanoseconds", false),
            f.createIntCounter("removeAlls", removeAllsDesc, "operations"),
            f.createLongCounter("removeAllTime", removeAllTimeDesc, "nanoseconds", false),
            f.createLongCounter("getTime", getTimeDesc, "nanoseconds", false),
            f.createIntGauge("eventQueueSize", eventQueueSizeDesc, "messages"),
            f.createIntGauge("eventQueueThrottleCount", eventQueueThrottleCountDesc, "delays"),
            f.createLongCounter("eventQueueThrottleTime", eventQueueThrottleTimeDesc, "nanoseconds",
                false),
            f.createIntGauge("eventThreads", eventThreadsDesc, "threads"),
            f.createIntCounter("queryExecutions", queryExecutionsDesc, "operations"),
            f.createLongCounter("queryExecutionTime", queryExecutionTimeDesc, "nanoseconds"),
            f.createIntCounter("queryResultsHashCollisions", queryResultsHashCollisionsDesc,
                "operations"),
            f.createLongCounter("queryResultsHashCollisionProbeTime",
                queryResultsHashCollisionProbeTimeDesc, "nanoseconds"),
            f.createLongCounter("partitionedRegionQueryRetries",
                partitionedRegionOQLQueryRetriesDesc, "retries"),

            f.createIntCounter("txCommits", txCommitsDesc, "commits"),
            f.createIntCounter("txCommitChanges", txCommitChangesDesc, "changes"),
            f.createLongCounter("txCommitTime", txCommitTimeDesc, "nanoseconds", false),
            f.createLongCounter("txSuccessLifeTime", txSuccessLifeTimeDesc, "nanoseconds", false),

            f.createIntCounter("txFailures", txFailuresDesc, "failures"),
            f.createIntCounter("txFailureChanges", txFailureChangesDesc, "changes"),
            f.createLongCounter("txFailureTime", txFailureTimeDesc, "nanoseconds", false),
            f.createLongCounter("txFailedLifeTime", txFailedLifeTimeDesc, "nanoseconds", false),

            f.createIntCounter("txRollbacks", txRollbacksDesc, "rollbacks"),
            f.createIntCounter("txRollbackChanges", txRollbackChangesDesc, "changes"),
            f.createLongCounter("txRollbackTime", txRollbackTimeDesc, "nanoseconds", false),
            f.createLongCounter("txRollbackLifeTime", txRollbackLifeTimeDesc, "nanoseconds", false),
            f.createLongCounter("txConflictCheckTime", txConflictCheckTimeDesc, "nanoseconds",
                false),

            f.createIntGauge("reliableQueuedOps", reliableQueuedOpsDesc, "operations"),
            f.createIntGauge("reliableQueueSize", reliableQueueSizeDesc, "megabytes"),
            f.createIntGauge("reliableQueueMax", reliableQueueMaxDesc, "megabytes"),
            f.createIntGauge("reliableRegions", reliableRegionsDesc, "regions"),
            f.createIntGauge("reliableRegionsMissing", reliableRegionsMissingDesc, "regions"),
            f.createIntGauge("reliableRegionsQueuing", reliableRegionsQueuingDesc, "regions"),
            f.createIntGauge("reliableRegionsMissingFullAccess",
                reliableRegionsMissingFullAccessDesc, "regions"),
            f.createIntGauge("reliableRegionsMissingLimitedAccess",
                reliableRegionsMissingLimitedAccessDesc, "regions"),
            f.createIntGauge("reliableRegionsMissingNoAccess", reliableRegionsMissingNoAccessDesc,
                "regions"),
            f.createLongGauge("entries",
                "Current number of entries in the cache. This does not include any entries that are tombstones. See tombstoneCount.",
                "entries"),
            f.createLongCounter("eventsQueued",
                "Number of events attached to " + "other events for callback invocation", "events"),
            f.createIntCounter("retries",
                "Number of times a concurrent destroy followed by a create has caused an entry operation to need to retry.",
                "operations"),
            f.createIntCounter("clears", clearsDesc, "operations"),
            f.createIntGauge("diskTasksWaiting",
                "Current number of disk tasks (oplog compactions, asynchronous recoveries, etc) that are waiting for a thread to run the operation",
                "operations"),
            f.createLongCounter("conflatedEvents", conflatedEventsDesc, "operations"),
            f.createIntGauge("tombstones", tombstoneCountDesc, "entries"),
            f.createIntCounter("tombstoneGCs", tombstoneGCCountDesc, "operations"),
            f.createLongGauge("replicatedTombstonesSize", tombstoneOverhead1Desc, "bytes"),
            f.createLongGauge("nonReplicatedTombstonesSize", tombstoneOverhead2Desc, "bytes"),
            f.createIntCounter("clearTimeouts", clearTimeoutsDesc, "timeouts"),
            f.createIntGauge("evictorJobsStarted", "Number of evictor jobs started", "jobs"),
            f.createIntGauge("evictorJobsCompleted", "Number of evictor jobs completed", "jobs"),
            f.createIntGauge("evictorQueueSize",
                "Number of jobs waiting to be picked up by evictor threads", "jobs"),
            f.createLongCounter("evictWorkTime",
                "Total time spent doing eviction work in background threads", "nanoseconds", false),
            f.createLongCounter("metaDataRefreshCount", metaDataRefreshCountDesc,
                "refreshes", false),
            f.createIntCounter("deltaUpdates", deltaUpdatesDesc, "operations"),
            f.createLongCounter("deltaUpdatesTime", deltaUpdatesTimeDesc, "nanoseconds", false),
            f.createIntCounter("deltaFailedUpdates", deltaFailedUpdatesDesc, "operations"),
            f.createIntCounter("deltasPrepared", deltasPreparedDesc, "operations"),
            f.createLongCounter("deltasPreparedTime", deltasPreparedTimeDesc, "nanoseconds", false),
            f.createIntCounter("deltasSent", deltasSentDesc, "operations"),
            f.createIntCounter("deltaFullValuesSent", deltaFullValuesSentDesc, "operations"),
            f.createIntCounter("deltaFullValuesRequested", deltaFullValuesRequestedDesc,
                "operations"),

            f.createLongCounter("importedEntries", importedEntriesCountDesc, "entries"),
            f.createLongCounter("importTime", importTimeDesc, "nanoseconds"),
            f.createLongCounter("exportedEntries", exportedEntriesCountDesc, "entries"),
            f.createLongCounter("exportTime", exportTimeDesc, "nanoseconds"),

            f.createLongCounter("compressTime", compressionCompressTimeDesc, "nanoseconds"),
            f.createLongCounter("decompressTime", compressionDecompressTimeDesc, "nanoseconds"),
            f.createLongCounter("compressions", compressionCompressionsDesc, "operations"),
            f.createLongCounter("decompressions", compressionDecompressionsDesc, "operations"),
            f.createLongCounter("preCompressedBytes", compressionPreCompresssedBytesDesc, "bytes"),
            f.createLongCounter("postCompressedBytes", compressionPostCompressedBytesDesc, "bytes"),

            f.createLongCounter("evictByCriteria_evictions", evictByCriteria_evictionsDesc,
                "operations"),
            f.createLongCounter("evictByCriteria_evictionTime", evictByCriteria_evictionTimeDesc,
                "nanoseconds"),
            f.createLongCounter("evictByCriteria_evictionsInProgress",
                evictByCriteria_evictionsInProgressDesc, "operations"),
            f.createLongCounter("evictByCriteria_evaluations", evictByCriteria_evaluationsDesc,
                "operations"),
            f.createLongCounter("evictByCriteria_evaluationTime",
                evictByCriteria_evaluationTimeDesc, "nanoseconds")});

    // Initialize id fields
    loadsInProgressId = type.nameToId("loadsInProgress");
    loadsCompletedId = type.nameToId("loadsCompleted");
    loadTimeId = type.nameToId("loadTime");
    netloadsInProgressId = type.nameToId("netloadsInProgress");
    netloadsCompletedId = type.nameToId("netloadsCompleted");
    netloadTimeId = type.nameToId("netloadTime");
    netsearchesInProgressId = type.nameToId("netsearchesInProgress");
    netsearchesCompletedId = type.nameToId("netsearchesCompleted");
    netsearchTimeId = type.nameToId("netsearchTime");
    cacheWriterCallsInProgressId = type.nameToId("cacheWriterCallsInProgress");
    cacheWriterCallsCompletedId = type.nameToId("cacheWriterCallsCompleted");
    cacheWriterCallTimeId = type.nameToId("cacheWriterCallTime");
    cacheListenerCallsInProgressId = type.nameToId("cacheListenerCallsInProgress");
    cacheListenerCallsCompletedId = type.nameToId("cacheListenerCallsCompleted");
    cacheListenerCallTimeId = type.nameToId("cacheListenerCallTime");
    indexUpdateInProgressId = type.nameToId("indexUpdateInProgress");
    indexUpdateCompletedId = type.nameToId("indexUpdateCompleted");
    indexUpdateTimeId = type.nameToId("indexUpdateTime");
    indexInitializationTimeId = type.nameToId("indexInitializationTime");
    indexInitializationInProgressId = type.nameToId("indexInitializationInProgress");
    indexInitializationCompletedId = type.nameToId("indexInitializationCompleted");
    getInitialImagesInProgressId = type.nameToId("getInitialImagesInProgress");
    getInitialImagesCompletedId = type.nameToId("getInitialImagesCompleted");
    deltaGetInitialImagesCompletedId = type.nameToId("deltaGetInitialImagesCompleted");
    getInitialImageTimeId = type.nameToId("getInitialImageTime");
    getInitialImageKeysReceivedId = type.nameToId("getInitialImageKeysReceived");
    regionsId = type.nameToId("regions");
    partitionedRegionsId = type.nameToId("partitionedRegions");
    destroysId = type.nameToId("destroys");
    createsId = type.nameToId("creates");
    putsId = type.nameToId("puts");
    putTimeId = type.nameToId("putTime");
    putallsId = type.nameToId("putalls");
    putallTimeId = type.nameToId("putallTime");
    removeAllsId = type.nameToId("removeAlls");
    removeAllTimeId = type.nameToId("removeAllTime");
    updatesId = type.nameToId("updates");
    updateTimeId = type.nameToId("updateTime");
    invalidatesId = type.nameToId("invalidates");
    getsId = type.nameToId("gets");
    getTimeId = type.nameToId("getTime");
    missesId = type.nameToId("misses");
    eventQueueSizeId = type.nameToId("eventQueueSize");
    eventQueueThrottleTimeId = type.nameToId("eventQueueThrottleTime");
    eventQueueThrottleCountId = type.nameToId("eventQueueThrottleCount");
    eventThreadsId = type.nameToId("eventThreads");
    queryExecutionsId = type.nameToId("queryExecutions");
    queryExecutionTimeId = type.nameToId("queryExecutionTime");
    queryResultsHashCollisionsId = type.nameToId("queryResultsHashCollisions");
    queryResultsHashCollisionProbeTimeId = type.nameToId("queryResultsHashCollisionProbeTime");
    partitionedRegionQueryRetriesId = type.nameToId("partitionedRegionQueryRetries");

    txSuccessLifeTimeId = type.nameToId("txSuccessLifeTime");
    txFailedLifeTimeId = type.nameToId("txFailedLifeTime");
    txRollbackLifeTimeId = type.nameToId("txRollbackLifeTime");
    txCommitsId = type.nameToId("txCommits");
    txFailuresId = type.nameToId("txFailures");
    txRollbacksId = type.nameToId("txRollbacks");
    txCommitTimeId = type.nameToId("txCommitTime");
    txFailureTimeId = type.nameToId("txFailureTime");
    txRollbackTimeId = type.nameToId("txRollbackTime");
    txCommitChangesId = type.nameToId("txCommitChanges");
    txFailureChangesId = type.nameToId("txFailureChanges");
    txRollbackChangesId = type.nameToId("txRollbackChanges");
    txConflictCheckTimeId = type.nameToId("txConflictCheckTime");

    reliableQueuedOpsId = type.nameToId("reliableQueuedOps");
    reliableQueueSizeId = type.nameToId("reliableQueueSize");
    reliableQueueMaxId = type.nameToId("reliableQueueMax");
    reliableRegionsId = type.nameToId("reliableRegions");
    reliableRegionsMissingId = type.nameToId("reliableRegionsMissing");
    reliableRegionsQueuingId = type.nameToId("reliableRegionsQueuing");
    reliableRegionsMissingFullAccessId = type.nameToId("reliableRegionsMissingFullAccess");
    reliableRegionsMissingLimitedAccessId = type.nameToId("reliableRegionsMissingLimitedAccess");
    reliableRegionsMissingNoAccessId = type.nameToId("reliableRegionsMissingNoAccess");
    entryCountId = type.nameToId("entries");

    eventsQueuedId = type.nameToId("eventsQueued");

    retriesId = type.nameToId("retries");
    clearsId = type.nameToId("clears");

    diskTasksWaitingId = type.nameToId("diskTasksWaiting");
    evictorJobsStartedId = type.nameToId("evictorJobsStarted");
    evictorJobsCompletedId = type.nameToId("evictorJobsCompleted");
    evictorQueueSizeId = type.nameToId("evictorQueueSize");
    evictWorkTimeId = type.nameToId("evictWorkTime");

    metaDataRefreshCountId = type.nameToId("metaDataRefreshCount");

    conflatedEventsId = type.nameToId("conflatedEvents");
    tombstoneCountId = type.nameToId("tombstones");
    tombstoneGCCountId = type.nameToId("tombstoneGCs");
    tombstoneOverhead1Id = type.nameToId("replicatedTombstonesSize");
    tombstoneOverhead2Id = type.nameToId("nonReplicatedTombstonesSize");
    clearTimeoutsId = type.nameToId("clearTimeouts");

    deltaUpdatesId = type.nameToId("deltaUpdates");
    deltaUpdatesTimeId = type.nameToId("deltaUpdatesTime");
    deltaFailedUpdatesId = type.nameToId("deltaFailedUpdates");

    deltasPreparedId = type.nameToId("deltasPrepared");
    deltasPreparedTimeId = type.nameToId("deltasPreparedTime");
    deltasSentId = type.nameToId("deltasSent");

    deltaFullValuesSentId = type.nameToId("deltaFullValuesSent");
    deltaFullValuesRequestedId = type.nameToId("deltaFullValuesRequested");

    importedEntriesCountId = type.nameToId("importedEntries");
    importTimeId = type.nameToId("importTime");
    exportedEntriesCountId = type.nameToId("exportedEntries");
    exportTimeId = type.nameToId("exportTime");

    compressionCompressTimeId = type.nameToId("compressTime");
    compressionDecompressTimeId = type.nameToId("decompressTime");
    compressionCompressionsId = type.nameToId("compressions");
    compressionDecompressionsId = type.nameToId("decompressions");
    compressionPreCompressedBytesId = type.nameToId("preCompressedBytes");
    compressionPostCompressedBytesId = type.nameToId("postCompressedBytes");
  }

  //////////////////////// Constructors ////////////////////////

  /**
   * Created specially for bug 39348. Should not be invoked in any other case.
   */
  public CachePerfStats() {
    stats = null;
  }

  /**
   * Creates a new <code>CachePerfStats</code> and registers itself with the given statistics
   * factory.
   */
  public CachePerfStats(StatisticsFactory factory) {
    stats = factory.createAtomicStatistics(type, "cachePerfStats");
  }

  /**
   * Creates a new <code>CachePerfStats</code> and registers itself with the given statistics
   * factory.
   */
  public CachePerfStats(StatisticsFactory factory, String name) {
    stats = factory.createAtomicStatistics(type, "RegionStats-" + name);
  }

  /**
   * Returns the current NanoTime or, if clock stats are disabled, zero.
   *
   * @since GemFire 5.0
   */
  public static long getStatTime() {
    return enableClockStats ? NanoTimer.getTime() : 0;
  }

  ////////////////////// Accessing Stats //////////////////////

  public int getLoadsInProgress() {
    return stats.getInt(loadsInProgressId);
  }

  public int getLoadsCompleted() {
    return stats.getInt(loadsCompletedId);
  }

  public long getLoadTime() {
    return stats.getLong(loadTimeId);
  }

  public int getNetloadsInProgress() {
    return stats.getInt(netloadsInProgressId);
  }

  public int getNetloadsCompleted() {
    return stats.getInt(netloadsCompletedId);
  }

  public long getNetloadTime() {
    return stats.getLong(netloadTimeId);
  }

  public int getNetsearchesInProgress() {
    return stats.getInt(netsearchesInProgressId);
  }

  public int getNetsearchesCompleted() {
    return stats.getInt(netsearchesCompletedId);
  }

  public long getNetsearchTime() {
    return stats.getLong(netsearchTimeId);
  }

  public int getGetInitialImagesInProgress() {
    return stats.getInt(getInitialImagesInProgressId);
  }

  public int getGetInitialImagesCompleted() {
    return stats.getInt(getInitialImagesCompletedId);
  }

  public int getDeltaGetInitialImagesCompleted() {
    return stats.getInt(deltaGetInitialImagesCompletedId);
  }

  public long getGetInitialImageTime() {
    return stats.getLong(getInitialImageTimeId);
  }

  public int getGetInitialImageKeysReceived() {
    return stats.getInt(getInitialImageKeysReceivedId);
  }

  public int getRegions() {
    return stats.getInt(regionsId);
  }

  public int getPartitionedRegions() {
    return stats.getInt(partitionedRegionsId);
  }

  public int getDestroys() {
    return stats.getInt(destroysId);
  }

  public int getCreates() {
    return stats.getInt(createsId);
  }

  public int getPuts() {
    return stats.getInt(putsId);
  }

  public int getPutAlls() {
    return stats.getInt(putallsId);
  }

  public int getRemoveAlls() {
    return stats.getInt(removeAllsId);
  }

  public int getUpdates() {
    return stats.getInt(updatesId);
  }

  public int getInvalidates() {
    return stats.getInt(invalidatesId);
  }

  public int getGets() {
    return stats.getInt(getsId);
  }

  public int getMisses() {
    return stats.getInt(missesId);
  }

  public int getReliableQueuedOps() {
    return stats.getInt(reliableQueuedOpsId);
  }

  public void incReliableQueuedOps(int inc) {
    stats.incInt(reliableQueuedOpsId, inc);
  }

  public int getReliableQueueSize() {
    return stats.getInt(reliableQueueSizeId);
  }

  public void incReliableQueueSize(int inc) {
    stats.incInt(reliableQueueSizeId, inc);
  }

  public int getReliableQueueMax() {
    return stats.getInt(reliableQueueMaxId);
  }

  public void incReliableQueueMax(int inc) {
    stats.incInt(reliableQueueMaxId, inc);
  }

  public int getReliableRegions() {
    return stats.getInt(reliableRegionsId);
  }

  public void incReliableRegions(int inc) {
    stats.incInt(reliableRegionsId, inc);
  }

  public int getReliableRegionsMissing() {
    return stats.getInt(reliableRegionsMissingId);
  }

  public void incReliableRegionsMissing(int inc) {
    stats.incInt(reliableRegionsMissingId, inc);
  }

  public int getReliableRegionsQueuing() {
    return stats.getInt(reliableRegionsQueuingId);
  }

  public void incReliableRegionsQueuing(int inc) {
    stats.incInt(reliableRegionsQueuingId, inc);
  }

  public int getReliableRegionsMissingFullAccess() {
    return stats.getInt(reliableRegionsMissingFullAccessId);
  }

  public void incReliableRegionsMissingFullAccess(int inc) {
    stats.incInt(reliableRegionsMissingFullAccessId, inc);
  }

  public int getReliableRegionsMissingLimitedAccess() {
    return stats.getInt(reliableRegionsMissingLimitedAccessId);
  }

  public void incReliableRegionsMissingLimitedAccess(int inc) {
    stats.incInt(reliableRegionsMissingLimitedAccessId, inc);
  }

  public int getReliableRegionsMissingNoAccess() {
    return stats.getInt(reliableRegionsMissingNoAccessId);
  }

  public void incReliableRegionsMissingNoAccess(int inc) {
    stats.incInt(reliableRegionsMissingNoAccessId, inc);
  }

  public void incQueuedEvents(int inc) {
    this.stats.incLong(eventsQueuedId, inc);
  }

  public long getQueuedEvents() {
    return this.stats.getInt(eventsQueuedId);
  }

  public int getDeltaUpdates() {
    return stats.getInt(deltaUpdatesId);
  }

  public long getDeltaUpdatesTime() {
    return stats.getLong(deltaUpdatesTimeId);
  }

  public int getDeltaFailedUpdates() {
    return stats.getInt(deltaFailedUpdatesId);
  }

  public int getDeltasPrepared() {
    return stats.getInt(deltasPreparedId);
  }

  public long getDeltasPreparedTime() {
    return stats.getLong(deltasPreparedTimeId);
  }

  public int getDeltasSent() {
    return stats.getInt(deltasSentId);
  }

  public int getDeltaFullValuesSent() {
    return stats.getInt(deltaFullValuesSentId);
  }

  public int getDeltaFullValuesRequested() {
    return stats.getInt(deltaFullValuesRequestedId);
  }

  public long getTotalCompressionTime() {
    return stats.getLong(compressionCompressTimeId);
  }

  public long getTotalDecompressionTime() {
    return stats.getLong(compressionDecompressTimeId);
  }

  public long getTotalCompressions() {
    return stats.getLong(compressionCompressionsId);
  }

  public long getTotalDecompressions() {
    return stats.getLong(compressionDecompressionsId);
  }

  public long getTotalPreCompressedBytes() {
    return stats.getLong(compressionPreCompressedBytesId);
  }

  public long getTotalPostCompressedBytes() {
    return stats.getLong(compressionPostCompressedBytesId);
  }

  ////////////////////// Updating Stats //////////////////////

  public long startCompression() {
    stats.incLong(compressionCompressionsId, 1);
    return getStatTime();
  }

  public void endCompression(long startTime, long startSize, long endSize) {
    if (enableClockStats) {
      stats.incLong(compressionCompressTimeId, getStatTime() - startTime);
    }
    stats.incLong(compressionPreCompressedBytesId, startSize);
    stats.incLong(compressionPostCompressedBytesId, endSize);
  }

  public long startDecompression() {
    stats.incLong(compressionDecompressionsId, 1);
    return getStatTime();
  }

  public void endDecompression(long startTime) {
    if (enableClockStats) {
      stats.incLong(compressionDecompressTimeId, getStatTime() - startTime);
    }
  }

  /**
   * @return the timestamp that marks the start of the operation
   */
  public long startLoad() {
    stats.incInt(loadsInProgressId, 1);
    return NanoTimer.getTime(); // don't use getStatTime so always enabled
  }

  /**
   * @param start the timestamp taken when the operation started
   */
  public void endLoad(long start) {
    // note that load times are used in health checks and
    // should not be disabled by enableClockStats==false
    long ts = NanoTimer.getTime(); // don't use getStatTime so always enabled
    stats.incLong(loadTimeId, ts - start);
    stats.incInt(loadsInProgressId, -1);
    stats.incInt(loadsCompletedId, 1);
  }

  /**
   * @return the timestamp that marks the start of the operation
   */
  public long startNetload() {
    stats.incInt(netloadsInProgressId, 1);
    return getStatTime();
  }

  /**
   * @param start the timestamp taken when the operation started
   */
  public void endNetload(long start) {
    if (enableClockStats) {
      stats.incLong(netloadTimeId, getStatTime() - start);
    }
    stats.incInt(netloadsInProgressId, -1);
    stats.incInt(netloadsCompletedId, 1);
  }

  /**
   * @return the timestamp that marks the start of the operation
   */
  public long startNetsearch() {
    stats.incInt(netsearchesInProgressId, 1);
    return NanoTimer.getTime(); // don't use getStatTime so always enabled
  }

  /**
   * @param start the timestamp taken when the operation started
   */
  public void endNetsearch(long start) {
    // note that netsearch is used in health checks and timings should
    // not be disabled by enableClockStats==false
    long ts = NanoTimer.getTime(); // don't use getStatTime so always enabled
    stats.incLong(netsearchTimeId, ts - start);
    stats.incInt(netsearchesInProgressId, -1);
    stats.incInt(netsearchesCompletedId, 1);
  }

  /**
   * @return the timestamp that marks the start of the operation
   */
  public long startCacheWriterCall() {
    stats.incInt(cacheWriterCallsInProgressId, 1);
    return getStatTime();
  }

  /**
   * @param start the timestamp taken when the operation started
   */
  public void endCacheWriterCall(long start) {
    if (enableClockStats) {
      stats.incLong(cacheWriterCallTimeId, getStatTime() - start);
    }
    stats.incInt(cacheWriterCallsInProgressId, -1);
    stats.incInt(cacheWriterCallsCompletedId, 1);
  }

  /**
   * @return the timestamp that marks the start of the operation
   * @since GemFire 3.5
   */
  public long startCacheListenerCall() {
    stats.incInt(cacheListenerCallsInProgressId, 1);
    return getStatTime();
  }

  /**
   * @param start the timestamp taken when the operation started
   * @since GemFire 3.5
   */
  public void endCacheListenerCall(long start) {
    if (enableClockStats) {
      stats.incLong(cacheListenerCallTimeId, getStatTime() - start);
    }
    stats.incInt(cacheListenerCallsInProgressId, -1);
    stats.incInt(cacheListenerCallsCompletedId, 1);
  }

  /**
   * @return the timestamp that marks the start of the operation
   */
  public long startGetInitialImage() {
    stats.incInt(getInitialImagesInProgressId, 1);
    return getStatTime();
  }

  /**
   * @param start the timestamp taken when the operation started
   */
  public void endGetInitialImage(long start) {
    if (enableClockStats) {
      stats.incLong(getInitialImageTimeId, getStatTime() - start);
    }
    stats.incInt(getInitialImagesInProgressId, -1);
    stats.incInt(getInitialImagesCompletedId, 1);
  }

  /**
   * @param start the timestamp taken when the operation started
   */
  public void endNoGIIDone(long start) {
    if (enableClockStats) {
      stats.incLong(getInitialImageTimeId, getStatTime() - start);
    }
    stats.incInt(getInitialImagesInProgressId, -1);
  }

  public void incDeltaGIICompleted() {
    stats.incInt(deltaGetInitialImagesCompletedId, 1);
  }

  public void incGetInitialImageKeysReceived() {
    stats.incInt(getInitialImageKeysReceivedId, 1);
  }

  public long startIndexUpdate() {
    stats.incInt(indexUpdateInProgressId, 1);
    return getStatTime();
  }

  public void endIndexUpdate(long start) {
    long ts = getStatTime();
    stats.incLong(indexUpdateTimeId, ts - start);
    stats.incInt(indexUpdateInProgressId, -1);
    stats.incInt(indexUpdateCompletedId, 1);
  }

  public long startIndexInitialization() {
    stats.incInt(indexInitializationInProgressId, 1);
    return getStatTime();
  }

  public void endIndexInitialization(long start) {
    long ts = getStatTime();
    stats.incLong(indexInitializationTimeId, ts - start);
    stats.incInt(indexInitializationInProgressId, -1);
    stats.incInt(indexInitializationCompletedId, 1);
  }

  public long getIndexInitializationTime() {
    return stats.getLong(indexInitializationTimeId);
  }

  public void incRegions(int inc) {
    stats.incInt(regionsId, inc);
  }

  public void incPartitionedRegions(int inc) {
    stats.incInt(partitionedRegionsId, inc);
  }

  public void incDestroys() {
    stats.incInt(destroysId, 1);
  }

  public void incCreates() {
    stats.incInt(createsId, 1);
  }

  public void incInvalidates() {
    stats.incInt(invalidatesId, 1);
  }

  /**
   * @return the timestamp that marks the start of the operation
   */
  public long startGet() {
    return getStatTime();
  }

  /**
   * @param start the timestamp taken when the operation started
   */
  public void endGet(long start, boolean miss) {
    if (enableClockStats) {
      stats.incLong(getTimeId, getStatTime() - start);
    }
    stats.incInt(getsId, 1);
    if (miss) {
      stats.incInt(missesId, 1);
    }
  }

  /**
   * @param start the timestamp taken when the operation started
   * @param isUpdate true if the put was an update (origin remote)
   */
  public long endPut(long start, boolean isUpdate) {
    long total = 0;
    if (isUpdate) {
      stats.incInt(updatesId, 1);
      if (enableClockStats) {
        total = getStatTime() - start;
        stats.incLong(updateTimeId, total);
      }
    } else {
      stats.incInt(putsId, 1);
      if (enableClockStats) {
        total = getStatTime() - start;
        stats.incLong(putTimeId, total);
      }
    }
    return total;
  }

  public void endPutAll(long start) {
    stats.incInt(putallsId, 1);
    if (enableClockStats)
      stats.incLong(putallTimeId, getStatTime() - start);
  }

  public void endRemoveAll(long start) {
    stats.incInt(removeAllsId, 1);
    if (enableClockStats)
      stats.incLong(removeAllTimeId, getStatTime() - start);
  }

  public void endQueryExecution(long executionTime) {
    stats.incInt(queryExecutionsId, 1);
    if (enableClockStats) {
      stats.incLong(queryExecutionTimeId, executionTime);
    }
  }

  public void endQueryResultsHashCollisionProbe(long start) {
    if (enableClockStats) {
      stats.incLong(queryResultsHashCollisionProbeTimeId, getStatTime() - start);
    }
  }

  public void incQueryResultsHashCollisions() {
    stats.incInt(queryResultsHashCollisionsId, 1);
  }

  public int getTxCommits() {
    return stats.getInt(txCommitsId);
  }

  public int getTxCommitChanges() {
    return stats.getInt(txCommitChangesId);
  }

  public long getTxCommitTime() {
    return stats.getLong(txCommitTimeId);
  }

  public long getTxSuccessLifeTime() {
    return stats.getLong(txSuccessLifeTimeId);
  }

  public int getTxFailures() {
    return stats.getInt(txFailuresId);
  }

  public int getTxFailureChanges() {
    return stats.getInt(txFailureChangesId);
  }

  public long getTxFailureTime() {
    return stats.getLong(txFailureTimeId);
  }

  public long getTxFailedLifeTime() {
    return stats.getLong(txFailedLifeTimeId);
  }

  public int getTxRollbacks() {
    return stats.getInt(txRollbacksId);
  }

  public int getTxRollbackChanges() {
    return stats.getInt(txRollbackChangesId);
  }

  public long getTxRollbackTime() {
    return stats.getLong(txRollbackTimeId);
  }

  public long getTxRollbackLifeTime() {
    return stats.getLong(txRollbackLifeTimeId);
  }

  public void incTxConflictCheckTime(long delta) {
    stats.incLong(txConflictCheckTimeId, delta);
  }

  public void txSuccess(long opTime, long txLifeTime, int txChanges) {
    stats.incInt(txCommitsId, 1);
    stats.incInt(txCommitChangesId, txChanges);
    stats.incLong(txCommitTimeId, opTime);
    stats.incLong(txSuccessLifeTimeId, txLifeTime);
  }

  public void txFailure(long opTime, long txLifeTime, int txChanges) {
    stats.incInt(txFailuresId, 1);
    stats.incInt(txFailureChangesId, txChanges);
    stats.incLong(txFailureTimeId, opTime);
    stats.incLong(txFailedLifeTimeId, txLifeTime);
  }

  public void txRollback(long opTime, long txLifeTime, int txChanges) {
    stats.incInt(txRollbacksId, 1);
    stats.incInt(txRollbackChangesId, txChanges);
    stats.incLong(txRollbackTimeId, opTime);
    stats.incLong(txRollbackLifeTimeId, txLifeTime);
  }

  public void endDeltaUpdate(long start) {
    stats.incInt(deltaUpdatesId, 1);
    if (enableClockStats) {
      stats.incLong(deltaUpdatesTimeId, getStatTime() - start);
    }
  }

  public void incDeltaFailedUpdates() {
    stats.incInt(deltaFailedUpdatesId, 1);
  }

  public void endDeltaPrepared(long start) {
    stats.incInt(deltasPreparedId, 1);
    if (enableClockStats) {
      stats.incLong(deltasPreparedTimeId, getStatTime() - start);
    }
  }

  public void incDeltasSent() {
    stats.incInt(deltasSentId, 1);
  }

  public void incDeltaFullValuesSent() {
    stats.incInt(deltaFullValuesSentId, 1);
  }

  public void incDeltaFullValuesRequested() {
    stats.incInt(deltaFullValuesRequestedId, 1);
  }

  /**
   * Closes these stats so that they can not longer be used. The stats are closed when the cache is
   * closed.
   *
   * @since GemFire 3.5
   */
  void close() {
    this.stats.close();
  }

  /**
   * Returns whether or not these stats have been closed
   *
   * @since GemFire 3.5
   */
  public boolean isClosed() {
    return this.stats.isClosed();
  }

  public int getEventQueueSize() {
    return this.stats.getInt(eventQueueSizeId);
  }

  public void incEventQueueSize(int items) {
    this.stats.incInt(eventQueueSizeId, items);
  }

  public void incEventQueueThrottleCount(int items) {
    this.stats.incInt(eventQueueThrottleCountId, items);
  }

  protected void incEventQueueThrottleTime(long nanos) {
    this.stats.incLong(eventQueueThrottleTimeId, nanos);
  }

  protected void incEventThreads(int items) {
    this.stats.incInt(eventThreadsId, items);
  }

  public void incEntryCount(int delta) {
    this.stats.incLong(entryCountId, delta);
  }

  public long getEntries() {
    return this.stats.getLong(entryCountId);
  }

  public void incRetries() {
    this.stats.incInt(retriesId, 1);
  }

  public void incDiskTasksWaiting() {
    this.stats.incInt(diskTasksWaitingId, 1);
  }

  public void decDiskTasksWaiting() {
    this.stats.incInt(diskTasksWaitingId, -1);
  }

  public int getDiskTasksWaiting() {
    return this.stats.getInt(diskTasksWaitingId);
  }

  public void decDiskTasksWaiting(int count) {
    this.stats.incInt(diskTasksWaitingId, -count);
  }

  public void incEvictorJobsStarted() {
    this.stats.incInt(evictorJobsStartedId, 1);
  }

  public void incEvictorJobsCompleted() {
    this.stats.incInt(evictorJobsCompletedId, 1);
  }

  public void incEvictorQueueSize(int delta) {
    this.stats.incInt(evictorQueueSizeId, delta);
  }

  public void incEvictWorkTime(long delta) {
    this.stats.incLong(evictWorkTimeId, delta);
  }

  /**
   * Returns the Statistics instance that stores the cache perf stats.
   *
   * @since GemFire 3.5
   */
  public Statistics getStats() {
    return this.stats;
  }

  // /**
  // * Returns a helper object so that the event queue can record its
  // * stats to the proper cache perf stats.
  // * @since GemFire 3.5
  // */
  // public ThrottledQueueStatHelper getEventQueueHelper() {
  // return new ThrottledQueueStatHelper() {
  // public void incThrottleCount() {
  // incEventQueueThrottleCount(1);
  // }
  // public void throttleTime(long nanos) {
  // incEventQueueThrottleTime(nanos);
  // }
  // public void add() {
  // incEventQueueSize(1);
  // }
  // public void remove() {
  // incEventQueueSize(-1);
  // }
  // public void remove(int count) {
  // incEventQueueSize(-count);
  // }
  // };
  // }
  /**
   * Returns a helper object so that the event pool can record its stats to the proper cache perf
   * stats.
   *
   * @since GemFire 3.5
   */
  public PoolStatHelper getEventPoolHelper() {
    return new PoolStatHelper() {
      public void startJob() {
        incEventThreads(1);
      }

      public void endJob() {
        incEventThreads(-1);
      }
    };
  }

  public int getClearCount() {
    return stats.getInt(clearsId);
  }

  public void incClearCount() {
    this.stats.incInt(clearsId, 1);
  }

  public long getConflatedEventsCount() {
    return stats.getLong(conflatedEventsId);
  }

  public void incConflatedEventsCount() {
    this.stats.incLong(conflatedEventsId, 1);
  }

  public int getTombstoneCount() {
    return this.stats.getInt(tombstoneCountId);
  }

  public void incTombstoneCount(int amount) {
    this.stats.incInt(tombstoneCountId, amount);
  }

  public int getTombstoneGCCount() {
    return this.stats.getInt(tombstoneGCCountId);
  }

  public void incTombstoneGCCount() {
    this.stats.incInt(tombstoneGCCountId, 1);
  }

  public void setReplicatedTombstonesSize(long size) {
    this.stats.setLong(tombstoneOverhead1Id, size);
  }

  public long getReplicatedTombstonesSize() {
    return this.stats.getLong(tombstoneOverhead1Id);
  }

  public void setNonReplicatedTombstonesSize(long size) {
    this.stats.setLong(tombstoneOverhead2Id, size);
  }

  public long getNonReplicatedTombstonesSize() {
    return this.stats.getLong(tombstoneOverhead2Id);
  }

  public int getClearTimeouts() {
    return this.stats.getInt(clearTimeoutsId);
  }

  public void incClearTimeouts() {
    this.stats.incInt(clearTimeoutsId, 1);
  }

  public void incPRQueryRetries() {
    this.stats.incLong(partitionedRegionQueryRetriesId, 1);
  }

  public long getPRQueryRetries() {
    return this.stats.getLong(partitionedRegionQueryRetriesId);
  }

  public QueueStatHelper getEvictionQueueStatHelper() {
    return new QueueStatHelper() {
      public void add() {
        incEvictorQueueSize(1);
      }

      public void remove() {
        incEvictorQueueSize(-1);
      }

      public void remove(int count) {
        incEvictorQueueSize(count * -1);
      }
    };
  }

  public void incMetaDataRefreshCount() {
    this.stats.incLong(metaDataRefreshCountId, 1);
  }

  public long getMetaDataRefreshCount() {
    return this.stats.getLong(metaDataRefreshCountId);
  }

  public long getImportedEntriesCount() {
    return stats.getLong(importedEntriesCountId);
  }

  public long getImportTime() {
    return stats.getLong(importTimeId);
  }

  public void endImport(long entryCount, long start) {
    stats.incLong(importedEntriesCountId, entryCount);
    if (enableClockStats) {
      stats.incLong(importTimeId, getStatTime() - start);
    }
  }

  public long getExportedEntriesCount() {
    return stats.getLong(exportedEntriesCountId);
  }

  public long getExportTime() {
    return stats.getLong(exportTimeId);
  }

  public void endExport(long entryCount, long start) {
    stats.incLong(exportedEntriesCountId, entryCount);
    if (enableClockStats) {
      stats.incLong(exportTimeId, getStatTime() - start);
    }
  }
}
