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
package org.apache.geode.management.internal.beans.stats;

/**
 * All the stats name which we will require from various MBean
 *
 */
public class StatsKey {



  public static final String GET_INITIAL_IMAGE_KEYS_RECEIVED = "getInitialImageKeysReceived";

  public static final String GET_INITIAL_IMAGE_TIME = "getInitialImageTime";

  public static final String GET_INITIAL_IMAGES_INPROGRESS = "getInitialImagesInProgress";

  public static final String GET_INITIAL_IMAGES_COMPLETED = "getInitialImagesCompleted";

  public static final String CACHE_WRITER_CALLS_COMPLETED = "cacheWriterCallsCompleted";

  public static final String CACHE_WRITER_CALL_TIME = "cacheWriterCallTime";

  public static final String CACHE_LISTENER_CALLS_COMPLETED = "cacheListenerCallsCompleted";

  public static final String CACHE_LISTENR_CALL_TIME = "cacheListenerCallTime";

  public static final String CREATES = "creates";

  public static final String DESTROYS = "destroys";

  public static final String GETS = "gets";

  public static final String GET_TIME = "getTime";

  public static final String UPDATES = "gets";

  public static final String UPDATE_TIME = "getTime";

  public static final String PUTS = "puts";

  public static final String PUT_TIME = "putTime";

  public static final String PUT_ALLS = "putalls";

  public static final String PUT_ALL_TIME = "putallTime";

  public static final String REGIONS = "regions";

  public static final String PARTITIONED_REGIONS = "partitionedRegions";

  public static final String ENTRIES = "entries";

  public static final String MISSES = "misses";

  public static final String LOADS_COMPLETED = "loadsCompleted";

  public static final String LOADS_TIME = "loadTime";

  public static final String NET_LOADS_COMPLETED = "netloadsCompleted";

  public static final String NET_LOADS_TIME = "netloadTime";

  public static final String NET_SEARCH_COMPLETED = "netsearchesCompleted";

  public static final String NET_SEARCH_TIME = "netsearchTime";

  public static final String TRANSACTION_COMMITS = "txCommits";

  public static final String TRANSACTION_COMMIT_TIME = "txCommitTime";

  public static final String TRANSACTION_ROLLBACKS = "txRollbacks";

  public static final String TRANSACTION_ROLLBACK_TIME = "txRollbackTime";

  public static final String TOTAL_DISK_TASK_WAITING = "diskTasksWaiting";

  public static final String TOTAL_INDEX_UPDATE_TIME = "indexUpdateTime";

  /** Lock Service Keys **/

  public static final String LOCK_WAITS_IN_PROGRESS = "lockWaitsInProgress";

  public static final String LOCK_REQUEST_QUEUE = "requestQueues";

  public static final String LOCK_WAIT_TIME = "lockWaitTime";

  public static final String LOCK_GRANTORS = "grantors";

  public static final String LOCK_SERVICES = "services";

  /**
   * Function Stats Keys
   */


  public static final String FUNCTION_EXECUTIONS_COMPLETED = "functionExecutionsCompleted";


  public static final String FUNCTION_EXECUTIONS_COMPLETED_PROCESSING_TIME =
      "functionExecutionsCompletedProcessingTime";

  public static final String FUNCTION_EXECUTIONS_RUNNING = "functionExecutionsRunning";

  public static final String RESULTS_SENT_TO_RESULTCOLLECTOR = "resultsSentToResultCollector";

  public static final String FUNCTION_EXECUTION_CALLS = "functionExecutionCalls";

  public static final String FUNCTION_EXECUTIONS_HASRESULT_COMPLETED_PROCESSING_TIME =
      "functionExecutionsHasResultCompletedProcessingTime";

  public static final String FUNCTION_EXECUTIONS_HASRESULT_RUNNING =
      "functionExecutionsHasResultRunning";

  public static final String FUNCTION_EXECUTION_EXCEPTIONS = "functionExecutionsExceptions";

  public static final String RESULTS_RECEIVED = "resultsReceived";

  /** Distribution Stats **/



  public static final String RECEIVED_BYTES = "receivedBytes";

  public static final String SENT_BYTES = "sentBytes";

  public static final String SERIALIZATION_TIME = "serializationTime";
  public static final String SERIALIZATIONS = "serializations";
  public static final String SERIALIZED_BYTES = "serializedBytes";
  public static final String DESERIALIZATIONS = "deserializations";
  public static final String DESERIALIZATION_TIME = "deserializationTime";
  public static final String DESERIALIZAED_BYTES = "deserializedBytes";

  public static final String PDX_SERIALIZATIONS = "pdxSerializations";
  public static final String PDX_SERIALIZATIONS_BYTES = "pdxSerializedBytes";
  public static final String PDX_DESERIALIZATIONS = "pdxDeserializations";
  public static final String PDX_DESERIALIZED_BYTES = "pdxDeserializedBytes";

  public static final String PDX_INSTANCE_DESERIALIZATIONS = "pdxInstanceDeserializations";
  public static final String PDX_INSTANCE_DESERIALIZATION_TIME = "pdxInstanceDeserializationTime";

  public static final String REPLY_WAITS_IN_PROGRESS = "replyWaitsInProgress";
  public static final String REPLY_WAITS_COMPLETED = "replyWaitsCompleted";
  public static final String NODES = "nodes";



  /** Disk Related stats **/

  public static final String DISK_READS = "reads";
  public static final String DISK_READS_TIME = "readTime";
  public static final String DISK_READ_BYTES = "readBytes";

  public static final String DISK_WRITES = "writes";
  public static final String DISK_WRITES_TIME = "writeTime";
  public static final String DISK_WRITEN_BYTES = "writtenBytes";

  public static final String DISK_RECOVERY_ENTRIES_CREATED = "recoveredEntryCreates";
  public static final String DISK_RECOVERED_BYTES = "recoveredBytes";

  public static final String BACKUPS_IN_PROGRESS = "backupsInProgress";

  public static final String BACKUPS_COMPLETED = "backupsCompleted";

  public static final String FLUSHED_BYTES = "flushedBytes";

  public static final String NUM_FLUSHES = "flushes";

  public static final String TOTAL_FLUSH_TIME = "flushTime";

  public static final String DISK_QUEUE_SIZE = "queueSize";

  public static final String RECOVERIES_IN_PROGRESS = "recoveriesInProgress";

  public static final String DISK_SPACE = "diskSpace";



  /** Cache Server Related Stats **/



  public static final String CONNECTION_LOAD = "connectionLoad";

  public static final String CONNECTION_THREADS = "connectionThreads";

  public static final String GET_REQUESTS = "getRequests";

  public static final String PROCESS_GET_TIME = "processGetTime";

  public static final String PUT_REQUESTS = "putRequests";

  public static final String PROCESS_PUT_TIME = "processPutTime";

  public static final String QUERY_REQUESTS = "queryRequests";

  public static final String CURRENT_CLIENT_CONNECTIONS = "currentClientConnections";

  public static final String THREAD_QUEUE_SIZE = "threadQueueSize";

  public static final String CURRENT_CLIENTS = "currentClients";

  public static final String LOAD_PER_CONNECTION = "loadPerConnection";

  public static final String LOAD_PER_QUEUE = "loadPerQueue";

  public static final String QUEUE_LOAD = "queueLoad";

  public static final String SERVER_RECEIVED_BYTES = "receivedBytes";

  public static final String SERVER_SENT_BYTES = "sentBytes";

  public static final String CONNECTIONS_TIMED_OUT = "connectionsTimedOut";

  public static final String FAILED_CONNECTION_ATTEMPT = "failedConnectionAttempts";

  public static final String NUM_CLIENT_NOTIFICATION_REQUEST = "clientNotificationRequests";

  public static final String CLIENT_NOTIFICATION_PROCESS_TIME = "processClientNotificationTime";


  /** Region and Partition Region Stats **/

  public static final String GETS_COMPLETED = "getsCompleted";
  public static final String PUTS_COMPLETED = "putsCompleted";
  public static final String PUTALL_COMPLETED = "putAllsCompleted";
  public static final String PUTALL_TIME = "putAllTime";
  public static final String CREATES_COMPLETED = "createsCompleted";
  public static final String DESTROYS_COMPLETED = "destroysCompleted";


  public static final String GETS_ENTRY_TIME = "getEntryTime";

  public static final String LOCAL_READS = "preferredReadLocal";

  public static final String REMOTE_READS = "preferredReadRemote";

  public static final String REMOTE_PUTS = "putRemoteCompleted";

  public static final String REMOTE_PUT_TIME = "putRemoteTime";

  public static final String PUT_LOCAL = "putLocalCompleted";

  public static final String BUCKET_COUNT = "bucketCount";
  public static final String TOTAL_BUCKET_SIZE = "dataStoreEntryCount";
  public static final String AVG_BUCKET_SIZE = "avgBucketSize";
  public static final String LOW_REDUNDANCYBUCKET_COUNT = "lowRedundancyBucketCount";
  public static final String CONFIGURED_REDUNDANT_COPIES = "configuredRedundantCopies";
  public static final String ACTUAL_REDUNDANT_COPIES = "actualRedundantCopies";
  public static final String PRIMARY_BUCKET_COUNT = "primaryBucketCount";
  public static final String DATA_STORE_ENTRY_COUNT = "dataStoreEntryCount";
  public static final String DATA_STORE_BYTES_IN_USE = "dataStoreBytesInUse";



  /** Disk Region Stats **/

  public static final String DISK_REGION_WRITES = "writes";
  public static final String DISK_REGION_WRITE_TIMES = "writeTime";
  public static final String DISK_REGION_WRITTEN_BYTES = "writtenBytes";
  public static final String DISK_REGION_READS = "reads";
  public static final String DISK_REGION_READ_TIME = "readTime";
  public static final String DISK_REGION_READ_BYTES = "readBytes";
  public static final String DISK_REGION_REMOVES = "removes";
  public static final String DISK_REGION_REMOVE_TIME = "removeTime";
  public static final String DISK_REGION_ENTRIES_IN_DISK = "entriesOnlyOnDisk";
  public static final String DISK_REGION_ENTRIES_IN_VM = "entriesInVM";
  public static final String DISK_REGION_WRITE_IN_PROGRESS = "writesInProgress";


  /** Gateway Receiver Stats **/

  public static final String DUPLICATE_BATCHES_RECEIVED = "duplicateBatchesReceived";
  public static final String OUT_OF_ORDER_BATCHES_RECEIVED = "outoforderBatchesReceived";
  public static final String EARLY_ACKS = "earlyAcks";
  public static final String EVENTS_RECEIVED = "eventsReceived";
  public static final String CREAT_REQUESTS = "createRequests";
  public static final String UPDATE_REQUESTS = "updateRequest";
  public static final String DESTROY_REQUESTS = "destroyRequest";
  public static final String UNKNOWN_OPERATIONS_RECEIVED = "unknowsOperationsReceived";
  public static final String EXCEPTIONS_OCCURRED = "exceptionsOccurred";
  public static final String BATCH_PROCESS_TIME = "processBatchTime";
  public static final String TOTAL_BATCHES = "processBatchRequests";

  /** Gateway Sender Stats **/

  public static final String GATEWAYSENDER_EVENTS_RECEIVED = "eventsReceived";
  public static final String GATEWAYSENDER_EVENTS_QUEUED = "eventsQueued";
  public static final String GATEWAYSENDER_BATCHES_DISTRIBUTED = "batchesDistributed";
  public static final String GATEWAYSENDER_BATCHES_DISTRIBUTE_TIME = "batchDistributionTime";
  public static final String GATEWAYSENDER_TOTAL_BATCHES_REDISTRIBUTED = "batchesRedistributed";
  public static final String GATEWAYSENDER_TOTAL_BATCHES_WITH_INCOMPLETE_TRANSACTIONS =
      "batchesWithIncompleteTransactions";
  public static final String GATEWAYSENDER_EVENTS_QUEUED_CONFLATED = "eventsNotQueuedConflated";
  public static final String GATEWAYSENDER_EVENTS_EXCEEDING_ALERT_THRESHOLD =
      "eventsExceedingAlertThreshold";
  public static final String GATEWAYSENDER_LRU_EVICTIONS = "lruEvictions";
  public static final String GATEWAYSENDER_ENTRIES_OVERFLOWED_TO_DISK = "entriesOnlyOnDisk";
  public static final String GATEWAYSENDER_BYTES_OVERFLOWED_TO_DISK = "bytesOnlyOnDisk";

  public static final String GATEWAYSENDER_BYTES_IN_MEMORY = "byteCount";

  /** AsyncEventQueue Stats **/
  public static final String ASYNCEVENTQUEUE_EVENTS_QUEUE_SIZE = "eventQueueSize";

  /** LRU stats **/

  public static final String LRU_EVICTIONS = "lruEvictions";
  public static final String LRU_DESTROYS = "lruDestroys";


  /** VM Stats **/

  public static final String VM_STATS_MAX_MEMORY = "maxMemory";
  public static final String VM_STATS_OPEN_FDS = "fdsOpen";
  public static final String VM_STATS_FDS_LIMIT = "fdLimit";
  public static final String VM_NUM_PROCESSOR = "cpus";
  public static final String VM_PROCESS_CPU_TIME = "processCpuTime";
  public static final String VM_GC_STATS_COLLECTIONS = "collections";
  public static final String VM_GC_STATS_COLLECTION_TIME = "collectionTime";
  public static final String VM_STATS_NUM_THREADS = "threads";
  public static final String VM_THREAD_STATS_BLOCKED = "blocked";
  public static final String VM_THREAD_STATS_BLOCKED_TIME = "blockedTime";
  public static final String VM_THREAD_STATS_WAITED = "waited";
  public static final String VM_THREAD_STATS_WAITED_TIME = "waitedTime";

  public static final String VM_INIT_MEMORY = "initMemory";
  public static final String VM_MAX_MEMORY = "maxMemory";
  public static final String VM_USED_MEMORY = "usedMemory";
  public static final String VM_COMMITTED_MEMORY = "committedMemory";


  /** System Stats **/

  public static final String SYSTEM_CPU_ACTIVE = "cpuActive";

  public static final String SYSTEM_NUM_PROCESSOR = "cpus";

  public static final String LINUX_SYSTEM_PHYSICAL_MEMORY = "physicalMemory";
  public static final String LINUX_SYSTEM_FREE_MEMORY = "freeMemory";
  public static final String LINUX_SYSTEM_TOTAL_SWAP_SIZE = "allocatedSwap";
  public static final String LINUX_SYSTEM_FREE_SWAP_SIZE = "unallocatedSwap";
  public static final String LINUX_SYSTEM_LOAD_AVERAGE5 = "loadAverage5";

  // Sampler Stats
  public static final String JVM_PAUSES = "jvmPauses"; // int

}
