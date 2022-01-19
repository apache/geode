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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.geode.management.internal.FederationComponent;

public class MemberClusterStatsMonitor {


  private static final String SYSTEM_DISK_STORE_COUNT = "DiskStores";

  private static final String DISK_READS_RATE = "DiskReadsRate";

  private static final String DISK_WRITES_RATE = "DiskWritesRate";

  private static final String DISK_FLUSH_AVG_LATENCY = "DiskFlushAvgLatency";

  private static final String TOTAL_BACKUP_IN_PROGRESS = "TotalBackupInProgress";

  private static final String TOTAL_HIT_COUNT = "TotalHitCount";

  private static final String TOTAL_MISS_COUNT = "TotalMissCount";

  private static final String NUM_INITIAL_IMAGE_IN_PROGRESSS = "numInitialImagesInProgress";

  private static final String TOTAL_DISK_USAGE = "TotalDiskUsage";

  private static final String NUM_RUNNING_FUNCTIONS = "NumRunningFunctions";

  private static final String AVERAGE_READS = "AverageReads";

  private static final String AVERAGE_WRITES = "AverageWrites";

  private static final String GARBAGE_COLL_COUNT = "GarbageCollectionCount";

  private static final String JVM_PAUSES = "JVMPauses";

  private static final String OFF_HEAP_MAX_MEMORY = "OffHeapMaxMemory";

  private static final String OFF_HEAP_USED_MEMORY = "OffHeapUsedMemory";

  private static final String OFF_HEAP_FREE_MEMORY = "OffHeapFreeMemory";

  private static final String TXN_COMMITTED_TOTAL_COUNT = "TransactionCommittedTotalCount";

  private static final String TXN_ROLLEDBACK_TOTAL_COUNT = "TransactionRolledBackTotalCount";

  private static final String MAX_MEMORY = "MaxMemory";

  private static final String USED_MEMORY = "UsedMemory";

  private static final String FREE_MEMORY = "FreeMemory";

  private final AtomicInteger systemDiskStoreCount = new AtomicInteger(0);

  private final StatsAggregator aggregator;

  private final IntegerStatsDeltaAggregator deltas;

  private final Map<String, Class<?>> typeMap;

  public void aggregate(FederationComponent newState, FederationComponent oldState) {
    aggregator.aggregate(newState, oldState);
    // Special Aggregations
    incSystemDiskStoreCount(newState, oldState);
    deltas.aggregate(newState, oldState);
  }

  public MemberClusterStatsMonitor() {
    typeMap = new HashMap<>();
    intTypeMap();
    aggregator = new StatsAggregator(typeMap);

    List<String> keysList = new ArrayList<>();
    keysList.add(TXN_COMMITTED_TOTAL_COUNT);
    keysList.add(TXN_ROLLEDBACK_TOTAL_COUNT);
    deltas = new IntegerStatsDeltaAggregator(keysList);
  }

  private void intTypeMap() {
    typeMap.put(DISK_FLUSH_AVG_LATENCY, Long.TYPE);
    typeMap.put(DISK_READS_RATE, Float.TYPE);
    typeMap.put(DISK_WRITES_RATE, Float.TYPE);
    typeMap.put(TOTAL_BACKUP_IN_PROGRESS, Integer.TYPE);
    typeMap.put(TOTAL_HIT_COUNT, Integer.TYPE);
    typeMap.put(TOTAL_MISS_COUNT, Integer.TYPE);
    typeMap.put(NUM_INITIAL_IMAGE_IN_PROGRESSS, Integer.TYPE);
    typeMap.put(TOTAL_DISK_USAGE, Long.TYPE);
    typeMap.put(NUM_RUNNING_FUNCTIONS, Integer.TYPE);
    typeMap.put(AVERAGE_READS, Float.TYPE);
    typeMap.put(AVERAGE_WRITES, Float.TYPE);
    typeMap.put(GARBAGE_COLL_COUNT, Long.TYPE);
    typeMap.put(JVM_PAUSES, Long.TYPE);
    typeMap.put(MAX_MEMORY, Long.TYPE);
    typeMap.put(USED_MEMORY, Long.TYPE);
    typeMap.put(FREE_MEMORY, Long.TYPE);
    typeMap.put(OFF_HEAP_MAX_MEMORY, Long.TYPE);
    typeMap.put(OFF_HEAP_USED_MEMORY, Long.TYPE);
    typeMap.put(OFF_HEAP_FREE_MEMORY, Long.TYPE);
  }

  public float getDiskReadsRate() {
    return aggregator.getFloatValue(DISK_READS_RATE);
  }

  public float getDiskWritesRate() {
    return aggregator.getFloatValue(DISK_WRITES_RATE);
  }

  public long getDiskFlushAvgLatency() {
    return aggregator.getLongValue(DISK_FLUSH_AVG_LATENCY);
  }

  public int getSystemDiskStoreCount() {
    return systemDiskStoreCount.get();
  }

  public int getTotalBackupInProgress() {
    return aggregator.getIntValue(TOTAL_BACKUP_IN_PROGRESS);
  }

  public long getTotalHeapSize() {
    return getMaxMemory();
  }

  public int getTotalHitCount() {
    return aggregator.getIntValue(TOTAL_HIT_COUNT);
  }

  public int getTotalMissCount() {
    return aggregator.getIntValue(TOTAL_MISS_COUNT);
  }

  public int getNumInitialImagesInProgress() {
    return aggregator.getIntValue(NUM_INITIAL_IMAGE_IN_PROGRESSS);
  }

  public void incSystemDiskStoreCount(FederationComponent newState, FederationComponent oldState) {
    if (oldState != null) {
      if (oldState.getValue(SYSTEM_DISK_STORE_COUNT) != null) {
        String[] diskStores = (String[]) oldState.getValue(SYSTEM_DISK_STORE_COUNT);
        if (diskStores != null) {
          systemDiskStoreCount.addAndGet(-diskStores.length);// Used Atomic
                                                             // Integer to avoid
                                                             // race condition
                                                             // between
                                                             // different
                                                             // members

        }

      }

    }
    if (newState != null) {
      if (newState.getValue(SYSTEM_DISK_STORE_COUNT) != null) {
        String[] diskStores = (String[]) newState.getValue(SYSTEM_DISK_STORE_COUNT);
        if (diskStores != null) {
          systemDiskStoreCount.addAndGet(diskStores.length);// Used Atomic
                                                            // Integer to
                                                            // avoid race
                                                            // condition
                                                            // between
                                                            // different
                                                            // members

        }
      }
    }

  }


  public int getNumRunningFunctions() {
    return aggregator.getIntValue(NUM_RUNNING_FUNCTIONS);
  }

  public long getTotalDiskUsage() {
    return aggregator.getLongValue(TOTAL_DISK_USAGE);
  }

  public float getAverageReads() {
    return aggregator.getFloatValue(AVERAGE_READS);
  }

  public float getAverageWrites() {
    return aggregator.getFloatValue(AVERAGE_WRITES);
  }

  public long getUsedHeapSize() {
    return getUsedMemory();
  }

  public long getGarbageCollectionCount() {
    return aggregator.getLongValue(GARBAGE_COLL_COUNT);
  }

  public long getJVMPauses() {
    return aggregator.getLongValue(JVM_PAUSES);
  }

  public long getOffHeapFreeMemory() {
    return aggregator.getLongValue(OFF_HEAP_FREE_MEMORY);
  }

  public long getOffHeapUsedMemory() {
    return aggregator.getLongValue(OFF_HEAP_USED_MEMORY);
  }

  public int getTransactionCommitted() {
    return deltas.getDelta(TXN_COMMITTED_TOTAL_COUNT);
  }

  public int getTransactionRolledBack() {
    return deltas.getDelta(TXN_ROLLEDBACK_TOTAL_COUNT);
  }

  public long getMaxMemory() {
    return aggregator.getLongValue(MAX_MEMORY);
  }

  public long getFreeMemory() {
    return aggregator.getLongValue(FREE_MEMORY);
  }

  public long getUsedMemory() {
    return aggregator.getLongValue(USED_MEMORY);
  }
}
