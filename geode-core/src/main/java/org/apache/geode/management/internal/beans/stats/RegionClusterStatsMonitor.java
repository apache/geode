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

import java.util.HashMap;
import java.util.Map;

import org.apache.geode.management.EvictionAttributesData;
import org.apache.geode.management.internal.FederationComponent;

/**
 * Not only statistics we can set different attributes also
 *
 *
 */
public class RegionClusterStatsMonitor {

  private static final String CACHE_LISTENER_CALLS_AVG_LATENCY = "CacheListenerCallsAvgLatency";

  private static final String CACHE_WRITER_CALLS_AVG_LATENCY = "CacheWriterCallsAvgLatency";

  private static final String CREATES_RATE = "CreatesRate";

  private static final String DESTROY_RATE = "DestroyRate";

  private static final String DISK_READS_RATES = "DiskReadsRate";

  private static final String DISK_WRITES_RATES = "DiskWritesRate";

  private static final String GETS_RATE = "GetsRate";

  private static final String HIT_COUNT = "HitCount";

  private static final String MISS_COUNT = "MissCount";

  private static final String HIT_RATIO = "HitRatio";

  private static final String LAST_ACCESSED_TIME = "LastAccessedTime";

  private static final String LAST_MODIFIED_TIME = "LastModifiedTime";

  private static final String LRU_DESTROY_RATE = "LruDestroyRate";

  private static final String LRU_EVICTION_RATE = "LruEvictionRate";

  private static final String PUT_ALL_RATE = "PutAllRate";

  private static final String PUT_LOCAL_RATE = "PutLocalRate";

  private static final String PUT_REMOTE_AVG_LATENCY = "PutRemoteAvgLatency";

  private static final String PUT_REMOTE_LATENCY = "PutRemoteLatency";

  private static final String PUT_REMOTE_RATE = "PutRemoteRate";

  private static final String PUTS_RATE = "PutsRate";

  private static final String ENTRY_COUNT = "EntryCount";

  private static final String TOTAL_DISK_WRITES_IN_PROGRESS = "TotalDiskWritesProgress";

  private static final String TOTAL_BYTES_ON_DISK = "TotalBytesOnDisk";

  private static final String TOTAL_DISK_ENTRIES_IN_VM = "TotalDiskEntriesInVM";

  private static final String TOTAL_ENTRIES_ONLY_ON_DISK = "TotalEntriesOnlyOnDisk";

  private static final String AVERAGE_BUCKET_SIZE = "AvgBucketSize";

  private static final String BUCKET_COUNT = "BucketCount";

  private static final String NUM_BUCKESTS_WITHOUT_REDUNDANCY = "NumBucketsWithoutRedundancy";

  private static final String PRIMARY_BUCKET_COUNT = "PrimaryBucketCount";

  private static final String TOTAL_BUCKET_SIZE = "TotalBucketSize";

  private static final String DISK_TASK_WAITING = "DiskTaskWaiting";

  private static final String DISK_USAGE = "DiskUsage";

  private static final String AVERAGE_READS = "AverageReads";

  private static final String AVERAGE_WRITES = "AverageWrites";

  private static final String ENTRY_SIZE = "EntrySize";

  private static final String REGION_NAME = "Name";

  private static final String PARENT_REGION_NAME = "ParentRegion";

  private static final String REGION_TYPE = "RegionType";

  private static final String FULL_PATH = "FullPath";

  private static final String GATEWAY_ENABLED = "GatewayEnabled";

  private static final String PERSISTENT_ENABLED = "PersistentEnabled";

  private volatile long lastAccessedTime = 0;

  private volatile long lastModifiedTime = 0;

  private String regionName;

  private String parentRegion;

  // this is actually the data policy of the region
  private String regionType;

  private String fullPath;

  private Boolean gatewayEnabled;

  private Boolean persistentEnabled;

  private long entryCount = 0;

  private volatile int numBucketsWithoutRedundancy = 0;

  /**
   * Eviction attributes
   */
  private EvictionAttributesData evictionAttributesData; // unused

  private final StatsAggregator aggregator;

  private final Map<String, Class<?>> typeMap;

  public void aggregate(FederationComponent newState, FederationComponent oldState) {
    aggregator.aggregate(newState, oldState);
    incLastAccessedTime(newState, oldState);
    incLastModifiedTime(newState, oldState);
    incNumBucketsWithoutRedundancy(newState, oldState);
    updateEntryCount(newState, oldState);
    setFixedAttributes(newState, oldState);
  }

  public RegionClusterStatsMonitor() {
    typeMap = new HashMap<String, Class<?>>();
    intTypeMap();
    aggregator = new StatsAggregator(typeMap);
  }

  private void intTypeMap() {
    typeMap.put(CACHE_LISTENER_CALLS_AVG_LATENCY, Long.TYPE);
    typeMap.put(CACHE_WRITER_CALLS_AVG_LATENCY, Long.TYPE);
    typeMap.put(CREATES_RATE, Float.TYPE);
    typeMap.put(DESTROY_RATE, Float.TYPE);
    typeMap.put(DISK_READS_RATES, Float.TYPE);
    typeMap.put(DISK_WRITES_RATES, Float.TYPE);
    typeMap.put(GETS_RATE, Float.TYPE);
    typeMap.put(HIT_COUNT, Long.TYPE);
    typeMap.put(MISS_COUNT, Long.TYPE);
    typeMap.put(HIT_RATIO, Float.TYPE);
    typeMap.put(LRU_DESTROY_RATE, Float.TYPE);
    typeMap.put(LRU_EVICTION_RATE, Float.TYPE);
    typeMap.put(PUT_ALL_RATE, Float.TYPE);
    typeMap.put(PUT_LOCAL_RATE, Float.TYPE);
    typeMap.put(PUT_REMOTE_AVG_LATENCY, Long.TYPE);
    typeMap.put(PUT_REMOTE_LATENCY, Long.TYPE);
    typeMap.put(PUT_REMOTE_RATE, Float.TYPE);
    typeMap.put(PUTS_RATE, Float.TYPE);
    typeMap.put(TOTAL_DISK_WRITES_IN_PROGRESS, Long.TYPE);
    typeMap.put(TOTAL_BYTES_ON_DISK, Long.TYPE);
    typeMap.put(TOTAL_DISK_ENTRIES_IN_VM, Long.TYPE);
    typeMap.put(TOTAL_ENTRIES_ONLY_ON_DISK, Long.TYPE);
    typeMap.put(ENTRY_COUNT, Long.TYPE);

    typeMap.put(AVERAGE_BUCKET_SIZE, Integer.TYPE);
    typeMap.put(BUCKET_COUNT, Integer.TYPE);
    typeMap.put(PRIMARY_BUCKET_COUNT, Integer.TYPE);
    typeMap.put(TOTAL_BUCKET_SIZE, Integer.TYPE);
    typeMap.put(DISK_TASK_WAITING, Long.TYPE);
    typeMap.put(DISK_USAGE, Long.TYPE);
    typeMap.put(AVERAGE_READS, Float.TYPE);
    typeMap.put(AVERAGE_WRITES, Float.TYPE);
    typeMap.put(ENTRY_SIZE, Long.TYPE);

  }

  private void incLastAccessedTime(FederationComponent newState, FederationComponent oldState) {
    if (newState != null) {
      if (newState.getValue(LAST_ACCESSED_TIME) != null) {
        lastAccessedTime = (Long) newState.getValue(LAST_ACCESSED_TIME);
      }

    }
  }

  private void incNumBucketsWithoutRedundancy(FederationComponent newState,
      FederationComponent oldState) {
    if (newState != null) {
      if (newState.getValue(NUM_BUCKESTS_WITHOUT_REDUNDANCY) != null) {
        numBucketsWithoutRedundancy = (Integer) newState.getValue(NUM_BUCKESTS_WITHOUT_REDUNDANCY);
      }

    }
  }

  private void incLastModifiedTime(FederationComponent newState, FederationComponent oldState) {
    if (newState != null) {
      if (newState.getValue(LAST_MODIFIED_TIME) != null) {
        lastModifiedTime = (Long) newState.getValue(LAST_MODIFIED_TIME);
      }

    }
  }

  private void updateEntryCount(FederationComponent newState, FederationComponent oldState) {
    if (newState != null) {
      if (newState.getValue(ENTRY_COUNT) != null) {
        entryCount = (Long) newState.getValue(ENTRY_COUNT);
      }

    }
  }

  public long getCacheListenerCallsAvgLatency() {
    return aggregator.getLongValue(CACHE_LISTENER_CALLS_AVG_LATENCY);
  }

  public long getCacheWriterCallsAvgLatency() {
    return aggregator.getLongValue(CACHE_WRITER_CALLS_AVG_LATENCY);
  }

  public float getCreatesRate() {
    return aggregator.getFloatValue(CREATES_RATE);
  }

  public float getDestroyRate() {
    return aggregator.getFloatValue(DESTROY_RATE);
  }

  public float getDiskReadsRate() {
    return aggregator.getFloatValue(DISK_READS_RATES);
  }

  public float getDiskWritesRate() {
    return aggregator.getFloatValue(DISK_WRITES_RATES);
  }

  public float getGetsRate() {
    return aggregator.getFloatValue(GETS_RATE);
  }

  public long getHitCount() {
    return aggregator.getLongValue(HIT_COUNT);
  }

  public float getHitRatio() {
    return aggregator.getFloatValue(HIT_COUNT);
  }

  public long getLastAccessedTime() {
    return lastAccessedTime;
  }

  public long getLastModifiedTime() {
    return lastModifiedTime;
  }

  public float getLruDestroyRate() {
    return aggregator.getFloatValue(LRU_DESTROY_RATE);
  }

  public float getLruEvictionRate() {
    return aggregator.getFloatValue(LRU_EVICTION_RATE);
  }

  public long getMissCount() {
    return aggregator.getLongValue(MISS_COUNT);
  }

  public float getPutAllRate() {
    return aggregator.getFloatValue(PUT_ALL_RATE);
  }

  public float getPutLocalRate() {
    return aggregator.getFloatValue(PUT_LOCAL_RATE);
  }

  public long getPutRemoteAvgLatency() {
    return aggregator.getLongValue(PUT_REMOTE_AVG_LATENCY);
  }

  public long getPutRemoteLatency() {
    return aggregator.getLongValue(PUT_REMOTE_LATENCY);
  }

  public float getPutRemoteRate() {
    return aggregator.getFloatValue(PUT_REMOTE_RATE);
  }

  public float getPutsRate() {
    return aggregator.getFloatValue(PUTS_RATE);
  }

  public long getSystemRegionEntryCount() {
    return aggregator.getLongValue(ENTRY_COUNT);
  }

  public long getTotalDiskWritesProgress() {
    return aggregator.getLongValue(TOTAL_DISK_WRITES_IN_PROGRESS);
  }

  public long getTotalBytesOnDisk() {
    return aggregator.getLongValue(TOTAL_BYTES_ON_DISK);
  }

  public long getTotalDiskEntriesInVM() {
    return aggregator.getLongValue(TOTAL_DISK_ENTRIES_IN_VM);
  }

  public long getTotalEntriesOnlyOnDisk() {
    return aggregator.getLongValue(TOTAL_ENTRIES_ONLY_ON_DISK);
  }

  public int getAvgBucketSize() {
    int bucketNum = getBucketCount();
    if (bucketNum > 0) {
      return getTotalBucketSize() / bucketNum;
    } else {
      return 0;
    }
  }

  public int getBucketCount() {
    return aggregator.getIntValue(BUCKET_COUNT);
  }

  public int getNumBucketsWithoutRedundancy() {
    return numBucketsWithoutRedundancy;
  }

  public int getPrimaryBucketCount() {
    return aggregator.getIntValue(PRIMARY_BUCKET_COUNT);
  }

  public int getTotalBucketSize() {
    return aggregator.getIntValue(TOTAL_BUCKET_SIZE);
  }

  public long getDiskTaskWaiting() {
    return aggregator.getLongValue(DISK_TASK_WAITING);
  }

  public long getDiskUsage() {
    return aggregator.getLongValue(DISK_USAGE);
  }

  public float getAverageReads() {
    return aggregator.getFloatValue(AVERAGE_READS);
  }

  public float getAverageWrites() {
    return aggregator.getFloatValue(AVERAGE_WRITES);
  }

  public long getEntrySize() {
    return aggregator.getLongValue(ENTRY_SIZE);
  }

  private void setFixedAttributes(FederationComponent newState, FederationComponent oldState) {
    if (regionName == null) {
      if (newState != null) {
        if (newState.getValue(REGION_NAME) != null) {
          regionName = (String) newState.getValue(REGION_NAME);
        }

      }
    }
    if (parentRegion == null) {
      if (newState != null) {
        if (newState.getValue(PARENT_REGION_NAME) != null) {
          parentRegion = (String) newState.getValue(PARENT_REGION_NAME);
        }

      }
    }

    if (regionType == null) {
      if (newState != null) {
        if (newState.getValue(REGION_TYPE) != null) {
          regionType = (String) newState.getValue(REGION_TYPE);
        }

      }
    }

    if (fullPath == null) {
      if (newState != null) {
        if (newState.getValue(FULL_PATH) != null) {
          fullPath = (String) newState.getValue(FULL_PATH);
        }

      }
    }
    if (gatewayEnabled == null) {
      if (newState != null) {
        if (newState.getValue(GATEWAY_ENABLED) != null) {
          gatewayEnabled = (Boolean) newState.getValue(GATEWAY_ENABLED);
        }

      }

    }

    if (persistentEnabled == null) {
      if (newState != null) {
        if (newState.getValue(PERSISTENT_ENABLED) != null) {
          persistentEnabled = (Boolean) newState.getValue(PERSISTENT_ENABLED);
        }

      }

    }
  }

  public String getName() {
    return regionName;
  }

  public String getParentRegion() {
    return parentRegion;
  }

  // returns the data policy of the region
  public String getRegionType() {
    return regionType;
  }

  public String getFullPath() {
    return fullPath;
  }

  public boolean isGatewayEnabled() {
    return gatewayEnabled;
  }

  public boolean isPersistentEnabled() {
    return persistentEnabled;
  }

  public long getEntryCount() {
    return entryCount;
  }

}
