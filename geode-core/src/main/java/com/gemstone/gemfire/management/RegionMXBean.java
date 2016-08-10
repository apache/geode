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
package com.gemstone.gemfire.management;

import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.CacheWriter;
import com.gemstone.gemfire.cache.EvictionAlgorithm;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

/**
 * MBean that provides access to information and management functionality for a
 * local {@link Region}.
 * 
 * For all the latency related attributes e.g. PutRemoteLatency ,DiskWritesAverageLatency etc..
 * "enable-time-statistics" should be set to true. 
 * @since GemFire 7.0
 *
 */
@ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
public interface RegionMXBean {

  /**
   * Returns the name of the Region.
   */
  public String getName();
  
  
  /**
   * Returns the type (data policy) of the Region.
   */
  public String getRegionType();
  
  /**
   * Returns the full path of the Region.
   */
  public String getFullPath();

  /**
   * The name of the parent Region or <code>null</code> if the Region
   * has no parent.
   */
  public String getParentRegion();

  /**
   * Returns a list of the names of the sub regions.
   * 
   * @param recursive
   *          <code>True</code> to recursively traverse and find sub-regions.
   */
  public String[] listSubregionPaths(boolean recursive);

  /**
   * Returns the attributes of the Region.
   */
  public RegionAttributesData listRegionAttributes();

  /**
   * Returns the partition attributes of the Region.
   */
  public PartitionAttributesData listPartitionAttributes();
  

  /**
   * Returns the fixed partition attributes of the Region.
   */
  
  public FixedPartitionAttributesData[] listFixedPartitionAttributes();

  /**
   * Returns the eviction attributes of the Region.
   */
  public EvictionAttributesData listEvictionAttributes();

  /**
   * Returns the membership attributes of the Region.
   * 
   * @deprecated this API is scheduled to be removed
   */
  public MembershipAttributesData listMembershipAttributes();

  /**
   * Returns the time of the most recent modification.
   * For partitioned region it will be -1 . This feature is not supported for partitioned regions.
   */
  public long getLastModifiedTime();

  /**
   * Returns the time of the most recent access.
   * For partitioned region it will be -1. This feature is not supported for partitioned regions.
   */
  public long getLastAccessedTime();

  /**
   * Returns the number of times that a cache miss occurred.
   * For partitioned region it will be -1 . This feature is not supported for partitioned regions.
   */
  public long getMissCount();

  /**
   * Returns the number of times that a hit occurred.
   * For partitioned region it will be -1.This feature is not supported for partitioned regions.
   */
  public long getHitCount();

  /**
   * Returns the hit to miss ratio.
   * For partitioned region it will be -1 .This feature is not supported for partitioned regions.
   */
  public float getHitRatio();

  /**
   * Returns the number of entries in the Region within this member. For 
   * partitioned regions it will be the entry count for the primary buckets
   * hosted within this member.
   *
   */
  public long getEntryCount();

  /**
   * Returns the number of gets per second.
   */
  public float getGetsRate();

  /**
   * Returns the number of puts per second.
   */
  public float getPutsRate();

  /**
   * Returns the number of creates per second.
   */
  public float getCreatesRate();

  /**
   * Returns the number of destroys per second.
   */
  public float getDestroyRate();

  /**
   * Returns the number of putAlls per second.
   */
  public float getPutAllRate();

  /**
   * Returns the number of local puts per second.Only applicable for partitioned
   * regions.
   */
  public float getPutLocalRate();

  /**
   * Returns the number of remote puts per second.Only applicable for
   * partitioned regions.
   */
  public float getPutRemoteRate();

  /**
   * Returns the latency for the most recent remote put in nanoseconds.Only
   * applicable for partitioned regions.
   */
  public long getPutRemoteLatency();

  /**
   * Returns the average latency for remote puts in nanoseconds.Only applicable
   * for partitioned regions.
   */
  public long getPutRemoteAvgLatency();

  /**
   * Returns the current number of entries whose values are only on disk (not in 
   * memory). Entries may not exist in memory if they have been overflowed to
   * disk or not yet been faulted in after a recovery.
   */
  public long getTotalEntriesOnlyOnDisk();

  /**
   * Returns the current number of entries held in memory.
   */
  public long getTotalDiskEntriesInVM();


  /**
   * Returns the number of entries reads per second from disk.
   */
  public float getDiskReadsRate();
  
  
  /**
   * Returns the average latency of disk reads in nanoseconds
   * @deprecated See corresponding DiskStores latency to get the latency.
   */
  public long getDiskReadsAverageLatency();
  
  /**
   * Returns the average latency of disk writes in nanoseconds.
   * @deprecated See corresponding DiskStores latency to get the latency.
   */
  public long getDiskWritesAverageLatency();

  /**
   * Returns the number of entries written per second to disk.
   */
  public float getDiskWritesRate();

  /**
   * Returns the current number of disk writes in progress.
   */
  public long getTotalDiskWritesProgress();
  
  /**
   * Returns the current number of disk tasks (op-log compaction, asynchronous
   * recoveries, etc) that are waiting for a thread to run.
   * @deprecated
   */
  public long getDiskTaskWaiting();

  /**
   * Returns the average latency of a call to a {@link CacheWriter} in nanoseconds.
   */
  public long getCacheWriterCallsAvgLatency();

  /**
   * Returns the average latency of a call to a {@link CacheListener} in nanoseconds.
   */
  public long getCacheListenerCallsAvgLatency();

  /**
   * Returns the entry eviction rate as triggered by the LRU policy.
   */

  public float getLruEvictionRate();

  /**
   * Returns the rate of entries destroyed either by destroy cache operations or
   * eviction.
   */
  public float getLruDestroyRate();
  
  /**
   * Returns the number of buckets on this member. Only applicable for partitioned
   * regions.
   */
  public int getBucketCount();
  
  /**
   * Returns the number of primary buckets on this member. Only applicable for
   * partitioned regions.
   */
  public int getPrimaryBucketCount();

  /**
   * Returns the number of buckets without full redundancy. Only applicable for
   * partitioned regions.
   */
  public int getNumBucketsWithoutRedundancy();

  /**
   * Returns the number of redundant copies configured for this partitioned
   * region.Only applicable for partitioned regions.
   */
  public int getConfiguredRedundancy();

  /**
   * Returns the actual number of redundant copies available for buckets in this
   * partitioned region. Usually this is the number of redundant copies
   * configured for buckets in the region. However, during initialization or
   * error states the actual number of copies for any given bucket may be less
   * than the configured number. In that case, the value returned will be the
   * smallest number of redundant copies available for any single bucket.
   */
  public int getActualRedundancy();

  /**
   * Returns the total number of entries in all buckets. Only applicable for
   * partitioned regions.
   */
  public int getTotalBucketSize();

  /**
   * Returns the average number of entries in bucket. Only applicable for
   * partitioned regions.
   * @deprecated This attribute is removed from 8.0 for being incorrect and impacting 
     performance. 
   */
  public int getAvgBucketSize();
  
  
  /**
   * Returns the total number of bytes stored in disk for this region.
   */
  public long getDiskUsage();
  
  /**
   * Returns the aggregate entry size (in bytes) of all entries. This will
   * provide a correct value only if the eviction algorithm has been set to
   * {@link EvictionAlgorithm#LRU_MEMORY}.
   * 
   * For all partition regions it will show entry size in bytes.
   * It will also include size of all the secondary entries in the data store.
   * So while referring to size one should take redundancy into account
   */
  public long getEntrySize();

  /**
   * Returns whether this region sends data using a GatewaySender.
   */
  public boolean isGatewayEnabled();
  
  
  /**
   * Returns the average number of read requests per second.
   */
  public float getAverageReads();

  /**
   * Returns the average number of write requests per second. This include rates
   * of put,putAll & create operations on the region
   */
  public float getAverageWrites();
  
  /**
   * Returns whether persistence is enabled.
   */
  public boolean isPersistentEnabled();

  /**
   * Returns the name/ID of the member hosting this Region.
   */
  public String getMember();
  
  
  /**
   * Returns the maximum amount of local memory that can be used by the region.
   * This attribute is applicable for PartitionedRegion only. For other regions it will be -1
   */
  public int getLocalMaxMemory();
  
}
