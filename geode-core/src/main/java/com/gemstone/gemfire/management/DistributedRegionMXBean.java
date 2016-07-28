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
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

/**
 * MBean that provides access to information and management functionality for a
 * {@link Region}.
 * 
 * @since GemFire 7.0
 * 
 */
@ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
public interface DistributedRegionMXBean {

  /**
   * Returns the name of the Region.
   */
  public String getName();

  /**
   * Returns the number of members hosting/using the Region.
   */
  public int getMemberCount();

  /**
   * Returns a list of names/IDs of the members hosting the Region.
   */
  public String[] getMembers();

  /**
   * Returns the type (data policy) of the Region.
   */
  public String getRegionType();

  /**
   * Returns the full path of the Region.
   */
  public String getFullPath();

  /**
   * Returns the name of the parent Region.
   */
  public String getParentRegion();

  /**
   * Returns a list of the names of the subregions.
   * 
   * @param recursive
   *          <code>true</code> to recursively traverse and find sub-regions.
   */
  public String[] listSubRegionPaths(boolean recursive);

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
  public FixedPartitionAttributesData[] listFixedPartitionAttributesData();

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
   * For partitioned region it will be -1. This feature is not supported for partitioned regions.
   */
  public long getLastModifiedTime();

  /**
   * Returns the time of the most recent access.
   * For partitioned region it will be -1. This feature is not supported for partitioned regions.
   */
  public long getLastAccessedTime();

  /**
   * Returns the number of times that a cache miss occurred.
   * For partitioned region it will be -1. This feature is not supported for partitioned regions.
   */
  public long getMissCount();

  /**
   * Returns the number of times that a hit occurred.
   * For partitioned region it will be -1. This feature is not supported for partitioned regions.
   */
  public long getHitCount();

  /**
   * Returns the hit to miss ratio.
   * For partitioned region it will be -1. This feature is not supported for partitioned regions.
   */
  public float getHitRatio();


  /**
   * Returns the number of entries in the Region.
   * 
   */
  public long getSystemRegionEntryCount();

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
   * Returns the number of local puts per second.
   */
  public float getPutLocalRate();

  /**
   * Returns the number of remote puts per second.
   */
  public float getPutRemoteRate();

  /**
   * Returns the latency for the most recent remote put.
   */
  public long getPutRemoteLatency();

  /**
   * Returns the average latency for remote puts.
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
   * Returns the number of entries per second for all disks.
   */
  public float getDiskReadsRate();

  /**
   * Returns the number of entries per second for all disks.
   */
  public float getDiskWritesRate();  
  
  /**
   * Returns the current number of disk tasks (op-log compaction, asynchronous
   * recoveries, etc) that are waiting for a thread to run.
   * @deprecated
   */
  public long getDiskTaskWaiting();

  /**
   * Returns the current number of disk writes in progress.
   */
  public long getTotalDiskWritesProgress();

  /**
   * Returns the average latency of a call to a {@link CacheWriter}.
   */
  public long getCacheWriterCallsAvgLatency();

  /**
   * Returns the average latency of a call to a {@link CacheListener}.
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
   * Returns the total number of entries in all buckets. Only applicable for
   * partitioned regions.
   */
  public int getTotalBucketSize();

  /**
   * Returns the average number of entries in bucket. Only applicable for
   * partitioned regions.
   */
  public int getAvgBucketSize();
  
  /**
   * Returns the total number of bytes used by all disks.
   */
  public long getDiskUsage();
  
  /**
   * Returns the average number of read requests per second.
   */
  public float getAverageReads();

  /**
   * Returns the average number of write requests per second.
   */
  public float getAverageWrites();
  
  /**
   * Returns whether the Region sends data to a {@link GatewaySender}.
   * 
   * @return True if the Region sends data, false otherwise.
   */
  public boolean isGatewayEnabled();

  /**
   * Returns whether persistence is enabled.
   * 
   * @return True if persistence is enabled, false otherwise.
   */
  public boolean isPersistentEnabled();
  
  /**
   * Returns the aggregate entry size (in megabytes) of all entries. This will
   * provide a correct value only if the eviction algorithm has been set to
   * {@link EvictionAlgorithm#LRU_MEMORY}.
   */
  public long getEntrySize();

  /**
   * Returns the number of members whose entry count is 0.
   */
  public int getEmptyNodes();
}
