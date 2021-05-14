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
package org.apache.geode.management;

import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

/**
 * MBean that provides access to information and management functionality for a {@link Region}.
 *
 * @since GemFire 7.0
 *
 */
@ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
public interface DistributedRegionMXBean {

  /**
   * Returns the name of the Region.
   */
  String getName();

  /**
   * Returns the number of members hosting/using the Region.
   */
  int getMemberCount();

  /**
   * Returns a list of names/IDs of the members hosting the Region.
   */
  String[] getMembers();

  /**
   * Returns the type (data policy) of the Region.
   * CreateRegionCommand will use this attribute
   */
  String getRegionType();

  /**
   * Returns the full path of the Region.
   */
  String getFullPath();

  /**
   * Returns the name of the parent Region.
   */
  String getParentRegion();

  /**
   * Returns a list of the names of the subregions.
   *
   * @param recursive <code>true</code> to recursively traverse and find sub-regions.
   */
  String[] listSubRegionPaths(boolean recursive);

  /**
   * Returns the attributes of the Region.
   */
  RegionAttributesData listRegionAttributes();

  /**
   * Returns the partition attributes of the Region.
   */
  PartitionAttributesData listPartitionAttributes();

  /**
   * Returns the fixed partition attributes of the Region.
   */
  FixedPartitionAttributesData[] listFixedPartitionAttributesData();

  /**
   * Returns the eviction attributes of the Region.
   */
  EvictionAttributesData listEvictionAttributes();

  /**
   * Returns the membership attributes of the Region.
   *
   * @deprecated this API is scheduled to be removed
   */
  MembershipAttributesData listMembershipAttributes();

  /**
   * Returns the time of the most recent modification. For partitioned region it will be -1. This
   * feature is not supported for partitioned regions.
   */
  long getLastModifiedTime();

  /**
   * Returns the time of the most recent access. For partitioned region it will be -1. This feature
   * is not supported for partitioned regions.
   */
  long getLastAccessedTime();

  /**
   * Returns the number of times that a cache miss occurred. For partitioned region it will be -1.
   * This feature is not supported for partitioned regions.
   */
  long getMissCount();

  /**
   * Returns the number of times that a hit occurred. For partitioned region it will be -1. This
   * feature is not supported for partitioned regions.
   */
  long getHitCount();

  /**
   * Returns the hit to miss ratio. For partitioned region it will be -1. This feature is not
   * supported for partitioned regions.
   */
  float getHitRatio();


  /**
   * Returns the number of entries in the Region.
   *
   */
  long getSystemRegionEntryCount();

  /**
   * Returns the number of gets per second.
   */
  float getGetsRate();

  /**
   * Returns the number of puts per second.
   */
  float getPutsRate();

  /**
   * Returns the number of creates per second.
   */
  float getCreatesRate();

  /**
   * Returns the number of destroys per second.
   */
  float getDestroyRate();

  /**
   * Returns the number of putAlls per second.
   */
  float getPutAllRate();

  /**
   * Returns the number of local puts per second.
   */
  float getPutLocalRate();

  /**
   * Returns the number of remote puts per second.
   */
  float getPutRemoteRate();

  /**
   * Returns the latency for the most recent remote put.
   */
  long getPutRemoteLatency();

  /**
   * Returns the average latency for remote puts.
   */
  long getPutRemoteAvgLatency();

  /**
   * Returns the current number of entries whose values are only on disk (not in memory). Entries
   * may not exist in memory if they have been overflowed to disk or not yet been faulted in after a
   * recovery.
   */
  long getTotalEntriesOnlyOnDisk();

  /**
   * Returns the current number of entries held in memory.
   */
  long getTotalDiskEntriesInVM();

  /**
   * Returns the number of entries per second for all disks.
   */
  float getDiskReadsRate();

  /**
   * Returns the number of entries per second for all disks.
   */
  float getDiskWritesRate();

  /**
   * Returns the current number of disk tasks (op-log compaction, asynchronous recoveries, etc) that
   * are waiting for a thread to run.
   *
   * @deprecated
   */
  long getDiskTaskWaiting();

  /**
   * Returns the current number of disk writes in progress.
   */
  long getTotalDiskWritesProgress();

  /**
   * Returns the average latency of a call to a {@link CacheWriter}.
   */
  long getCacheWriterCallsAvgLatency();

  /**
   * Returns the average latency of a call to a {@link CacheListener}.
   */
  long getCacheListenerCallsAvgLatency();

  /**
   * Returns the entry eviction rate as triggered by the LRU policy.
   */

  float getLruEvictionRate();

  /**
   * Returns the rate of entries destroyed either by destroy cache operations or eviction.
   */
  float getLruDestroyRate();

  /**
   * Returns the number of buckets on this member. Only applicable for partitioned regions.
   */
  int getBucketCount();

  /**
   * Returns the number of primary buckets on this member. Only applicable for partitioned regions.
   */
  int getPrimaryBucketCount();

  /**
   * Returns the number of buckets without full redundancy. Only applicable for partitioned regions.
   */
  int getNumBucketsWithoutRedundancy();

  /**
   * Returns the total number of entries in all buckets. Only applicable for partitioned regions.
   */
  int getTotalBucketSize();

  /**
   * Returns the average number of entries in bucket. Only applicable for partitioned regions.
   */
  int getAvgBucketSize();

  /**
   * Returns the total number of bytes used by all disks.
   */
  long getDiskUsage();

  /**
   * Returns the average number of read requests per second.
   */
  float getAverageReads();

  /**
   * Returns the average number of write requests per second.
   */
  float getAverageWrites();

  /**
   * Returns whether the Region sends data to a {@link GatewaySender}.
   *
   * @return True if the Region sends data, false otherwise.
   */
  boolean isGatewayEnabled();

  /**
   * Returns whether persistence is enabled.
   *
   * @return True if persistence is enabled, false otherwise.
   */
  boolean isPersistentEnabled();

  /**
   * Returns the aggregate entry size (in bytes) of all entries. 
   * For replicated regions, provides a value only if the eviction algorithm
   * is set to {@link EvictionAlgorithm#LRU_MEMORY}.
   * The entry size in this distributed context represents the sum total of memory
   * used for data in the region across all members, so the reported value will
   * include any redundant copies.
   *
   * All partitioned regions can report entry size, but the value also includes
   * redundant entries and also counts the size of all the secondary entries in
   * the node.
   */
  long getEntrySize();

  /**
   * Returns the number of members whose entry count is 0.
   */
  int getEmptyNodes();
}
