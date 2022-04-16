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
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

/**
 * MBean that provides access to information and management functionality for a local
 * {@link Region}.
 *
 * For all the latency related attributes e.g. PutRemoteLatency ,DiskWritesAverageLatency etc..
 * "enable-time-statistics" should be set to true.
 *
 * @since GemFire 7.0
 *
 */
@ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
public interface RegionMXBean {

  /**
   * Returns the name of the Region.
   *
   * @return the name of the Region
   */
  String getName();


  /**
   * Returns the type (data policy) of the Region.
   *
   * @return the type (data policy) of the Region
   */
  String getRegionType();

  /**
   * Returns the full path of the Region.
   *
   * @return the full path of the Region
   */
  String getFullPath();

  /**
   * The name of the parent Region or <code>null</code> if the Region has no parent.
   *
   * @return name of the parent Region or <code>null</code> if the Region has no parent
   */
  String getParentRegion();

  /**
   * Returns a list of the names of the sub regions.
   *
   * @param recursive <code>True</code> to recursively traverse and find sub-regions.
   * @return a list of the names of the sub regions
   */
  String[] listSubregionPaths(boolean recursive);

  /**
   * Returns the attributes of the Region.
   *
   * @return the attributes of the Region
   */
  RegionAttributesData listRegionAttributes();

  /**
   * Returns the partition attributes of the Region.
   *
   * @return the partition attributes of the Region
   */
  PartitionAttributesData listPartitionAttributes();


  /**
   * Returns the fixed partition attributes of the Region.
   *
   * @return the fixed partition attributes of the Region
   */

  FixedPartitionAttributesData[] listFixedPartitionAttributes();

  /**
   * Returns the eviction attributes of the Region.
   *
   * @return the eviction attributes of the Region
   */
  EvictionAttributesData listEvictionAttributes();

  /**
   * Returns the membership attributes of the Region.
   *
   * @return the membership attributes of the Region
   *
   * @deprecated this API is scheduled to be removed
   */
  @Deprecated
  MembershipAttributesData listMembershipAttributes();

  /**
   * Returns the time of the most recent modification. For partitioned region it will be -1 . This
   * feature is not supported for partitioned regions.
   *
   * @return the time of the most recent modification
   */
  long getLastModifiedTime();

  /**
   * Returns the time of the most recent access. For partitioned region it will be -1. This feature
   * is not supported for partitioned regions.
   *
   * @return the time of the most recent access
   */
  long getLastAccessedTime();

  /**
   * Returns the number of times that a cache miss occurred. For partitioned region it will be -1 .
   * This feature is not supported for partitioned regions.
   *
   * @return the number of times that a cache miss occurred
   */
  long getMissCount();

  /**
   * Returns the number of times that a hit occurred. For partitioned region it will be -1.This
   * feature is not supported for partitioned regions.
   *
   * @return the number of times that a hit occurred
   */
  long getHitCount();

  /**
   * Returns the hit to miss ratio. For partitioned region it will be -1 .This feature is not
   * supported for partitioned regions.
   *
   * @return the hit to miss ratio
   */
  float getHitRatio();

  /**
   * Returns the number of entries in the Region within this member. For partitioned regions it will
   * be the entry count for the primary buckets hosted within this member.
   *
   * @return the number of entries in the Region within this member
   */
  long getEntryCount();

  /**
   * Returns the number of gets per second.
   *
   * @return the number of gets per second
   */
  float getGetsRate();

  /**
   * Returns the number of puts per second.
   *
   * @return the number of puts per second
   */
  float getPutsRate();

  /**
   * Returns the number of creates per second.
   *
   * @return the number of creates per second
   */
  float getCreatesRate();

  /**
   * Returns the number of destroys per second.
   *
   * @return the number of destroys per second
   */
  float getDestroyRate();

  /**
   * Returns the number of putAlls per second.
   *
   * @return the number of putAlls per second
   */
  float getPutAllRate();

  /**
   * Returns the number of local puts per second. Only applicable for partitioned regions.
   *
   * @return the number of local puts per second
   */
  float getPutLocalRate();

  /**
   * Returns the number of remote puts per second. Only applicable for partitioned regions.
   *
   * @return the number of remote puts per second
   */
  float getPutRemoteRate();

  /**
   * Returns the latency for the most recent remote put in nanoseconds. Only applicable for
   * partitioned regions.
   *
   * @return the latency for the most recent remote put in nanoseconds
   */
  long getPutRemoteLatency();

  /**
   * Returns the average latency for remote puts in nanoseconds. Only applicable for partitioned
   * regions.
   *
   * @return the average latency for remote puts in nanoseconds
   */
  long getPutRemoteAvgLatency();

  /**
   * Returns the current number of entries whose values are only on disk (not in memory). Entries
   * may not exist in memory if they have been overflowed to disk or not yet been faulted in after a
   * recovery.
   *
   * @return the current number of entries whose values are only on disk
   */
  long getTotalEntriesOnlyOnDisk();

  /**
   * Returns the current number of entries held in memory.
   *
   * @return the current number of entries held in memory
   */
  long getTotalDiskEntriesInVM();


  /**
   * Returns the number of entries reads per second from disk.
   *
   * @return the number of entries reads per second from disk
   */
  float getDiskReadsRate();


  /**
   * Returns the average latency of disk reads in nanoseconds
   *
   * @return the average latency of disk reads in nanoseconds
   *
   * @deprecated See corresponding DiskStores latency to get the latency.
   */
  @Deprecated
  long getDiskReadsAverageLatency();

  /**
   * Returns the average latency of disk writes in nanoseconds.
   *
   * @return the average latency of disk writes in nanoseconds
   *
   * @deprecated See corresponding DiskStores latency to get the latency.
   */
  @Deprecated
  long getDiskWritesAverageLatency();

  /**
   * Returns the number of entries written per second to disk.
   *
   * @return the number of entries written per second to disk
   */
  float getDiskWritesRate();

  /**
   * Returns the current number of disk writes in progress.
   *
   * @return the current number of disk writes in progress
   */
  long getTotalDiskWritesProgress();

  /**
   * Returns the current number of disk tasks (op-log compaction, asynchronous recoveries, etc) that
   * are waiting for a thread to run.
   *
   * @return the current number of disk tasks that are waiting for a thread to run
   *
   * @deprecated
   */
  @Deprecated
  long getDiskTaskWaiting();

  /**
   * Returns the average latency of a call to a {@link CacheWriter} in nanoseconds.
   *
   * @return the average latency of a call to a {@link CacheWriter} in nanoseconds
   */
  long getCacheWriterCallsAvgLatency();

  /**
   * Returns the average latency of a call to a {@link CacheListener} in nanoseconds.
   *
   * @return the average latency of a call to a {@link CacheListener} in nanoseconds
   */
  long getCacheListenerCallsAvgLatency();

  /**
   * Returns the entry eviction rate as triggered by the LRU policy.
   *
   * @return the entry eviction rate as triggered by the LRU policy
   */

  float getLruEvictionRate();

  /**
   * Returns the rate of entries destroyed either by destroy cache operations or eviction.
   *
   * @return the rate of entries destroyed either by destroy cache operations or eviction
   */
  float getLruDestroyRate();

  /**
   * Returns the number of buckets on this member. Only applicable for partitioned regions.
   *
   * @return the number of buckets on this member
   */
  int getBucketCount();

  /**
   * Returns the number of primary buckets on this member. Only applicable for partitioned regions.
   *
   * @return the number of primary buckets on this member
   */
  int getPrimaryBucketCount();

  /**
   * Returns the number of buckets without full redundancy. Only applicable for partitioned regions.
   *
   * @return the number of buckets without full redundancy
   */
  int getNumBucketsWithoutRedundancy();

  /**
   * Returns the number of redundant copies configured for this partitioned region. Only applicable
   * for partitioned regions.
   *
   * @return the number of redundant copies configured for this partitioned region
   */
  int getConfiguredRedundancy();

  /**
   * Returns the actual number of redundant copies available for buckets in this partitioned region.
   * Usually this is the number of redundant copies configured for buckets in the region. However,
   * during initialization or error states the actual number of copies for any given bucket may be
   * less than the configured number. In that case, the value returned will be the smallest number
   * of redundant copies available for any single bucket.
   *
   * @return the actual number of redundant copies available for buckets in this partitioned region
   */
  int getActualRedundancy();

  /**
   * Returns the total number of entries in all buckets. Only applicable for partitioned regions.
   *
   * @return the total number of entries in all buckets
   */
  int getTotalBucketSize();

  /**
   * Returns the average number of entries in bucket. Only applicable for partitioned regions.
   *
   * @return the average number of entries in bucket
   *
   * @deprecated This attribute is removed from 8.0 for being incorrect and impacting performance.
   */
  @Deprecated
  int getAvgBucketSize();


  /**
   * Returns the total number of bytes stored in disk for this region.
   *
   * @return the total number of bytes stored in disk for this region
   */
  long getDiskUsage();

  /**
   * Returns the aggregate entry size (in bytes) of all entries.
   * For replicated regions, provides a value only if the eviction algorithm
   * is set to {@link EvictionAlgorithm#LRU_MEMORY}.
   *
   * All partitioned regions can report entry size, but the value also includes
   * redundant entries and also counts the size of all the secondary entries in
   * the node.
   *
   * @return total entry size in bytes, -1 for replicated regions without LRU_MEMORY eviction
   */
  long getEntrySize();

  /**
   * Returns whether this region sends data using a GatewaySender.
   *
   * @return whether this region sends data using a GatewaySender
   */
  boolean isGatewayEnabled();


  /**
   * Returns the average number of read requests per second.
   *
   * @return the average number of read requests per second
   */
  float getAverageReads();

  /**
   * Returns the average number of write requests per second. This includes rates of put, putAll
   * &amp; create operations on the region
   *
   * @return the average number of write requests per second
   */
  float getAverageWrites();

  /**
   * Returns whether persistence is enabled.
   *
   * @return whether persistence is enabled
   */
  boolean isPersistentEnabled();

  /**
   * Returns the name/ID of the member hosting this Region.
   *
   * @return the name/ID of the member hosting this Region
   */
  String getMember();

  /**
   * Returns the maximum amount of local memory that can be used by the region. This attribute is
   * applicable for PartitionedRegion only. For other regions it will be -1
   *
   * @return the maximum amount of local memory that can be used by the region
   */
  int getLocalMaxMemory();

}
