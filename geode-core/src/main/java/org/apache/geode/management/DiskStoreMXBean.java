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

import org.apache.geode.cache.DiskStore;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;
import org.apache.geode.security.ResourcePermission.Target;

/**
 * MBean that provides access to information and management functionality for a {@link DiskStore}.
 * 
 * @since GemFire 7.0
 * 
 */
@ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
public interface DiskStoreMXBean {

  /**
   * Returns the name of the DiskStore.
   */
  String getName();

  /**
   * Returns whether disk files are to be automatically compacted.
   * 
   * @return True if disk files are automatically compacted, false otherwise
   */
  boolean isAutoCompact();

  /**
   * Returns the threshold at which an op-log may be compacted. Until it reaches this threshold the
   * op-log will not be compacted. The threshold is a percentage in the range 0..100.
   */
  int getCompactionThreshold();

  /**
   * Returns whether manual compaction of disk files is allowed.
   * 
   * @return True if manual compaction is allowed, false otherwise.
   */
  boolean isForceCompactionAllowed();

  /**
   * Returns the maximum size (in megabytes) that a single op-log can grow to.
   */
  long getMaxOpLogSize();

  /**
   * Returns the time (in milliseconds) that can elapse before unwritten data is saved to disk.
   */
  long getTimeInterval();

  /**
   * Returns the size of the write buffer that this DiskStore will use when writing data to disk.
   */
  int getWriteBufferSize();

  /**
   * Returns the path of the directories to which the region's data will be written.
   */
  String[] getDiskDirectories();

  /**
   * Returns the maximum number of operations that can be asynchronously queued for saving to disk.
   * When this limit is reached operations will block until they can be put in the queue.
   */
  int getQueueSize();

  /**
   * Returns the total number of bytes of space this DiskStore has used.
   */
  long getTotalBytesOnDisk();

  /**
   * Returns the average latency of disk reads in nanoseconds Its the average latency required to
   * read a byte from disk.
   * 
   * Each entry in region has some overhead in terms of number of extra bytes while persisting data.
   * So this rate won't match the number of bytes put in all regions.This is rate of actual bytes
   * system is persisting.
   */
  float getDiskReadsRate();

  /**
   * Returns the average latency of disk writes in nanoseconds. Its the average latency required to
   * write a byte to disk.
   * 
   * Each entry in region has some overhead in terms of number of extra bytes while persisting data.
   * So this rate won't match the number of bytes put in all regions. This is rate of actual bytes
   * system is persisting.
   */
  float getDiskWritesRate();

  /**
   * Returns the disk reads average latency in nanoseconds. It depicts average time needed to read
   * one byte of data from disk.
   */
  long getDiskReadsAvgLatency();

  /**
   * Returns the disk writes average latency in nanoseconds. It depicts average time needed to write
   * one byte of data to disk.
   */
  long getDiskWritesAvgLatency();

  /**
   * Returns the flush time average latency.
   */
  long getFlushTimeAvgLatency();

  /**
   * Returns the number of entries in the asynchronous queue waiting to be written to disk.
   */
  int getTotalQueueSize();

  /**
   * Returns the number of backups currently in progress on this DiskStore.
   */
  int getTotalBackupInProgress();

  /**
   * Returns the number of backups of this DiskStore that have been completed.
   */
  int getTotalBackupCompleted();

  /**
   * Returns the number of persistent regions currently being recovered from disk.
   */
  int getTotalRecoveriesInProgress();

  /**
   * Requests the DiskStore to start writing to a new op-log. The old oplog will be asynchronously
   * compressed if compaction is set to true. The new op-log will be created in the next available
   * directory with free space. If there is no directory with free space available and compaction is
   * set to false, then a DiskAccessException saying that the disk is full will be thrown. If
   * compaction is true then the application will wait for the other op-logs to be compacted and
   * additional space is available.
   */
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE,
      target = Target.DISK)
  void forceRoll();

  /**
   * Requests the DiskStore to start compacting. The compaction is done even if automatic compaction
   * is not configured. If the current, active op-log has had data written to it, and may be
   * compacted, then an implicit call to forceRoll will be made so that the active op-log can be
   * compacted. This method will block until compaction finishes.
   * 
   * @return True if one or more op-logs were compacted or false to indicate that no op-logs were
   *         ready to be compacted or that a compaction was already in progress.
   */
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE,
      target = Target.DISK)
  boolean forceCompaction();

  /**
   * Causes any data that is currently in the asynchronous queue to be written to disk. Does not
   * return until the flush is complete.
   */
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE,
      target = Target.DISK)
  void flush();

  /**
   * Returns the warning threshold for disk usage as a percentage of the total disk volume.
   * 
   * @return the warning percent
   * @since GemFire 8.0
   */
  float getDiskUsageWarningPercentage();

  /**
   * Returns the critical threshold for disk usage as a percentage of the total disk volume.
   * 
   * @return the critical percent
   * @since GemFire 8.0
   */
  float getDiskUsageCriticalPercentage();

  /**
   * Sets the value of the disk usage warning percentage.
   * 
   * @param warningPercent the warning percent
   */
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE,
      target = Target.DISK)
  void setDiskUsageWarningPercentage(float warningPercent);

  /**
   * Sets the value of the disk usage critical percentage.
   * 
   * @param criticalPercent the critical percent
   */
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE,
      target = Target.DISK)
  void setDiskUsageCriticalPercentage(float criticalPercent);
}
