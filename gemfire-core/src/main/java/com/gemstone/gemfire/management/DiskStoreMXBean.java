/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management;

import com.gemstone.gemfire.cache.DiskStore;


/**
 * MBean that provides access to information and management functionality for a
 * {@link DiskStore}.
 * 
 * @author rishim
 * @since 7.0
 * 
 */
public interface DiskStoreMXBean {
  
  /**
   * Returns the name of the DiskStore.
   */
  public String getName();

  /**
   * Returns whether disk files are to be automatically compacted.
   * 
   * @return True if disk files are automatically compacted, false otherwise
   */
  public boolean isAutoCompact();
  
  /**
   * Returns the threshold at which an op-log may be compacted. Until it
   * reaches this threshold the op-log will not be compacted. The threshold is
   * a percentage in the range 0..100.
   */
  public int getCompactionThreshold();

  /**
   * Returns whether manual compaction of disk files is allowed.
   * 
   * @return True if manual compaction is allowed, false otherwise.
   */
  public boolean isForceCompactionAllowed();

  /**
   * Returns the maximum size (in megabytes) that a single op-log can grow to.
   */
  public long getMaxOpLogSize();

  /**
   * Returns the time (in milliseconds) that can elapse before unwritten
   * data is saved to disk.
   */
  public long getTimeInterval();
  
  /**
   * Returns the size of the write buffer that this DiskStore will use when
   * writing data to disk.
   */
  public int getWriteBufferSize();

  /**
   * Returns the path of the directories to which the region's data will be
   * written.
   */
  public String[] getDiskDirectories();

  /**
   * Returns the maximum number of operations that can be asynchronously
   * queued for saving to disk. When this limit is reached operations
   * will block until they can be put in the queue.
   */
  public int getQueueSize();

  /**
   * Returns the total number of bytes of space this DiskStore has used.
   */
  public long getTotalBytesOnDisk();

  /**
   * Returns the average latency of disk reads in nanoseconds Its the average
   * latency required to read a byte from disk.
   * 
   * Each entry in region has some overhead in terms of number of extra bytes
   * while persisting data. So this rate won't match the number of bytes put in
   * all regions.This is rate of actual bytes system is persisting.
   */
  public float getDiskReadsRate();

  /**
   * Returns the average latency of disk writes in nanoseconds. Its the average
   * latency required to write a byte to disk.
   * 
   * Each entry in region has some overhead in terms of number of extra bytes
   * while persisting data. So this rate won't match the number of bytes put in
   * all regions. This is rate of actual bytes system is persisting.
   */
  public float getDiskWritesRate();

  /**
   * Returns the disk reads average latency in nanoseconds. It depicts average
   * time needed to read one byte of data from disk.
   */
  public long getDiskReadsAvgLatency();

  /**
   * Returns the disk writes average latency in nanoseconds. It depicts average
   * time needed to write one byte of data to disk.
   */
  public long getDiskWritesAvgLatency();

  /**
   * Returns the flush time average latency.
   */
  public long getFlushTimeAvgLatency();

  /**
   * Returns the number of entries in the asynchronous queue waiting to be written
   * to disk.
   */
  public int getTotalQueueSize();

  /**
   * Returns the number of backups currently in progress on this DiskStore.
   */
  public int getTotalBackupInProgress();
  
  /**
   * Returns the number of backups of this DiskStore that have been completed.
   */
  public int getTotalBackupCompleted();

  /**
   * Returns the number of persistent regions currently being recovered from disk.
   */
  public int getTotalRecoveriesInProgress();

  /**
   * Requests the DiskStore to start writing to a new op-log. The old oplog will
   * be asynchronously compressed if compaction is set to true. The new op-log will
   * be created in the next available directory with free space. If there is no
   * directory with free space available and compaction is set to false, then a
   * DiskAccessException saying that the disk is full will be thrown. If
   * compaction is true then the application will wait for the other op-logs to
   * be compacted and additional space is available.
   */
  public void forceRoll();

  /**
   * Requests the DiskStore to start compacting. The compaction is done even if
   * automatic compaction is not configured. If the current, active op-log has
   * had data written to it, and may be compacted, then an implicit  call to
   * forceRoll will be made so that the active op-log can be compacted. This
   * method will block until compaction finishes.
   * 
   * @return True if one or more op-logs were compacted or false to indicate
   *         that no op-logs were ready to be compacted or that a compaction was
   *         already in progress.
   */
  public boolean forceCompaction();
  
  /**
   * Causes any data that is currently in the asynchronous queue to be written
   * to disk. Does not return until the flush is complete.
   */
  public void flush();

  /**
   * Returns the warning threshold for disk usage as a percentage of the total 
   * disk volume.
   * 
   * @return the warning percent
   * @since 8.0
   */
  public float getDiskUsageWarningPercentage();

  /**
   * Returns the critical threshold for disk usage as a percentage of the total 
   * disk volume.
   * 
   * @return the critical percent
   * @since 8.0
   */
  public float getDiskUsageCriticalPercentage();
  
  /**
   * Sets the value of the disk usage warning percentage.
   * 
   * @param warningPercent the warning percent
   */
  public void setDiskUsageWarningPercentage(float warningPercent);
  
  /**
   * Sets the value of the disk usage critical percentage.
   * 
   * @param criticalPercent the critical percent
   */
  public void setDiskUsageCriticalPercentage(float criticalPercent);
}
