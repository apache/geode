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
 * Composite data type used to distribute metrics for a {@link DiskStore}.
 * 
 * @author rishim
 * @since 7.0
 */
public class DiskMetrics {
  
  private float diskReadsRate;

  private float diskWritesRate;
  
  private int totalBackupInProgress;

  private int totalBackupCompleted;

  private long totalBytesOnDisk;

  private long diskFlushAvgLatency;
  
  /**
   * Returns the average number of disk reads per second.
   */
  public float getDiskReadsRate() {
    return diskReadsRate;
  }

  /**
   * Returns the average number of disk writes per second.
   */
  public float getDiskWritesRate() {
    return diskWritesRate;
  }

  /**
   * Returns the number of backups currently in progress on this DiskStore.
   */
  public int getTotalBackupInProgress() {
    return totalBackupInProgress;
  }

  /**
   * Returns the number of backups of this DiskStore that have been completed.
   */
  public int getTotalBackupCompleted() {
    return totalBackupCompleted;
  }

  /**
   * Returns the total number of bytes of space that have been used.
   */
  public long getTotalBytesOnDisk() {
    return totalBytesOnDisk;
  }

  /**
   * Returns the flush time average latency.
   */
  public long getDiskFlushAvgLatency() {
    return diskFlushAvgLatency;
  }

  /**
   * Sets the average number of disk reads per second.
   */
  public void setDiskReadsRate(float diskReadsRate) {
    this.diskReadsRate = diskReadsRate;
  }

  /**
   * Sets the average number of disk writes per second.
   */
  public void setDiskWritesRate(float diskWritesRate) {
    this.diskWritesRate = diskWritesRate;
  }

  /**
   * Sets the number of backups currently in progress on this DiskStore.
   */
  public void setTotalBackupInProgress(int totalBackupInProgress) {
    this.totalBackupInProgress = totalBackupInProgress;
  }

  /**
   * Sets the number of backups of this DiskStore that have been completed.
   */
  public void setTotalBackupCompleted(int totalBackupCompleted) {
    this.totalBackupCompleted = totalBackupCompleted;
  }

  /**
   * Sets the total number of bytes of space that have been used.
   */
  public void setTotalBytesOnDisk(long totalBytesOnDisk) {
    this.totalBytesOnDisk = totalBytesOnDisk;
  }

  /**
   * Sets the flush time average latency.
   */
  public void setDiskFlushAvgLatency(long diskFlushAvgLatency) {
    this.diskFlushAvgLatency = diskFlushAvgLatency;
  }
}
