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

import com.gemstone.gemfire.cache.DiskStore;

/**
 * Composite data type used to distribute metrics for a {@link DiskStore}.
 * 
 * @since GemFire 7.0
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
