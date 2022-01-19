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
package org.apache.geode.internal.cache;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.UUID;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.internal.cache.persistence.DefaultDiskDirs;

/**
 * Creates an attribute object for DiskStore.
 * </p>
 *
 * @since GemFire prPersistSprint2
 */
public class DiskStoreAttributes implements Serializable, DiskStore {
  private static final long serialVersionUID = 1L;

  public boolean allowForceCompaction;
  public boolean autoCompact;

  public int compactionThreshold;
  public int queueSize;
  public int writeBufferSize;

  public long maxOplogSizeInBytes;
  public long timeInterval;

  public int[] diskDirSizes;
  private DiskDirSizesUnit diskDirSizesUnit;
  public File[] diskDirs;

  public String name;

  private volatile float diskUsageWarningPct;
  private volatile float diskUsageCriticalPct;

  /**
   * The default disk directory size unit.
   */
  @Immutable
  static final DiskDirSizesUnit DEFAULT_DISK_DIR_SIZES_UNIT = DiskDirSizesUnit.MEGABYTES;

  public DiskStoreAttributes() {
    // set all to defaults
    autoCompact = DiskStoreFactory.DEFAULT_AUTO_COMPACT;
    compactionThreshold = DiskStoreFactory.DEFAULT_COMPACTION_THRESHOLD;
    allowForceCompaction = DiskStoreFactory.DEFAULT_ALLOW_FORCE_COMPACTION;
    maxOplogSizeInBytes = DiskStoreFactory.DEFAULT_MAX_OPLOG_SIZE * (1024 * 1024);
    timeInterval = DiskStoreFactory.DEFAULT_TIME_INTERVAL;
    writeBufferSize = DiskStoreFactory.DEFAULT_WRITE_BUFFER_SIZE;
    queueSize = DiskStoreFactory.DEFAULT_QUEUE_SIZE;
    diskDirs = DefaultDiskDirs.getDefaultDiskDirs();
    diskDirSizes = DiskStoreFactory.DEFAULT_DISK_DIR_SIZES;
    diskDirSizesUnit = DEFAULT_DISK_DIR_SIZES_UNIT;
    diskUsageWarningPct = DiskStoreFactory.DEFAULT_DISK_USAGE_WARNING_PERCENTAGE;
    diskUsageCriticalPct = DiskStoreFactory.DEFAULT_DISK_USAGE_CRITICAL_PERCENTAGE;
  }

  @Override
  public UUID getDiskStoreUUID() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.DiskStore#getAllowForceCompaction()
   */
  @Override
  public boolean getAllowForceCompaction() {
    return allowForceCompaction;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.DiskStore#getAutoCompact()
   */
  @Override
  public boolean getAutoCompact() {
    return autoCompact;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.DiskStore#getCompactionThreshold()
   */
  @Override
  public int getCompactionThreshold() {
    return compactionThreshold;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.DiskStore#getDiskDirSizes()
   */
  @Override
  public int[] getDiskDirSizes() {
    int[] result = new int[diskDirSizes.length];
    System.arraycopy(diskDirSizes, 0, result, 0, diskDirSizes.length);
    return result;
  }

  public DiskDirSizesUnit getDiskDirSizesUnit() {
    return diskDirSizesUnit;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.DiskStore#getDiskDirs()
   */
  @Override
  public File[] getDiskDirs() {
    File[] result = new File[diskDirs.length];
    System.arraycopy(diskDirs, 0, result, 0, diskDirs.length);
    return result;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.DiskStore#getMaxOplogSize()
   */
  @Override
  public long getMaxOplogSize() {
    return maxOplogSizeInBytes / (1024 * 1024);
  }

  /**
   * Used by unit tests
   */
  public long getMaxOplogSizeInBytes() {
    return maxOplogSizeInBytes;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.DiskStore#getName()
   */
  @Override
  public String getName() {
    return name;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.DiskStore#getQueueSize()
   */
  @Override
  public int getQueueSize() {
    return queueSize;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.DiskStore#getTimeInterval()
   */
  @Override
  public long getTimeInterval() {
    return timeInterval;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.DiskStore#getWriteBufferSize()
   */
  @Override
  public int getWriteBufferSize() {
    return writeBufferSize;
  }

  @Override
  public void flush() {
    // nothing needed
  }

  @Override
  public void forceRoll() {
    // nothing needed
  }

  @Override
  public boolean forceCompaction() {
    return false;
  }

  @Override
  public void destroy() {
    // nothing needed
  }

  @Override
  public float getDiskUsageWarningPercentage() {
    return diskUsageWarningPct;
  }

  @Override
  public float getDiskUsageCriticalPercentage() {
    return diskUsageCriticalPct;
  }

  @Override
  public void setDiskUsageWarningPercentage(float warningPercent) {
    DiskStoreMonitor.checkWarning(warningPercent);
    diskUsageWarningPct = warningPercent;
  }

  @Override
  public void setDiskUsageCriticalPercentage(float criticalPercent) {
    DiskStoreMonitor.checkCritical(criticalPercent);
    diskUsageCriticalPct = criticalPercent;
  }

  public void setDiskDirSizesUnit(DiskDirSizesUnit unit) {
    diskDirSizesUnit = unit;
  }

  private void readObject(final java.io.ObjectInputStream in)
      throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    if (diskDirSizesUnit == null) {
      diskDirSizesUnit = DEFAULT_DISK_DIR_SIZES_UNIT;
    }
  }

  public static void checkMinAndMaxOplogSize(long maxOplogSize) {
    long MAX = Long.MAX_VALUE / (1024 * 1024);
    if (maxOplogSize > MAX) {
      throw new IllegalArgumentException(
          String.format(
              "%s has to be a number that does not exceed %s so the value given %s is not acceptable",
              "max oplog size", maxOplogSize, MAX));
    }
    checkMinOplogSize(maxOplogSize);
  }

  public static void checkMinOplogSize(long maxOplogSize) {
    if (maxOplogSize < 0) {
      throw new IllegalArgumentException(
          String.format(
              "Maximum Oplog size specified has to be a non-negative number and the value given %s is not acceptable",
              maxOplogSize));
    }
  }

  public static void checkQueueSize(int queueSize) {
    if (queueSize < 0) {
      throw new IllegalArgumentException(
          String.format(
              "Queue size specified has to be a non-negative number and the value given %s is not acceptable",
              queueSize));
    }
  }

  public static void checkWriteBufferSize(int writeBufferSize) {
    if (writeBufferSize < 0) {
      throw new IllegalArgumentException(
          String.format(
              "Write buffer size specified has to be a non-negative number and the value given %s is not acceptable",
              writeBufferSize));
    }
  }

  /**
   * Verify all directory sizes are positive
   */
  public static void verifyNonNegativeDirSize(int[] sizes) {
    for (int size : sizes) {
      if (size < 0) {
        throw new IllegalArgumentException(
            String.format("Dir size cannot be negative : %s",
                size));
      }
    }
  }

  public static void checkTimeInterval(long timeInterval) {
    if (timeInterval < 0) {
      throw new IllegalArgumentException(
          String.format(
              "Time Interval specified has to be a non-negative number and the value given %s is not acceptable",
              timeInterval));
    }
  }
}
