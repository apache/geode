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
/**
 *
 */
package org.apache.geode.internal.cache;

import java.io.File;
import java.io.Serializable;
import java.util.UUID;

import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;

/**
 * Creates an attribute object for DiskStore.
 * </p>
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

  public File[] diskDirs;

  public String name;
  
  private volatile float diskUsageWarningPct;
  private volatile float diskUsageCriticalPct;

  public DiskStoreAttributes() {
    // set all to defaults
    this.autoCompact = DiskStoreFactory.DEFAULT_AUTO_COMPACT;
    this.compactionThreshold = DiskStoreFactory.DEFAULT_COMPACTION_THRESHOLD;
    this.allowForceCompaction = DiskStoreFactory.DEFAULT_ALLOW_FORCE_COMPACTION;
    this.maxOplogSizeInBytes = DiskStoreFactory.DEFAULT_MAX_OPLOG_SIZE * (1024*1024);
    this.timeInterval = DiskStoreFactory.DEFAULT_TIME_INTERVAL;
    this.writeBufferSize = DiskStoreFactory.DEFAULT_WRITE_BUFFER_SIZE;
    this.queueSize = DiskStoreFactory.DEFAULT_QUEUE_SIZE;
    this.diskDirs = DiskStoreFactory.DEFAULT_DISK_DIRS;
    this.diskDirSizes = DiskStoreFactory.DEFAULT_DISK_DIR_SIZES;
    this.diskUsageWarningPct = DiskStoreFactory.DEFAULT_DISK_USAGE_WARNING_PERCENTAGE;
    this.diskUsageCriticalPct = DiskStoreFactory.DEFAULT_DISK_USAGE_CRITICAL_PERCENTAGE;
  }

  public UUID getDiskStoreUUID() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  /* (non-Javadoc)
  * @see org.apache.geode.cache.DiskStore#getAllowForceCompaction()
  */
  public boolean getAllowForceCompaction() {
    return this.allowForceCompaction;
  }

  /* (non-Javadoc)
   * @see org.apache.geode.cache.DiskStore#getAutoCompact()
   */
  public boolean getAutoCompact() {
    return this.autoCompact;
  }

  /* (non-Javadoc)
   * @see org.apache.geode.cache.DiskStore#getCompactionThreshold()
   */
  public int getCompactionThreshold() {
    return this.compactionThreshold;
  }

  /* (non-Javadoc)
   * @see org.apache.geode.cache.DiskStore#getDiskDirSizes()
   */
  public int[] getDiskDirSizes() {
    int[] result = new int[this.diskDirSizes.length];
    System.arraycopy(this.diskDirSizes, 0, result, 0, this.diskDirSizes.length);
    return result;
  }

  /* (non-Javadoc)
   * @see org.apache.geode.cache.DiskStore#getDiskDirs()
   */
  public File[] getDiskDirs() {
    File[] result = new File[this.diskDirs.length];
    System.arraycopy(this.diskDirs, 0, result, 0, this.diskDirs.length);
    return result;
  }

  /* (non-Javadoc)
   * @see org.apache.geode.cache.DiskStore#getMaxOplogSize()
   */
  public long getMaxOplogSize() {
    // TODO Auto-generated method stub
    return this.maxOplogSizeInBytes / (1024 * 1024);
  }

  /**
   * Used by unit tests
   */
  public long getMaxOplogSizeInBytes() {
    return this.maxOplogSizeInBytes;
  }

  /* (non-Javadoc)
   * @see org.apache.geode.cache.DiskStore#getName()
   */
  public String getName() {
    return this.name;
  }

  /* (non-Javadoc)
   * @see org.apache.geode.cache.DiskStore#getQueueSize()
   */
  public int getQueueSize() {
    return this.queueSize;
  }

  /* (non-Javadoc)
   * @see org.apache.geode.cache.DiskStore#getTimeInterval()
   */
  public long getTimeInterval() {
    return this.timeInterval;
  }

  /* (non-Javadoc)
   * @see org.apache.geode.cache.DiskStore#getWriteBufferSize()
   */
  public int getWriteBufferSize() {
    return this.writeBufferSize;
  }

  public void flush() {
    // nothing needed
  }

  public void forceRoll() {
    // nothing needed
  }

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
}
