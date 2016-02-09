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
package com.gemstone.gemfire.cache;


import java.io.File;

/**
 * Factory for creating instances of {@link DiskStore}.
 * To get an instance of this factory call {@link Cache#createDiskStoreFactory}.
 * If all you want to do is find an existing disk store see {@link Cache#findDiskStore}.
 * <P>
 * To use this factory configure it with the <code>set</code> methods and then
 * call {@link #create} to produce a disk store instance.
 * 
 * @author Gester
 * @since 6.5
 */
public interface DiskStoreFactory
{
  /**
   * The name of the default disk store is "DEFAULT".
   * This name can be used to redefine the default disk store.
   * Regions that have not had their disk-store-name set will
   * use this disk store.
   */
  public static final String DEFAULT_DISK_STORE_NAME = "DEFAULT";
  /**
   * The default setting for auto compaction. 
   * <p>Current value: <code>true</code>.
   */
  public static final boolean DEFAULT_AUTO_COMPACT = true;
  
  /**
   * The default compaction threshold.
   * <p>Current value: <code>50</code>.
   */
  public static final int DEFAULT_COMPACTION_THRESHOLD = 50;

  /**
   * The default value of the allow force compaction attribute.
   * <p>Current value: <code>false</code>.
   */
  public static final boolean DEFAULT_ALLOW_FORCE_COMPACTION = false;

  /**
   * The default maximum oplog file size in megabytes.
   * <p>Current value: <code>1024</code> which is one gigabyte.
   */
  public static final long DEFAULT_MAX_OPLOG_SIZE = Long.getLong("gemfire.DEFAULT_MAX_OPLOG_SIZE", 1024L).longValue(); // 1024 == 1 GB; // sys prop used by dunit and junit

  /**
   * The default time interval in milliseconds.
   * <p>Current value: <code>1000</code>.
   */
  public static final long DEFAULT_TIME_INTERVAL = 1000; // 1 sec;
  
  /**
   * The default write buffer size.
   * <p>Current value: <code>32768</code>.
   */
  public static final int DEFAULT_WRITE_BUFFER_SIZE = 32 * 1024;

  /**
   * The default maximum number of operations that can be asynchronously queued.
   * <p>Current value: <code>0</code>.
   */
  public static final int DEFAULT_QUEUE_SIZE = 0;
  
  /**
   * The default disk directories.
   * <p>Current value: <code>current directory</code>.
   */
  public static final File[] DEFAULT_DISK_DIRS = new File[] { new File(".") };
  
  /**
   * The default disk directory size in megabytes.
   * <p>Current value: <code>2,147,483,647</code> which is two petabytes.
   */
  public static final int DEFAULT_DISK_DIR_SIZE = Integer.MAX_VALUE; // unlimited for bug 41863
  
  /**
   * The default disk directory sizes.
   * <p>Current value: {@link #DEFAULT_DISK_DIR_SIZE} which is two petabytes each.
   */
  public static final int[] DEFAULT_DISK_DIR_SIZES = new int[] {DEFAULT_DISK_DIR_SIZE};

  /**
   * The default disk usage warning percentage.
   * <p>Current value: <code>90</code>.
   */
  public static final float DEFAULT_DISK_USAGE_WARNING_PERCENTAGE = 90;
  
  /**
   * The default disk usage critical percentage.
   * <p>Current value: <code>99</code>.
   */
  public static final float DEFAULT_DISK_USAGE_CRITICAL_PERCENTAGE = 99;
  
  /** 
   * Set to <code>true</code> to cause the disk files to be automatically compacted.
   * Set to <code>false</code> if no compaction is needed or manual compaction will be used.
   * @param isAutoCompact if true then use auto compaction
   * @return a reference to <code>this</code>
   */
  public DiskStoreFactory setAutoCompact(boolean isAutoCompact);

  /**
   * Sets the threshold at which an oplog will become compactable. Until it
   * reaches this threshold the oplog will not be compacted. The threshold is a
   * percentage in the range 0..100. When the amount of live data in an oplog
   * becomes less than this percentage then when a compaction is done this
   * garbage will be cleaned up freeing up disk space. Garbage is created by
   * entry destroys, entry updates, and region destroys.
   * 
   * @param compactionThreshold
   *          percentage of remaining live data in the oplog at which an oplog
   *          is compactable
   * @return a reference to <code>this</code>
   */
  public DiskStoreFactory setCompactionThreshold(int compactionThreshold);

  /** 
   * Set to <code>true</code> to allow {@link DiskStore#forceCompaction} to be called
   * on regions using this disk store.
   *
   * @param allowForceCompaction if true then allow force compaction.
   * @return a reference to <code>this</code>
   */
  public DiskStoreFactory setAllowForceCompaction(boolean allowForceCompaction);

  /** 
   * Sets the maximum size in megabytes a single oplog (operation log) is allowed to be.
   * When an oplog is created this amount of file space will be immediately reserved.
   * 
   * @param maxOplogSize maximum size in megabytes for one single oplog file.
   * @return a reference to <code>this</code>
   */
  public DiskStoreFactory setMaxOplogSize(long maxOplogSize);

  /**
   * Sets the number of milliseconds that can elapse before
   * data written asynchronously is flushed to disk.
   * <p>For how to configure a region to be asynchronous see: {@link AttributesFactory#setDiskSynchronous}.
   * 
   * @param timeInterval number of milliseconds that can elapse before
   * async data is flushed to disk.
   * @return a reference to <code>this</code>
   */
  public DiskStoreFactory setTimeInterval(long timeInterval);

  /**
   * Sets the write buffer size in bytes.
   * 
   * @param writeBufferSize write buffer size in bytes.
   * @return a reference to <code>this</code>
   */
  public DiskStoreFactory setWriteBufferSize(int writeBufferSize);

  /**
   * Sets the maximum number of operations that can be asynchronously queued.
   * Once this many pending async operations have been queued async ops will
   * begin blocking until some of the queued ops have been flushed to disk.
   * <p>
   * For how to configure a region to be asynchronous see:
   * {@link AttributesFactory#setDiskSynchronous}.
   * 
   * @param queueSize
   *          number of operations that can be asynchronously queued. If 0, the
   *          queue will be unlimited.
   * @return a reference to <code>this</code>
   */
  public DiskStoreFactory setQueueSize(int queueSize);

  /**
   * Sets the directories to which this disk store's data is written. If multiple
   * directories are used, GemFire will attempt to distribute the data evenly
   * amongst them.
   * The size of each directory will be set to the default of {@link #DEFAULT_DISK_DIR_SIZE}.
   * 
   * @param diskDirs directories to put the oplog files.
   * @return a reference to <code>this</code>
   */
  public DiskStoreFactory setDiskDirs(File[] diskDirs);

  /**
   * Sets the directories to which this disk store's data is written
   * and also set the sizes in megabytes of each directory.
   * 
   * @param diskDirs directories to put the oplog files.
   * @param diskDirSizes sizes of disk directories in megabytes
   * @return a reference to <code>this</code>
   * 
   * @throws IllegalArgumentException if length of the size array
   * does not match to the length of the dir array
   */
  public DiskStoreFactory setDiskDirsAndSizes(File[] diskDirs,int[] diskDirSizes);

  /**
   * Sets the warning threshold for disk usage as a percentage of the total disk
   * volume.
   * 
   * @param warningPercent warning percent of disk usage
   * @return a reference to <code>this</code>
   * @since 8.0
   */
  public DiskStoreFactory setDiskUsageWarningPercentage(float warningPercent);

  /**
   * Sets the critical threshold for disk usage as a percentage of the total disk
   * volume.
   * 
   * @param criticalPercent critical percent of disk usage
   * @return a reference to <code>this</code>
   * @since 8.0
   */
  public DiskStoreFactory setDiskUsageCriticalPercentage(float criticalPercent);

  /**
   * Create a new disk store or find an existing one. In either case the returned disk store's
   * configuration will be the same as this factory's configuration.
   * 
   * @param name the name of the DiskStore
   * @return the newly created DiskStore.
   * @throws IllegalStateException if a disk store with the given name already exists
   * and its configuration is not consistent with this factory.
   */
  public DiskStore create(String name);
}
