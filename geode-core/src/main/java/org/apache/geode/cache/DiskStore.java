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
package org.apache.geode.cache;

import java.io.File;
import java.util.UUID;

/**
 * Provides disk storage for one or more regions. The regions in the same disk store will
 * share the same disk persistence attributes. A region without a disk store name
 * belongs to the default disk store.
 * <p>Instances of this interface are created using
 * {@link DiskStoreFactory#create}. So to create a <code>DiskStore</code> named <code>myDiskStore</code> do this:
 * <PRE>
 *   new DiskStoreFactory().create("myDiskStore");
 * </PRE>
 * <p>Existing DiskStore instances can be found using {@link Cache#findDiskStore(String)}
 *
 * @since GemFire 6.5
 *
 */
public interface DiskStore {

  /**
   * Get the name of the DiskStore
   *
   * @return the name of the DiskStore
   * @see DiskStoreFactory#create
   */
  public String getName();

  /**
   * Returns true if the disk files are to be automatically compacted.
   *
   * @return Returns true if the disk files are to be automatically compacted;
   *         false if automatic compaction is turned off
   */
  public boolean getAutoCompact();

  /**
   * Returns the threshold at which an oplog will become compactable. Until it reaches
   * this threshold the oplog will not be compacted.
   * The threshold is a percentage in the range 0..100.
   * @return the threshold, as a percentage, at which an oplog is considered compactable.
   */
  public int getCompactionThreshold();

  /**
   * Returns true if manual compaction of disk files is allowed on this region.
   * Manual compaction is done be calling {@link #forceCompaction}.
   * <p>Note that calls to {@link #forceCompaction} will also be allowed if {@link #getAutoCompact automatic compaction} is enabled.
   *
   * @return Returns true if manual compaction of disk files is allowed on this region.
   */
  public boolean getAllowForceCompaction();

  /**
   * Get the maximum size in megabytes a single oplog (operation log) file should be
   *
   * @return the maximum size in megabyte the operations log file can be
   */
  public long getMaxOplogSize();

  /**
   * Returns the number of milliseconds that can elapse before
   * unwritten data is written to disk.
   *
   * @return Returns the time interval in milliseconds that can elapse between two writes to disk
   */
  public long getTimeInterval();

  /**
   * Returns the size of the write buffer that this disk store will use when
   * writing data to disk. Larger values may increase performance but will use
   * more memory.
   * The disk store will allocate one direct memory buffer of this size.
   *
   * @return Returns the size of the write buffer.
   */
  public int getWriteBufferSize();

  /**
   * Returns the directories to which the region's data are written.  If
   * multiple directories are used, GemFire will attempt to distribute the
   * data evenly amongst them.
   *
   */
  public File[] getDiskDirs();

  /**
   * Returns the sizes of the disk directories in megabytes
   */
  public int[] getDiskDirSizes();

  /**
   * Returns the universally unique identifier for the Disk Store across the GemFire distributed system.
   * </p>
   * @return a UUID uniquely identifying this Disk Store in the GemFire distributed system.
   * @see java.util.UUID
   */
  public UUID getDiskStoreUUID();

  /**
   * Returns the maximum number of operations that can be asynchronously
   * queued to be written to disk. When this limit is reached, it will cause
   * operations to block until they can be put in the queue.
   * If this <code>DiskStore</code> configures synchronous writing, then
   * <code>queueSize</code> is ignored.
   *
   * @return the maxinum number of entries that can be queued concurrently
   * for asynchronous writting to disk.
   *
   */
  public int getQueueSize();

  /**
   * Causes any data that is currently in the asynchronous queue to be written
   * to disk. Does not return until the flush is complete.
   *
   * @throws DiskAccessException
   *         If problems are encounter while writing to disk
   */
  public void flush();

  /**
   * Asks the disk store to start writing to a new oplog.
   * The old oplog will be asynchronously compressed if compaction is set to true.
   * The new oplog will be created in the next available directory with free space.
   * If there is no directory with free space available and compaction is set to false,
   * then a <code>DiskAccessException</code> saying that the disk is full will be
   * thrown.
   * If compaction is true then the application will wait for the other oplogs
   * to be compacted and more space to be created.
   *
   * @throws DiskAccessException
   */
  public void forceRoll();

   /**
    * Allows a disk compaction to be forced on this disk store. The compaction
    * is done even if automatic compaction is not configured.
    * If the current active oplog has had data written to it and it is
    * compactable then an implicit call to {@link #forceRoll} will be made
    * so that the active oplog can be compacted.
    * <P>This method will block until the compaction completes.
    * @return <code>true</code> if one or more oplogs were compacted;
    * <code>false</code> indicates that no oplogs were ready
    * to be compacted or that a compaction was already in progress.
    * @see #getAllowForceCompaction
    */
  public boolean forceCompaction();
  
  /**
   * Destroys this disk store. Removes the disk store from the cache,
   * and removes all files related to the disk store from disk.
   * 
   * If there are any currently open regions in the disk store
   * this method will throw an exception. If there are any closed regions that 
   * are persisted in this disk store, the data for those regions 
   * will be destroyed. 
   *
   * @throws IllegalStateException if the disk store is currently
   * in use by any regions, gateway senders, or a PDX type registry.
   * 
   * @since GemFire 8.0
   */
  public void destroy();
  

  /**
   * Returns the warning threshold for disk usage as a percentage of the total 
   * disk volume.
   * 
   * @return the warning percent
   * @since GemFire 8.0
   */
  public float getDiskUsageWarningPercentage();

  /**
   * Returns the critical threshold for disk usage as a percentage of the total 
   * disk volume.
   * 
   * @return the critical percent
   * @since GemFire 8.0
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
