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
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.StatisticsTypeFactory;
import com.gemstone.gemfire.internal.StatisticsTypeFactoryImpl;

/**
 * GemFire statistics about a {@link DiskRegion}.
 *
 *
 * @since GemFire 3.2
 */
public class DiskRegionStats {

  private static final StatisticsType type;

  ////////////////////  Statistic "Id" Fields  ////////////////////

  private static final int writesId;
  private static final int writeTimeId;
  private static final int bytesWrittenId;
  private static final int readsId;
  private static final int readTimeId;
  private static final int writesInProgressId;
  private static final int bytesReadId;
  private static final int removesId;
  private static final int removeTimeId;
  private static final int numOverflowOnDiskId;
  private static final int numEntriesInVMId;
  private static final int numOverflowBytesOnDiskId;

  private static final int localInitializationsId;
  private static final int remoteInitializationsId;
  
  

  static {
    String statName = "DiskRegionStatistics";
    String statDescription =
      "Statistics about a Region's use of the disk";

    final String writesDesc =
      "The total number of region entries that have been written to disk. A write is done every time an entry is created on disk or every time its value is modified on disk.";
    final String writeTimeDesc =
      "The total amount of time spent writing to disk";
    final String bytesWrittenDesc =
      "The total number of bytes that have been written to disk";
    final String readsDesc =
      "The total number of region entries that have been read from disk";
    final String readTimeDesc =
      "The total amount of time spent reading from disk";
    final String bytesReadDesc =
      "The total number of bytes that have been read from disk";
    final String removesDesc =
      "The total number of region entries that have been removed from disk";
    final String removeTimeDesc =
      "The total amount of time spent removing from disk";
    final String numOverflowOnDiskDesc =
      "The current number of entries whose value is on disk and is not in memory. This is true of overflowed entries. It is also true of recovered entries that have not yet been faulted in.";
    final String numOverflowBytesOnDiskDesc =
        "The current number bytes on disk and not in memory. This is true of overflowed entries. It is also true of recovered entries that have not yet been faulted in.";
    final String numEntriesInVMDesc =
      "The current number of entries whose value resides in the VM. The value may also have been written to disk.";
    final String localInitializationsDesc =
      "The number of times that this region has been initialized solely from the local disk files (0 or 1)";
    final String remoteInitializationsDesc =
      "The number of times that this region has been initialized by doing GII from a peer (0 or 1)";

    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();

    type = f.createType(statName, statDescription,
       new StatisticDescriptor[] {
         f.createLongCounter("writes", writesDesc, "ops"),
         f.createLongCounter("writeTime", writeTimeDesc, "nanoseconds"),
         f.createLongCounter("writtenBytes", bytesWrittenDesc, "bytes"),
         f.createLongCounter("reads", readsDesc, "ops"),
         f.createLongCounter("readTime", readTimeDesc, "nanoseconds"),
         f.createLongCounter("readBytes", bytesReadDesc, "bytes"),
         f.createLongCounter("removes", removesDesc, "ops"),
         f.createLongCounter("removeTime", removeTimeDesc, "nanoseconds"),
         f.createLongGauge("entriesOnlyOnDisk", numOverflowOnDiskDesc, "entries"),
         f.createLongGauge("bytesOnlyOnDisk", numOverflowBytesOnDiskDesc, "bytes"),
         f.createLongGauge("entriesInVM", numEntriesInVMDesc, "entries"),
         f.createIntGauge("writesInProgress", "current number of oplog writes that are in progress", "writes"),
         f.createIntGauge("localInitializations", localInitializationsDesc, "initializations"),
         f.createIntGauge("remoteInitializations", remoteInitializationsDesc, "initializations"),
       });

    // Initialize id fields
    writesId = type.nameToId("writes");
    writeTimeId = type.nameToId("writeTime");
    bytesWrittenId = type.nameToId("writtenBytes");
    readsId = type.nameToId("reads");
    readTimeId = type.nameToId("readTime");
    bytesReadId = type.nameToId("readBytes");
    writesInProgressId = type.nameToId("writesInProgress");
    removesId = type.nameToId("removes");
    removeTimeId = type.nameToId("removeTime");
    numOverflowOnDiskId = type.nameToId("entriesOnlyOnDisk");
    numOverflowBytesOnDiskId = type.nameToId("bytesOnlyOnDisk");
    numEntriesInVMId = type.nameToId("entriesInVM");

    localInitializationsId = type.nameToId("localInitializations");
    remoteInitializationsId = type.nameToId("remoteInitializations");
  }

  //////////////////////  Instance Fields  //////////////////////

  /** The Statistics object that we delegate most behavior to */
  private final Statistics stats;
  
  ///////////////////////  Constructors  ///////////////////////

  /**
   * Creates a new <code>DiskRegionStatistics</code> for the given
   * region. 
   */
  public DiskRegionStats(StatisticsFactory f, String name) {
    this.stats = f.createAtomicStatistics(type, name);
  }

  /////////////////////  Instance Methods  /////////////////////

  public void close() {
    this.stats.close();
  }

  /**
   * Returns the total number of region entries that have been written
   * to disk.
   */
  public long getWrites() {
    return this.stats.getLong(writesId);
  }

  /**
   * Returns the total number of nanoseconds spent writing to disk
   */
  public long getWriteTime() {
    return this.stats.getLong(writeTimeId);
  }

  /**
   * Returns the total number of bytes that have been written to disk
   */
  public long getBytesWritten() {
    return this.stats.getLong(bytesWrittenId);
  }

  /**
   * Returns the total number of region entries that have been read
   * from disk.
   */
  public long getReads() {
    return this.stats.getLong(readsId);
  }

  /**
   * Returns the total number of nanoseconds spent reading from disk
   */
  public long getReadTime() {
    return this.stats.getLong(readTimeId);
  }

  /**
   * Returns the total number of bytes that have been read from disk
   */
  public long getBytesRead() {
    return this.stats.getLong(bytesReadId);
  }

  /**
   * Returns the total number of region entries that have been removed
   * from disk.
   */
  public long getRemoves() {
    return this.stats.getLong(removesId);
  }

  /**
   * Returns the total number of nanoseconds spent removing from disk
   */
  public long getRemoveTime() {
    return this.stats.getLong(removeTimeId);
  }

  /**
   * Returns the current number of entries whose value has been
   * overflowed to disk.  This value will decrease when a value is
   * faulted in. 
   */
  public long getNumOverflowOnDisk() {
    return this.stats.getLong(numOverflowOnDiskId);
  }
  
  /**
   * Returns the current number of entries whose value has been
   * overflowed to disk.  This value will decrease when a value is
   * faulted in. 
   */
  public long getNumOverflowBytesOnDisk() {
    return this.stats.getLong(numOverflowBytesOnDiskId);
  }

  /**
   * Returns the current number of entries whose value resides in the
   * VM.  This value will decrease when the entry is overflowed to
   * disk. 
   */
  public long getNumEntriesInVM() {
    return this.stats.getLong(numEntriesInVMId);
  }

  /**
   * Increments the current number of entries whose value has been
   * overflowed to disk by a given amount.
   */
  public void incNumOverflowOnDisk(long delta) {
    this.stats.incLong(numOverflowOnDiskId, delta);
  }

  /**
   * Increments the current number of entries whose value has been
   * overflowed to disk by a given amount.
   */
  public void incNumEntriesInVM(long delta) {
    this.stats.incLong(numEntriesInVMId, delta);
  }
  
  /**
   * Increments the current number of entries whose value has been
   * overflowed to disk by a given amount.
   */
  public void incNumOverflowBytesOnDisk(long delta) {
    this.stats.incLong(numOverflowBytesOnDiskId, delta);
  }

  /**
   * Invoked before data is written to disk.
   *
   * @see DiskRegion#put
   */
  public void startWrite() {
    this.stats.incInt(writesInProgressId, 1);
  }

  public void incWrittenBytes(long bytesWritten) {
    this.stats.incLong(bytesWrittenId, bytesWritten);
  }
  /**
   * Invoked after data has been written to disk
   *
   * @param start
   *        The time at which the write operation started
   */
  public void endWrite(long start, long end) {
    this.stats.incInt(writesInProgressId, -1);
    this.stats.incLong(writesId, 1);
    this.stats.incLong(writeTimeId, end - start);
  }

  /**
   * Invoked after data has been read from disk
   *
   * @param start
   *        The time at which the read operation started
   * @param bytesRead
   *        The number of bytes that were read
   */
  public void endRead(long start, long end, long bytesRead) {
    this.stats.incLong(readsId, 1);
    this.stats.incLong(readTimeId, end - start);
    this.stats.incLong(bytesReadId, bytesRead);
  }

  /**
   * Invoked after data has been removed from disk
   *
   * @param start
   *        The time at which the read operation started
   */
  public void endRemove(long start, long end) {
    this.stats.incLong(removesId, 1);
    this.stats.incLong(removeTimeId, end - start);
  }

  public void incInitializations(boolean local) {
    if(local) {
      this.stats.incInt(localInitializationsId, 1);
    } else {
      this.stats.incInt(remoteInitializationsId, 1);
    }
  }
  
  public int getLocalInitializations() {
    return this.stats.getInt(localInitializationsId);
  }
  
  public int getRemoteInitializations() {
    return this.stats.getInt(remoteInitializationsId);
  }
  
  public Statistics getStats(){
    return stats;
  }
}
