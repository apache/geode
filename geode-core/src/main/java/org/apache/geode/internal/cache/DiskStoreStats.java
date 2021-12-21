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

import java.util.function.LongSupplier;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;

/**
 * GemFire statistics about a {@link DiskStoreImpl}.
 *
 *
 * @since GemFire prPersistSprint2
 */
public class DiskStoreStats {

  @Immutable
  private static final StatisticsType type;

  //////////////////// Statistic "Id" Fields ////////////////////

  private static final int writesId;
  private static final int writeTimeId;
  private static final int bytesWrittenId;
  private static final int flushesId;
  private static final int flushTimeId;
  private static final int bytesFlushedId;
  private static final int readsId;
  private static final int readTimeId;
  private static final int recoveriesInProgressId;
  private static final int recoveryTimeId;
  private static final int recoveredBytesId;
  private static final int recoveredEntryCreatesId;
  private static final int recoveredEntryUpdatesId;
  private static final int recoveredEntryDestroysId;
  private static final int recoveredValuesSkippedDueToLRUId;
  private static final int recoveryRecordsSkippedId;
  private static final int compactsInProgressId;
  private static final int writesInProgressId;
  private static final int flushesInProgressId;
  private static final int compactTimeId;
  private static final int compactsId;
  private static final int oplogRecoveriesId;
  private static final int oplogRecoveryTimeId;
  private static final int oplogRecoveredBytesId;
  private static final int bytesReadId;
  private static final int removesId;
  private static final int removeTimeId;
  private static final int queueSizeId;

  private static final int compactInsertsId;
  private static final int compactInsertTimeId;
  private static final int compactUpdatesId;
  private static final int compactUpdateTimeId;
  private static final int compactDeletesId;
  private static final int compactDeleteTimeId;

  private static final int openOplogsId;
  private static final int inactiveOplogsId;
  private static final int compactableOplogsId;

  private static final int oplogReadsId;
  private static final int oplogSeeksId;

  private static final int uncreatedRecoveredRegionsId;
  private static final int backupsInProgress;
  private static final int backupsCompleted;

  static {
    String statName = "DiskStoreStatistics";
    String statDescription = "Statistics about a Region's use of the disk";

    final String writesDesc =
        "The total number of region entries that have been written to disk. A write is done every time an entry is created on disk or every time its value is modified on disk.";
    final String writeTimeDesc = "The total amount of time spent writing to disk";
    final String bytesWrittenDesc = "The total number of bytes that have been written to disk";
    final String flushesDesc =
        "The total number of times the an entry has been flushed from the async queue.";
    final String flushTimeDesc = "The total amount of time spent doing an async queue flush.";
    final String bytesFlushedDesc =
        "The total number of bytes written to disk by async queue flushes.";
    final String readsDesc = "The total number of region entries that have been read from disk";
    final String readTimeDesc = "The total amount of time spent reading from disk";
    final String bytesReadDesc = "The total number of bytes that have been read from disk";
    final String recoveryTimeDesc = "The total amount of time spent doing a recovery";
    final String recoveredBytesDesc =
        "The total number of bytes that have been read from disk during a recovery";
    final String oplogRecoveriesDesc = "The total number of oplogs recovered";
    final String oplogRecoveryTimeDesc = "The total amount of time spent doing an oplog recovery";
    final String oplogRecoveredBytesDesc =
        "The total number of bytes that have been read from oplogs during a recovery";
    final String removesDesc =
        "The total number of region entries that have been removed from disk";
    final String removeTimeDesc = "The total amount of time spent removing from disk";
    final String queueSizeDesc =
        "The current number of entries in the async queue waiting to be flushed to disk";
    final String backupsInProgressDesc =
        "The current number of backups in progress on this disk store";
    final String backupsCompletedDesc =
        "The number of backups of this disk store that have been taking while this VM was alive";

    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();

    type = f.createType(statName, statDescription,
        new StatisticDescriptor[] {f.createLongCounter("writes", writesDesc, "ops"),
            f.createLongCounter("writeTime", writeTimeDesc, "nanoseconds"),
            f.createLongCounter("writtenBytes", bytesWrittenDesc, "bytes"),
            f.createLongCounter("flushes", flushesDesc, "ops"),
            f.createLongCounter("flushTime", flushTimeDesc, "nanoseconds"),
            f.createLongCounter("flushedBytes", bytesFlushedDesc, "bytes"),
            f.createLongCounter("reads", readsDesc, "ops"),
            f.createLongCounter("readTime", readTimeDesc, "nanoseconds"),
            f.createLongCounter("readBytes", bytesReadDesc, "bytes"),
            f.createIntGauge("recoveriesInProgress",
                "current number of persistent regions being recovered from disk", "ops"),
            f.createLongCounter("recoveryTime", recoveryTimeDesc, "nanoseconds"),
            f.createLongCounter("recoveredBytes", recoveredBytesDesc, "bytes"),
            f.createLongCounter("recoveredEntryCreates",
                "The total number of entry create records processed while recovering oplog data.",
                "ops"),
            f.createLongCounter("recoveredEntryUpdates",
                "The total number of entry update records processed while recovering oplog data.",
                "ops"),
            f.createLongCounter("recoveredEntryDestroys",
                "The total number of entry destroy records processed while recovering oplog data.",
                "ops"),
            f.createLongCounter("recoveredValuesSkippedDueToLRU",
                "The total number of entry values that did not need to be recovered due to the LRU.",
                "values"),

            f.createLongCounter("recoveryRecordsSkipped",
                "The total number of oplog records skipped during recovery.", "ops"),

            f.createIntCounter("oplogRecoveries", oplogRecoveriesDesc, "ops"),
            f.createLongCounter("oplogRecoveryTime", oplogRecoveryTimeDesc, "nanoseconds"),
            f.createLongCounter("oplogRecoveredBytes", oplogRecoveredBytesDesc, "bytes"),
            f.createLongCounter("removes", removesDesc, "ops"),
            f.createLongCounter("removeTime", removeTimeDesc, "nanoseconds"),
            f.createIntGauge("queueSize", queueSizeDesc, "entries"),
            f.createLongCounter("compactInserts",
                "Total number of times an oplog compact did a db insert", "inserts"),
            f.createLongCounter("compactInsertTime",
                "Total amount of time, in nanoseconds, spent doing inserts during a compact",
                "nanoseconds"),
            f.createLongCounter("compactUpdates",
                "Total number of times an oplog compact did an update", "updates"),
            f.createLongCounter("compactUpdateTime",
                "Total amount of time, in nanoseconds, spent doing updates during a compact",
                "nanoseconds"),
            f.createLongCounter("compactDeletes",
                "Total number of times an oplog compact did a delete", "deletes"),
            f.createLongCounter("compactDeleteTime",
                "Total amount of time, in nanoseconds, spent doing deletes during a compact",
                "nanoseconds"),
            f.createIntGauge("compactsInProgress",
                "current number of oplog compacts that are in progress", "compacts"),
            f.createIntGauge("writesInProgress",
                "current number of oplog writes that are in progress", "writes"),
            f.createIntGauge("flushesInProgress",
                "current number of oplog flushes that are in progress", "flushes"),
            f.createLongCounter("compactTime",
                "Total amount of time, in nanoseconds, spent compacting oplogs", "nanoseconds"),
            f.createIntCounter("compacts", "Total number of completed oplog compacts", "compacts"),
            f.createIntGauge("openOplogs", "Current number of oplogs this disk store has open",
                "oplogs"),
            f.createIntGauge("compactableOplogs", "Current number of oplogs ready to be compacted",
                "oplogs"),
            f.createIntGauge("inactiveOplogs",
                "Current number of oplogs that are no longer being written but are not ready ready to compact",
                "oplogs"),
            f.createLongCounter("oplogReads", "Total number of oplog reads", "reads"),
            f.createLongCounter("oplogSeeks", "Total number of oplog seeks", "seeks"),
            f.createIntGauge("uncreatedRecoveredRegions",
                "The current number of regions that have been recovered but have not yet been created.",
                "regions"),
            f.createIntGauge("backupsInProgress", backupsInProgressDesc, "backups"),
            f.createIntCounter("backupsCompleted", backupsCompletedDesc, "backups"),});

    // Initialize id fields
    writesId = type.nameToId("writes");
    writeTimeId = type.nameToId("writeTime");
    bytesWrittenId = type.nameToId("writtenBytes");
    flushesId = type.nameToId("flushes");
    flushTimeId = type.nameToId("flushTime");
    bytesFlushedId = type.nameToId("flushedBytes");
    readsId = type.nameToId("reads");
    readTimeId = type.nameToId("readTime");
    bytesReadId = type.nameToId("readBytes");
    recoveriesInProgressId = type.nameToId("recoveriesInProgress");
    recoveryTimeId = type.nameToId("recoveryTime");
    recoveredBytesId = type.nameToId("recoveredBytes");
    recoveredEntryCreatesId = type.nameToId("recoveredEntryCreates");
    recoveredEntryUpdatesId = type.nameToId("recoveredEntryUpdates");
    recoveredEntryDestroysId = type.nameToId("recoveredEntryDestroys");
    recoveredValuesSkippedDueToLRUId = type.nameToId("recoveredValuesSkippedDueToLRU");
    recoveryRecordsSkippedId = type.nameToId("recoveryRecordsSkipped");

    compactsInProgressId = type.nameToId("compactsInProgress");
    writesInProgressId = type.nameToId("writesInProgress");
    flushesInProgressId = type.nameToId("flushesInProgress");
    compactTimeId = type.nameToId("compactTime");
    compactsId = type.nameToId("compacts");
    oplogRecoveriesId = type.nameToId("oplogRecoveries");
    oplogRecoveryTimeId = type.nameToId("oplogRecoveryTime");
    oplogRecoveredBytesId = type.nameToId("oplogRecoveredBytes");
    removesId = type.nameToId("removes");
    removeTimeId = type.nameToId("removeTime");
    queueSizeId = type.nameToId("queueSize");

    compactDeletesId = type.nameToId("compactDeletes");
    compactDeleteTimeId = type.nameToId("compactDeleteTime");
    compactInsertsId = type.nameToId("compactInserts");
    compactInsertTimeId = type.nameToId("compactInsertTime");
    compactUpdatesId = type.nameToId("compactUpdates");
    compactUpdateTimeId = type.nameToId("compactUpdateTime");
    oplogReadsId = type.nameToId("oplogReads");
    oplogSeeksId = type.nameToId("oplogSeeks");

    openOplogsId = type.nameToId("openOplogs");
    inactiveOplogsId = type.nameToId("inactiveOplogs");
    compactableOplogsId = type.nameToId("compactableOplogs");
    uncreatedRecoveredRegionsId = type.nameToId("uncreatedRecoveredRegions");
    backupsInProgress = type.nameToId("backupsInProgress");
    backupsCompleted = type.nameToId("backupsCompleted");
  }

  ////////////////////// Instance Fields //////////////////////

  /** The Statistics object that we delegate most behavior to */
  private final Statistics stats;

  private final LongSupplier clock;

  /////////////////////// Constructors ///////////////////////

  /**
   * Creates a new <code>DiskStoreStatistics</code> for the given region.
   */
  public DiskStoreStats(StatisticsFactory f, String name) {
    this(f, name, DistributionStats::getStatTime);
  }

  @VisibleForTesting
  public DiskStoreStats(StatisticsFactory factory, String name, LongSupplier clock) {
    stats = factory.createAtomicStatistics(type, name);
    this.clock = clock;
  }

  ///////////////////// Instance Methods /////////////////////

  private long getTime() {
    return clock.getAsLong();
  }

  public void close() {
    stats.close();
  }

  /**
   * Returns the total number of region entries that have been written to disk.
   */
  public long getWrites() {
    return stats.getLong(writesId);
  }

  /**
   * Returns the total number of nanoseconds spent writing to disk
   */
  public long getWriteTime() {
    return stats.getLong(writeTimeId);
  }

  /**
   * Returns the total number of bytes that have been written to disk
   */
  public long getBytesWritten() {
    return stats.getLong(bytesWrittenId);
  }

  /**
   * Returns the total number of region entries that have been read from disk.
   */
  public long getReads() {
    return stats.getLong(readsId);
  }

  /**
   * Returns the total number of nanoseconds spent reading from disk
   */
  public long getReadTime() {
    return stats.getLong(readTimeId);
  }

  /**
   * Returns the total number of bytes that have been read from disk
   */
  public long getBytesRead() {
    return stats.getLong(bytesReadId);
  }

  /**
   * Returns the total number of region entries that have been removed from disk.
   */
  public long getRemoves() {
    return stats.getLong(removesId);
  }

  /**
   * Returns the total number of nanoseconds spent removing from disk
   */
  public long getRemoveTime() {
    return stats.getLong(removeTimeId);
  }

  /**
   * Return the current number of entries in the async queue
   */
  public long getQueueSize() {
    return stats.getInt(queueSizeId);
  }

  public void setQueueSize(int value) {
    stats.setInt(queueSizeId, value);
  }

  public void incQueueSize(int delta) {
    stats.incInt(queueSizeId, delta);
  }

  public void incUncreatedRecoveredRegions(int delta) {
    stats.incInt(uncreatedRecoveredRegionsId, delta);
  }

  /**
   * Invoked before data is written to disk.
   *
   * @return The timestamp that marks the start of the operation
   *
   * @see DiskRegion#put
   */
  public long startWrite() {
    stats.incInt(writesInProgressId, 1);
    return getTime();
  }

  public long startFlush() {
    stats.incInt(flushesInProgressId, 1);
    return getTime();
  }

  public void incWrittenBytes(long bytesWritten, boolean async) {
    stats.incLong(async ? bytesFlushedId : bytesWrittenId, bytesWritten);
  }

  /**
   * Invoked after data has been written to disk
   *
   * @param start The time at which the write operation started
   */
  public long endWrite(long start) {
    stats.incInt(writesInProgressId, -1);
    long end = getTime();
    stats.incLong(writesId, 1);
    stats.incLong(writeTimeId, end - start);
    return end;
  }

  public void endFlush(long start) {
    stats.incInt(flushesInProgressId, -1);
    long end = getTime();
    stats.incLong(flushesId, 1);
    stats.incLong(flushTimeId, end - start);
  }

  public long getFlushes() {
    return stats.getLong(flushesId);
  }

  /**
   * Invoked before data is read from disk.
   *
   * @return The timestamp that marks the start of the operation
   *
   * @see DiskRegion#get
   */
  public long startRead() {
    return getTime();
  }

  /**
   * Invoked after data has been read from disk
   *
   * @param start The time at which the read operation started
   * @param bytesRead The number of bytes that were read
   */
  public long endRead(long start, long bytesRead) {
    long end = getTime();
    stats.incLong(readsId, 1);
    stats.incLong(readTimeId, end - start);
    stats.incLong(bytesReadId, bytesRead);
    return end;
  }

  /**
   * Invoked before data is recovered from disk.
   *
   * @return The timestamp that marks the start of the operation
   *
   */
  public long startRecovery() {
    stats.incInt(recoveriesInProgressId, 1);
    return getTime();
  }

  public long startCompaction() {
    stats.incInt(compactsInProgressId, 1);
    return getTime();
  }

  public long startOplogRead() {
    return getTime();
  }

  /**
   * Invoked after data has been recovered from disk
   *
   * @param start The time at which the recovery operation started
   * @param bytesRead The number of bytes that were recovered
   */
  public void endRecovery(long start, long bytesRead) {
    stats.incInt(recoveriesInProgressId, -1);
    long end = getTime();
    stats.incLong(recoveryTimeId, end - start);
    stats.incLong(recoveredBytesId, bytesRead);
  }

  public void endCompaction(long start) {
    stats.incInt(compactsInProgressId, -1);
    long end = getTime();
    stats.incInt(compactsId, 1);
    stats.incLong(compactTimeId, end - start);
  }

  public void endOplogRead(long start, long bytesRead) {
    long end = getTime();
    stats.incInt(oplogRecoveriesId, 1);
    stats.incLong(oplogRecoveryTimeId, end - start);
    stats.incLong(oplogRecoveredBytesId, bytesRead);
  }

  public void incRecoveredEntryCreates() {
    stats.incLong(recoveredEntryCreatesId, 1);
  }

  public void incRecoveredEntryUpdates() {
    stats.incLong(recoveredEntryUpdatesId, 1);
  }

  public void incRecoveredEntryDestroys() {
    stats.incLong(recoveredEntryDestroysId, 1);
  }

  public void incRecoveryRecordsSkipped() {
    stats.incLong(recoveryRecordsSkippedId, 1);
  }

  public void incRecoveredValuesSkippedDueToLRU() {
    stats.incLong(recoveredValuesSkippedDueToLRUId, 1);
  }

  /**
   * Invoked before data is removed from disk.
   *
   * @return The timestamp that marks the start of the operation
   *
   * @see DiskRegion#remove
   */
  public long startRemove() {
    return getTime();
  }

  /**
   * Invoked after data has been removed from disk
   *
   * @param start The time at which the read operation started
   */
  public long endRemove(long start) {
    long end = getTime();
    stats.incLong(removesId, 1);
    stats.incLong(removeTimeId, end - start);
    return end;
  }

  public void incOplogReads() {
    stats.incLong(oplogReadsId, 1);
  }

  public void incOplogSeeks() {
    stats.incLong(oplogSeeksId, 1);
  }

  public void incInactiveOplogs(int delta) {
    stats.incInt(inactiveOplogsId, delta);
  }

  public void incCompactableOplogs(int delta) {
    stats.incInt(compactableOplogsId, delta);
  }

  public void endCompactionDeletes(int count, long delta) {
    stats.incLong(compactDeletesId, count);
    stats.incLong(compactDeleteTimeId, delta);
  }

  public void endCompactionInsert(long start) {
    stats.incLong(compactInsertsId, 1);
    stats.incLong(compactInsertTimeId, getStatTime() - start);
  }

  public void endCompactionUpdate(long start) {
    stats.incLong(compactUpdatesId, 1);
    stats.incLong(compactUpdateTimeId, getStatTime() - start);
  }

  public long getStatTime() {
    return getTime();
  }

  public void incOpenOplogs() {
    stats.incInt(openOplogsId, 1);
  }

  public void decOpenOplogs() {
    stats.incInt(openOplogsId, -1);
  }

  public void startBackup() {
    stats.incInt(backupsInProgress, 1);
  }

  public void endBackup() {
    stats.incInt(backupsInProgress, -1);
    stats.incInt(backupsCompleted, 1);
  }

  public Statistics getStats() {
    return stats;
  }
}
