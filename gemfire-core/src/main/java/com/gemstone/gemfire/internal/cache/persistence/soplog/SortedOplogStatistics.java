/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence.soplog;

import static com.gemstone.gemfire.distributed.internal.DistributionStats.getStatTime;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.StatisticsTypeFactory;
import com.gemstone.gemfire.internal.DummyStatisticsFactory;
import com.gemstone.gemfire.internal.StatisticsTypeFactoryImpl;

public class SortedOplogStatistics {
  private final Statistics stats;
  
  private final IOOperation read;
  private final ScanOperation scan;
  private final IOOperation write;
  private final IOOperation put;
  private final IOOperation flush;
  private final IOOperation minorCompaction;
  private final IOOperation majorCompaction;
  private final BloomOperation bloom;
  private final TimedOperation clear;
  private final TimedOperation destroy;
  
  private final IOOperation blockRead;
  private final CacheOperation blockCache;
  
  private final int activeFilesId;
  private final int inactiveFilesId;

  public SortedOplogStatistics(String typeName, String name) {
    this(new DummyStatisticsFactory(), typeName, name);
  }
  
  public SortedOplogStatistics(StatisticsFactory factory, String typeName, String name) {
    StatisticsTypeFactory tf = StatisticsTypeFactoryImpl.singleton();
    
    StatisticDescriptor readCount = tf.createLongCounter("reads", "The total number of read operations", "ops");
    StatisticDescriptor readInProgress = tf.createLongGauge("readsInProgress", "The number of read operations in progress", "ops");
    StatisticDescriptor readTime = tf.createLongCounter("readTime", "The total time spent reading from disk", "nanoseconds");
    StatisticDescriptor readBytes = tf.createLongCounter("readBytes", "The total number of bytes read from disk", "bytes");
    StatisticDescriptor readErrors = tf.createLongCounter("readErrors", "The total number of read errors", "errors");

    StatisticDescriptor scanCount = tf.createLongCounter("scans", "The total number of scan operations", "ops");
    StatisticDescriptor scanInProgress = tf.createLongGauge("scansInProgress", "The number of scan operations in progress", "ops");
    StatisticDescriptor scanTime = tf.createLongCounter("scanTime", "The total time scanner was operational", "nanoseconds");
    StatisticDescriptor scanBytes = tf.createLongCounter("scanBytes", "The total number of bytes scanned from disk", "bytes");
    StatisticDescriptor scanErrors = tf.createLongCounter("scanErrors", "The total number of scan errors", "errors");
    StatisticDescriptor scanIterations = tf.createLongCounter("scanIterations", "The total number of scan iterations", "ops");
    StatisticDescriptor scanIterationTime = tf.createLongCounter("scanIterationTime", "The total time spent scanning from persistence layer", "nanoseconds");

    StatisticDescriptor writeCount = tf.createLongCounter("writes", "The total number of write operations", "ops");
    StatisticDescriptor writeInProgress = tf.createLongGauge("writesInProgress", "The number of write operations in progress", "ops");
    StatisticDescriptor writeTime = tf.createLongCounter("writeTime", "The total time spent writing to disk", "nanoseconds");
    StatisticDescriptor writeBytes = tf.createLongCounter("writeBytes", "The total number of bytes written to disk", "bytes");
    StatisticDescriptor writeErrors = tf.createLongCounter("writeErrors", "The total number of write errors", "errors");

    StatisticDescriptor putCount = tf.createLongCounter("puts", "The total number of put operations", "ops");
    StatisticDescriptor putInProgress = tf.createLongGauge("putsInProgress", "The number of put operations in progress", "ops");
    StatisticDescriptor putTime = tf.createLongCounter("putTime", "The total time spent in put calls", "nanoseconds");
    StatisticDescriptor putBytes = tf.createLongCounter("putBytes", "The total number of bytes put", "bytes");
    StatisticDescriptor putErrors = tf.createLongCounter("putErrors", "The total number of put errors", "errors");

    StatisticDescriptor flushCount = tf.createLongCounter("flushes", "The total number of flush operations", "ops");
    StatisticDescriptor flushInProgress = tf.createLongGauge("flushesInProgress", "The number of flush operations in progress", "ops");
    StatisticDescriptor flushTime = tf.createLongCounter("flushTime", "The total time spent flushing to disk", "nanoseconds");
    StatisticDescriptor flushBytes = tf.createLongCounter("flushBytes", "The total number of bytes flushed to disk", "bytes");
    StatisticDescriptor flushErrors = tf.createLongCounter("flushErrors", "The total number of flush errors", "errors");

    StatisticDescriptor minorCompactionCount = tf.createLongCounter("minorCompactions", "The total number of minor compaction operations", "ops");
    StatisticDescriptor minorCompactionInProgress = tf.createLongGauge("minorCompactionsInProgress", "The number of minor compaction operations in progress", "ops");
    StatisticDescriptor minorCompactionTime = tf.createLongCounter("minorCompactionTime", "The total time spent in minor compactions", "nanoseconds");
    StatisticDescriptor minorCompactionBytes = tf.createLongCounter("minorCompactionBytes", "The total number of bytes collected during minor compactions", "bytes");
    StatisticDescriptor minorCompactionErrors = tf.createLongCounter("minorCompactionErrors", "The total number of minor compaction errors", "errors");

    StatisticDescriptor majorCompactionCount = tf.createLongCounter("majorCompactions", "The total number of major compaction operations", "ops");
    StatisticDescriptor majorCompactionInProgress = tf.createLongGauge("majorCompactionsInProgress", "The number of major compaction operations in progress", "ops");
    StatisticDescriptor majorCompactionTime = tf.createLongCounter("majorCompactionTime", "The total time spent in major compactions", "nanoseconds");
    StatisticDescriptor majorCompactionBytes = tf.createLongCounter("majorCompactionBytes", "The total number of bytes collected during major compactions", "bytes");
    StatisticDescriptor majorCompactionErrors = tf.createLongCounter("majorCompactionErrors", "The total number of major compaction errors", "errors");

    StatisticDescriptor bloomCount = tf.createLongCounter("bloomFilterCheck", "The total number of Bloom Filter checks", "ops");
    StatisticDescriptor bloomInProgress = tf.createLongGauge("bloomFilterChecksInProgress", "The number of Bloom Filter checks in progress", "ops");
    StatisticDescriptor bloomTime = tf.createLongCounter("bloomFilterCheckTime", "The total time spent checking the Bloom Filter", "nanoseconds");
    StatisticDescriptor bloomErrors = tf.createLongCounter("bloomFilterErrors", "The total number of Bloom Filter errors", "errors");
    StatisticDescriptor bloomFalsePositive = tf.createLongCounter("bloomFilterFalsePositives", "The total number of Bloom Filter false positives", "false positives");

    StatisticDescriptor clearCount = tf.createLongCounter("clears", "The total number of clear operations", "ops");
    StatisticDescriptor clearInProgress = tf.createLongGauge("clearsInProgress", "The number of clear operations in progress", "ops");
    StatisticDescriptor clearTime = tf.createLongCounter("clearTime", "The total time spent in clear operations", "nanoseconds");
    StatisticDescriptor clearErrors = tf.createLongGauge("clearErrors", "The total number of clear errors", "errors");

    StatisticDescriptor destroyCount = tf.createLongCounter("destroys", "The total number of destroy operations", "ops");
    StatisticDescriptor destroyInProgress = tf.createLongGauge("destroysInProgress", "The number of destroy operations in progress", "ops");
    StatisticDescriptor destroyTime = tf.createLongCounter("destroyTime", "The total time spent in destroy operations", "nanoseconds");
    StatisticDescriptor destroyErrors = tf.createLongGauge("destroyErrors", "The total number of destroy errors", "errors");

    StatisticDescriptor brCount = tf.createLongCounter("blockReads", "The total number of block read operations", "ops");
    StatisticDescriptor brInProgress = tf.createLongGauge("blockReadsInProgress", "The number of block read operations in progress", "ops");
    StatisticDescriptor brTime = tf.createLongCounter("blockReadTime", "The total time spent reading blocks from disk", "nanoseconds");
    StatisticDescriptor brBytes = tf.createLongCounter("blockReadBytes", "The total number of block bytes read from disk", "bytes");
    StatisticDescriptor brErrors = tf.createLongCounter("blockReadErrors", "The total number of block read errors", "errors");

    StatisticDescriptor bcMisses = tf.createLongCounter("blockCacheMisses", "The total number of block cache misses", "misses");
    StatisticDescriptor bcHits = tf.createLongCounter("blockCacheHits", "The total number of block cache hits", "hits");
    StatisticDescriptor bcCached = tf.createLongGauge("blocksCached", "The current number of cached blocks", "blocks");
    StatisticDescriptor bcBytesCached = tf.createLongGauge("blockBytesCached", "The current number of bytes cached", "bytes");
    StatisticDescriptor bcBytesEvicted = tf.createLongCounter("blockBytesEvicted", "The total number of bytes cached", "bytes");

    StatisticDescriptor activeFileCount = tf.createLongGauge("activeFileCount", "The total number of active files", "files");
    StatisticDescriptor inactiveFileCount = tf.createLongGauge("inactiveFileCount", "The total number of inactive files", "files");
    
    StatisticsType type = tf.createType(typeName, 
        "Statistics about structured I/O operations for a region", new StatisticDescriptor[] {
        readCount, readInProgress, readTime, readBytes, readErrors,
        scanCount, scanInProgress, scanTime, scanBytes, scanErrors, scanIterations, scanIterationTime,
        writeCount, writeInProgress, writeTime, writeBytes, writeErrors,
        putCount, putInProgress, putTime, putBytes, putErrors,
        flushCount, flushInProgress, flushTime, flushBytes, flushErrors,
        minorCompactionCount, minorCompactionInProgress, minorCompactionTime, minorCompactionBytes, minorCompactionErrors,
        majorCompactionCount, majorCompactionInProgress, majorCompactionTime, majorCompactionBytes, majorCompactionErrors,
        bloomCount, bloomInProgress, bloomTime, bloomErrors, bloomFalsePositive,
        clearCount, clearInProgress, clearTime, clearErrors,
        destroyCount, destroyInProgress, destroyTime, destroyErrors,
        brCount, brInProgress, brTime, brBytes, brErrors,
        bcMisses, bcHits, bcCached, bcBytesCached, bcBytesEvicted,
        activeFileCount, inactiveFileCount
    });

    read = new IOOperation(readCount.getId(), readInProgress.getId(), readTime.getId(), readBytes.getId(), readErrors.getId());
    scan = new ScanOperation(scanCount.getId(), scanInProgress.getId(), scanTime.getId(), scanBytes.getId(), scanErrors.getId(), scanIterations.getId(), scanIterationTime.getId());    
    write = new IOOperation(writeCount.getId(), writeInProgress.getId(), writeTime.getId(), writeBytes.getId(), writeErrors.getId());
    put = new IOOperation(putCount.getId(), putInProgress.getId(), putTime.getId(), putBytes.getId(), putErrors.getId());
    flush = new IOOperation(flushCount.getId(), flushInProgress.getId(), flushTime.getId(), flushBytes.getId(), flushErrors.getId());
    minorCompaction = new IOOperation(minorCompactionCount.getId(), minorCompactionInProgress.getId(), minorCompactionTime.getId(), minorCompactionBytes.getId(), minorCompactionErrors.getId());
    majorCompaction = new IOOperation(majorCompactionCount.getId(), majorCompactionInProgress.getId(), majorCompactionTime.getId(), majorCompactionBytes.getId(), majorCompactionErrors.getId());
    bloom = new BloomOperation(bloomCount.getId(), bloomInProgress.getId(), bloomTime.getId(), bloomErrors.getId(), bloomFalsePositive.getId());
    clear = new TimedOperation(clearCount.getId(), clearInProgress.getId(), clearTime.getId(), clearErrors.getId());
    destroy = new TimedOperation(destroyCount.getId(), destroyInProgress.getId(), destroyTime.getId(), destroyErrors.getId());
    
    blockRead = new IOOperation(brCount.getId(), brInProgress.getId(), brTime.getId(), brBytes.getId(), brErrors.getId());
    blockCache = new CacheOperation(bcMisses.getId(), bcHits.getId(), bcCached.getId(), bcBytesCached.getId(), bcBytesEvicted.getId());

    activeFilesId = activeFileCount.getId();
    inactiveFilesId = inactiveFileCount.getId();

    stats = factory.createAtomicStatistics(type, name);
  }

  public void close() {
    stats.close();
  }
  
  public Statistics getStats() {
    return stats;
  }
  
  public IOOperation getRead() {
    return read;
  }
  
  public ScanOperation getScan() {
    return scan;
  }
  
  public IOOperation getWrite() {
    return write;
  }
  
  public IOOperation getPut() {
    return put;
  }
  
  public IOOperation getFlush() {
    return flush;
  }
  
  public IOOperation getMinorCompaction() {
    return minorCompaction;
  }
  
  public IOOperation getMajorCompaction() {
    return majorCompaction;
  }
  
  public BloomOperation getBloom() {
    return bloom;
  }
  
  public TimedOperation getClear() {
    return clear;
  }
  
  public TimedOperation getDestroy() {
    return destroy;
  }

  public IOOperation getBlockRead() {
    return blockRead;
  }
  
  public CacheOperation getBlockCache() {
    return blockCache;
  }
  
  public long getActiveFileCount() {
    return stats.getLong(activeFilesId);
  }
  
  public long getInactiveFileCount() {
    return stats.getLong(inactiveFilesId);
  }
  
  public void incActiveFiles(int amt) {
    stats.incLong(activeFilesId, amt);
    assert stats.getLong(activeFilesId) >= 0;
  }
  
  public void incInactiveFiles(int amt) {
    stats.incLong(inactiveFilesId, amt);
    assert stats.getLong(inactiveFilesId) >= 0;
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("read = {").append(read).append("}\n");
    sb.append("scan = {").append(scan).append("}\n");
    sb.append("write = {").append(write).append("}\n");
    sb.append("put = {").append(put).append("}\n");
    sb.append("flush = {").append(flush).append("}\n");
    sb.append("minorCompaction = {").append(minorCompaction).append("}\n");
    sb.append("majorCompaction = {").append(majorCompaction).append("}\n");
    sb.append("bloom = {").append(bloom).append("}\n");
    sb.append("clear = {").append(clear).append("}\n");
    sb.append("destroy = {").append(destroy).append("}\n");
    sb.append("blockRead = {").append(blockRead).append("}\n");
    sb.append("blockCache = {").append(blockCache).append("}\n");
    sb.append("activeFiles = ").append(stats.getLong(activeFilesId)).append("\n");
    sb.append("inactiveFiles = ").append(stats.getLong(inactiveFilesId)).append("\n");
    
    return sb.toString();
  }
  
  public class TimedOperation {
    protected final int countId;
    protected final int inProgressId;
    protected final int timeId;
    private final int errorsId;
    
    public TimedOperation(int count, int inProgress, int time, int errors) {
      this.countId = count;
      this.inProgressId = inProgress;
      this.timeId = time;
      this.errorsId = errors;
    }
    
    public long begin() {
      stats.incLong(inProgressId, 1);
      return getStatTime();
    }
    
    public long end(long start) {
      stats.incLong(inProgressId, -1);
      stats.incLong(countId, 1);
      stats.incLong(timeId, getStatTime() - start);
      return getStatTime();
    }
    
    public void error(long start) {
      end(start);
      stats.incLong(errorsId, 1);
    }
    
    public long getCount() {
      return stats.getLong(countId);
    }
    
    public long getInProgress() {
      return stats.getLong(inProgressId);
    }
    
    public long getTime() {
      return stats.getLong(timeId);
    }
    
    public long getErrors() {
      return stats.getLong(errorsId);
    }
    
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("count=").append(getCount());
      sb.append(";inProgress=").append(getInProgress());
      sb.append(";errors=").append(getErrors());
      sb.append(";time=").append(getTime());
      
      return sb.toString();
    }
  }
  
  public class IOOperation extends TimedOperation {
    protected final int bytesId;
    
    public IOOperation(int count, int inProgress, int time, int bytes, int errors) {
      super(count, inProgress, time, errors);
      this.bytesId = bytes;
    }
    
    public long end(long bytes, long start) {
      stats.incLong(bytesId, bytes);
      return super.end(start);
    }
    
    public long getBytes() {
      return stats.getLong(bytesId);
    }
    
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder(super.toString());
      sb.append(";bytes=").append(getBytes());
      
      return sb.toString();
    }
  }

  public class ScanOperation extends IOOperation {
    private final int iterationsId;
    private final int iterationTimeId;

    public ScanOperation(int count, int inProgress, int time, int bytes, int errors, int iterCount, int iterTime) {
      super(count, inProgress, time, bytes, errors);
      iterationsId = iterCount;
      iterationTimeId = iterTime;
    }
    
    public long beginIteration() {
      return getStatTime();
    }
    
    public void endIteration(long bytes, long start){
      stats.incLong(iterationsId, 1);
      stats.incLong(bytesId, bytes);
      stats.incLong(iterationTimeId, getStatTime() - start);
    }
    
    public long getIterations() {
      return stats.getLong(iterationsId);
    }
    
    public long getIterationTime() {
      return stats.getLong(iterationTimeId);
    }
    
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder(super.toString());
      sb.append(";iterations=").append(getIterations());
      sb.append(";iterationTime=").append(getIterationTime());
      
      return sb.toString();
    }
  }

  public class BloomOperation extends TimedOperation {
    private final int falsePositiveId;
    
    public BloomOperation(int count, int inProgress, int time, int errors, int falsePositive) {
      super(count, inProgress, time, errors);
      this.falsePositiveId = falsePositive;
    }
    
    public void falsePositive() {
      stats.incLong(falsePositiveId, 1);
    }
    
    public long getFalsePositives() {
      return stats.getLong(falsePositiveId);
    }
    
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder(super.toString());
      sb.append(";falsePositives=").append(getFalsePositives());
      
      return sb.toString();
    }
  }
  
  public class CacheOperation {
    private final int missesId;
    private final int hitsId;
    private final int cachedId;
    private final int bytesCachedId;
    private final int bytesEvictedId;
    
    public CacheOperation(int missesId, int hitsId, int cachedId, 
        int bytesCachedId, int bytesEvictedId) {
      this.missesId = missesId;
      this.hitsId = hitsId;
      this.cachedId = cachedId;
      this.bytesCachedId = bytesCachedId;
      this.bytesEvictedId = bytesEvictedId;
    }
    
    public void store(long bytes) {
      stats.incLong(cachedId, 1);
      stats.incLong(bytesCachedId, bytes);
    }
    
    public void evict(long bytes) {
      stats.incLong(cachedId, -1);
      stats.incLong(bytesCachedId, -bytes);
      stats.incLong(bytesEvictedId, bytes);
    }
    
    public void hit() {
      stats.incLong(hitsId, 1);
    }
    
    public void miss() {
      stats.incLong(missesId, 1);
    }
    
    public long getMisses() {
      return stats.getLong(missesId);
    }
    
    public long getHits() {
      return stats.getLong(hitsId);
    }
    
    public long getCached() {
      return stats.getLong(cachedId);
    }
    
    public long getBytesCached() {
      return stats.getLong(bytesCachedId);
    }
    
    public long getBytesEvicted() {
      return stats.getLong(bytesEvictedId);
    }
    
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("misses=").append(getMisses());
      sb.append(";hits=").append(getHits());
      sb.append(";cached=").append(getCached());
      sb.append(";bytesCached=").append(getBytesCached());
      sb.append(";bytesEvicted=").append(getBytesEvicted());
      
      return sb.toString();
    }
  }
}
