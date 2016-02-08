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
package com.gemstone.gemfire.cache.hdfs.internal.hoplog;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.ShutdownHookManager;

import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.hdfs.HDFSIOException;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreImpl;
import com.gemstone.gemfire.cache.hdfs.internal.cardinality.HyperLogLog;
import com.gemstone.gemfire.cache.hdfs.internal.cardinality.ICardinality;
import com.gemstone.gemfire.internal.cache.persistence.soplog.DelegatingSerializedComparator;
import com.gemstone.gemfire.internal.cache.persistence.soplog.HFileStoreStatistics;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplogStatistics;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplogStatistics.ScanOperation;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedReader.SerializedComparator;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.util.Hex;
import com.gemstone.gemfire.internal.util.SingletonValue;
import com.gemstone.gemfire.internal.util.SingletonValue.SingletonBuilder;

import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.BlockType.BlockCategory;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFile.Reader;
import org.apache.hadoop.hbase.io.hfile.HFile.Writer;
import org.apache.hadoop.hbase.io.hfile.HFileBlockIndex.BlockIndexReader;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.BloomFilterWriter;

/**
 * Implements hfile based {@link Hoplog}
 */
public final class HFileSortedOplog extends AbstractHoplog {

//  private static final boolean CACHE_DATA_BLOCKS_ON_READ = !Boolean.getBoolean("gemfire.HFileSortedOplog.DISABLE_CACHE_ON_READ");
  private final CacheConfig cacheConf;
  private ICardinality entryCountEstimate;
  
  // a cached reader for the file
  private final SingletonValue<HFileReader> reader;

  public HFileSortedOplog(HDFSStoreImpl store, Path hfilePath,
      BlockCache blockCache, SortedOplogStatistics stats,
      HFileStoreStatistics storeStats) throws IOException {
    super(store, hfilePath, stats);
    cacheConf = getCacheConfInstance(blockCache, stats, storeStats);
    reader = getReaderContainer();
  }

  /**
   * THIS METHOD SHOULD BE USED FOR LONER ONLY
   */
  public static HFileSortedOplog getHoplogForLoner(FileSystem inputFS,
      Path hfilePath) throws IOException {
    return new HFileSortedOplog(inputFS, hfilePath, null, null, null);
  }

  private HFileSortedOplog(FileSystem inputFS, Path hfilePath,
      BlockCache blockCache, SortedOplogStatistics stats,
      HFileStoreStatistics storeStats) throws IOException {
    super(inputFS, hfilePath, stats);
    cacheConf = getCacheConfInstance(blockCache, stats, storeStats);
    reader = getReaderContainer();
  }

  protected CacheConfig getCacheConfInstance(BlockCache blockCache,
      SortedOplogStatistics stats, HFileStoreStatistics storeStats) {
    CacheConfig tmpConfig = null;
//    if (stats == null) {
      tmpConfig = new CacheConfig(conf);
//    } else {
//      tmpConfig = new CacheConfig(conf, CACHE_DATA_BLOCKS_ON_READ, blockCache,
//          HFileSortedOplogFactory.convertStatistics(stats, storeStats));
//    }
    tmpConfig.shouldCacheBlockOnRead(BlockCategory.ALL_CATEGORIES);
    return tmpConfig;
  }  

  private SingletonValue<HFileReader> getReaderContainer() {
    return new SingletonValue<HFileReader>(new SingletonBuilder<HFileReader>() {
      @Override
      public HFileReader create() throws IOException {
        if (logger.isDebugEnabled())
          logger.debug("{}Creating hoplog reader", logPrefix);
        return new HFileReader();
      }

      @Override
      public void postCreate() {
        if (readerListener != null) {
          readerListener.readerCreated();
        }
      }
      
      @Override
      public void createInProgress() {
      }
    });
  }
  
  @Override
  public HoplogReader getReader() throws IOException {
    return reader.get();
  }
  
  @Override
  public ICardinality getEntryCountEstimate() throws IOException {
    ICardinality result = entryCountEstimate;
    if (result == null) {
      HoplogReader rdr = getReader(); // keep this out of the critical section
      synchronized(this) {
        result = entryCountEstimate;
          if (result == null) {
            entryCountEstimate = result = rdr.getCardinalityEstimator();
          }
        }
    }
    return result;
  }
  
  @Override
  public HoplogWriter createWriter(int keys) throws IOException {
    return new HFileSortedOplogWriter(keys);
  }

  @Override
  public boolean isClosed() {
    HFileReader rdr = reader.getCachedValue();
    return rdr == null || rdr.isClosed();
  }
  
  @Override
  public void close() throws IOException {
    close(true);
  }

  @Override
  public void close(boolean clearCache) throws IOException {
    compareAndClose(null, clearCache);
  }
  
  private void compareAndClose(HFileReader hfileReader, boolean clearCache) throws IOException {
    HFileReader rdr ;
    if (hfileReader == null) {
      rdr = reader.clear(true);
    } else {
      boolean result = reader.clear(hfileReader, true);
      if (! result) {
        if (logger.isDebugEnabled())
          logger.debug("{}skipping close, provided hfileReader mismatched", logPrefix);
        return;
      } 
      rdr = hfileReader;
    }
    
    if (rdr != null) {
      try {
        rdr.close(clearCache);
      } finally {
        if (readerListener != null) {
          readerListener.readerClosed();
        }
      }
    }
  }
  
  @Override
  public String toString() {
    return "HFileSortedOplog[" + getFileName() + "]";
  }

  private class HFileSortedOplogWriter implements HoplogWriter {
    private final Writer writer;
    private final BloomFilterWriter bfw;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public HFileSortedOplogWriter(int keys) throws IOException {
      try {
        int hfileBlockSize = Integer.getInteger(
            HoplogConfig.HFILE_BLOCK_SIZE_CONF, (1 << 16));

        Algorithm compress = Algorithm.valueOf(System.getProperty(HoplogConfig.COMPRESSION,
            HoplogConfig.COMPRESSION_DEFAULT));

//        ByteComparator bc = new ByteComparator();
        writer = HFile.getWriterFactory(conf, cacheConf)
            .withPath(fsProvider.getFS(), path)
            .withBlockSize(hfileBlockSize)
//            .withComparator(bc)
            .withCompression(compress)
            .create();
//        bfw = BloomFilterFactory.createGeneralBloomAtWrite(conf, cacheConf, BloomType.ROW, keys,
//            writer, bc);
        bfw = BloomFilterFactory.createGeneralBloomAtWrite(conf, cacheConf, BloomType.ROW, keys,
            writer);

        if (logger.isDebugEnabled())
          logger.debug("{}Created hoplog writer with compression " + compress, logPrefix);
      } catch (IOException e) {
        if (logger.isDebugEnabled())
          logger.debug("{}IO Error while creating writer", logPrefix);
        throw e;
      }
    }

    @Override
    public void append(byte[] key, byte[] value) throws IOException {
      writer.append(key, value);
      bfw.add(key, 0, key.length);
    }

    @Override
    public void append(ByteBuffer key, ByteBuffer value) throws IOException {
      byte[] keyBytes = byteBufferToArray(key);
      byte[] valueBytes = byteBufferToArray(value);
      writer.append(keyBytes, valueBytes);
      bfw.add(keyBytes, 0, keyBytes.length);
    }

    @Override
    public void close() throws IOException {
      close(null);
    }

    @Override
    public void close(EnumMap<Meta, byte[]> metadata) throws IOException {
      if (closed.get()) {
        if (logger.isDebugEnabled())
          logger.debug("{}Writer already closed", logPrefix);
        return;
      }
      
      bfw.compactBloom();
      writer.addGeneralBloomFilter(bfw);

      // append system metadata
      writer.appendFileInfo(Meta.GEMFIRE_MAGIC.toBytes(), Hoplog.MAGIC);
      writer.appendFileInfo(Meta.SORTED_OPLOG_VERSION.toBytes(), HoplogVersion.V1.toBytes());
      writer.appendFileInfo(Meta.GEMFIRE_VERSION.toBytes(), Version.CURRENT.toBytes());
      
      // append comparator info
//      if (writer.getComparator() instanceof DelegatingSerializedComparator) {
//        ByteArrayOutputStream bos = new ByteArrayOutputStream();
//        DataOutput out = new DataOutputStream(bos);
//        
//        writeComparatorInfo(out, ((DelegatingSerializedComparator) writer.getComparator()).getComparators());
//        writer.appendFileInfo(Meta.COMPARATORS.toBytes(), bos.toByteArray());
//      }
      
      // append user metadata
      HyperLogLog cachedEntryCountEstimate = null;
      if (metadata != null) {
        for (Entry<Meta, byte[]> entry : metadata.entrySet()) {
          writer.appendFileInfo(entry.getKey().toBytes(), entry.getValue());
          if (Meta.LOCAL_CARDINALITY_ESTIMATE_V2.equals(entry.getKey())) {
             cachedEntryCountEstimate = HyperLogLog.Builder.build(entry.getValue()); 
          }
        }
      }
      
      writer.close();
      if (logger.isDebugEnabled())
        logger.debug("{}Completed closing writer", logPrefix);
      closed.set(true);
      // cache estimate value to avoid reads later
      entryCountEstimate = cachedEntryCountEstimate;
    }

    @Override
    public void hsync() throws IOException {
      throw new UnsupportedOperationException("hsync is not supported for HFiles"); 
    }

    @Override
    public long getCurrentSize() throws IOException {
      throw new UnsupportedOperationException("getCurrentSize is not supported for HFiles"); 
    }
    
//    private void writeComparatorInfo(DataOutput out, SerializedComparator[] comparators) throws IOException {
//      out.writeInt(comparators.length);
//      for (SerializedComparator sc : comparators) {
//        out.writeUTF(sc.getClass().getName());
//        if (sc instanceof DelegatingSerializedComparator) {
//          writeComparatorInfo(out, ((DelegatingSerializedComparator) sc).getComparators());
//        }
//      }
//    }
  }
  
  private void handleReadIOError(HFileReader hfileReader, IOException e, boolean skipFailIfSafe) {
    if (logger.isDebugEnabled())
      logger.debug("Read IO error", e);
    boolean safeError = ShutdownHookManager.get().isShutdownInProgress();
    if (safeError) {
      // IOException because of closed file system. This happens when member is
      // shutting down
      if (logger.isDebugEnabled())
        logger.debug("IO error caused by filesystem shutdown", e);
      throw new CacheClosedException("IO error caused by filesystem shutdown", e);
    } 
    
    // expose the error wrapped inside remote exception. Remote exceptions are
    // handled by file system client. So let the caller handle this error
    if (e instanceof RemoteException) {
      e = ((RemoteException) e).unwrapRemoteException();
      throw new HDFSIOException(LocalizedStrings.HOPLOG_FAILED_TO_READ_HDFS_FILE.toLocalizedString(path), e);
    } 
    
    FileSystem currentFs = fsProvider.checkFileSystem();
    if (hfileReader != null && hfileReader.previousFS != currentFs) {
      if (logger.isDebugEnabled()) {
        logger.debug("{}Detected new FS client, closing old reader", logPrefix);
        if (currentFs != null) {
          if (logger.isDebugEnabled())
            logger.debug("CurrentFs:" + currentFs.getUri() + "-"
                + currentFs.hashCode(), logPrefix);
        }
        if (hfileReader.previousFS != null) {
          if (logger.isDebugEnabled())
            logger.debug("OldFs:" + hfileReader.previousFS.getUri() + "-"
                + hfileReader.previousFS.hashCode() + ", closing old reader", logPrefix);
        }
      }
      try {
        HFileSortedOplog.this.compareAndClose(hfileReader, false);
      } catch (Exception ex) {
        if (logger.isDebugEnabled())
          logger.debug("Failed to close reader", ex);
      }
      if (skipFailIfSafe) {
        if (logger.isDebugEnabled())
          logger.debug("Not faling after io error since FS client changed");
        return;
      }
    }

    // it is not a safe error. let the caller handle it
    throw new HDFSIOException(LocalizedStrings.HOPLOG_FAILED_TO_READ_HDFS_FILE.toLocalizedString(path), e);
  }

  class HFileReader implements HoplogReader, Closeable {
    private final Reader reader;
    private volatile BloomFilter hoplogBloom;
    private final AtomicBoolean closed;
    private final Map<byte[], byte[]> fileInfo;
    private final HyperLogLog estimator;
    private final FileSystem previousFS;
    
    public HFileReader() throws IOException {
      try {
        FileSystem fs = fsProvider.getFS();
        reader = HFile.createReader(fs, path, cacheConf);
        fileInfo = reader.loadFileInfo();
        closed = new AtomicBoolean(false);

        validate();
        if (reader.getComparator() instanceof DelegatingSerializedComparator) {
          loadComparators((DelegatingSerializedComparator) reader.getComparator());
        }

        // read the old HLL if it exists so that a CardinalityMergeException will trigger a Major Compaction
        byte[] hll = fileInfo.get(Meta.LOCAL_CARDINALITY_ESTIMATE.toBytes());
        if (hll != null) {
          entryCountEstimate = estimator = HyperLogLog.Builder.build(hll);
        } else if ((hll = fileInfo.get(Meta.LOCAL_CARDINALITY_ESTIMATE_V2.toBytes())) != null) {
          entryCountEstimate = estimator = HyperLogLog.Builder.build(hll);
        } else {
          estimator = new HyperLogLog(HdfsSortedOplogOrganizer.HLL_CONSTANT);
        }
        
        previousFS = fs;
      } catch (IOException e) {
        if (logger.isDebugEnabled())
          logger.debug("IO Error while creating reader", e);
        throw e;
      }
    }

    @Override
    public byte[] read(byte[] key) throws IOException {
      IOException err = null;
      HFileReader delegateReader = this;
      for (int retry = 1; retry >= 0; retry --) {
        try {
          return delegateReader.readDelegate(key);
        } catch (IOException e) {
          err = e;
          handleReadIOError(delegateReader, e, retry > 0);
          // Current reader may have got closed in error handling. Get the new
          // one for retry attempt
          try {
            delegateReader = (HFileReader) HFileSortedOplog.this.getReader(); 
          } catch (IOException ex) {
            handleReadIOError(null, e, false);
          }
        }
      }

      if (logger.isDebugEnabled())
        logger.debug("Throwing err from read delegate ", err);
      throw err;
    }

    private byte[] readDelegate(byte[] key) throws IOException {
      try {
        if (!getBloomFilter().mightContain(key)) {
          // bloom filter check failed, the key is not present in this hoplog
          return null;
        }
      } catch (IllegalArgumentException e) {
        if (IOException.class.isAssignableFrom(e.getCause().getClass())) {
          throw (IOException) e.getCause();
        } else {
          throw e;
        }
      }
      
      byte[] valueBytes = null;
      ByteBuffer bb = get(key);
      if (bb != null) {
        valueBytes = new byte[bb.remaining()];
        bb.get(valueBytes);
      } else {
        stats.getBloom().falsePositive();
      }
      return valueBytes;
    }

    @Override
    public ByteBuffer get(byte[] key) throws IOException {
      assert key != null;
      HFileScanner seek = reader.getScanner(false, true);
      if (seek.seekTo(key) == 0) {
        return seek.getValue();
      }
      return null;
    }

    @Override
    public HoplogIterator<byte[], byte[]> scan(byte[] from, boolean fromInclusive, byte[] to,
        boolean toInclusive) throws IOException {
      IOException err = null;
      HFileReader delegateReader = this;
      for (int retry = 1; retry >= 0; retry --) {
        try {
          return delegateReader.scanDelegate(from, fromInclusive, to, toInclusive);
        } catch (IOException e) {
          err = e;
          handleReadIOError(delegateReader, e, retry > 0);
          // Current reader may have got closed in error handling. Get the new
          // one for retry attempt
          try {
            delegateReader = (HFileReader) HFileSortedOplog.this.getReader(); 
          } catch (IOException ex) {
            handleReadIOError(null, e, false);
          }
        }
      }
      if (logger.isDebugEnabled())
        logger.debug("Throwing err from scan delegate ", err);
      throw err;
    }

    private HoplogIterator<byte[], byte[]> scanDelegate(byte[] from, boolean fromInclusive, byte[] to,
        boolean toInclusive) throws IOException {
      return new HFileSortedIterator(reader.getScanner(true, false), from,
          fromInclusive, to, toInclusive);
    }
    
    @Override
    public HoplogIterator<byte[], byte[]> scan(long offset, long length)
        throws IOException {
      /**
       * Identifies the first and last key to be scanned based on offset and
       * length. It loads hfile block index and identifies the first hfile block
       * starting after offset. The key of that block is from key for scanner.
       * Similarly it locates first block starting beyond offset + length range.
       * It uses key of that block as the to key for scanner
       */

      // load block indexes in memory
      BlockIndexReader bir = reader.getDataBlockIndexReader();
      int blockCount = bir.getRootBlockCount();
      
      byte[] fromKey = null, toKey = null;

      // find from key
      int i = 0;
      for (; i < blockCount; i++) {
        if (bir.getRootBlockOffset(i) < offset) {
          // hfile block has offset less than this reader's split offset. check
          // the next block
          continue;
        }

        // found the first hfile block starting after offset
        fromKey = bir.getRootBlockKey(i);
        break;
      }

      if (fromKey == null) {
        // seems no block starts after the offset. return no-op scanner
        return new HFileSortedIterator(null, null, false, null, false);
      }
      
      // find to key
      for (; i < blockCount; i++) {
        if (bir.getRootBlockOffset(i) < (offset + length)) {
          // this hfile block lies within the offset+lenght range. check the
          // next block for a higher offset
          continue;
        }

        // found the first block starting beyong offset+length range.
        toKey = bir.getRootBlockKey(i);
        break;
      }

      // from key is included in scan and to key is excluded
      HFileScanner scanner = reader.getScanner(true, false);
      return new HFileSortedIterator(scanner, fromKey, true, toKey, false);
    }
    
    @Override
    public HoplogIterator<byte[], byte[]> scan() throws IOException {
      return scan(null, null);
    }

    public HoplogIterator<byte[], byte[]> scan(byte[] from, byte[] to)
        throws IOException {
      return scan(from, true, to, false);
    }

    @Override
    public BloomFilter getBloomFilter() throws IOException {
      BloomFilter result = hoplogBloom;
      if (result == null) {
        synchronized (this) {
          result = hoplogBloom;
          if (result == null) {
            hoplogBloom = result = new BloomFilterImpl();
          }
        }
      }
      return result;
    }

    @Override
    public boolean isClosed() {
      return closed.get();
    }
    
    @Override
    public void close() throws IOException {
      close(true);
    }
    
    public void close(boolean clearCache) throws IOException {
      if (closed.compareAndSet(false, true)) {
        if (logger.isDebugEnabled())
          logger.debug("{}Closing reader", logPrefix);
        reader.close(clearCache);
      }
    }

    @Override
    public long getEntryCount() {
      return reader.getEntries();
    }

    public ICardinality getCardinalityEstimator() {
      return estimator;
    }

    @Override
    public long sizeEstimate() {
      return getCardinalityEstimator().cardinality();
    }

    private void validate() throws IOException {
      // check magic
      byte[] magic = fileInfo.get(Meta.GEMFIRE_MAGIC.toBytes());
      if (!Arrays.equals(magic, MAGIC)) {
        throw new IOException(LocalizedStrings.Soplog_INVALID_MAGIC.toLocalizedString(Hex.toHex(magic)));
      }
      
      // check version compatibility
      byte[] ver = fileInfo.get(Meta.SORTED_OPLOG_VERSION.toBytes());
      if (logger.isDebugEnabled()) {
        logger.debug("{}Hoplog version is " + Hex.toHex(ver), logPrefix);
      }
      
      if (!Arrays.equals(ver, HoplogVersion.V1.toBytes())) {
        throw new IOException(LocalizedStrings.Soplog_UNRECOGNIZED_VERSION.toLocalizedString(Hex.toHex(ver)));
      }
    }
    
    private void loadComparators(DelegatingSerializedComparator comparator) throws IOException {
      byte[] raw = fileInfo.get(Meta.COMPARATORS.toBytes());
      assert raw != null;

      DataInput in = new DataInputStream(new ByteArrayInputStream(raw));
      comparator.setComparators(readComparators(in));
    }
    
    private SerializedComparator[] readComparators(DataInput in) throws IOException {
      try {
        SerializedComparator[] comps = new SerializedComparator[in.readInt()];
        assert comps.length > 0;
        
        for (int i = 0; i < comps.length; i++) {
          comps[i] = (SerializedComparator) Class.forName(in.readUTF()).newInstance();
          if (comps[i] instanceof DelegatingSerializedComparator) {
            ((DelegatingSerializedComparator) comps[i]).setComparators(readComparators(in));
          }
        }
        return comps;
        
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
    
    class BloomFilterImpl implements BloomFilter {
      private final org.apache.hadoop.hbase.util.BloomFilter hfileBloom;

      public BloomFilterImpl() throws IOException {
        DataInput bin = reader.getGeneralBloomFilterMetadata();
        // instantiate bloom filter if meta present in hfile
        if (bin != null) {
          hfileBloom = BloomFilterFactory.createFromMeta(bin, reader);
          if (reader.getComparator() instanceof DelegatingSerializedComparator) {
            loadComparators((DelegatingSerializedComparator) hfileBloom.getComparator());
          }
        } else {
          hfileBloom = null;
        }
      }

      @Override
      public boolean mightContain(byte[] key) {
        assert key != null;
        return mightContain(key, 0, key.length);
      }

      @Override
      public boolean mightContain(byte[] key, int keyOffset, int keyLength) {
        assert key != null;
        long start = stats.getBloom().begin();
        boolean found = hfileBloom == null ? true : hfileBloom.contains(key, keyOffset, keyLength, null);
        stats.getBloom().end(start);
        return found;
      }

      @Override
      public long getBloomSize() {
        return hfileBloom == null ? 0 : hfileBloom.getByteSize();
      }
    }

    // TODO change the KV types to ByteBuffer instead of byte[]
    public final class HFileSortedIterator implements HoplogIterator<byte[], byte[]> {
      private final HFileScanner scan;
      
      private final byte[] from;
      private final boolean fromInclusive;
      
      private final byte[] to;
      private final boolean toInclusive;
      
      private ByteBuffer prefetchedKey;
      private ByteBuffer prefetchedValue;
      private ByteBuffer currentKey;
      private ByteBuffer currentValue;
      
      // variable linked to scan stats
      ScanOperation scanStat;
      private long scanStart;
      
      public HFileSortedIterator(HFileScanner scan, byte[] from, boolean fromInclusive, byte[] to, 
          boolean toInclusive) throws IOException {
        this.scan = scan;
        this.from = from;
        this.fromInclusive = fromInclusive;
        this.to = to;
        this.toInclusive = toInclusive;

        scanStat = (stats == null) ? new SortedOplogStatistics("", "").new ScanOperation(
            0, 0, 0, 0, 0, 0, 0) : stats.getScan();
        scanStart = scanStat.begin();

        if (scan == null) {
          return;
        }

        assert from == null || to == null
            || scan.getReader().getComparator().compare(from, to) <= 0;

        initIterator();
      }
      
      /*
       * prefetches first key and value from the file for hasnext to work
       */
      private void initIterator() throws IOException {
        long startNext = scanStat.beginIteration();
        boolean scanSuccessful = true;
        if (from == null) {
          scanSuccessful = scan.seekTo();
        } else {
          int compare = scan.seekTo(from);
          if (compare == 0 && !fromInclusive || compare > 0) {
            // as from in exclusive and first key is same as from, skip the first key
            scanSuccessful = scan.next();
          }
        }
        
        populateKV(startNext, scanSuccessful);
      }
      
      @Override
      public boolean hasNext() {
        return prefetchedKey != null;
      }

      @Override
      public byte[] next() throws IOException {
        return byteBufferToArray(nextBB());
      }

      public ByteBuffer nextBB() throws IOException {
        long startNext = scanStat.beginIteration();
        if (prefetchedKey == null) {
          throw new NoSuchElementException();
        }

        currentKey = prefetchedKey;
        currentValue = prefetchedValue;

        prefetchedKey = null;
        prefetchedValue = null;

        if (scan.next()) {
          populateKV(startNext, true);
        }
        
        return currentKey;
      }

      
      private void populateKV(long nextStartTime, boolean scanSuccessful) {
        if (!scanSuccessful) {
          //end of file reached. collect stats and return
          scanStat.endIteration(0, nextStartTime);
          return;
        }
        
        prefetchedKey = scan.getKey();
        prefetchedValue = scan.getValue();
        
        if (to != null) {
          // TODO Optimization? Perform int comparison instead of byte[]. Identify
          // offset of key greater than two.
          int compare = -1;
          compare = scan.getReader().getComparator().compare
              (prefetchedKey.array(), prefetchedKey.arrayOffset(), prefetchedKey.remaining(), to, 0, to.length);
          if (compare > 0 || (compare == 0 && !toInclusive)) {
            prefetchedKey = null;
            prefetchedValue = null;
            return;
          }
        }
        
        // account for bytes read and time spent
        int byteCount = prefetchedKey.remaining() + prefetchedValue.remaining();
        scanStat.endIteration(byteCount, nextStartTime);
      }
      

      @Override
      public byte[] getKey() {
        return byteBufferToArray(getKeyBB());
      }
      public ByteBuffer getKeyBB() {
        return currentKey;
      }

      @Override
      public byte[] getValue() {
        return byteBufferToArray(getValueBB());
      }
      public ByteBuffer getValueBB() {
        return currentValue;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("Cannot delete a key-value from a hfile sorted oplog");
      }
      
      @Override
      public void close() {
        scanStat.end(scanStart);
      }
    }
  }
  
  public static byte[] byteBufferToArray(ByteBuffer bb) {
    if (bb == null) {
      return null;
    }
    
    byte[] tmp = new byte[bb.remaining()];
    bb.duplicate().get(tmp);
    return tmp;
  }
}
