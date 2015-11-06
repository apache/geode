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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.ShutdownHookManager;

import com.gemstone.gemfire.InternalGemFireException;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.hdfs.HDFSIOException;
import com.gemstone.gemfire.cache.hdfs.HDFSStore;
import com.gemstone.gemfire.cache.hdfs.internal.QueuedPersistentEvent;
import com.gemstone.gemfire.cache.hdfs.internal.SortedHoplogPersistedEvent;
import com.gemstone.gemfire.cache.hdfs.internal.cardinality.CardinalityMergeException;
import com.gemstone.gemfire.cache.hdfs.internal.cardinality.HyperLogLog;
import com.gemstone.gemfire.cache.hdfs.internal.cardinality.ICardinality;
import com.gemstone.gemfire.cache.hdfs.internal.cardinality.MurmurHash;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HDFSCompactionManager.CompactionRequest;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HDFSRegionDirector.HdfsRegionManager;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.Hoplog.HoplogReader;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.Hoplog.HoplogReaderActivityListener;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.Hoplog.HoplogWriter;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.Hoplog.Meta;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.mapreduce.HoplogUtil;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.PrimaryBucketException;
import com.gemstone.gemfire.internal.cache.execute.BucketMovedException;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplogStatistics.IOOperation;
import com.gemstone.gemfire.internal.cache.persistence.soplog.TrackedReference;
import com.gemstone.gemfire.internal.concurrent.ConcurrentHashSet;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * Manages sorted oplog files for a bucket. An instance per bucket will exist in
 * each PR
 * 
 * @author ashvina
 */
public class HdfsSortedOplogOrganizer extends AbstractHoplogOrganizer<SortedHoplogPersistedEvent> {
  public static final int AVG_NUM_KEYS_PER_INDEX_BLOCK = 200;
  
  // all valid sorted hoplogs will follow the following name pattern
  public static final String SORTED_HOPLOG_REGEX = HOPLOG_NAME_REGEX + "("
      + FLUSH_HOPLOG_EXTENSION + "|" + MINOR_HOPLOG_EXTENSION + "|"
      + MAJOR_HOPLOG_EXTENSION + ")";
  public static final Pattern SORTED_HOPLOG_PATTERN = Pattern.compile(SORTED_HOPLOG_REGEX);
  
  //Amount of time before deleting old temporary files
  final long TMP_FILE_EXPIRATION_TIME_MS = Long.getLong(HoplogConfig.TMP_FILE_EXPIRATION, HoplogConfig.TMP_FILE_EXPIRATION_DEFAULT);
  
  static float RATIO = HoplogConfig.COMPACTION_FILE_RATIO_DEFAULT;

  // Compacter for this bucket
  private Compactor compacter;
    
  private final HoplogReadersController hoplogReadersController;
  private AtomicLong previousCleanupTimestamp = new AtomicLong(Long.MIN_VALUE);

  /**
   * The default HLL constant. gives an accuracy of about 3.25%
   * public only for testing upgrade from 1.3 to 1.4
   */
  public static double HLL_CONSTANT = 0.03;
  /**
   * This estimator keeps track of this buckets entry count. This value is
   * affected by flush and compaction cycles
   */
  private volatile ICardinality bucketSize = new HyperLogLog(HLL_CONSTANT);
  //A set of tmp files that existed when this bucket organizer was originally
  //created. These may still be open by the old primary, or they may be
  //abandoned files.
  private LinkedList<FileStatus> oldTmpFiles;

  private ConcurrentMap<Hoplog, Boolean> tmpFiles = new ConcurrentHashMap<Hoplog, Boolean>();

  protected volatile boolean organizerClosed = false;

  /**
   * For the 1.4 release we are changing the HLL_CONSTANT which will make the
   * old persisted HLLs incompatible with the new HLLs. To fix this we will
   * force a major compaction when the system starts up so that we will only
   * have new HLLs in the system (see bug 51403)
   */
  private boolean startCompactionOnStartup = false;

  /**
   * @param region
   *          Region manager instance. Instances of hdfs listener instance,
   *          stats collector, file system, etc are shared by all buckets of a
   *          region and provided by region manager instance
   * @param bucketId bucket id to be managed by this organizer
   * @throws IOException
   */
  public HdfsSortedOplogOrganizer(HdfsRegionManager region, int bucketId) throws IOException{
    super(region, bucketId);
    
    String val = System.getProperty(HoplogConfig.COMPACTION_FILE_RATIO);
    try {
      RATIO = Float.parseFloat(val);
    } catch (Exception e) {
    }

    hoplogReadersController = new HoplogReadersController();
    
    // initialize with all the files in the directory
    List<Hoplog> hoplogs = identifyAndLoadSortedOplogs(true);
    if (logger.isDebugEnabled()) {
      logger.debug("{}Initializing bucket with existing hoplogs, count = " + hoplogs.size(), logPrefix);
    }
    for (Hoplog hoplog : hoplogs) {
      addSortedOplog(hoplog, false, true);
    }

    // initialize sequence to the current maximum
    sequence = new AtomicInteger(findMaxSequenceNumber(hoplogs));
    
    initOldTmpFiles();
    
    FileSystem fs = store.getFileSystem();
    Path cleanUpIntervalPath = new Path(store.getHomeDir(), HoplogConfig.CLEAN_UP_INTERVAL_FILE_NAME); 
    if (!fs.exists(cleanUpIntervalPath)) {
      long intervalDurationMillis = store.getPurgeInterval() * 60 * 1000;
      HoplogUtil.exposeCleanupIntervalMillis(fs, cleanUpIntervalPath, intervalDurationMillis);
    }

    if (startCompactionOnStartup) {
      forceCompactionOnVersionUpgrade();
      if (logger.isInfoEnabled()) {
        logger.info(LocalizedStrings.HOPLOG_MAJOR_COMPACTION_SCHEDULED_FOR_BETTER_ESTIMATE);
      }
    }
  }

  /**
   * Iterates on the input buffer and persists it in a new sorted oplog. This operation is
   * synchronous and blocks the thread.
   */
  @Override
  public void flush(Iterator<? extends QueuedPersistentEvent> iterator, final int count)
      throws IOException, ForceReattemptException {
    assert iterator != null;

    if (logger.isDebugEnabled())
      logger.debug("{}Initializing flush operation", logPrefix);
    
    final Hoplog so = getTmpSortedOplog(null, FLUSH_HOPLOG_EXTENSION);
    HoplogWriter writer = null;
    ICardinality localHLL = new HyperLogLog(HLL_CONSTANT);
    
    // variables for updating stats
    long start = stats.getFlush().begin();
    int byteCount = 0;
    
    try {
      /**MergeGemXDHDFSToGFE changed the following statement as the code of HeapDataOutputStream is not merged */
      //HeapDataOutputStream out = new HeapDataOutputStream();
      
      try {
        writer = this.store.getSingletonWriter().runSerially(new Callable<Hoplog.HoplogWriter>() {
          @Override
          public HoplogWriter call() throws Exception {
            return so.createWriter(count);
          }
        });
      } catch (Exception e) {
        if (e instanceof IOException) {
          throw (IOException)e;
        }
        throw new IOException(e);
      }

      while (iterator.hasNext() && !this.organizerClosed) {
        HeapDataOutputStream out = new HeapDataOutputStream(1024, null);
        
        QueuedPersistentEvent item = iterator.next();
        item.toHoplogEventBytes(out);
        byte[] valueBytes = out.toByteArray();
        writer.append(item.getRawKey(), valueBytes);
        
        // add key length and value length to stats byte counter
        byteCount += (item.getRawKey().length + valueBytes.length);

        // increment size only if entry is not deleted
        if (!isDeletedEntry(valueBytes, 0)) {
          int hash = MurmurHash.hash(item.getRawKey());
          localHLL.offerHashed(hash);
        }
        /**MergeGemXDHDFSToGFE how to clear for reuse. Leaving it for Darrel to merge this change*/
        //out.clearForReuse();
      }
      if (organizerClosed)
        throw new BucketMovedException("The current bucket is moved BucketID: "+  
            this.bucketId + " Region name: " +  this.regionManager.getRegion().getName());
      
      // append completed. provide cardinality and close writer
      writer.close(buildMetaData(localHLL));
      writer = null;
    } catch (IOException e) {
      stats.getFlush().error(start);
      try {
        e = handleWriteHdfsIOError(writer, so, e);
      } finally {
        //Set the writer to null because handleWriteHDFSIOError has
        //already closed the writer.
        writer = null;
      }
      throw e;
    } catch (BucketMovedException e) {
      stats.getFlush().error(start);
      deleteTmpFile(writer, so);
      writer = null;
      throw e;
    } finally {
      if (writer != null) {
        writer.close();
      }
    }

    try{
      
      // ping secondaries before making the file a legitimate file to ensure 
      // that in case of split brain, no other vm has taken up as primary. #50110.  
      pingSecondaries();
      
      // rename file and check if renaming was successful
      synchronized (changePrimarylockObject) {
        if (!organizerClosed)
          makeLegitimate(so);
        else 
          throw new BucketMovedException("The current bucket is moved BucketID: "+  
              this.bucketId + " Region name: " +  this.regionManager.getRegion().getName());
      }
      try {
        so.getSize();
      } catch (IllegalStateException e) {
        throw new IOException("Failed to rename hoplog file:" + so.getFileName());
      }
      
      //Disabling this assertion due to bug 49740
      // check to make sure the sequence number is correct
//      if (ENABLE_INTEGRITY_CHECKS) {
//        Assert.assertTrue(getSequenceNumber(so) == findMaxSequenceNumber(identifyAndLoadSortedOplogs(false)), 
//            "Invalid sequence number detected for " + so.getFileName());
//      }
      
      // record the file for future maintenance and reads
      addSortedOplog(so, false, true);
      stats.getFlush().end(byteCount, start);
      incrementDiskUsage(so.getSize());
    } catch (BucketMovedException e) {
      stats.getFlush().error(start);
      deleteTmpFile(writer, so);
      writer = null;
      throw e;
    } catch (IOException e) {
      stats.getFlush().error(start);
      logger.warn(LocalizedStrings.HOPLOG_FLUSH_OPERATION_FAILED, e);
      throw e;
    }

    submitCompactionRequests();
  }


  /**
   * store cardinality information in metadata
   * @param localHLL the hll estimate for this hoplog only
   */
  private EnumMap<Meta, byte[]> buildMetaData(ICardinality localHLL) throws IOException {
    EnumMap<Meta, byte[]> map = new EnumMap<Hoplog.Meta, byte[]>(Meta.class);
    map.put(Meta.LOCAL_CARDINALITY_ESTIMATE_V2, localHLL.getBytes());
    return map;
  }

  private void submitCompactionRequests() throws IOException {
    CompactionRequest req;
    
    // determine if a major compaction is needed and create a compaction request
    // with compaction manager
    if (store.getMajorCompaction()) {
      if (isMajorCompactionNeeded()) {
        req = new CompactionRequest(regionFolder, bucketId, getCompactor(), true);
        HDFSCompactionManager.getInstance(store).submitRequest(req);
      }
    }
    
    // submit a minor compaction task. It will be ignored if there is no work to
    // be done.
    if (store.getMinorCompaction()) {
      req = new CompactionRequest(regionFolder, bucketId, getCompactor(), false);
      HDFSCompactionManager.getInstance(store).submitRequest(req);
    }
  }

  /**
   * @return true if the oldest hoplog was created 1 major compaction interval ago
   */
  private boolean isMajorCompactionNeeded() throws IOException {
    // major compaction interval in milliseconds
    
    long majorCInterval = ((long)store.getMajorCompactionInterval()) * 60 * 1000;

    Hoplog oplog = hoplogReadersController.getOldestHoplog();
    if (oplog == null) {
      return false;
    }
    
    long oldestFileTime = oplog.getModificationTimeStamp();
    long now = System.currentTimeMillis();
    if (logger.isDebugEnabled()) {
      logger.debug("{}Checking oldest hop " + oplog.getFileName()
          + " for majorCompactionInterval=" + majorCInterval
          + " + now=" + now, logPrefix);
    }
    if (oldestFileTime > 0l && oldestFileTime < (now - majorCInterval)) {
      return true;
    }
    return false;
  }

  @Override
  public SortedHoplogPersistedEvent read(byte[] key) throws IOException {
    long startTime = stats.getRead().begin();
    String user = logger.isDebugEnabled() ? "Read" : null;
    
    // collect snapshot of hoplogs
    List<TrackedReference<Hoplog>> hoplogs = null;
    hoplogs = hoplogReadersController.getTrackedSortedOplogList(user);
    try {
      // search for the key in order starting with the youngest oplog
      for (TrackedReference<Hoplog> hoplog : hoplogs) {
        HoplogReader reader = hoplog.get().getReader();
        byte[] val = reader.read(key);
        if (val != null) {
          // value found in a younger hoplog. stop iteration
          SortedHoplogPersistedEvent eventObj = deserializeValue(val);
          stats.getRead().end(val.length, startTime);
          return eventObj;
        }
      }
    } catch (IllegalArgumentException e) {
      if (IOException.class.isAssignableFrom(e.getCause().getClass())) {
        throw handleIOError((IOException) e.getCause());
      } else {
        throw e;
      }
    } catch (IOException e) {
      throw handleIOError(e);
    } catch (HDFSIOException e) {
        throw handleIOError(e);
    } finally {
      hoplogReadersController.releaseHoplogs(hoplogs, user);
    }
    
    stats.getRead().end(0, startTime);
    return null;
  }

  protected IOException handleIOError(IOException e) {
    // expose the error wrapped inside remote exception
    if (e instanceof RemoteException) {
      return ((RemoteException) e).unwrapRemoteException();
    } 
    
    checkForSafeError(e);
    
    // it is not a safe error. let the caller handle it
    return e;
  }
  
  protected HDFSIOException handleIOError(HDFSIOException e) {
    checkForSafeError(e);
    return e;
  }

  protected void checkForSafeError(Exception e) {
    boolean safeError = ShutdownHookManager.get().isShutdownInProgress();
    if (safeError) {
      // IOException because of closed file system. This happens when member is
      // shutting down
      if (logger.isDebugEnabled())
        logger.debug("IO error caused by filesystem shutdown", e);
      throw new CacheClosedException("IO error caused by filesystem shutdown", e);
    } 

    if(isClosed()) {
      //If the hoplog organizer is closed, throw an exception to indicate the 
      //caller should retry on the new primary.
      throw new PrimaryBucketException(e);
    }
  }
  
  protected IOException handleWriteHdfsIOError(HoplogWriter writer, Hoplog so, IOException e)
      throws IOException {
    if (logger.isDebugEnabled()) {
      logger.debug("{}Handle write error:" + so, logPrefix);
    }
    
    closeWriter(writer);
    // add to the janitor queue
    tmpFiles.put(so, Boolean.TRUE);

    return handleIOError(e);
  }

  private void deleteTmpFile(HoplogWriter writer, Hoplog so) {
    closeWriter(writer);
    
    // delete the temporary hoplog
    try {
      if (so != null) {
        so.delete();
      }
    } catch (IOException e1) {
      logger.info(e1);
    }
  }

  private void closeWriter(HoplogWriter writer) {
    if (writer != null) {
      // close writer before deleting it
      try {
        writer.close();
      } catch (Throwable e1) {
        // error to close hoplog will happen if no connections to datanode are
        // available. Try to delete the file on namenode
        if(!isClosed()) {
          logger.info(e1);
        }
      }
    }
  }

  /**
   * Closes hoplog and suppresses IO during reader close. Suppressing IO errors
   * when the organizer is closing or an hoplog becomes inactive lets the system
   * continue freeing other resources. It could potentially lead to socket
   * leaks though.
   */
  private void closeReaderAndSuppressError(Hoplog hoplog, boolean clearCache) {
    try {
      hoplog.close();
    } catch (IOException e) {
      // expose the error wrapped inside remote exception
      if (e instanceof RemoteException) {
        e = ((RemoteException) e).unwrapRemoteException();
      } 
      logger.info(e);
    }
  }

  @Override
  public BucketIterator scan() throws IOException {
    String user = logger.isDebugEnabled() ? "Scan" : null;
    List<TrackedReference<Hoplog>> hoplogs = null;
    BucketIterator iter = null;
    try {
      hoplogs = hoplogReadersController.getTrackedSortedOplogList(user);
      iter = new BucketIterator(hoplogs);
      return iter;
    }  finally {
      // Normally the hoplogs will be released when the iterator is closed. The
      // hoplogs must be released only if creating the iterator has failed.
      if (iter == null) {
        hoplogReadersController.releaseHoplogs(hoplogs, user);
      }
    }
  }

  @Override
  public BucketIterator scan(byte[] from, byte[] to) throws IOException {
    throw new NotImplementedException();
  }

  @Override
  public BucketIterator scan(byte[] from, boolean fromInclusive, byte[] to, boolean toInclusive) throws IOException {
    throw new NotImplementedException();
  }

  @Override
  public HoplogIterator<byte[], SortedHoplogPersistedEvent> scan(
      long startOffset, long length) throws IOException {
    throw new UnsupportedOperationException("Not supported for " + this.getClass().getSimpleName());
  }

  @Override
  public void close() throws IOException {
    super.close();
    
    synchronized (changePrimarylockObject) {
      organizerClosed = true;
    }
    //Suspend compaction
    getCompactor().suspend();
    
    //Close the readers controller.
    hoplogReadersController.close();
    
    previousCleanupTimestamp.set(Long.MIN_VALUE);
    
  }

  /**
   * This method call will happen on secondary node. The secondary node needs to update its data
   * structures
   */
  @Override
  public void hoplogCreated(String region, int bucketId, Hoplog... oplogs)
      throws IOException {
    for (Hoplog oplog : oplogs) {
      addSortedOplog(oplog, false, true);
    }
  }

  @Override
  public long sizeEstimate() {
    return this.bucketSize.cardinality();
  }

  private void addSortedOplog(Hoplog so, boolean notify, boolean addsToBucketSize)
  throws IOException {
    if (!hoplogReadersController.addSortedOplog(so)) {
      so.close();
      throw new InternalGemFireException("Failed to add " + so);
    }

    String user = logger.isDebugEnabled() ? "Add" : null;
    if (addsToBucketSize) {
      TrackedReference<Hoplog> ref = null;
      try {
        ref = hoplogReadersController.trackHoplog(so, user);
        synchronized (bucketSize) {
          ICardinality localHLL = ref.get().getEntryCountEstimate();
          if (localHLL != null) {
            bucketSize = mergeHLL(bucketSize, localHLL);
          }
        }
      } finally {
        if (ref != null) {
          hoplogReadersController.releaseHoplog(ref, user);
        }
      }
    }

    if (notify && listener != null) {
      listener.hoplogCreated(regionFolder, bucketId, so);
    }
  }

  private void reEstimateBucketSize() throws IOException {
    ICardinality global = null;
    String user = logger.isDebugEnabled() ? "HLL" : null;
    List<TrackedReference<Hoplog>> hoplogs = null;
    try {
      hoplogs = hoplogReadersController.getTrackedSortedOplogList(user);
      global = new HyperLogLog(HLL_CONSTANT);
      for (TrackedReference<Hoplog> hop : hoplogs) {
        global = mergeHLL(global, hop.get().getEntryCountEstimate());
      }
    } finally {
      hoplogReadersController.releaseHoplogs(hoplogs, user);
    }
    bucketSize = global;
  }

  protected ICardinality mergeHLL(ICardinality global, ICardinality local)
  /*throws IOException*/ {
    try {
      return global.merge(local);
    } catch (CardinalityMergeException e) {
      // uncomment this after the 1.4 release
      //throw new InternalGemFireException(e.getLocalizedMessage(), e);
      startCompactionOnStartup = true;
      return global;
    }
  }

  private void removeSortedOplog(TrackedReference<Hoplog> so, boolean notify) throws IOException {
    hoplogReadersController.removeSortedOplog(so);
    
    // release lock before notifying listeners
    if (notify && listener != null) {
      listener.hoplogDeleted(regionFolder, bucketId, so.get());
    }
  }
  
  private void notifyCompactionListeners(boolean isMajor) {
    listener.compactionCompleted(regionFolder, bucketId, isMajor);
  }
  
  /**
   * This method call will happen on secondary node. The secondary node needs to update its data
   * structures
   * @throws IOException 
   */
  @Override
  public void hoplogDeleted(String region, int bucketId, Hoplog... oplogs) throws IOException {
    throw new NotImplementedException();
  }

  @Override
  public synchronized Compactor getCompactor() {
    if (compacter == null) {
      compacter = new HoplogCompactor();
    }
    return compacter;
  }

  @Override
  protected Hoplog getHoplog(Path hoplogPath) throws IOException {
    Hoplog so = new HFileSortedOplog(store, hoplogPath, store.getBlockCache(), stats, store.getStats());
    return so;
  }

  /**
   * locks sorted oplogs collection, removes oplog and renames for deletion later
   * @throws IOException 
   */
  void markSortedOplogForDeletion(List<TrackedReference<Hoplog>> targets, boolean notify) throws IOException {
    for (int i = targets.size(); i > 0; i--) {
      TrackedReference<Hoplog> so = targets.get(i - 1);
      removeSortedOplog(so, true);
      if (!store.getFileSystem().exists(new Path(bucketPath, so.get().getFileName()))) {
        // the hoplog does not even exist on file system. Skip remaining steps
        continue;
      }
      addExpiryMarkerForAFile(so.get());
    }
  }
  
  /**
   * Deletes expired hoplogs and expiry markers from the file system. Calculates
   * a target timestamp based on cleanup interval. Then gets list of target
   * hoplogs. It also updates the disk usage state
   * 
   * @return number of files deleted
   */
   synchronized int initiateCleanup() throws IOException {
    int conf = store.getPurgeInterval();
    // minutes to milliseconds
    long intervalDurationMillis = conf * 60 * 1000;
    // Any expired hoplog with timestamp less than targetTS is a delete
    // candidate.
    long targetTS = System.currentTimeMillis() - intervalDurationMillis;
    if (logger.isDebugEnabled()) {
      logger.debug("Target timestamp for expired hoplog deletion " + targetTS, logPrefix);
    }
    // avoid too frequent cleanup invocations. Exit cleanup invocation if the
    // previous cleanup was executed within 10% range of cleanup interval
    if (previousCleanupTimestamp.get() > targetTS
        && (previousCleanupTimestamp.get() - targetTS) < (intervalDurationMillis / 10)) {
      if (logger.isDebugEnabled()) {
        logger.debug("Skip cleanup, previous " + previousCleanupTimestamp.get(), logPrefix);
      }
      return 0;
    }

    List<FileStatus> targets = getOptimizationTargets(targetTS);
    return deleteExpiredFiles(targets);
  }

  protected int deleteExpiredFiles(List<FileStatus> targets) throws IOException {
    if (targets == null) {
      return 0;
    }

    for (FileStatus file : targets) {
      if (logger.isDebugEnabled()) {
        logger.debug("{}Deleting file: " + file.getPath(), logPrefix);
      }
      store.getFileSystem().delete(file.getPath(), false);
      
      if (isClosed()) {
        if (logger.isDebugEnabled())
          logger.debug("{}Expiry file cleanup interupted by bucket close", logPrefix);
        return 0;
      }
      incrementDiskUsage(-1 * file.getLen());
    }

    previousCleanupTimestamp.set(System.currentTimeMillis());
    return targets.size();
  }

  /**
   * @param ts
   *          target timestamp
   * @return list of hoplogs, whose expiry markers were created before target
   *         timestamp, and the expiry marker itself.
   * @throws IOException
   */
  protected List<FileStatus> getOptimizationTargets(long ts) throws IOException {
    if (logger.isDebugEnabled()) {
      logger.debug("{}Identifying optimization targets " + ts, logPrefix);
    }

    List<FileStatus> deleteTargets = new ArrayList<FileStatus>();
    FileStatus[] markers = getExpiryMarkers();
    if (markers != null) {
      for (FileStatus marker : markers) {
        String name = truncateExpiryExtension(marker.getPath().getName());
        long timestamp = marker.getModificationTime();

        // expired minor compacted files are not being used anywhere. These can
        // be removed immediately. All the other expired files should be removed
        // when the files have aged
        boolean isTarget = false;
        
        if (name.endsWith(MINOR_HOPLOG_EXTENSION)) {
          isTarget = true;
        } else if (timestamp < ts && name.endsWith(FLUSH_HOPLOG_EXTENSION)) {
          isTarget = true;
        } else if (timestamp < ts && name.endsWith(MAJOR_HOPLOG_EXTENSION)) {
          long majorCInterval = ((long)store.getMajorCompactionInterval()) * 60 * 1000;
          if (timestamp < (System.currentTimeMillis() - majorCInterval)) {
            isTarget = true;
          }
        }
        if (!isTarget) {
          continue;
        }
        
        // if the file is still being read, do not delete or rename it
        TrackedReference<Hoplog> used = hoplogReadersController.getInactiveHoplog(name);
        if (used != null) {
          if (used.inUse() && logger.isDebugEnabled()) {
            logger.debug("{}Optimizer: found active expired hoplog:" + name, logPrefix);
          } else if (logger.isDebugEnabled()) {
            logger.debug("{}Optimizer: found open expired hoplog:" + name, logPrefix);
          }
          continue;
        }
        
        if (logger.isDebugEnabled()) {
          logger.debug("{}Delete target identified " + marker.getPath(), logPrefix);
        }
        
        deleteTargets.add(marker);
        Path hoplogPath = new Path(bucketPath, name);
        if (store.getFileSystem().exists(hoplogPath)) {
          FileStatus hoplog = store.getFileSystem().getFileStatus(hoplogPath);
          deleteTargets.add(hoplog);
        }
      }
    }
    return deleteTargets;
  }

  /**
   * Returns a list of of hoplogs present in the bucket's directory, expected to be called during
   * hoplog set initialization
   */
  List<Hoplog> identifyAndLoadSortedOplogs(boolean countSize) throws IOException {
    FileSystem fs = store.getFileSystem();
    if (! fs.exists(bucketPath)) {
      return new ArrayList<Hoplog>();
    }
    
    FileStatus allFiles[] = fs.listStatus(bucketPath);
    ArrayList<FileStatus> validFiles = new ArrayList<FileStatus>();
    for (FileStatus file : allFiles) {
      // All hoplog files contribute to disk usage
      Matcher matcher = HOPLOG_NAME_PATTERN.matcher(file.getPath().getName());
      if (! matcher.matches()) {
        // not a hoplog
        continue;
      }
      
      // account for the disk used by this file
      if (countSize) {
        incrementDiskUsage(file.getLen());
      }
      
      // All valid hoplog files must match the regex
      matcher = SORTED_HOPLOG_PATTERN.matcher(file.getPath().getName());
      if (matcher.matches()) {
        validFiles.add(file);
      }
    }
    
    FileStatus[] markers = getExpiryMarkers();
    FileStatus[] validHoplogs = filterValidHoplogs(
        validFiles.toArray(new FileStatus[validFiles.size()]), markers);

    ArrayList<Hoplog> results = new ArrayList<Hoplog>();
    if (validHoplogs == null || validHoplogs.length == 0) {
      return results;
    }

    for (int i = 0; i < validHoplogs.length; i++) {
      // Skip directories
      if (validHoplogs[i].isDirectory()) {
        continue;
      }

      final Path p = validHoplogs[i].getPath();
      // skip empty file
      if (fs.getFileStatus(p).getLen() <= 0) {
        continue;
      }

      Hoplog hoplog = new HFileSortedOplog(store, p, store.getBlockCache(), stats, store.getStats());
      results.add(hoplog);
    }

    return results;
  }

  private static int findMaxSequenceNumber(List<Hoplog> hoplogs) throws IOException {
    int maxSeq = 0;
    for (Hoplog hoplog : hoplogs) {
      maxSeq = Math.max(maxSeq, getSequenceNumber(hoplog));
    }
    return maxSeq;
  }

  /**
   * @return the sequence number associate with a hoplog file
   */
  static int getSequenceNumber(Hoplog hoplog) {
    Matcher matcher = SORTED_HOPLOG_PATTERN.matcher(hoplog.getFileName());
    boolean matched = matcher.find();
    assert matched;
    return Integer.valueOf(matcher.group(3));
  }

  protected FileStatus[] getExpiryMarkers() throws IOException {
    FileSystem fs = store.getFileSystem();
    if (hoplogReadersController.hoplogs == null
        || hoplogReadersController.hoplogs.size() == 0) {
      // there are no hoplogs in the system. May be the bucket is not existing
      // at all.
      if (!fs.exists(bucketPath)) {
        if (logger.isDebugEnabled())
          logger.debug("{}This bucket is unused, skipping expired hoplog check", logPrefix);
        return null;
      }
    }
    
    FileStatus files[] = FSUtils.listStatus(fs, bucketPath, new PathFilter() {
      @Override
      public boolean accept(Path file) {
        // All expired hoplog end with expire extension and must match the valid file regex
        String fileName = file.getName();
        if (! fileName.endsWith(EXPIRED_HOPLOG_EXTENSION)) {
          return false;
        }
        fileName = truncateExpiryExtension(fileName);
        Matcher matcher = SORTED_HOPLOG_PATTERN.matcher(fileName);
        return matcher.find();
      }

    });
    return files;
  }
  
  @Override
  public void clear() throws IOException {
    //Suspend compaction while we are doing the clear. This
    //aborts the currently in progress compaction.
    getCompactor().suspend();
    
    // while compaction is suspended, clear method marks hoplogs for deletion
    // only. Files will be removed by cleanup thread after active gets and
    // iterations are completed
    String user = logger.isDebugEnabled() ? "clear" : null;
    List<TrackedReference<Hoplog>> oplogs = null;
    try {
      oplogs = hoplogReadersController.getTrackedSortedOplogList(user);
      markSortedOplogForDeletion(oplogs, true);
    } finally {
      if (oplogs != null) {
        hoplogReadersController.releaseHoplogs(oplogs, user);
      }
      //Resume compaction
      getCompactor().resume();
    }
  }

  /**
   * Performs the following activities
   * <UL>
   * <LI>Submits compaction requests as needed
   * <LI>Deletes tmp files which the system failed to removed earlier
   */
  @Override
  public void performMaintenance() throws IOException {
    long startTime = System.currentTimeMillis();
    
    if (logger.isDebugEnabled())
      logger.debug("{}Executing bucket maintenance", logPrefix);

    submitCompactionRequests();
    hoplogReadersController.closeInactiveHoplogs();
    initiateCleanup();
    
    cleanupTmpFiles();
    
    if (logger.isDebugEnabled()) {
      logger.debug("{}Time spent in bucket maintenance (in ms): "
          + (System.currentTimeMillis() - startTime), logPrefix);
    }
  }

  @Override
  public Future<CompactionStatus> forceCompaction(boolean isMajor) {
    CompactionRequest request = new CompactionRequest(regionFolder, bucketId,
        getCompactor(), isMajor, true/*force*/);
    return HDFSCompactionManager.getInstance(store).submitRequest(request);
  }

  private Future<CompactionStatus> forceCompactionOnVersionUpgrade() {
    CompactionRequest request = new CompactionRequest(regionFolder, bucketId, getCompactor(), true, true, true);
    return HDFSCompactionManager.getInstance(store).submitRequest(request);
  }

  @Override
  public long getLastMajorCompactionTimestamp() {
    long ts = 0;
    String user = logger.isDebugEnabled() ? "StoredProc" : null;
    List<TrackedReference<Hoplog>> hoplogs = hoplogReadersController.getTrackedSortedOplogList(user);
    try {
      for (TrackedReference<Hoplog> hoplog : hoplogs) {
        String fileName = hoplog.get().getFileName();
        Matcher file = HOPLOG_NAME_PATTERN.matcher(fileName);
        if (file.matches() && fileName.endsWith(MAJOR_HOPLOG_EXTENSION)) {
          ts = getHoplogTimestamp(file);
          break;
        }
      }
    } finally {
      hoplogReadersController.releaseHoplogs(hoplogs, user);
    }
    if (logger.isDebugEnabled()) {
      logger.debug("{}HDFS: for bucket:"+getRegionBucketStr()+" returning last major compaction timestamp "+ts, logPrefix);
    }
    return ts;
  }

  private void initOldTmpFiles() throws IOException {
    FileSystem fs = store.getFileSystem();
    if (! fs.exists(bucketPath)) {
      return;
    }
    
    oldTmpFiles = new LinkedList<FileStatus>(Arrays.asList(fs.listStatus(bucketPath, new TmpFilePathFilter())));
  }
  
  private void cleanupTmpFiles() throws IOException {
    if(oldTmpFiles == null && tmpFiles == null) {
      return;
    }
    
    if (oldTmpFiles != null) {
      FileSystem fs = store.getFileSystem();
      long now = System.currentTimeMillis();
      for (Iterator<FileStatus> itr = oldTmpFiles.iterator(); itr.hasNext();) {
        FileStatus file = itr.next();
        if(file.getModificationTime() + TMP_FILE_EXPIRATION_TIME_MS > now) {
          if (logger.isDebugEnabled()) {
            logger.debug("{}Deleting temporary file:" + file.getPath(), logPrefix);
          }
          fs.delete(file.getPath(), false);
          itr.remove();
        }
      }
    }
    if (tmpFiles != null) {
      for (Hoplog so : tmpFiles.keySet()) {
        if (logger.isDebugEnabled()) {
          logger.debug("{}Deleting temporary file:" + so.getFileName(), logPrefix);
        }
        deleteTmpFile(null, so);
      }
    }
  }
  
  /**
   * Executes tiered compaction of hoplog files. One instance of compacter per bucket will exist
   */
  protected class HoplogCompactor implements Compactor {
    private volatile boolean suspend = false;
    
    // the following boolean will be used to synchronize minor compaction
    private AtomicBoolean isMinorCompactionActive = new AtomicBoolean(false);
    // the following boolean will be used to synchronize major compaction
    private AtomicBoolean isMajorCompactionActive = new AtomicBoolean(false);
    // the following integer tracks the max sequence number amongst the
    // target files being major compacted. This value will be used to prevent
    // concurrent MajorC and minorC. MinorC is preempted in case of an
    // overlap. This object is also used as a lock. The lock is acquired before
    // identifying compaction targets and before marking targets for expiry
    final AtomicInteger maxMajorCSeqNum = new AtomicInteger(-1);

    @Override
    public void suspend() {
      long wait = Long.getLong(HoplogConfig.SUSPEND_MAX_WAIT_MS, HoplogConfig.SUSPEND_MAX_WAIT_MS_DEFAULT);
      this.suspend=true;
      //this forces the compact method to finish.
      while (isMajorCompactionActive.get() || isMinorCompactionActive.get()) {
        if (wait < 0) {
          wait = Long.getLong(HoplogConfig.SUSPEND_MAX_WAIT_MS, HoplogConfig.SUSPEND_MAX_WAIT_MS_DEFAULT);
          String act = isMajorCompactionActive.get() ? "MajorC" : "MinorC";
          logger.warn(LocalizedMessage.create(LocalizedStrings.HOPLOG_SUSPEND_OF_0_FAILED_IN_1, new Object[] {act, wait}));
          break;
        }
        try {
          TimeUnit.MILLISECONDS.sleep(50);
          wait -= 50;
        } catch (InterruptedException e) {
          break;
        }
      }
    }
    
    @Override
    public void resume() {
      this.suspend = false;
    }
    
    @Override
    public boolean isBusy(boolean isMajor) {
      if (isMajor) {
        return isMajorCompactionActive.get();
      } else {
        return isMinorCompactionActive.get();
      }
    }
    
    /**
     * compacts hoplogs. The method takes a minor or major compaction "lock" to
     * prevent concurrent execution of compaction cycles. A possible improvement
     * is to allow parallel execution of minor compaction if the sets of
     * hoplogs being compacted are disjoint.
     */
    @Override
    public boolean compact(boolean isMajor, boolean isForced) throws IOException {
      if(suspend) {
        return false;
      }

      String extension = null;
      IOOperation compactionStats = null;
      long startTime = 0; 
      final AtomicBoolean lock;
      Hoplog compactedHoplog = null;
      List<TrackedReference<Hoplog>> targets = null;
      String user = logger.isDebugEnabled() ? (isMajor ? "MajorC" : "MinorC") : null;
      
      if (isMajor) {
        lock = isMajorCompactionActive;
        extension = MAJOR_HOPLOG_EXTENSION;
        compactionStats = stats.getMajorCompaction();
      } else {
        lock = isMinorCompactionActive;
        extension = MINOR_HOPLOG_EXTENSION;
        compactionStats = stats.getMinorCompaction();
      }

      // final check before beginning compaction. Return if compaction is active
      if (! lock.compareAndSet(false, true)) {
        if (isMajor) {
          if (logger.isDebugEnabled())
            logger.debug("{}Major compaction already active. Ignoring new request", logPrefix);
        } else {
          if (logger.isDebugEnabled())
            logger.debug("Minor compaction already active. Ignoring new request", logPrefix);
        }
        return false;
      }
      
      try {
        if(suspend) {
          return false;
        }
        
        // variables for updating stats
        startTime = compactionStats.begin();
        
        int seqNum = -1;
        int lastKnownMajorCSeqNum;
        synchronized (maxMajorCSeqNum) {
          lastKnownMajorCSeqNum = maxMajorCSeqNum.get();
          targets = hoplogReadersController.getTrackedSortedOplogList(user);
          getCompactionTargets(isMajor, targets, lastKnownMajorCSeqNum);
          if (targets != null && targets.size() > 0) {
            targets = Collections.unmodifiableList(targets);
            seqNum = getSequenceNumber(targets.get(0).get());
            if (isMajor) {
              maxMajorCSeqNum.set(seqNum);
            }
          }
        }
        
        if (targets == null || targets.isEmpty() || (!isMajor && targets.size() == 1 && !isForced)) {
          if (logger.isDebugEnabled()){
            logger.debug("{}Skipping compaction, too few hoplops to compact. Major?" + isMajor, logPrefix);
          }
            
          compactionStats.end(0, startTime);
          return true;
        }
        
        //In case that we only have one major compacted file, we don't need to run major compaction to
        //generate a copy of the same content
        if (targets.size() == 1 && !isForced) {
        String hoplogName = targets.get(0).get().getFileName();
          if (hoplogName.endsWith(MAJOR_HOPLOG_EXTENSION)){
            if (logger.isDebugEnabled()){
              logger.debug("{}Skipping compaction, no need to compact a major compacted file. Major?" + isMajor, logPrefix);
            }
            compactionStats.end(0, startTime);
            return true;
          }
        }
        
        if (logger.isDebugEnabled()) {
          for (TrackedReference<Hoplog> target : targets) {
            if (logger.isDebugEnabled()) {
              fineLog("Target:", target, " size:", target.get().getSize());
            }
          }
        }
        
        // Create a temporary hoplog for compacted hoplog. The compacted hoplog
        // will have the seq number same as that of youngest target file. Any
        // hoplog younger than target hoplogs will have a higher sequence number
        compactedHoplog = getTmpSortedOplog(seqNum, extension);
        
        long byteCount;
        try {
          byteCount = fillCompactionHoplog(isMajor, targets, compactedHoplog, lastKnownMajorCSeqNum);
          compactionStats.end(byteCount, startTime);
        } catch (InterruptedException e) {
          if (logger.isDebugEnabled())
            logger.debug("{}Compaction execution suspended", logPrefix);
          compactionStats.error(startTime);
          return false;
        } catch (ForceReattemptException e) {
          if (logger.isDebugEnabled())
            logger.debug("{}Compaction execution suspended", logPrefix);
          compactionStats.error(startTime);
          return false;
        }
        
        // creation of compacted hoplog completed, its time to use it for
        // reading. Before using it, make sure minorC and mojorC were not
        // executing on overlapping sets of files. All targets can be marked for
        // expiration. Notify listener if configured. Update bucket size
        synchronized (maxMajorCSeqNum) {
          if (!isMajor && isMinorMajorOverlap(targets, maxMajorCSeqNum.get())) {
            // MajorC is higher priority. In case of any overlap kill minorC
            if (logger.isDebugEnabled())
              logger.debug("{}Interrupting MinorC for a concurrent MajorC", logPrefix);
            compactionStats.error(startTime);
            return false;
          }
          addSortedOplog(compactedHoplog, true, false);
          markSortedOplogForDeletion(targets, true);
        }
      } catch (IOException e) {
        compactionStats.error(startTime);
        throw e;
      } finally {
        if (isMajor) {
          maxMajorCSeqNum.set(-1);
        }
        lock.set(false);
        hoplogReadersController.releaseHoplogs(targets, user);
      }
      
      incrementDiskUsage(compactedHoplog.getSize());
      reEstimateBucketSize();
      
      notifyCompactionListeners(isMajor);
      return true;
    }

    /**
     * Major compaction compacts all files. Seq number of the youngest file
     * being MajorCed is known. If MinorC is operating on any file with a seq
     * number less than this number, there is a overlap
     * @param num 
     */
    boolean isMinorMajorOverlap(List<TrackedReference<Hoplog>> targets, int num) {
      if (num < 0 || targets == null || targets.isEmpty()) {
        return false;
      }

      for (TrackedReference<Hoplog> hop : targets) {
        if (getSequenceNumber(hop.get()) <= num) {
          return true;
        }
      }
      
      return false;
    }

    /**
     * Iterates over targets and writes eligible targets to the output hoplog.
     * Handles creation of iterators and writer and closing it in case of
     * errors.
     */
    public long fillCompactionHoplog(boolean isMajor,
        List<TrackedReference<Hoplog>> targets, Hoplog output, int majorCSeqNum)
        throws IOException, InterruptedException, ForceReattemptException {

      HoplogWriter writer = null;
      ICardinality localHLL = new HyperLogLog(HLL_CONSTANT);
      HoplogSetIterator mergedIter = null;
      int byteCount = 0;
      
      try {
        // create a merged iterator over the targets and write entries into
        // output hoplog
        mergedIter = new HoplogSetIterator(targets);
        writer = output.createWriter(mergedIter.getRemainingEntryCount());

        boolean interrupted = false;
        for (; mergedIter.hasNext(); ) {
          if (suspend) {
            interrupted = true;
            break;
          } else if (!isMajor &&  maxMajorCSeqNum.get() > majorCSeqNum) {
            // A new major compaction cycle is starting, quit minorC to avoid
            // duplicate work and missing deletes
            if (logger.isDebugEnabled())
              logger.debug("{}Preempting MinorC, new MajorC cycle detected ", logPrefix);
            interrupted = true;
            break;
          }

          mergedIter.nextBB();
          
          ByteBuffer k = mergedIter.getKeyBB();
          ByteBuffer v = mergedIter.getValueBB();
          
          boolean isDeletedEntry = isDeletedEntry(v.array(), v.arrayOffset());
          if (isMajor && isDeletedEntry) {
            // its major compaction, time to ignore deleted entries
            continue;
          }

          if (!isDeletedEntry) {
            int hash = MurmurHash.hash(k.array(), k.arrayOffset(), k.remaining(), -1);
            localHLL.offerHashed(hash);
          }

          writer.append(k, v);
          byteCount += (k.remaining() + v.remaining());
        }

        mergedIter.close();
        mergedIter = null;

        writer.close(buildMetaData(localHLL));
        writer = null;

        if (interrupted) {
          // If we suspended compaction operations, delete the partially written
          // file and return.
          output.delete();
          throw new InterruptedException();
        }
        
        // ping secondaries before making the file a legitimate file to ensure 
        // that in case of split brain, no other vm has taken up as primary. #50110. 
        pingSecondaries();
        
        makeLegitimate(output);
        return byteCount;
      } catch (IOException e) {
        e = handleWriteHdfsIOError(writer, output, e);
        writer = null;
        throw e;
      } catch (ForceReattemptException e) {
        output.delete();
        throw e;
      }finally {
        if (mergedIter != null) {
          mergedIter.close();
        }

        if (writer != null) {
          writer.close();
        }
      }
    }

    /**
     * identifies targets. For major compaction all sorted oplogs will be
     * iterated on. For minor compaction, policy driven fewer targets will take
     * place.
     */
    protected void getCompactionTargets(boolean major,
        List<TrackedReference<Hoplog>> targets, int majorCSeqNum) {
      if (!major) {
        getMinorCompactionTargets(targets, majorCSeqNum);
      }
    }

    /**
     * list of oplogs most suitable for compaction. The alogrithm selects m
     * smallest oplogs which are not bigger than X in size. Null if valid
     * candidates are not found
     */
    void getMinorCompactionTargets(List<TrackedReference<Hoplog>> targets, int majorCSeqNum) 
    {
      List<TrackedReference<Hoplog>> omittedHoplogs = new ArrayList<TrackedReference<Hoplog>>();

      // reverse the order of hoplogs in list. the oldest file becomes the first file.
      Collections.reverse(targets);

      // hoplog greater than this size will not be minor-compacted
      final long MAX_COMPACTION_FILE_SIZE;
      // maximum number of files to be included in any compaction cycle
      final int MAX_FILE_COUNT_COMPACTION;
      // minimum number of files that must be present for compaction to be worth
      final int MIN_FILE_COUNT_COMPACTION;
      
      MAX_COMPACTION_FILE_SIZE = ((long)store.getInputFileSizeMax()) * 1024 *1024;
      MAX_FILE_COUNT_COMPACTION = store.getInputFileCountMax();
      MIN_FILE_COUNT_COMPACTION = store.getInputFileCountMin();

      try {
        // skip till first file smaller than the max compaction file size is
        // found. And if MajorC is active, move to a file which is also outside
        // scope of MajorC
        for (Iterator<TrackedReference<Hoplog>> iterator = targets.iterator(); iterator.hasNext();) {
          TrackedReference<Hoplog> oplog = iterator.next();
          if (majorCSeqNum >= getSequenceNumber(oplog.get())) {
            iterator.remove();
            omittedHoplogs.add(oplog);
            if (logger.isDebugEnabled()){
              fineLog("Overlap with MajorC, excluding hoplog " + oplog.get());
            }
            continue;
          }
          
          if (oplog.get().getSize() > MAX_COMPACTION_FILE_SIZE || oplog.get().getFileName().endsWith(MAJOR_HOPLOG_EXTENSION)) {
          // big file will not be included for minor compaction
          // major compacted file will not be converted to minor compacted file
            iterator.remove();
            omittedHoplogs.add(oplog);
            if (logger.isDebugEnabled()) {
              fineLog("Excluding big hoplog from minor cycle:",
                  oplog.get(), " size:", oplog.get().getSize(), " limit:",
                  MAX_COMPACTION_FILE_SIZE);
            }
          } else {
            // first small hoplog found, skip the loop
            break;
          }
        }

        // If there are too few files no need to perform compaction
        if (targets.size() < MIN_FILE_COUNT_COMPACTION) {
          if (logger.isDebugEnabled()){
            logger.debug("{}Too few hoplogs for minor cycle:" + targets.size(), logPrefix);
          }
          omittedHoplogs.addAll(targets);
          targets.clear();
          return;
        }
        
        float maxGain = Float.MIN_VALUE;
        int bestFrom = -1; 
        int bestTo = -1; 
        
        // for listSize=5 list, minFile=3; maxIndex=5-3. 
        // so from takes values 0,1,2
        int maxIndexForFrom = targets.size() - MIN_FILE_COUNT_COMPACTION;
        for (int from = 0; from <= maxIndexForFrom ; from++) {
          // for listSize=6 list, minFile=3, maxFile=5; minTo=0+3-1, maxTo=0+5-1
          // so to takes values 2,3,4
          int minIndexForTo = from + MIN_FILE_COUNT_COMPACTION - 1;
          int maxIndexForTo = Math.min(from + MAX_FILE_COUNT_COMPACTION, targets.size());
          
          for (int i = minIndexForTo; i < maxIndexForTo; i++) {
            Float gain = computeGain(from, i, targets);
            if (gain == null) {
              continue;
            }
            
            if (gain > maxGain) {
              maxGain = gain;
              bestFrom = from;
              bestTo = i;
            }
          }
        }
        
        if (bestFrom == -1) {
          if (logger.isDebugEnabled())
            logger.debug("{}Failed to find optimal target set for MinorC", logPrefix);
          omittedHoplogs.addAll(targets);
          targets.clear();
          return;
        }

        if (logger.isDebugEnabled()) {
          fineLog("MinorCTarget optimal result from:", bestFrom, " to:", bestTo);
        }

        // remove hoplogs they do not fall in the bestFrom-bestTo range
        int i = 0;
        for (Iterator<TrackedReference<Hoplog>> iter = targets.iterator(); iter.hasNext();) {
          TrackedReference<Hoplog> hop = iter.next();
          if (i < bestFrom || i > bestTo) {
            iter.remove();
            omittedHoplogs.add(hop);
          }
          i++;
        }
      } finally {
        // release readers of targets not included in the compaction cycle 
        String user = logger.isDebugEnabled() ? "MinorC" : null;
        hoplogReadersController.releaseHoplogs(omittedHoplogs, user);
      }
      
      // restore the order, youngest file is the first file again
      Collections.reverse(targets);
    }

    @Override
    public HDFSStore getHdfsStore() {
      return store;
    }
  }
  
  Float computeGain(int from, int to, List<TrackedReference<Hoplog>> targets) {
    double SIZE_64K = 64.0 * 1024;
    // TODO the base for log should depend on the average number of keys a index block will contain
    double LOG_BASE = Math.log(AVG_NUM_KEYS_PER_INDEX_BLOCK);
    
    long totalSize = 0;
    double costBefore = 0f;
    for (int i = from; i <= to; i++) {
      long size = targets.get(i).get().getSize();
      if (size == 0) {
        continue;
      }
      totalSize += size;
      
      // For each hoplog file, read cost is number of index block reads and 1
      // data block read. Index blocks on an average contain N keys and are
      // organized in a N-ary tree structure. Hence the number of index block
      // reads will be logBaseN(number of data blocks)
      costBefore += Math.ceil(Math.max(1.0, Math.log(size / SIZE_64K) / LOG_BASE)) + 1;
    }
    
    // if the first file is relatively too large this set is bad for compaction
    long firstFileSize = targets.get(from).get().getSize();
    if (firstFileSize > (totalSize - firstFileSize) * RATIO) {
      if (logger.isDebugEnabled()){
        fineLog("First file too big:", firstFileSize, " totalSize:", totalSize);
      }
      return null;
    }
        
    // compute size in mb so that the value of gain is in few decimals
    long totalSizeInMb = totalSize / 1024 / 1024;
    if (totalSizeInMb == 0) {
      // the files are tooooo small, just return the count. The more we compact
      // the better it is
      if (logger.isDebugEnabled()) {
        logger.debug("{}Total size too small:" +totalSize, logPrefix);
      }
      return (float) costBefore;
    }
    
    double costAfter = Math.ceil(Math.log(totalSize / SIZE_64K) / LOG_BASE) + 1;
    return (float) ((costBefore - costAfter) / totalSizeInMb);
  }
  
  /**
   * Hoplog readers are accessed asynchronously. There could be a window in
   * which, while a hoplog is being iterated on, it gets compacted and becomes
   * expired or inactive. The reader of the hoplog must not be closed till the
   * iterator completes. All such scenarios will be managed by this class. It
   * will keep all the reader, active and inactive, and reference counter to the
   * readers. An inactive reader will be closed if the reference count goes down
   * to 0.
   * 
   * One important point, only compaction process makes a hoplog inactive.
   * Compaction process in a bucket is single threaded. So compaction itself
   * will not face race condition. Read and scan operations on the bucket will
   * be affected. So reference counter is incremented for each read and scan.
   * 
   * @author ashvina
   */
  private class HoplogReadersController implements HoplogReaderActivityListener {
    private Integer maxOpenFilesLimit;

    // sorted collection of all the active oplog files associated with this bucket. Instead of a
    // queue, a set is used. New files created as part of compaction may be inserted after a few
    // hoplogs were created. The compacted file is such a case but should not be treated newest.
    private final ConcurrentSkipListSet<TrackedReference<Hoplog>> hoplogs;
    
    // list of all the hoplogs that have been compacted and need to be closed
    // once the reference count reduces to 0
    private final ConcurrentHashSet<TrackedReference<Hoplog>> inactiveHoplogs;
    
    // ReadWriteLock on list of oplogs to allow for consistent reads and scans
    // while hoplog set changes. A write lock is needed on completion of
    // compaction, addition of a new hoplog or on receiving updates message from
    // other GF nodes
    private final ReadWriteLock hoplogRWLock = new ReentrantReadWriteLock(true);

    // tracks the number of active readers for hoplogs of this bucket
    private AtomicInteger activeReaderCount = new AtomicInteger(0);
    
    public HoplogReadersController() {
      HoplogComparator comp = new HoplogComparator();
      hoplogs = new ConcurrentSkipListSet<TrackedReference<Hoplog>>(comp) {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean add(TrackedReference<Hoplog> e) {
          // increment number of hoplogs active for this bucket
          boolean result =  super.add(e);
          if (result)
            stats.incActiveFiles(1);
          return result;
        }
        
        @Override
        public boolean remove(Object o) {
          // decrement the number of hoplogs active for this bucket
          boolean result =  super.remove(o);
          if (result)
            stats.incActiveFiles(-1);
          return result;
        }
      };
      
      inactiveHoplogs = new ConcurrentHashSet<TrackedReference<Hoplog>>() {
        private static final long serialVersionUID = 1L;
        
        @Override
        public boolean add(TrackedReference<Hoplog> e) {
          boolean result =  super.add(e);
          if (result)
            stats.incInactiveFiles(1);
          return result;
        }
        
        @Override
        public boolean remove(Object o) {
          boolean result =  super.remove(o);
          if (result)
            stats.incInactiveFiles(-1);
          return result;
        }
      };
      
      maxOpenFilesLimit = Integer.getInteger(
          HoplogConfig.BUCKET_MAX_OPEN_HFILES_CONF,
          HoplogConfig.BUCKET_MAX_OPEN_HFILES_DEFAULT);
    }
    
    Hoplog getOldestHoplog() {
      if (hoplogs.isEmpty()) {
        return null;
      }
      return hoplogs.last().get();
    }

    /**
     * locks sorted oplogs collection and performs add operation
     * @return if addition was successful
     */
    private boolean addSortedOplog(Hoplog so) throws IOException {
      if (logger.isDebugEnabled()) {
        logger.debug("{}Try add " + so, logPrefix);
      }
      hoplogRWLock.writeLock().lock();
      try {
        int size = hoplogs.size();
        boolean result = hoplogs.add(new TrackedReference<Hoplog>(so));
        so.setReaderActivityListener(this);
        if (logger.isDebugEnabled()){
          fineLog("Added: ", so, " Before:", size, " After:", hoplogs.size());
        }
        return result;
      } finally {
        hoplogRWLock.writeLock().unlock();
      }
    }
    
    /**
     * locks sorted oplogs collection and performs remove operation and updates readers also
     */
    private void removeSortedOplog(TrackedReference<Hoplog> so) throws IOException {
      if (logger.isDebugEnabled()) {
        logger.debug("Try remove " + so, logPrefix);
      }
      hoplogRWLock.writeLock().lock();
      try {
        int size = hoplogs.size();
        boolean result = hoplogs.remove(so);
        if (result) {
          inactiveHoplogs.add(so);
          if (logger.isDebugEnabled()) {
            fineLog("Removed: ", so, " Before:", size, " After:", hoplogs.size());
          }
        } else {
          if (inactiveHoplogs.contains(so)) {
            if (logger.isDebugEnabled()) {
              logger.debug("{}Found a missing active hoplog in inactive list." + so, logPrefix);
            }
          } else {
            so.get().close();
            logger.warn(LocalizedMessage.create(LocalizedStrings.HOPLOG_MISSING_IN_BUCKET_FORCED_CLOSED, so.get()));
          }
        }
      } finally {
        hoplogRWLock.writeLock().unlock();
      }
    }
    
    private  void closeInactiveHoplogs() throws IOException {
      hoplogRWLock.writeLock().lock();
      try {
        for (TrackedReference<Hoplog> hoplog : inactiveHoplogs) {
          if (logger.isDebugEnabled()){
            logger.debug("{}Try close inactive " + hoplog, logPrefix);
          }

          if (!hoplog.inUse()) {
            int size = inactiveHoplogs.size();            
            inactiveHoplogs.remove(hoplog);
            closeReaderAndSuppressError(hoplog.get(), true);
            if (logger.isDebugEnabled()){
              fineLog("Closed inactive: ", hoplog.get(), " Before:", size,
                  " After:", inactiveHoplogs.size());
            }
          }
        }
      } finally {
        hoplogRWLock.writeLock().unlock();
      }
    }
    
    /**
     * @param target
     *          name of the hoplog file
     * @return trackedReference if target exists in inactive hoplog list.
     * @throws IOException
     */
    TrackedReference<Hoplog> getInactiveHoplog(String target) throws IOException {
      hoplogRWLock.writeLock().lock();
      try {
        for (TrackedReference<Hoplog> hoplog : inactiveHoplogs) {
          if (hoplog.get().getFileName().equals(target)) {
            if (logger.isDebugEnabled()) {
              logger.debug("{}Target found in inactive hoplogs list: " + hoplog, logPrefix);
            }
            return hoplog;
          }
        }
        if (logger.isDebugEnabled()){
          logger.debug("{}Target not found in inactive hoplogs list: " + target, logPrefix);
        }
        return null;
      } finally {
        hoplogRWLock.writeLock().unlock();
      }
    }
    
    /**
     * force closes all readers
     */
    public void close() throws IOException {
      hoplogRWLock.writeLock().lock();
      try {
        for (TrackedReference<Hoplog> hoplog : hoplogs) {
          closeReaderAndSuppressError(hoplog.get(), true);
        }
        
        for (TrackedReference<Hoplog> hoplog : inactiveHoplogs) {
          closeReaderAndSuppressError(hoplog.get(), true);
        }
      } finally {
        hoplogs.clear();
        inactiveHoplogs.clear();
        hoplogRWLock.writeLock().unlock();
      }
    }
    
    /**
     * locks hoplogs to create a snapshot of active hoplogs. reference of each
     * reader is incremented to keep it from getting closed
     * 
     * @return ordered list of sorted oplogs
     */
    private List<TrackedReference<Hoplog>> getTrackedSortedOplogList(String user) {
      List<TrackedReference<Hoplog>> oplogs = new ArrayList<TrackedReference<Hoplog>>();
      hoplogRWLock.readLock().lock();
      try {
        for (TrackedReference<Hoplog> oplog : hoplogs) {
          oplog.increment(user);
          oplogs.add(oplog);
          if (logger.isDebugEnabled()) {
            logger.debug("{}Track ref " + oplog, logPrefix);
          }
        }
      } finally {
        hoplogRWLock.readLock().unlock();
      }
      return oplogs;
    }

    private TrackedReference<Hoplog> trackHoplog(Hoplog hoplog, String user) {
      hoplogRWLock.readLock().lock();
      try {
        for (TrackedReference<Hoplog> oplog : hoplogs) {
          if (oplog.get().getFileName().equals(hoplog.getFileName())) {
            oplog.increment(user);
            if (logger.isDebugEnabled()) {
              logger.debug("{}Track " + oplog, logPrefix);
            }
            return oplog;
          }
        }
      } finally {
        hoplogRWLock.readLock().unlock();
      }
      throw new NoSuchElementException(hoplog.getFileName());
    }
    
    public void releaseHoplogs(List<TrackedReference<Hoplog>> targets, String user) {
      if (targets == null) {
        return;
      }
      
      for (int i = targets.size() - 1; i >= 0; i--) {
        TrackedReference<Hoplog> hoplog = targets.get(i);
        releaseHoplog(hoplog, user);
      }
    }

    public void releaseHoplog(TrackedReference<Hoplog> target, String user) {
      if (target ==  null) {
        return;
      }
      
      target.decrement(user);
      if (logger.isDebugEnabled()) {
        logger.debug("{}Try release " + target, logPrefix);
      }
      if (target.inUse()) {
        return;
      }
      
      // there are no users of this hoplog. if it is inactive close it.
      hoplogRWLock.writeLock().lock();
      try {
        if (!target.inUse()) {
          if (inactiveHoplogs.contains(target) ) {
            int sizeBefore = inactiveHoplogs.size();
            inactiveHoplogs.remove(target);
            closeReaderAndSuppressError(target.get(), true);
            if (logger.isDebugEnabled()) {
              fineLog("Closed inactive: ", target, " totalBefore:", sizeBefore,
                  " totalAfter:", inactiveHoplogs.size());
            }
          } else if (hoplogs.contains(target)) {
            closeExcessReaders();              
          }
        }
      } catch (IOException e) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.HOPLOG_IO_ERROR, 
            "Close reader: " + target.get().getFileName()), e);
      } finally {
        hoplogRWLock.writeLock().unlock();
      }
    }

    /*
     * detects if the total number of open hdfs readers is more than configured
     * max file limit. In case the limit is exceeded, some readers need to be
     * closed to avoid dadanode receiver overflow error.
     */
    private void closeExcessReaders() throws IOException {
      if (logger.isDebugEnabled()) {
        logger.debug("{}Close excess readers. Size:" + hoplogs.size()
            + " activeReaders:" + activeReaderCount.get() + " limit:"
            + maxOpenFilesLimit, logPrefix);
      }

      if (hoplogs.size() <= maxOpenFilesLimit) {
        return;
      }
      
      if (activeReaderCount.get() <= maxOpenFilesLimit) {
        return;
      }
      
      for (TrackedReference<Hoplog> hoplog : hoplogs.descendingSet()) {
        if (!hoplog.inUse() && !hoplog.get().isClosed()) {
          hoplog.get().close(false);
          if (logger.isDebugEnabled()) {
            logger.debug("{}Excess reader closed " + hoplog, logPrefix);
          }
        }
        
        if (activeReaderCount.get() <= maxOpenFilesLimit) {
          return;
        }
      }
    }

    @Override
    public void readerCreated() {
      activeReaderCount.incrementAndGet();
      stats.incActiveReaders(1);
      if (logger.isDebugEnabled())
        logger.debug("{}ActiveReader++", logPrefix);
    }

    @Override
    public void readerClosed() {
      activeReaderCount.decrementAndGet(); 
      stats.incActiveReaders(-1);
      if (logger.isDebugEnabled())
        logger.debug("{}ActiveReader--", logPrefix);
    }
  }

  /**
   * returns an ordered list of oplogs, FOR TESTING ONLY
   */
  public List<TrackedReference<Hoplog>> getSortedOplogs() throws IOException {
    List<TrackedReference<Hoplog>> oplogs = new ArrayList<TrackedReference<Hoplog>>();
    for (TrackedReference<Hoplog> oplog : hoplogReadersController.hoplogs) {
        oplogs.add(oplog);
    }
    return oplogs;
  }

  /**
   * Merged iterator on a list of hoplogs. 
   */
  public class BucketIterator implements HoplogIterator<byte[], SortedHoplogPersistedEvent> {
    // list of hoplogs to be iterated on.
    final List<TrackedReference<Hoplog>> hoplogList;
    HoplogSetIterator mergedIter;

    public BucketIterator(List<TrackedReference<Hoplog>> hoplogs) throws IOException {
      this.hoplogList = hoplogs;
      try {
        mergedIter = new HoplogSetIterator(this.hoplogList);
        if (logger.isDebugEnabled()) {
          for (TrackedReference<Hoplog> hoplog : hoplogs) {
            logger.debug("{}BucketIter target hop:" + hoplog.get().getFileName(), logPrefix);
          }
        }
      } catch (IllegalArgumentException e) {
        if (IOException.class.isAssignableFrom(e.getCause().getClass())) {
          throw handleIOError((IOException) e.getCause());
        } else {
          throw e;
        }
      } catch (IOException e) {
        throw handleIOError(e);
      } catch (HDFSIOException e) {
        throw handleIOError(e);
      } 
    }

    @Override
    public boolean hasNext() {
      return mergedIter.hasNext();
    }

    @Override
    public byte[] next() throws IOException {
      try {
        return HFileSortedOplog.byteBufferToArray(mergedIter.next());
      } catch (IllegalArgumentException e) {
        if (IOException.class.isAssignableFrom(e.getCause().getClass())) {
          throw handleIOError((IOException) e.getCause());
        } else {
          throw e;
        }
      } catch (IOException e) {
        throw handleIOError(e);
      }  
    }

    @Override
    public byte[] getKey() {
      // merged iterator returns a byte[]. This needs to be deserialized to the object which was
      // provided during flush operation
      return HFileSortedOplog.byteBufferToArray(mergedIter.getKey());
    }

    @Override
    public SortedHoplogPersistedEvent getValue() {
      // merged iterator returns a byte[]. This needs to be deserialized to the
      // object which was provided during flush operation
      try {
        return deserializeValue(HFileSortedOplog.byteBufferToArray(mergedIter.getValue()));
      } catch (IOException e) {
        throw new HDFSIOException("Failed to deserialize byte while iterating on partition", e);
      }
    }

    @Override
    public void remove() {
      mergedIter.remove();
    }

    @Override
    public void close() {
      // TODO release the closed iterators early
      String user = logger.isDebugEnabled() ? "Scan" : null;
      hoplogReadersController.releaseHoplogs(hoplogList, user);
    }
  }
  
  /**
   * This utility class is used to filter temporary hoplogs in a bucket
   * directory
   * 
   * @author ashvina
   */
  private static class TmpFilePathFilter implements PathFilter {
    @Override
    public boolean accept(Path path) {
      Matcher matcher = HOPLOG_NAME_PATTERN.matcher(path.getName());
      if (matcher.matches() && path.getName().endsWith(TEMP_HOPLOG_EXTENSION)) {
        return true;
      }
      return false;
    }
  }

  private void fineLog(Object... strings) {
    if (logger.isDebugEnabled()) {
      StringBuffer sb = concatString(strings);
      logger.debug(logPrefix + sb.toString());
    }
  }

  private StringBuffer concatString(Object... strings) {
    StringBuffer sb = new StringBuffer();
    for (Object str : strings) {
      sb.append(str.toString());
    }
    return sb;
  }

  @Override
  public void compactionCompleted(String region, int bucket, boolean isMajor) {
    // do nothing for compaction events. Hoplog Organizer depends on addition
    // and deletion of hoplogs only
  }
}

