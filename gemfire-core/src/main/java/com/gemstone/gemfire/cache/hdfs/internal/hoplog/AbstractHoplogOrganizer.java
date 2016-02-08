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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreImpl;
import com.gemstone.gemfire.cache.hdfs.internal.PersistedEventImpl;
import com.gemstone.gemfire.cache.hdfs.internal.QueuedPersistentEvent;
import com.gemstone.gemfire.cache.hdfs.internal.SortedHoplogPersistedEvent;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.AbstractHoplog.HoplogDescriptor;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HDFSRegionDirector.HdfsRegionManager;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplogStatistics;
import com.gemstone.gemfire.internal.cache.persistence.soplog.TrackedReference;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import org.apache.logging.log4j.Logger;


public abstract class AbstractHoplogOrganizer<T extends PersistedEventImpl> implements HoplogOrganizer<T> {

  public static final String MINOR_HOPLOG_EXTENSION = ".ihop";
  public static final String MAJOR_HOPLOG_EXTENSION = ".chop";
  public static final String EXPIRED_HOPLOG_EXTENSION = ".exp";
  public static final String TEMP_HOPLOG_EXTENSION = ".tmp";

  public static final String FLUSH_HOPLOG_EXTENSION = ".hop";
  public static final String SEQ_HOPLOG_EXTENSION = ".shop";

  // all valid hoplogs will follow the following name pattern
  public static final String HOPLOG_NAME_REGEX = "(.+?)-(\\d+?)-(\\d+?)";
  public static final Pattern HOPLOG_NAME_PATTERN = Pattern.compile(HOPLOG_NAME_REGEX
      + "\\.(.*)");
  
  public static boolean JUNIT_TEST_RUN = false; 

  protected static final boolean ENABLE_INTEGRITY_CHECKS = Boolean
      .getBoolean("gemfire.HdfsSortedOplogOrganizer.ENABLE_INTEGRITY_CHECKS")
      || assertionsEnabled();

  private static boolean assertionsEnabled() {
    boolean enabled = false;
    assert enabled = true;
    return enabled;
  }

  protected HdfsRegionManager regionManager;
  // name or id of bucket managed by this organizer
  protected final String regionFolder;
  protected final int bucketId;

  // path of the region directory
  protected final Path basePath;
  // identifies path of directory containing a bucket's oplog files
  protected final Path bucketPath;

  protected final HDFSStoreImpl store;

  // assigns a unique increasing number to each oplog file
  protected AtomicInteger sequence;

  //logger instance
  protected static final Logger logger = LogService.getLogger();
  protected final String logPrefix;

  protected SortedOplogStatistics stats;
  AtomicLong bucketDiskUsage = new AtomicLong(0);

  // creation of new files and expiration of files will be synchronously
  // notified to the listener
  protected HoplogListener listener;

  private volatile boolean closed = false;
  
  protected Object changePrimarylockObject = new Object();
  
  public AbstractHoplogOrganizer(HdfsRegionManager region, int bucketId) {

    assert region != null;

    this.regionManager = region;
    this.regionFolder = region.getRegionFolder();
    this.store = region.getStore();
    this.listener = region.getListener();
    this.stats = region.getHdfsStats();
    
    this.bucketId = bucketId;

    this.basePath = new Path(store.getHomeDir());
    this.bucketPath = new Path(basePath, regionFolder + "/" + bucketId);

    this.logPrefix = "<" + getRegionBucketStr() + "> ";
    
  }

  @Override
  public boolean isClosed() {
    return closed || regionManager.isClosed();
  }
  
  @Override
  public void close() throws IOException {
    closed = true;
    
    // this bucket is closed and may be owned by a new node. So reduce the store
    // usage stat, as the new owner adds the usage metric
    incrementDiskUsage((-1) * bucketDiskUsage.get());
  }

  @Override
  public abstract void flush(Iterator<? extends QueuedPersistentEvent> bufferIter,
      int count) throws IOException, ForceReattemptException;

  @Override
  public abstract void clear() throws IOException;

  protected abstract Hoplog getHoplog(Path hoplogPath) throws IOException;

  @Override
  public void hoplogCreated(String region, int bucketId, Hoplog... oplogs)
      throws IOException {
    throw new UnsupportedOperationException("Not supported for "
        + this.getClass().getSimpleName());
  }

  @Override
  public void hoplogDeleted(String region, int bucketId, Hoplog... oplogs)
      throws IOException {
    throw new UnsupportedOperationException("Not supported for "
        + this.getClass().getSimpleName());
  }

  @Override
  public void compactionCompleted(String region, int bucket, boolean isMajor) {
    throw new UnsupportedOperationException("Not supported for "
        + this.getClass().getSimpleName());
  }
  
  @Override
  public T read(byte[] key) throws IOException {
    throw new UnsupportedOperationException("Not supported for "
        + this.getClass().getSimpleName());
  }

  @Override
  public HoplogIterator<byte[], T> scan() throws IOException {
    throw new UnsupportedOperationException("Not supported for "
        + this.getClass().getSimpleName());
  }

  @Override
  public HoplogIterator<byte[], T> scan(byte[] from, byte[] to)
      throws IOException {
    throw new UnsupportedOperationException("Not supported for "
        + this.getClass().getSimpleName());
  }

  @Override
  public HoplogIterator<byte[], T> scan(byte[] from,
      boolean fromInclusive, byte[] to, boolean toInclusive) throws IOException {
    throw new UnsupportedOperationException("Not supported for "
        + this.getClass().getSimpleName());
  }

  @Override
  public long sizeEstimate() {
    throw new UnsupportedOperationException("Not supported for "
        + this.getClass().getSimpleName());
  }

  /**
   * @return returns an oplogs full path after prefixing bucket path to the file
   *         name
   */
  protected String getPathStr(Hoplog oplog) {
    return bucketPath.toString() + "/" + oplog.getFileName();
  }

  protected String getRegionBucketStr() {
    return regionFolder + "/" + bucketId;
  }

  protected SortedHoplogPersistedEvent deserializeValue(byte[] val) throws IOException {
    try {
      return SortedHoplogPersistedEvent.fromBytes(val);
    } catch (ClassNotFoundException e) {
      logger
          .error(
              LocalizedStrings.GetMessage_UNABLE_TO_DESERIALIZE_VALUE_CLASSNOTFOUNDEXCEPTION,
              e);
      return null;
    }
  }

  /**
   * @return true if the entry belongs to an destroy event
   */
  protected boolean isDeletedEntry(byte[] value, int offset) throws IOException {
    // Read only the first byte of PersistedEventImpl for the operation
    assert value != null && value.length > 0 && offset >= 0 && offset < value.length;
    Operation op = Operation.fromOrdinal(value[offset]);

    if (op.isDestroy() || op.isInvalidate()) {
      return true;
    }
    return false;
  }

  /**
   * @param seqNum
   *          desired sequence number of the hoplog. If null a highest number is
   *          choosen
   * @param extension
   *          file extension representing the type of file, e.g. ihop for
   *          intermediate hoplog
   * @return a new temporary file for a new sorted oplog. The name consists of
   *         bucket name, a sequence number for ordering the files followed by a
   *         timestamp
   */
  Hoplog getTmpSortedOplog(Integer seqNum, String extension) throws IOException {
    if (seqNum == null) {
      seqNum = sequence.incrementAndGet();
    }
    String name = bucketId + "-" + System.currentTimeMillis() + "-" + seqNum 
        + extension;
    Path soplogPath = new Path(bucketPath, name + TEMP_HOPLOG_EXTENSION);
    return getHoplog(soplogPath);
  }
  
  /**
   * renames a temporary hoplog file to a legitimate name.
   */
  static void makeLegitimate(Hoplog so) throws IOException {
    String name = so.getFileName();
    assert name.endsWith(TEMP_HOPLOG_EXTENSION);

    int index = name.lastIndexOf(TEMP_HOPLOG_EXTENSION);
    name = name.substring(0, index);
    so.rename(name);
  }

  /**
   * creates a expiry marker for a file on file system
   * 
   * @param hoplog
   * @throws IOException
   */
  protected void addExpiryMarkerForAFile(Hoplog hoplog) throws IOException {
    FileSystem fs = store.getFileSystem();

    // TODO optimization needed here. instead of creating expired marker
    // file per file, create a meta file. the main thing to worry is
    // compaction of meta file itself
    Path expiryMarker = getExpiryMarkerPath(hoplog.getFileName());

    // uh-oh, why are we trying to expire an already expired file?
    if (ENABLE_INTEGRITY_CHECKS) {
      Assert.assertTrue(!fs.exists(expiryMarker),
          "Expiry marker already exists: " + expiryMarker);
    }

    FSDataOutputStream expiryMarkerFile = fs.create(expiryMarker);
    expiryMarkerFile.close();

    if (logger.isDebugEnabled())
      logger.debug("Hoplog marked expired: " + getPathStr(hoplog));
  }

  protected Path getExpiryMarkerPath(String name) {
    return new Path(bucketPath, name + EXPIRED_HOPLOG_EXTENSION);
  }
  
  protected String truncateExpiryExtension(String name) {
    if (name.endsWith(EXPIRED_HOPLOG_EXTENSION)) {
      return name.substring(0, name.length() - EXPIRED_HOPLOG_EXTENSION.length());
    }
    
    return name;
  }
  
  /**
   * updates region stats and a local copy of bucket level store usage metric.
   * 
   * @param delta
   */
  protected void incrementDiskUsage(long delta) {
    long newSize = bucketDiskUsage.addAndGet(delta);
    if (newSize < 0 && delta < 0) {
      if (logger.isDebugEnabled()){
        logger.debug("{}Invalid diskUsage size:" + newSize + " caused by delta:"
            + delta + ", parallel del & close?" + isClosed(), logPrefix);
      }
      if (isClosed()) {
        // avoid corrupting disk usage size during close by reducing residue
        // size only
        delta = delta + (-1 * newSize);
      }
    }
    stats.incStoreUsageBytes(delta);
  }

  /**
   * Utility method to remove a file from valid file list if a expired marker
   * for the file exists
   * 
   * @param valid
   *          list of valid files
   * @param expired
   *          list of expired file markers
   * @return list f valid files that do not have a expired file marker
   */
  public static FileStatus[] filterValidHoplogs(FileStatus[] valid,
      FileStatus[] expired) {
    if (valid == null) {
      return null;
    }

    if (expired == null) {
      return valid;
    }

    ArrayList<FileStatus> result = new ArrayList<FileStatus>();
    for (FileStatus vs : valid) {
      boolean found = false;
      for (FileStatus ex : expired) {
        if (ex
            .getPath()
            .getName()
            .equals(
                vs.getPath().getName()
                    + HdfsSortedOplogOrganizer.EXPIRED_HOPLOG_EXTENSION)) {
          found = true;
        }
      }
      if (!found) {
        result.add(vs);
      }
    }

    return result.toArray(new FileStatus[result.size()]);
  }

  protected void pingSecondaries() throws ForceReattemptException {

    if (JUNIT_TEST_RUN)
      return;
    BucketRegion br = ((PartitionedRegion)this.regionManager.getRegion()).getDataStore().getLocalBucketById(this.bucketId);
    boolean secondariesPingable = false;
    try {
      secondariesPingable = br.areSecondariesPingable();
    } catch (Throwable e) {
      throw new ForceReattemptException("Failed to ping secondary servers of bucket: " + 
          this.bucketId + ", region: " + ((PartitionedRegion)this.regionManager.getRegion()), e);
    }
    if (!secondariesPingable)
      throw new ForceReattemptException("Failed to ping secondary servers of bucket: " + 
          this.bucketId + ", region: " + ((PartitionedRegion)this.regionManager.getRegion()));
  }

  

  
  /**
   * A comparator for ordering soplogs based on the file name. The file names
   * are assigned incrementally and hint at the age of the file
   */
  public static final class HoplogComparator implements
      Comparator<TrackedReference<Hoplog>> {
    /**
     * a file with a higher sequence or timestamp is the younger and hence the
     * smaller
     */
    @Override
    public int compare(TrackedReference<Hoplog> o1, TrackedReference<Hoplog> o2) {
      return o1.get().compareTo(o2.get());
    }

    /**
     * Compares age of files based on file names and returns 1 if name1 is
     * older, -1 if name1 is yonger and 0 if the two files are same age
     */
    public static int compareByName(String name1, String name2) {
      HoplogDescriptor hd1 = new HoplogDescriptor(name1);
      HoplogDescriptor hd2 = new HoplogDescriptor(name2);
      
      return hd1.compareTo(hd2);
    }
  }

  /**
   * @param matcher
   *          A preinitialized / matched regex pattern
   * @return Timestamp of the
   */
  public static long getHoplogTimestamp(Matcher matcher) {
    return Long.valueOf(matcher.group(2));
  }
}
