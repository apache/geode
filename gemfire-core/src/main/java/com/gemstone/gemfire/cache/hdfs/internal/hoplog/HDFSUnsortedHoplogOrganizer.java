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
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.hdfs.internal.QueuedPersistentEvent;
import com.gemstone.gemfire.cache.hdfs.internal.UnsortedHoplogPersistedEvent;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HDFSRegionDirector.HdfsRegionManager;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.Hoplog.HoplogWriter;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * Manages unsorted Hoplog files for a bucket (Streaming Ingest option). An instance per bucket 
 * will exist in each PR
 * 
 * @author hemantb
 *
 */
public class HDFSUnsortedHoplogOrganizer extends AbstractHoplogOrganizer<UnsortedHoplogPersistedEvent> {
  public static final String HOPLOG_REGEX = HOPLOG_NAME_REGEX + "("
      + SEQ_HOPLOG_EXTENSION + "|" + TEMP_HOPLOG_EXTENSION + ")";
  public static final Pattern HOPLOG_PATTERN = Pattern.compile(HOPLOG_REGEX);
  protected static String TMP_FILE_NAME_REGEX = HOPLOG_NAME_REGEX + SEQ_HOPLOG_EXTENSION + TEMP_HOPLOG_EXTENSION + "$";
  protected static final Pattern patternForTmpHoplog = Pattern.compile(TMP_FILE_NAME_REGEX);
  
   volatile private HoplogWriter writer;
   volatile private Hoplog currentHoplog;
   
   volatile private long lastFlushTime = System.currentTimeMillis();
   
   volatile private boolean abortFlush = false;
   private FileSystem fileSystem;
   
   public HDFSUnsortedHoplogOrganizer(HdfsRegionManager region, int bucketId) throws IOException{
    super(region, bucketId);
    writer = null;
    sequence = new AtomicInteger(0);

    fileSystem = store.getFileSystem();
    if (! fileSystem.exists(bucketPath)) {
      return;
    }
    
    FileStatus validHoplogs[] = FSUtils.listStatus(fileSystem, bucketPath, new PathFilter() {
      @Override
      public boolean accept(Path file) {
        // All valid hoplog files must match the regex
        Matcher matcher = HOPLOG_PATTERN.matcher(file.getName());
        return matcher.matches();
      }
    });

    if (validHoplogs != null && validHoplogs.length > 0) {
      for (FileStatus file : validHoplogs) {
        // account for the disk used by this file
        incrementDiskUsage(file.getLen());
      }
    }

  }
  
    @Override
    public void close() throws IOException {
      super.close();
      if (logger.isDebugEnabled())
        logger.debug("{}Closing the hoplog organizer and the open files", logPrefix);
      // abort the flush so that we can immediately call the close current writer. 
      abortFlush = true;
      synchronizedCloseWriter(true, 0, 0);
    }
    
    
    /**
     * Flushes the data to HDFS. 
     * Synchronization ensures that the writer is not closed when flush is happening.
     * To abort the flush, abortFlush needs to be set.  
     * @throws ForceReattemptException 
     */
     @Override
    public synchronized void flush(Iterator<? extends QueuedPersistentEvent> bufferIter, final int count)
        throws IOException, ForceReattemptException {
      assert bufferIter != null;
      
      if (abortFlush)
        throw new CacheClosedException("Either the region has been cleared " +
            "or closed. Aborting the ongoing flush operation.");
      if (logger.isDebugEnabled())
        logger.debug("{}Initializing flush operation", logPrefix);
      
      // variables for updating stats
      long start = stats.getFlush().begin();
      int byteCount = 0;
      if (writer == null) {
        // Hoplogs of sequence files are always created with a 0 sequence number
        currentHoplog = getTmpSortedOplog(0, SEQ_HOPLOG_EXTENSION);
        try {
          writer = this.store.getSingletonWriter().runSerially(new Callable<Hoplog.HoplogWriter>() {
            @Override
            public HoplogWriter call() throws Exception {
              return currentHoplog.createWriter(count);
            }
          });
        } catch (Exception e) {
          if (e instanceof IOException) {
            throw (IOException)e;
          }
          throw new IOException(e);
        }
      }
      long timeSinceLastFlush = (System.currentTimeMillis() - lastFlushTime)/1000 ;
      
      try {
        /**MergeGemXDHDFSToGFE changed the following statement as the code of HeapDataOutputStream is not merged */
        //HeapDataOutputStream out = new HeapDataOutputStream();
        while (bufferIter.hasNext()) {
          HeapDataOutputStream out = new HeapDataOutputStream(1024, null);
          if (abortFlush) {
            stats.getFlush().end(byteCount, start);
            throw new CacheClosedException("Either the region has been cleared " +
            		"or closed. Aborting the ongoing flush operation.");
          }
          QueuedPersistentEvent item = bufferIter.next();
          item.toHoplogEventBytes(out);
          byte[] valueBytes = out.toByteArray();
          writer.append(item.getRawKey(), valueBytes);
          // add key length and value length to stats byte counter
          byteCount += (item.getRawKey().length + valueBytes.length);
          /**MergeGemXDHDFSToGFE how to clear for reuse. Leaving it for Darrel to merge this change*/
          //out.clearForReuse();
        }
        // ping secondaries before making the file a legitimate file to ensure 
        // that in case of split brain, no other vm has taken up as primary. #50110. 
        if (!abortFlush)
          pingSecondaries();
        // append completed. If the file is to be rolled over, 
        // close writer and rename the file to a legitimate name.
        // Else, sync the already written data with HDFS nodes. 
        int maxFileSize = this.store.getWriteOnlyFileRolloverSize() * 1024 * 1024;  
        int fileRolloverInterval = this.store.getWriteOnlyFileRolloverInterval(); 
        if (writer.getCurrentSize() >= maxFileSize || 
            timeSinceLastFlush >= fileRolloverInterval) {
          closeCurrentWriter();
        }
        else {
          // if flush is not aborted, hsync the batch. It ensures that 
          // the batch has reached HDFS and we can discard it. 
          if (!abortFlush)
            writer.hsync();
        }
      } catch (IOException e) {
        stats.getFlush().error(start);
        // as there is an exception, it can be probably be a file specific problem.
        // close the current file to avoid any file specific issues next time  
        closeCurrentWriter();
        // throw the exception so that async queue will dispatch the same batch again 
        throw e;
      } 
      
      stats.getFlush().end(byteCount, start);
    }
    
    /**
     * Synchronization ensures that the writer is not closed when flush is happening. 
     */
    synchronized void synchronizedCloseWriter(boolean forceClose, 
        long timeSinceLastFlush, int minsizeforrollover) throws IOException { 
      long writerSize = 0;
      if (writer != null){
        writerSize = writer.getCurrentSize();
      }
      
      if (writerSize < (minsizeforrollover * 1024L))
        return;
      
      int maxFileSize = this.store.getWriteOnlyFileRolloverSize() * 1024 * 1024;  
      int fileRolloverInterval = this.store.getWriteOnlyFileRolloverInterval(); 
      if (writerSize >= maxFileSize || 
          timeSinceLastFlush >= fileRolloverInterval || forceClose) {
        closeCurrentWriter();
      }
      }
        
    
    /**
     * Closes the current writer so that next time a new hoplog can 
     * be created. Also, fixes any tmp hoplogs. 
     * 
     * @throws IOException
     */
    void closeCurrentWriter() throws IOException {
      
      if (writer != null) {
        // If this organizer is closing, it is ok to ignore exceptions here
        // because CloseTmpHoplogsTimerTask
        // on another member may have already renamed the hoplog
        // fixes bug 49141
        boolean isClosing = abortFlush;
        try {
          incrementDiskUsage(writer.getCurrentSize());
        } catch (IOException e) {
          if (!isClosing) {
            throw e;
          }
        }
        if (logger.isDebugEnabled())
          logger.debug("{}Closing hoplog " + currentHoplog.getFileName(), logPrefix);
        try{
          writer.close();
          makeLegitimate(currentHoplog);
        } catch (IOException e) {
          if (!isClosing) {
            logger.warn(LocalizedStrings.HOPLOG_FLUSH_OPERATION_FAILED, e);
            throw e;
          }
        } finally {
          writer = null;
          lastFlushTime = System.currentTimeMillis();
        }
      }
      else
        lastFlushTime = System.currentTimeMillis();
    }

    @Override
    public void clear() throws IOException {
      boolean prevAbortFlushFlag = abortFlush;
      // abort the flush so that we can immediately call the close current writer. 
      abortFlush = true;
      
      // Close if there is any existing writer. 
      try {
        synchronizedCloseWriter(true, 0, 0);
      } catch (IOException e) {
        logger.warn(LocalizedStrings.HOPLOG_CLOSE_FAILED, e);
      }
      
      // reenable the aborted flush
      abortFlush = prevAbortFlushFlag;
      
      // Mark the hoplogs for deletion
      markHoplogsForDeletion();
      
    }
  
    @Override
    public void performMaintenance() {
      // TODO remove the timer for tmp file conversion. Use this instead
    }

    @Override
    public Future<CompactionStatus> forceCompaction(boolean isMajor) {
      return null;
    }

    @Override
    protected Hoplog getHoplog(Path hoplogPath) throws IOException {
      Hoplog so = new SequenceFileHoplog(fileSystem, hoplogPath, stats);
      return so;
    }
  
  /**
   * Fixes the size of hoplogs that were not closed properly last time. 
   * Such hoplogs are *.tmphop files. Identify them and open them and close 
   * them, this fixes the size. After doing this rename them to *.hop. 
   * 
   * @throws IOException
   * @throws ForceReattemptException 
   */
  void identifyAndFixTmpHoplogs(FileSystem fs) throws IOException, ForceReattemptException {
    if (logger.isDebugEnabled())
      logger.debug("{}Fixing temporary hoplogs", logPrefix);
    
    // A different filesystem is passed to this function for the following reason: 
    // For HDFS, if a file wasn't closed properly last time, 
    // while calling FileSystem.append for this file, FSNamesystem.startFileInternal->
    // FSNamesystem.recoverLeaseInternal function gets called. 
    // This function throws AlreadyBeingCreatedException if there is an open handle, to any other file, 
    // created using the same FileSystem object. This is a bug and is being tracked at: 
    // https://issues.apache.org/jira/browse/HDFS-3848?page=com.atlassian.jira.plugin.system.issuetabpanels:all-tabpanel
    // 
    // The fix for this bug is not yet part of Pivotal HD. So to overcome the bug, 
    // we create a new file system for the timer task so that it does not encounter the bug. 
    
    FileStatus tmpHoplogs[] = FSUtils.listStatus(fs, fs.makeQualified(bucketPath), new PathFilter() {
      @Override
      public boolean accept(Path file) {
        // All valid hoplog files must match the regex
        Matcher matcher = patternForTmpHoplog.matcher(file.getName());
        return matcher.matches();
      }
    });
    
    if (tmpHoplogs == null || tmpHoplogs.length == 0) {
      if (logger.isDebugEnabled())
        logger.debug("{}No files to fix", logPrefix);
      return;
    }
    // ping secondaries so that in case of split brain, no other vm has taken up 
    // as primary. #50110. 
    pingSecondaries();
    if (logger.isDebugEnabled())
      logger.debug("{}Files to fix " + tmpHoplogs.length, logPrefix);

    String currentHoplogName = null;
    // get the current hoplog name. We need to ignore current hoplog while fixing. 
    if (currentHoplog != null) {
      currentHoplogName = currentHoplog.getFileName();
    }
    
    for (int i = 0; i < tmpHoplogs.length; i++) {
      // Skip directories
      if (tmpHoplogs[i].isDirectory()) {
        continue;
      }

      final Path p = tmpHoplogs[i].getPath();
      
      if (tmpHoplogs[i].getPath().getName().equals(currentHoplogName)){
        if (logger.isDebugEnabled())
          logger.debug("Skipping current file: " + tmpHoplogs[i].getPath().getName(), logPrefix);
        continue;
      } 
      
      SequenceFileHoplog hoplog = new SequenceFileHoplog(fs, p, stats);
      try {
        makeLegitimate(hoplog);
        logger.info (LocalizedMessage.create(LocalizedStrings.DEBUG, "Hoplog " + p + " was a temporary " +
            "hoplog because the node managing it wasn't shutdown properly last time. Fixed the hoplog name."));
      } catch (IOException e) {
        logger.info (LocalizedMessage.create(LocalizedStrings.DEBUG, "Hoplog " + p + " is still a temporary " +
            "hoplog because the node managing it wasn't shutdown properly last time. Failed to " +
            "change the hoplog name because an exception was thrown while fixing it. " + e));
      }
    }
  }
  
  private FileStatus[] getExpiredHoplogs() throws IOException {
    FileStatus files[] = FSUtils.listStatus(fileSystem, bucketPath, new PathFilter() {
      @Override
      public boolean accept(Path file) {
        // All expired hoplog end with expire extension and must match the valid file regex
        String fileName = file.getName();
        if (! fileName.endsWith(EXPIRED_HOPLOG_EXTENSION)) {
          return false;
        }
        return true;
      }
    });
    return files;
  }
  /**
   * locks sorted oplogs collection, removes oplog and renames for deletion later
   * @throws IOException 
   */
  private void markHoplogsForDeletion() throws IOException {
    
    ArrayList<IOException> errors = new ArrayList<IOException>();
    FileStatus validHoplogs[] = FSUtils.listStatus(fileSystem, bucketPath, new PathFilter() {
      @Override
      public boolean accept(Path file) {
        // All valid hoplog files must match the regex
        Matcher matcher = HOPLOG_PATTERN.matcher(file.getName());
        return matcher.matches();
      }
    });
    
    FileStatus[] expired = getExpiredHoplogs();
    validHoplogs = filterValidHoplogs(validHoplogs, expired);

    if (validHoplogs == null || validHoplogs.length == 0) {
      return;
    }
    for (FileStatus fileStatus : validHoplogs) {
      try {
        addExpiryMarkerForAFile(getHoplog(fileStatus.getPath()));
      } catch (IOException e) {
        // even if there is an IO error continue removing other hoplogs and
        // notify at the end
        errors.add(e);
      }
    }
    
    if (!errors.isEmpty()) {
      for (IOException e : errors) {
        logger.warn(LocalizedStrings.HOPLOG_HOPLOG_REMOVE_FAILED, e);
      }
    }
  }
  
  @Override
  public Compactor getCompactor() {
    throw new UnsupportedOperationException("Not supported for " + this.getClass().getSimpleName());
  }
  
    @Override
  public HoplogIterator<byte[], UnsortedHoplogPersistedEvent> scan(
      long startOffset, long length) throws IOException {
    throw new UnsupportedOperationException("Not supported for " + this.getClass().getSimpleName());
    }

  public long getLastFlushTime() {
    return this.lastFlushTime;
      }
  
  public long getfileRolloverInterval(){
    int fileRolloverInterval = this.store.getWriteOnlyFileRolloverInterval(); 
    return fileRolloverInterval;
    }

  @Override
  public long getLastMajorCompactionTimestamp() {
    throw new UnsupportedOperationException();
  }

}
