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

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.Future;

import com.gemstone.gemfire.cache.hdfs.HDFSStore;
import com.gemstone.gemfire.cache.hdfs.internal.PersistedEventImpl;
import com.gemstone.gemfire.cache.hdfs.internal.QueuedPersistentEvent;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;

/**
 * Manages bucket level operations on sorted oplog files including creation, reading, serde, bloom
 * buffering and compaction. Abstracts existence of multiple sorted oplog files
 */
public interface HoplogOrganizer<T extends PersistedEventImpl> extends HoplogSetReader<byte[], T>,
    HoplogListener, Closeable {

  /**
   * Iterates on the input buffer and persists it in a new sorted oplog. This invocation may block
   * if there are too many outstanding write requests.
   * 
   * @param bufferIter
   *          ordered iterator on a buffer of objects to be persisted
   * @param count
   *          number of K,V pairs expected to be part of flush, 0 if unknown
   * @throws IOException
   */
  public void flush(Iterator<? extends QueuedPersistentEvent> bufferIter, int count) 
      throws IOException, ForceReattemptException;
  
  
  /**
   * Clear the data in HDFS. This method assumes that the
   * dispatcher thread has already been paused, so there should be
   * no concurrent flushes to HDFS when this method is called.
   * 
   * @throws IOException
   */
  public void clear() throws IOException;

  /**
   * returns the compactor associated with this set
   */
  public Compactor getCompactor();
  
  /**
   * Called to execute bucket maintenance activities, like purge expired files
   * and create compaction task. Long running activities must be executed
   * asynchronously, not on this thread, to avoid impact on other buckets
   * @throws IOException 
   */
  public void performMaintenance() throws IOException;

  /**
   * Schedules a compaction task and returns immediately.
   * 
   * @param isMajor true for major compaction, false for minor compaction
   * @return future for status of compaction request
   */
  public Future<CompactionStatus> forceCompaction(boolean isMajor);

  /**
   * Returns the timestamp of the last completed major compaction
   * 
   * @return the timestamp or 0 if a major compaction has not taken place yet
   */
  public long getLastMajorCompactionTimestamp();

  public interface Compactor {
    /**
     * Requests a compaction operation be performed on this set of sorted oplogs.
     *
     * @param isMajor true for major compaction
     * @param isForced true if the compaction should be carried out even if there
     * is only one hoplog to compact
     * 
     * @return true if compaction was performed, false otherwise
     * @throws IOException
     */
    boolean compact(boolean isMajor, boolean isForced) throws IOException;

    /**
     * Stop the current compaction operation in the middle and suspend
     * compaction operations. The current current compaction data
     * will be thrown away, and no more compaction will be performend
     * until resume is called. 
     */
    void suspend();
    
    /**
     * Resume compaction operations. 
     */
    void resume();

    /**
     * @return true if the compactor is not ready or busy
     */
    boolean isBusy(boolean isMajor);

    /**
     * @return the hdfsStore configuration used by this compactor
     */
    public HDFSStore getHdfsStore();
  }
}
