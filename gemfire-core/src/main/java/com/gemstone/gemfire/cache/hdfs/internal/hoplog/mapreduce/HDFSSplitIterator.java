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
package com.gemstone.gemfire.cache.hdfs.internal.hoplog.mapreduce;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import com.gemstone.gemfire.cache.hdfs.HDFSIOException;
import com.gemstone.gemfire.cache.hdfs.internal.PersistedEventImpl;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.AbstractHoplog;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.AbstractHoplogOrganizer;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HoplogSetReader.HoplogIterator;
import com.gemstone.gemfire.i18n.LogWriterI18n;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;

/**
 * Iterates over the records in part of a hoplog. This iterator
 * is passed from the map reduce job into the gemfirexd LanguageConnectionContext
 * for gemfirexd to use as the iterator during the map phase.
 * @author dsmith
 *
 */
public abstract class HDFSSplitIterator {
  // data object for holding path, offset and length, of all the blocks this
  // iterator needs to iterate on
  private CombineFileSplit split;

  // the following members are pointers to current hoplog which is being
  // iterated upon
  private int currentHopIndex = 0;
  private AbstractHoplog hoplog;
  protected HoplogIterator<byte[], byte[]> iterator;
  byte[] key;
  byte[] value;
  
  private long bytesRead;
  protected long RECORD_OVERHEAD = 8;

  private long startTime = 0l;
  private long endTime = 0l;

  protected FileSystem fs;
  private static final Logger logger = LogService.getLogger();
  protected final String logPrefix = "<" + "HDFSSplitIterator" + "> ";

  public HDFSSplitIterator(FileSystem fs, Path[] paths, long[] offsets, long[] lengths, long startTime, long endTime) throws IOException {
    this.fs = fs;
    this.split = new CombineFileSplit(paths, offsets, lengths, null);
    while(currentHopIndex < split.getNumPaths() && !fs.exists(split.getPath(currentHopIndex))){
      logger.warn(LocalizedMessage.create(LocalizedStrings.HOPLOG_CLEANED_UP_BY_JANITOR, split.getPath(currentHopIndex)));
      currentHopIndex++;
    }
    if(currentHopIndex == split.getNumPaths()){
      this.hoplog = null;
      iterator = null;
    } else {
      this.hoplog = getHoplog(fs,split.getPath(currentHopIndex));
      iterator = hoplog.getReader().scan(split.getOffset(currentHopIndex), split.getLength(currentHopIndex));
    }
    this.startTime = startTime;
    this.endTime = endTime;
  }

  /**
   * Get the appropriate iterator for the file type.
   */
  public static HDFSSplitIterator newInstance(FileSystem fs, Path[] path,
      long[] start, long[] len, long startTime, long endTime)
      throws IOException {
    String fileName = path[0].getName();
    if (fileName.endsWith(AbstractHoplogOrganizer.SEQ_HOPLOG_EXTENSION)) {
      return new StreamSplitIterator(fs, path, start, len, startTime, endTime);
    } else {
      return new RWSplitIterator(fs, path, start, len, startTime, endTime);
    }
  }

  public final boolean hasNext() throws IOException {
    while (currentHopIndex < split.getNumPaths()) {
      if (iterator != null) {
        if(iterator.hasNext()) {
          return true;
        } else {
          iterator.close();
          iterator = null;
          hoplog.close();
          hoplog = null;
        }
      }
      
      if (iterator == null) {
        // Iterator is null if this is first read from this iterator or all the
        // entries from the previous iterator have been read. create iterator on
        // the next hoplog.
        currentHopIndex++;
        while (currentHopIndex < split.getNumPaths() && !fs.exists(split.getPath(currentHopIndex))){
          logger.warn(LocalizedMessage.create(LocalizedStrings.HOPLOG_CLEANED_UP_BY_JANITOR, split.getPath(currentHopIndex).toString()));
          currentHopIndex++;
        }
        if (currentHopIndex >= split.getNumPaths()) {
          return false;
        }
        hoplog = getHoplog(fs, split.getPath(currentHopIndex));
        iterator = hoplog.getReader().scan(split.getOffset(currentHopIndex), split.getLength(currentHopIndex));
      }
    }
    
    return false;
  } 

  public final boolean next() throws IOException {
    while (hasNext()) {
      key = iterator.next();
      value = iterator.getValue();
      bytesRead += (key.length + value.length);
      bytesRead += RECORD_OVERHEAD;
      
      // if any filter is set, check if the event's timestamp matches the
      // filter. The events returned by the iterator may not be time ordered. So
      // it is important to check filters everytime.
      if (startTime > 0 || endTime > 0) {
        try {
          PersistedEventImpl event = getDeserializedValue();
          long timestamp = event.getTimstamp();
          if (startTime > 0l && timestamp < startTime) {
            continue;
          }
          
          if (endTime > 0l && timestamp > endTime) {
            continue;
          }
        } catch (ClassNotFoundException e) {
          throw new HDFSIOException("Error reading from HDFS", e);
        } 
      }
        
      return true;
    }
    
    return false;
  }

  public final long getBytesRead() {
    return this.bytesRead;
  }

  public final byte[] getKey() {
    return key;
  }

  public abstract PersistedEventImpl getDeserializedValue()
      throws ClassNotFoundException, IOException;

  protected abstract AbstractHoplog getHoplog(FileSystem fs, Path path)
      throws IOException;

  public final byte[] getValue() {
    return value;
  }

  public final long getLength() {
    return split.getLength();
  }

  public void close() throws IOException {
    if (iterator != null) {
      iterator.close();
      iterator = null;
    }
    
    if (hoplog != null) {
      hoplog.close();
      hoplog.close();
    }
  }
}
