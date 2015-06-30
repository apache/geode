/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.hdfs.internal.hoplog.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import com.gemstone.gemfire.cache.hdfs.internal.PersistedEventImpl;
import com.gemstone.gemfire.cache.hdfs.internal.SortedHoplogPersistedEvent;
import com.gemstone.gemfire.cache.hdfs.internal.UnsortedHoplogPersistedEvent;
import com.gemstone.gemfire.internal.util.BlobHelper;

public class AbstractGFRecordReader extends
    RecordReader<GFKey, PersistedEventImpl> {

  // constant overhead of each KV in hfile. This is used in computing the
  // progress of record reader
  protected long RECORD_OVERHEAD = 8;

  // accounting for number of bytes already read from the hfile
  private long bytesRead;
  
  protected boolean isSequential;
  
  protected HDFSSplitIterator splitIterator;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
  throws IOException, InterruptedException {
    CombineFileSplit cSplit = (CombineFileSplit) split;
    Path[] path = cSplit.getPaths();
    long[] start = cSplit.getStartOffsets();
    long[] len = cSplit.getLengths();

    Configuration conf = context.getConfiguration();
    FileSystem fs = cSplit.getPath(0).getFileSystem(conf);
    
    this.splitIterator = HDFSSplitIterator.newInstance(fs, path, start, len, 0l, 0l);
  }
  
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return next();
  }

  protected boolean next() throws IOException {
    if (!hasNext()) {
      return false;
    }
    
    splitIterator.next();
    bytesRead += (splitIterator.getKey().length + splitIterator.getValue().length);
    bytesRead += RECORD_OVERHEAD;
    return true;
  }
  
  protected boolean hasNext() throws IOException {
    return splitIterator.hasNext();
  }

  @Override
  public GFKey getCurrentKey() throws IOException, InterruptedException {
    return getKey();
  }

  protected GFKey getKey() throws IOException {
    try {
      GFKey key = new GFKey();
      key.setKey(BlobHelper.deserializeBlob(splitIterator.getKey()));
      return key;
    } catch (ClassNotFoundException e) {
      // TODO resolve logging
      return null;
    }
  }

  @Override
  public PersistedEventImpl getCurrentValue() throws IOException,
      InterruptedException {
    return getValue();
  }

  protected PersistedEventImpl getValue() throws IOException {
    try {
      byte[] valueBytes = splitIterator.getValue();
      if(isSequential) {
        return UnsortedHoplogPersistedEvent.fromBytes(valueBytes);
      } else {
        return SortedHoplogPersistedEvent.fromBytes(valueBytes);
      }
    } catch (ClassNotFoundException e) {
      // TODO resolve logging
      return null;
    }
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return getProgressRatio();
  }

  protected float getProgressRatio() throws IOException {
    if (!splitIterator.hasNext()) {
      return 1.0f;
    } else if (bytesRead > splitIterator.getLength()) {
      // the record reader is expected to read more number of bytes as it
      // continues till beginning of next block. hence if extra reading has
      // started return fixed value
      return 0.95f;
    } else {
      return Math.min(1.0f, bytesRead / (float) (splitIterator.getLength()));
    }
  }

  @Override
  public void close() throws IOException {
    splitIterator.close();
  }
}
