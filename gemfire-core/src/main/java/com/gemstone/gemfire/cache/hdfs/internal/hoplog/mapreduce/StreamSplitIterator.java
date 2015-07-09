/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.hdfs.internal.hoplog.mapreduce;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.gemstone.gemfire.cache.hdfs.internal.PersistedEventImpl;
import com.gemstone.gemfire.cache.hdfs.internal.UnsortedHDFSQueuePersistedEvent;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.AbstractHoplog;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.SequenceFileHoplog;

/**
 * An iterator that iterates over a split in a sequential hoplog.
 * @author dsmith
 */
public class StreamSplitIterator extends HDFSSplitIterator {

  public StreamSplitIterator(FileSystem fs, Path[] path, long[] start, long[] len, long startTime, long endTime) throws IOException {
    super(fs, path, start, len, startTime, endTime);
  }

  public PersistedEventImpl getDeserializedValue() throws ClassNotFoundException, IOException {
    return UnsortedHDFSQueuePersistedEvent.fromBytes(iterator.getValue());
  }

  @Override
  protected AbstractHoplog getHoplog(FileSystem fs, Path path) throws IOException {
    return new SequenceFileHoplog(fs, path, null);
  }
}
