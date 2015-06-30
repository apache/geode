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
import com.gemstone.gemfire.cache.hdfs.internal.SortedHDFSQueuePersistedEvent;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.AbstractHoplog;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HFileSortedOplog;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;

/**
 * An iterator that iterates over a split in a read/write hoplog
 * @author dsmith
 */
public class RWSplitIterator extends HDFSSplitIterator {

  public RWSplitIterator(FileSystem fs, Path[] path, long[] start, long[] len, long startTime, long endTime) throws IOException {
    super(fs, path, start, len, startTime, endTime);
  }

  @Override
  protected AbstractHoplog getHoplog(FileSystem fs, Path path) throws IOException {
    SchemaMetrics.configureGlobally(fs.getConf());
    return HFileSortedOplog.getHoplogForLoner(fs, path); 
  }

  public PersistedEventImpl getDeserializedValue() throws ClassNotFoundException, IOException {
    return SortedHDFSQueuePersistedEvent.fromBytes(iterator.getValue());
  }
}
