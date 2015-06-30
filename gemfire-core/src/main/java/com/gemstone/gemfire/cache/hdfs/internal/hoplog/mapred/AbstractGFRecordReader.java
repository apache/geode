/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.hdfs.internal.hoplog.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

import com.gemstone.gemfire.cache.hdfs.internal.PersistedEventImpl;
import com.gemstone.gemfire.cache.hdfs.internal.SortedHoplogPersistedEvent;
import com.gemstone.gemfire.cache.hdfs.internal.UnsortedHoplogPersistedEvent;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.mapreduce.GFKey;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.mapreduce.HDFSSplitIterator;

public class AbstractGFRecordReader
    extends
    com.gemstone.gemfire.cache.hdfs.internal.hoplog.mapreduce.AbstractGFRecordReader
    implements RecordReader<GFKey, PersistedEventImpl> {

  /**
   * Initializes instance of record reader using file split and job
   * configuration
   * 
   * @param split
   * @param conf
   * @throws IOException
   */
  public void initialize(CombineFileSplit split, JobConf conf) throws IOException {
    CombineFileSplit cSplit = (CombineFileSplit) split;
    Path[] path = cSplit.getPaths();
    long[] start = cSplit.getStartOffsets();
    long[] len = cSplit.getLengths();

    FileSystem fs = cSplit.getPath(0).getFileSystem(conf);
    this.splitIterator = HDFSSplitIterator.newInstance(fs, path, start, len, 0l, 0l);
  }

  @Override
  public boolean next(GFKey key, PersistedEventImpl value) throws IOException {
    /*
     * if there are more records in the hoplog, iterate to the next record. Set
     * key object as is. 
     */

    if (!super.hasNext()) {
      key.setKey(null);
      // TODO make value null;
      return false;
    }

    super.next();

    key.setKey(super.getKey().getKey());
    PersistedEventImpl usersValue = super.getValue();
    value.copy(usersValue);
    return true;
  }

  @Override
  public GFKey createKey() {
    return new GFKey();
  }

  @Override
  public PersistedEventImpl createValue() {
    if(this.isSequential) {
      return new UnsortedHoplogPersistedEvent();
    } else {
      return new SortedHoplogPersistedEvent();
    }
  }

  @Override
  public long getPos() throws IOException {
    // there is no efficient way to find the position of key in hoplog file.
    return 0;
  }

  @Override
  public void close() throws IOException {
    super.close();
  }

  @Override
  public float getProgress() throws IOException {
    return super.getProgressRatio();
  }
}
