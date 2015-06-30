/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.hdfs.internal.hoplog.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.gemstone.gemfire.cache.hdfs.HDFSStore;
import com.gemstone.gemfire.cache.hdfs.internal.PersistedEventImpl;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HDFSRegionDirector.HdfsRegionManager;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.mapreduce.HoplogUtil.HoplogOptimizedSplitter;

public class GFInputFormat extends InputFormat<GFKey, PersistedEventImpl>
    implements Configurable {
  public static final String HOME_DIR = "mapreduce.input.gfinputformat.homedir";
  public static final String INPUT_REGION = "mapreduce.input.gfinputformat.inputregion";
  public static final String START_TIME = "mapreduce.input.gfinputformat.starttime";
  public static final String END_TIME = "mapreduce.input.gfinputformat.endtime";
  public static final String CHECKPOINT = "mapreduce.input.gfinputformat.checkpoint";
  
  protected Configuration conf;

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    this.conf = job.getConfiguration();
    
    Collection<FileStatus> hoplogs = getHoplogs();
    return createSplits(hoplogs);
  }

  /**
   * Identifies filters provided in the job configuration and creates a list of
   * sorted hoplogs. If there are no sorted hoplogs, checks if the region has
   * sequential hoplogs
   * 
   * @return list of hoplogs
   * @throws IOException
   */
  protected Collection<FileStatus> getHoplogs() throws IOException {
    String regionName = conf.get(INPUT_REGION);
    System.out.println("GFInputFormat: Region Name is " + regionName);
    if (regionName == null || regionName.trim().isEmpty()) {
      // incomplete job configuration, region name must be provided
      return new ArrayList<FileStatus>();
    }

    String home = conf.get(HOME_DIR, HDFSStore.DEFAULT_HOME_DIR);
    regionName = HdfsRegionManager.getRegionFolder(regionName);
    Path regionPath = new Path(home + "/" + regionName);
    FileSystem fs = regionPath.getFileSystem(conf);

    long start = conf.getLong(START_TIME, 0l);
    long end = conf.getLong(END_TIME, 0l);
    boolean checkpoint = conf.getBoolean(CHECKPOINT, true);

    // if the region contains flush hoplogs then the region is of type RW.
    Collection<FileStatus> hoplogs;
    hoplogs = HoplogUtil.filterHoplogs(fs, regionPath, start, end, checkpoint);
    return hoplogs == null ? new ArrayList<FileStatus>() : hoplogs;
  }
  
  /**
   * Creates an input split for every block occupied by hoplogs of the input
   * regions
   * 
   * @param hoplogs
   * @return list of input splits of type file input split
   * @throws IOException
   */
  private List<InputSplit> createSplits(Collection<FileStatus> hoplogs)
      throws IOException {
    List<InputSplit> splits = new ArrayList<InputSplit>();
    if (hoplogs == null || hoplogs.isEmpty()) {
      return splits;
    }
    
    HoplogOptimizedSplitter splitter = new HoplogOptimizedSplitter(hoplogs);
    return splitter.getOptimizedSplits(conf);
  }

  @Override
  public RecordReader<GFKey, PersistedEventImpl> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
    return new AbstractGFRecordReader();
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
