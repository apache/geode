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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import com.gemstone.gemfire.cache.hdfs.internal.hoplog.AbstractHoplogOrganizer;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.AbstractHoplogOrganizer.HoplogComparator;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HoplogConfig;

public class HoplogUtil {
  /**
   * @param regionPath
   *          HDFS path of the region
   * @param fs
   *          file system associated with the region
   * @param type
   *          type of hoplog to be fetched; flush hoplog or sequence hoplog
   * @return All hoplog file paths belonging to the region provided
   * @throws IOException
   */
  public static Collection<FileStatus> getAllRegionHoplogs(Path regionPath,
      FileSystem fs, String type) throws IOException {
    return getRegionHoplogs(regionPath, fs, type, 0, 0);
  }

  /**
   * @param regionPath
   *          Region path
   * @param fs
   *          file system associated with the region
   * @param type
   *          type of hoplog to be fetched; flush hoplog or sequence hoplog
   * @param start
   *          Exclude files that do not contain records mutated after start time
   * @param end
   *          Exclude files that do not contain records mutated before end time
   * @return All hoplog file paths belonging to the region provided
   * @throws IOException
   */
  public static Collection<FileStatus> getRegionHoplogs(Path regionPath,
      FileSystem fs, String type, long start, long end) throws IOException {
    Collection<Collection<FileStatus>> allBuckets = getBucketHoplogs(
        regionPath, fs, type, start, end);

    ArrayList<FileStatus> hoplogs = new ArrayList<FileStatus>();
    for (Collection<FileStatus> bucket : allBuckets) {
      for (FileStatus file : bucket) {
        hoplogs.add(file);
      }
    }
    return hoplogs;
  }

  public static Collection<Collection<FileStatus>> getBucketHoplogs(Path regionPath,
      FileSystem fs, String type, long start, long end) throws IOException {
    Collection<Collection<FileStatus>> allBuckets = new ArrayList<Collection<FileStatus>>();

    // hoplog files names follow this pattern
    String HOPLOG_NAME_REGEX = AbstractHoplogOrganizer.HOPLOG_NAME_REGEX + type;
    String EXPIRED_HOPLOG_NAME_REGEX = HOPLOG_NAME_REGEX + AbstractHoplogOrganizer.EXPIRED_HOPLOG_EXTENSION;
    final Pattern pattern = Pattern.compile(HOPLOG_NAME_REGEX);
    final Pattern expiredPattern = Pattern.compile(EXPIRED_HOPLOG_NAME_REGEX);
    
    Path cleanUpIntervalPath = new Path(regionPath.getParent(), HoplogConfig.CLEAN_UP_INTERVAL_FILE_NAME);
    long intervalDurationMillis = readCleanUpIntervalMillis(fs, cleanUpIntervalPath);

    // a region directory contains directories for individual buckets. A bucket
    // has a integer name.
    FileStatus[] bucketDirs = fs.listStatus(regionPath);
    
    for (FileStatus bucket : bucketDirs) {
      if (!bucket.isDirectory()) {
        continue;
      }
      try {
        Integer.valueOf(bucket.getPath().getName());
      } catch (NumberFormatException e) {
        continue;
      }

      ArrayList<FileStatus> bucketHoplogs = new ArrayList<FileStatus>();

      // identify all the flush hoplogs and seq hoplogs by visiting all the
      // bucket directories
      FileStatus[] bucketFiles = fs.listStatus(bucket.getPath());
      
      Map<String, Long> expiredHoplogs = getExpiredHoplogs(fs, bucketFiles, expiredPattern);
      
      FileStatus oldestHopAfterEndTS = null;
      long oldestHopTS = Long.MAX_VALUE;
      long currentTimeStamp = System.currentTimeMillis();
      for (FileStatus file : bucketFiles) {
        if (!file.isFile()) {
          continue;
        }

        Matcher match = pattern.matcher(file.getPath().getName());
        if (!match.matches()) {
          continue;
        }
        
        long timeStamp = AbstractHoplogOrganizer.getHoplogTimestamp(match);
        if (start > 0 && timeStamp < start) {
          // this hoplog contains records less than the start time stamp
          continue;
        }

        if (end > 0 && timeStamp > end) {
          // this hoplog contains records mutated after end time stamp. Ignore
          // this hoplog if it is not the oldest.
          if (oldestHopTS > timeStamp) {
            oldestHopTS = timeStamp;
            oldestHopAfterEndTS = file;
          }
          continue;
        }
        long expiredTimeStamp = expiredTime(file, expiredHoplogs);
        if (expiredTimeStamp > 0 && intervalDurationMillis > 0) {
          if ((currentTimeStamp - expiredTimeStamp) > 0.8 * intervalDurationMillis) {
            continue;
          }
        }
        bucketHoplogs.add(file);
      }

      if (oldestHopAfterEndTS != null) {
        long expiredTimeStamp = expiredTime(oldestHopAfterEndTS, expiredHoplogs);
        if (expiredTimeStamp <= 0 || intervalDurationMillis <=0  || 
            (currentTimeStamp - expiredTimeStamp) <= 0.8 * intervalDurationMillis) {
          bucketHoplogs.add(oldestHopAfterEndTS);
        }
      }

      if (bucketHoplogs.size() > 0) {
        allBuckets.add(bucketHoplogs);
      }
    }
    
    return allBuckets;
  }
  
  private static Map<String, Long> getExpiredHoplogs(FileSystem fs, FileStatus[] bucketFiles, 
      Pattern expiredPattern) throws IOException{
    Map<String, Long> expiredHoplogs = new HashMap<String,Long>();
    
    for(FileStatus file : bucketFiles) {
      if(!file.isFile()) {
        continue;
      }
      String fileName = file.getPath().getName();
      Matcher match = expiredPattern.matcher(fileName);
      if (!match.matches()){
        continue;
      }
      expiredHoplogs.put(fileName,file.getModificationTime());
    }
    return expiredHoplogs;
  }
  
  private static long expiredTime(FileStatus file, Map<String, Long> expiredHoplogs){
    String expiredMarkerName = file.getPath().getName() + 
        AbstractHoplogOrganizer.EXPIRED_HOPLOG_EXTENSION;
    
    long expiredTimeStamp = -1;
    if (expiredHoplogs.containsKey(expiredMarkerName)) {
      expiredTimeStamp = expiredHoplogs.get(expiredMarkerName);
    }
    return expiredTimeStamp;
  }
  
  public static long readCleanUpIntervalMillis(FileSystem fs, Path cleanUpIntervalPath) throws IOException{
    if (fs.exists(cleanUpIntervalPath)) {
      FSDataInputStream input = new FSDataInputStream(fs.open(cleanUpIntervalPath));
      long intervalDurationMillis = input.readLong();
      input.close();
      return intervalDurationMillis;
    } else {
      return -1l;
    }
  }
  
  public static void exposeCleanupIntervalMillis(FileSystem fs, Path path, long intervalDurationMillis){
    FSDataInputStream input = null;
    FSDataOutputStream output = null;
    try {
      if(fs.exists(path)){
        input = new FSDataInputStream(fs.open(path));
        if (intervalDurationMillis == input.readLong()) {
          input.close();
          return;
        }
        input.close();
        fs.delete(path, true);
      } 
      output = fs.create(path);
      output.writeLong(intervalDurationMillis);
      output.close();
    } catch (IOException e) {
      return;
    } finally {
      try {
        if (input != null){
          input.close();
        }
        if (output != null) {
          output.close();
        }
      } catch(IOException e2) {
        
      } 
    }
  }

  /**
   * @param regionPath
   * @param fs
   * @return list of latest checkpoint files of all buckets in the region
   * @throws IOException
   */
  public static Collection<FileStatus> getCheckpointFiles(Path regionPath,
      FileSystem fs) throws IOException {
    ArrayList<FileStatus> latestSnapshots = new ArrayList<FileStatus>();

    Collection<Collection<FileStatus>> allBuckets = getBucketHoplogs(
        regionPath, fs, AbstractHoplogOrganizer.MAJOR_HOPLOG_EXTENSION, 0, 0);

    // extract the latest major compacted hoplog from each bucket
    for (Collection<FileStatus> bucket : allBuckets) {
      FileStatus latestSnapshot = null;
      for (FileStatus file : bucket) {
        if (latestSnapshot == null) {
          latestSnapshot = file;
        } else {
          String name1 = latestSnapshot.getPath().getName();
          String name2 = file.getPath().getName();
          
          if (HoplogComparator.compareByName(name1, name2) > 0) {
            latestSnapshot = file;
          }
        }
      }
      
      if (latestSnapshot != null) {
        latestSnapshots.add(latestSnapshot);
      }
    }

    return latestSnapshots;
  }
  
  /**
   * Creates a mapping of hoplog to hdfs blocks on disk
   * 
   * @param files
   *          list of hoplog file status objects
   * @return array of hdfs block location objects associated with a hoplog
   * @throws IOException
   */
  public static Map<FileStatus, BlockLocation[]> getBlocks(Configuration config,
      Collection<FileStatus> files) throws IOException {
    Map<FileStatus, BlockLocation[]> blocks = new HashMap<FileStatus, BlockLocation[]>();
    if (files == null || files.isEmpty()) {
      return blocks;
    }

    FileSystem fs = files.iterator().next().getPath().getFileSystem(config);

    for (FileStatus hoplog : files) {
      long length = hoplog.getLen();
      BlockLocation[] fileBlocks = fs.getFileBlockLocations(hoplog, 0, length);
      blocks.put(hoplog, fileBlocks);
    }

    return blocks;
  }
  
  /**
   * Filters out hoplogs of a region that do not match time filters and creates
   * a list of hoplogs that may be used by hadoop jobs.
   * 
   * @param fs
   *          file system instance
   * @param path
   *          region path
   * @param start
   *          start time in milliseconds
   * @param end
   *          end time in milliseconds
   * @param snapshot
   *          if true latest snapshot hoplog will be included in the final
   *          return list
   * @return filtered collection of hoplogs
   * @throws IOException
   */
  public static Collection<FileStatus> filterHoplogs(FileSystem fs, Path path,
      long start, long end, boolean snapshot) throws IOException {
    ArrayList<FileStatus> hoplogs = new ArrayList<FileStatus>();

    // if the region contains flush hoplogs or major compacted files then the
    // region is of type RW.
    // check if the intent is to operate on major compacted files only
    if (snapshot) {
      hoplogs.addAll(getCheckpointFiles(path, fs));
    } else {
      hoplogs.addAll(getRegionHoplogs(path, fs,
          AbstractHoplogOrganizer.FLUSH_HOPLOG_EXTENSION, start, end));
    }

    if (hoplogs == null || hoplogs.isEmpty()) {
      // there are no sorted hoplogs. Check if sequence hoplogs are present
      // there is no checkpoint mode for write only tables
      hoplogs.addAll(getRegionHoplogs(path, fs,
          AbstractHoplogOrganizer.SEQ_HOPLOG_EXTENSION, start, end));
    }

    return hoplogs == null ? new ArrayList<FileStatus>() : hoplogs;
  }
  
  private HoplogUtil() {
    //static methods only.
  }
  
  /**
   * This class creates MR splits from hoplog files. This class leverages
   * CombineFileInputFormat to create locality, node and rack, aware splits
   * 
   * @author ashvina
   */
  public static class HoplogOptimizedSplitter extends CombineFileInputFormat<Long, Long> {
    private Collection<FileStatus> hoplogs;

    public HoplogOptimizedSplitter(Collection<FileStatus> hoplogs) {
      this.hoplogs = hoplogs;
    }
    
    @Override
    protected List<FileStatus> listStatus(JobContext job) throws IOException {
      /**
       * listStatus in super collects fileStatus for each file again. It also
       * tries to recursively list files in subdirectories. None of this is
       * applicable in this case. Splitter has already collected fileStatus for
       * all files. So bypassing super's method will improve performance as NN
       * chatter will be reduced. Specially helpful if NN is not colocated.
       */
      return new ArrayList<FileStatus>(hoplogs);
    }
    
    /**
     * Creates an array of splits for the input list of hoplogs. Each split is
     * roughly the size of an hdfs block. Hdfs blocks of a hoplog may be smaller
     * than hdfs block size, for e.g. if the hoplog is very small. The method
     * keeps adding hdfs blocks of a hoplog to a split till the split is less
     * than hdfs block size and the block is local to the split.
     */
    public List<InputSplit> getOptimizedSplits(Configuration conf) throws IOException {
      
      if (hoplogs == null || hoplogs.isEmpty()) {
        return null;
      }
      Path[] paths = new Path[hoplogs.size()];
      int i = 0;
      for (FileStatus file : hoplogs) {
        paths[i] = file.getPath();
        i++;
      }

      FileStatus hoplog = hoplogs.iterator().next();
      long blockSize = hoplog.getBlockSize();
      setMaxSplitSize(blockSize);

      Job job = Job.getInstance(conf);
      setInputPaths(job, paths);
      List<InputSplit> splits = super.getSplits(job);
      
      // in some cases a split may not get populated with host location
      // information. If such a split is created, fill location information of
      // the first file in the split
      ArrayList<CombineFileSplit> newSplits = new ArrayList<CombineFileSplit>();
      for (Iterator<InputSplit> iter = splits.iterator(); iter.hasNext();) {
        CombineFileSplit split = (CombineFileSplit) iter.next();
        if (split.getLocations() != null && split.getLocations().length > 0) {
          continue;
        }
        
        paths = split.getPaths();
        if (paths.length == 0) {
          continue;
        }
        long[] starts = split.getStartOffsets();
        long[] ends = split.getLengths();
        
        FileSystem fs = paths[0].getFileSystem(conf);
        FileStatus file = fs.getFileStatus(paths[0]);
        BlockLocation[] blks = fs.getFileBlockLocations(file, starts[0], ends[0]);
        if (blks != null && blks.length > 0) {
          // hosts found. Need to create a new split and replace the one missing
          // hosts.
          iter.remove();
          String hosts[] = blks[0].getHosts();
          split = new CombineFileSplit(paths, starts, ends, hosts);
          newSplits.add(split);
        }
      }
      splits.addAll(newSplits);
      
      return splits;
    }
    
    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
      // a call to this method is invalid. This class is only meant to create
      // optimized splits independent of the api type
      throw new IllegalStateException();
    }

    @Override
    public RecordReader<Long, Long> createRecordReader(InputSplit split,
        TaskAttemptContext arg1) throws IOException {
      // Record reader creation is managed by GFInputFormat. This method should
      // not be called
      throw new IllegalStateException();
    }
  }
}
