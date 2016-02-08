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
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;

import com.gemstone.gemfire.cache.hdfs.HDFSIOException;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreImpl;
import com.gemstone.gemfire.cache.hdfs.internal.cardinality.ICardinality;
import com.gemstone.gemfire.cache.hdfs.internal.org.apache.hadoop.io.SequenceFile;
import com.gemstone.gemfire.cache.hdfs.internal.org.apache.hadoop.io.SequenceFile.CompressionType;
import com.gemstone.gemfire.cache.hdfs.internal.org.apache.hadoop.io.SequenceFile.Writer.Option;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplogStatistics;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import org.apache.hadoop.hbase.util.FSUtils;

import org.apache.logging.log4j.Logger;

/**
 * Abstract class for {@link Hoplog} with common functionality
 */
public abstract class AbstractHoplog implements Hoplog {
  protected final FSProvider fsProvider;
  
  // path of the oplog file
  protected volatile Path path;
  private volatile HoplogDescriptor hfd;
  protected Configuration conf;
  protected SortedOplogStatistics stats;
  protected Long hoplogModificationTime;
  protected Long hoplogSize;

  protected HoplogReaderActivityListener readerListener;
  
  // logger instance
  protected static final Logger logger = LogService.getLogger();
  
  protected static String logPrefix;
  // THIS CONSTRUCTOR SHOULD BE USED FOR LONER ONLY
  AbstractHoplog(FileSystem inputFS, Path filePath, SortedOplogStatistics stats)
      throws IOException {
    logPrefix = "<" + filePath.getName() + "> ";
    this.fsProvider = new FSProvider(inputFS);
    initialize(filePath, stats, inputFS);
  }

  public AbstractHoplog(HDFSStoreImpl store, Path filePath,
      SortedOplogStatistics stats) throws IOException {
    logPrefix = "<" + filePath.getName() + "> ";
    this.fsProvider = new FSProvider(store);
    initialize(filePath, stats, store.getFileSystem());
  }

  private void initialize(Path path, SortedOplogStatistics stats, FileSystem fs) {
    this.conf = fs.getConf();
    this.stats = stats;
    this.path = fs.makeQualified(path);
    this.hfd = new HoplogDescriptor(this.path.getName());
  }
  
  @Override
  public abstract void close() throws IOException; 
  @Override
  public abstract HoplogReader getReader() throws IOException;

  @Override
  public abstract HoplogWriter createWriter(int keys) throws IOException;

  @Override
  abstract public void close(boolean clearCache) throws IOException;

  @Override
  public void setReaderActivityListener(HoplogReaderActivityListener listener) {
    this.readerListener = listener;
  }
  
  @Override
  public String getFileName() {
    return this.hfd.getFileName();
  }
  
  public final int compareTo(Hoplog o) {
    return hfd.compareTo( ((AbstractHoplog)o).hfd);
  }

  @Override
  public ICardinality getEntryCountEstimate() throws IOException {
    return null;
  }
  
  @Override
  public synchronized void rename(String name) throws IOException {
    if (logger.isDebugEnabled())
      logger.debug("{}Renaming hoplog to " + name, logPrefix);
    Path parent = path.getParent();
    Path newPath = new Path(parent, name);
    fsProvider.getFS().rename(path, new Path(parent, newPath));

    // close the old reader and let the new one get created lazily
    close();
    
    // update path to point to the new path
    path = newPath;
    this.hfd = new HoplogDescriptor(this.path.getName());
    logPrefix = "<" + path.getName() + "> ";
  }
  
  @Override
  public synchronized void delete() throws IOException {
    if (logger.isDebugEnabled())
      logger.debug("{}Deleting hoplog", logPrefix);
    close();
    this.hoplogModificationTime = null;
    this.hoplogSize = null;
    fsProvider.getFS().delete(path, false);
  }

  @Override
  public long getModificationTimeStamp() {
    initHoplogSizeTimeInfo();

    // modification time will not be null if this hoplog is existing. Otherwise
    // invocation of this method should is invalid
    if (hoplogModificationTime == null) {
      throw new IllegalStateException();
    }
    
    return hoplogModificationTime;
  }

  @Override
  public long getSize() {
    initHoplogSizeTimeInfo();
    
    // size will not be null if this hoplog is existing. Otherwise
    // invocation of this method should is invalid
    if (hoplogSize == null) {
      throw new IllegalStateException();
    }
    
    return hoplogSize;
  }
  
  private synchronized void initHoplogSizeTimeInfo() {
    if (hoplogSize != null && hoplogModificationTime != null) {
      // time and size info is already initialized. no work needed here
      return;
    }

    try {
      FileStatus[] filesInfo = FSUtils.listStatus(fsProvider.getFS(), path, null);
      if (filesInfo != null && filesInfo.length == 1) {
        this.hoplogModificationTime = filesInfo[0].getModificationTime();
        this.hoplogSize = filesInfo[0].getLen();
      }
      // TODO else condition may happen if user deletes hoplog from the file system.
    } catch (IOException e) {
      logger.error(LocalizedMessage.create(LocalizedStrings.HOPLOG_FAILED_TO_READ_HDFS_FILE, path), e);
      throw new HDFSIOException(
          LocalizedStrings.HOPLOG_FAILED_TO_READ_HDFS_FILE.toLocalizedString(path),e);
    }
  }
  public static SequenceFile.Writer getSequenceFileWriter(Path path, 
      Configuration conf, Logger logger) throws IOException {
    return getSequenceFileWriter(path,conf, logger, null); 
  }
  
  /**
   * 
   * @param path
   * @param conf
   * @param logger
   * @param version - is being used only for testing. Should be passed as null for other purposes. 
   * @return SequenceFile.Writer 
   * @throws IOException
   */
  public static SequenceFile.Writer getSequenceFileWriter(Path path, 
    Configuration conf, Logger logger, Version version) throws IOException {
    Option optPath = SequenceFile.Writer.file(path);
    Option optKey = SequenceFile.Writer.keyClass(BytesWritable.class);
    Option optVal = SequenceFile.Writer.valueClass(BytesWritable.class);
    Option optCom = withCompression(logger);
    if (logger.isDebugEnabled())
      logger.debug("{}Started creating hoplog " + path, logPrefix);
    
    if (version == null)
      version = Version.CURRENT;
    //Create a metadata option with the gemfire version, for future versioning
    //of the key and value format
    SequenceFile.Metadata metadata = new SequenceFile.Metadata();
    metadata.set(new Text(Meta.GEMFIRE_VERSION.name()), new Text(String.valueOf(version.ordinal())));
    Option optMeta = SequenceFile.Writer.metadata(metadata);
    
    SequenceFile.Writer writer = SequenceFile.createWriter(conf, optPath, optKey, optVal, optCom, optMeta);
    
    return writer;
  }
  
  private static Option withCompression(Logger logger) {
    String prop = System.getProperty(HoplogConfig.COMPRESSION);
    if (prop != null) {
      CompressionCodec codec;
      if (prop.equalsIgnoreCase("SNAPPY")) {
        codec = new SnappyCodec();
      } else if (prop.equalsIgnoreCase("LZ4")) {
        codec = new Lz4Codec();
      } else if (prop.equals("GZ")) {
        codec = new GzipCodec();
      } else {
        throw new IllegalStateException("Unsupported codec: " + prop);
      }
      if (logger.isDebugEnabled())
        logger.debug("{}Using compression codec " + codec, logPrefix);
      return SequenceFile.Writer.compression(CompressionType.BLOCK, codec);
    }
    return SequenceFile.Writer.compression(CompressionType.NONE, null);
  }
  
  public static final class HoplogDescriptor implements Comparable<HoplogDescriptor> {
     private final String fileName;
     private final String bucket;
     private final int sequence;
     private final long timestamp;
     private final String extension;
     
     HoplogDescriptor(final String fileName) {
       this.fileName = fileName;
       final Matcher matcher = AbstractHoplogOrganizer.HOPLOG_NAME_PATTERN.matcher(fileName);
       final boolean matched = matcher.find();
       assert matched;
       this.bucket = matcher.group(1);
       this.sequence = Integer.valueOf(matcher.group(3));
       this.timestamp = Long.valueOf(matcher.group(2)); 
       this.extension = matcher.group(4);
     }
     
     public final String getFileName() {
       return fileName;
     }
     
     @Override
     public boolean equals(Object o) {
       if (this == o) {
         return true;
       }
       
       if (!(o instanceof HoplogDescriptor)) {
         return false;
       }
       
       final HoplogDescriptor other = (HoplogDescriptor)o;
       // the two files should belong to same bucket
       assert this.bucket.equals(other.bucket);
       
       // compare sequence first
       if (this.sequence != other.sequence) {
         return false;
       }
       
       // sequence is same, compare timestamps
       if (this.timestamp != other.timestamp) {
         return false;
       }
       
       return extension.equals(other.extension);
     }

    @Override
    public int compareTo(HoplogDescriptor o) {
      if (this == o) {
        return 0;
      }
      
      // the two files should belong to same bucket
      assert this.bucket.equals(o.bucket);
      
      // compare sequence first
      if (sequence > o.sequence) {
        return -1;
      } else if (sequence < o.sequence) {
        return 1;
      }
      
      // sequence is same, compare timestamps
      if(timestamp > o.timestamp) {
        return -1; 
      } else if (timestamp < o.timestamp) {
        return 1;
      }
      
      //timestamp is the same, compare the file extension. It's
      //possible a major compaction and minor compaction could finish
      //at the same time and create the same timestamp and sequence number
      //it doesn't matter which file we look at first in that case.
      return extension.compareTo(o.extension);
    }
     
     
  }
  
  protected static final class FSProvider {
    final FileSystem fs;
    final HDFSStoreImpl store;
    
    // THIS METHOD IS FOR TESTING ONLY
    FSProvider(FileSystem fs) {
      this.fs = fs;
      this.store = null;
    }
    
    FSProvider(HDFSStoreImpl store) {
      this.store = store;
      fs = null;
    }
    
    public FileSystem getFS() throws IOException {
      if (store != null) {
        return store.getFileSystem();
      }
      return fs;
    }

    public FileSystem checkFileSystem() {
      store.checkAndClearFileSystem();
      return store.getCachedFileSystem();
    }
  }
}
