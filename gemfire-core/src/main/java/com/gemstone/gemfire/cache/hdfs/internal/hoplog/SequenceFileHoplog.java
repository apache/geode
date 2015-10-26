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
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;

import com.gemstone.gemfire.cache.hdfs.HDFSIOException;
import com.gemstone.gemfire.cache.hdfs.internal.cardinality.ICardinality;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HoplogSetReader.HoplogIterator;
import com.gemstone.gemfire.cache.hdfs.internal.org.apache.hadoop.io.SequenceFile;
import com.gemstone.gemfire.cache.hdfs.internal.org.apache.hadoop.io.SequenceFile.Reader;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplogStatistics;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.Version;

import org.apache.logging.log4j.Logger;

/**
 * Implements Sequence file based {@link Hoplog}
 * 
 * @author hemantb
 *
 */
public class SequenceFileHoplog extends AbstractHoplog{
  
   public SequenceFileHoplog(FileSystem inputFS, Path filePath,  
      SortedOplogStatistics stats)
  throws IOException
  {
     super(inputFS, filePath, stats);
  }
  @Override
  public void close() throws IOException {
    // Nothing to do 
  }

  @Override
  public HoplogReader getReader() throws IOException {
    return new SequenceFileReader();
  }

  @Override
  /**
   * gets the writer for sequence file. 
   * 
   * @param keys is not used for SequenceFileHoplog class 
   */
  public HoplogWriter createWriter(int keys) throws IOException {
    return new SequenceFileHoplogWriter();
  }

  @Override
  public boolean isClosed() {
    return false;
  }
  
  @Override
  public void close(boolean clearCache) throws IOException {
    // Nothing to do 
  }

  /**
   * Currently, hsync does not update the file size on namenode. So, if last time the 
   * process died after calling hsync but before calling file close, the file is 
   * left with an inconsistent file size. This is a workaround that - open the file stream in append 
   * mode and close it. This fixes the file size on the namenode.
   * 
   * @throws IOException
   * @return true if the file size was fixed 
   */
  public boolean fixFileSize() throws IOException {
    // Try to fix the file size
    // Loop so that the expected expceptions can be ignored 3
   // times
    if (logger.isDebugEnabled())
      logger.debug("{}Fixing size of hoplog " + path, logPrefix);
    Exception e = null;
    boolean exceptionThrown = false;
    for (int i =0; i < 3; i++) {
      try {
        FSDataOutputStream stream = fsProvider.getFS().append(path);
        stream.close();
        stream = null;
      } catch (IOException ie) {
        exceptionThrown = true;
        e = ie;
        if (logger.isDebugEnabled())
        logger.debug("{}Retry run " + (i + 1) + ": Hoplog " + path + " is still a temporary " +
            "hoplog because the node managing it wasn't shutdown properly last time. Failed to " +
            "fix the hoplog because an exception was thrown " + e, logPrefix );
      }
      // As either RecoveryInProgressException was thrown or 
      // Already being created exception was thrown, wait for 
      // sometime before next retry. 
      if (exceptionThrown) {
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e1) {
        } 
        exceptionThrown = false;
      } else {
        // no exception was thrown, break;
        return true;
      }
    }
    logger.info (logPrefix, LocalizedMessage.create(LocalizedStrings.DEBUG, "Hoplog " + path + " is still a temporary " +
        "hoplog because the node managing it wasn't shutdown properly last time. Failed to " +
        "fix the hoplog because an exception was thrown " + e));
    
    return false;
  }
  
  @Override
  public String toString() {
    return "SequenceFileHplog[" + getFileName() + "]";
  }
  
  private class SequenceFileHoplogWriter implements HoplogWriter {
    
    private SequenceFile.Writer writer = null;
    
    public SequenceFileHoplogWriter() throws IOException{
      writer = AbstractHoplog.getSequenceFileWriter(path, conf, logger);
    }
   
    @Override
    public void close() throws IOException {
      writer.close();
      if (logger.isDebugEnabled())
        logger.debug("{}Completed creating hoplog " + path, logPrefix);
    }
    
    @Override
    public void hsync() throws IOException {
      writer.hsyncWithSizeUpdate();
      if (logger.isDebugEnabled())
        logger.debug("{}hsync'ed a batch of data to hoplog " + path, logPrefix);
    }
    
    @Override
    public void append(byte[] key, byte[] value) throws IOException {
      writer.append(new BytesWritable(key), new BytesWritable(value));
    }

    @Override
    public void append(ByteBuffer key, ByteBuffer value) throws IOException {
      throw new UnsupportedOperationException("Not supported for Sequence files");
    }

    @Override
    public void close(EnumMap<Meta, byte[]> metadata) throws IOException {
      throw new UnsupportedOperationException("Not supported for Sequence files");
    }
    @Override
    public long getCurrentSize() throws IOException {
      return writer.getLength();
    }
    
  }
  /**
   * Sequence file reader. This is currently to be used only by MapReduce jobs and 
   * test functions
   * 
   */
  public class SequenceFileReader implements HoplogReader, Closeable {
    @Override
    public byte[] read(byte[] key) throws IOException {
      throw new UnsupportedOperationException("Not supported for Sequence files");
    }

    @Override
    public HoplogIterator<byte[], byte[]> scan()
        throws IOException {
      return  new SequenceFileIterator(fsProvider.getFS(), path, 0, Long.MAX_VALUE, conf, logger);
    }

    @Override
    public HoplogIterator<byte[], byte[]> scan(
        byte[] from, byte[] to) throws IOException {
      throw new UnsupportedOperationException("Not supported for Sequence files");
    }
    
    @Override
    public HoplogIterator<byte[], byte[]> scan(
        long startOffset, long length) throws IOException {
      return  new SequenceFileIterator(fsProvider.getFS(), path, startOffset, length, conf, logger);
    }
    
    @Override
    public HoplogIterator<byte[], byte[]> scan(
        byte[] from, boolean fromInclusive, byte[] to, boolean toInclusive)
        throws IOException {
      throw new UnsupportedOperationException("Not supported for Sequence files");
    }

    @Override
    public boolean isClosed() {
      throw new UnsupportedOperationException("Not supported for Sequence files.");
    }
    
    @Override
    public void close() throws IOException {
      throw new UnsupportedOperationException("Not supported for Sequence files. Close the iterator instead.");
    }

    @Override
    public ByteBuffer get(byte[] key) throws IOException {
      throw new UnsupportedOperationException("Not supported for Sequence files");
    }

    @Override
    public BloomFilter getBloomFilter() throws IOException {
      throw new UnsupportedOperationException("Not supported for Sequence files");
    }

    @Override
    public long getEntryCount() {
      throw new UnsupportedOperationException("Not supported for Sequence files");
    }

    @Override
    public ICardinality getCardinalityEstimator() {
      throw new UnsupportedOperationException("Not supported for Sequence files");
    }

    @Override
    public long sizeEstimate() {
      throw new UnsupportedOperationException("Not supported for Sequence files");
    }


  }
  
  /**
   * Sequence file iterator. This is currently to be used only by MapReduce jobs and 
   * test functions
   * 
   */
  public static class SequenceFileIterator implements HoplogIterator<byte[], byte[]> {
    
    SequenceFile.Reader reader = null;
    private BytesWritable prefetchedKey = null;
    private BytesWritable prefetchedValue = null;
    private byte[] currentKey;
    private byte[] currentValue;
    boolean hasNext = false;
    Logger logger; 
    Path path;
    private long start;
    private long end;
    
    public SequenceFileIterator(FileSystem fs, Path path, long startOffset, 
        long length, Configuration conf, Logger logger) 
        throws IOException {
      Reader.Option optPath = SequenceFile.Reader.file(path);
      
      // Hadoop has a configuration parameter io.serializations that is a list of serialization 
      // classes which can be used for obtaining serializers and deserializers. This parameter 
      // by default contains avro classes. When a sequence file is created, it calls 
      // SerializationFactory.getSerializer(keyclass). This internally creates objects using 
      // reflection of all the classes that were part of io.serializations. But since, there is 
      // no avro class available it throws an exception. 
      // Before creating a sequenceFile, override the io.serializations parameter and pass only the classes 
      // that are important to us. 
      String serializations[] = conf.getStrings("io.serializations",
          new String[]{"org.apache.hadoop.io.serializer.WritableSerialization"});
      conf.setStrings("io.serializations",
          new String[]{"org.apache.hadoop.io.serializer.WritableSerialization"});
      // create reader
      boolean emptyFile = false;
      try {
        reader = new SequenceFile.Reader(conf, optPath);
      }catch (EOFException e) {
        // this is ok as the file has ended. just return false that no more records available
        emptyFile = true;
      }
      // reset the configuration to its original value 
      conf.setStrings("io.serializations", serializations);
      this.logger = logger;
      this.path = path;
      
      if (emptyFile) {
        hasNext = false;
      } else {
        // The file should be read from the first sync marker after the start position and 
        // until the first sync marker after the end position is seen. 
        this.end = startOffset + length;
        if (startOffset > reader.getPosition()) {
          reader.sync(startOffset);                  // sync to start
        }
        this.start = reader.getPosition();
        this.hasNext = this.start < this.end;
        if (hasNext)
          readNext();
      } 
    }
  

    public Version getVersion(){
      String version = reader.getMetadata().get(new Text(Meta.GEMFIRE_VERSION.name())).toString();
      return Version.fromOrdinalOrCurrent(Short.parseShort(version)); 
    }
    @Override
    public boolean hasNext() {
      return hasNext;
    }

    @Override
    public byte[] next() {
      currentKey = prefetchedKey.getBytes();
      currentValue = prefetchedValue.getBytes();
      
      readNext();

      return currentKey;
    }
    
    private void readNext() {
      try {
        long pos = reader.getPosition();
        prefetchedKey = new BytesWritable();
        prefetchedValue = new BytesWritable();
        hasNext = reader.next(prefetchedKey, prefetchedValue);
        // The file should be read from the first sync marker after the start position and 
        // until the first sync marker after the end position is seen. 
        if (pos >= end && reader.syncSeen()) {
          hasNext = false;
        }
      } catch (EOFException e) {
        // this is ok as the file has ended. just return false that no more records available
        hasNext = false;
      } 
      catch (IOException e) {
        hasNext = false;
        logger.error(LocalizedMessage.create(LocalizedStrings.HOPLOG_FAILED_TO_READ_HDFS_FILE, path), e);
        throw new HDFSIOException(
            LocalizedStrings.HOPLOG_FAILED_TO_READ_HDFS_FILE.toLocalizedString(path), e);
      }
    }
    @Override
    public void remove() {
      throw new UnsupportedOperationException("Not supported for Sequence files");
    }

    @Override
    public void close() {
      IOUtils.closeStream(reader);
    }

    @Override
    public byte[] getKey() {
      return currentKey;
    }

    @Override
    public byte[] getValue() {
      return currentValue;
    }
    
    /** Returns true iff the previous call to next passed a sync mark.*/
    public boolean syncSeen() { return reader.syncSeen(); }

    /** Return the current byte position in the input file. */
    public synchronized long getPosition() throws IOException {
      return reader.getPosition();
    }
  }
}
