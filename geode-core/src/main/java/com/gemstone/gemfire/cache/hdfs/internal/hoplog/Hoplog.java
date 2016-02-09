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
import java.nio.ByteBuffer;
import java.util.EnumMap;

import com.gemstone.gemfire.cache.hdfs.internal.cardinality.ICardinality;

/**
 * Ordered sequence file
 */
public interface Hoplog extends Closeable, Comparable<Hoplog>  {
  public static final boolean NOP_WRITE = Boolean.getBoolean("Hoplog.NOP_WRITE");
  
  /** the gemfire magic number for sorted oplogs */
  public static final byte[] MAGIC = new byte[] { 0x47, 0x53, 0x4F, 0x50 };

  /**
   * @return an instance of cached reader, creates one if does not exist
   * @throws IOException
   */
  HoplogReader getReader() throws IOException;

  /**
   * Creates a new sorted writer.
   * 
   * @param keys
   *          an estimate of the number of keys to be written
   * @return the writer
   * @throws IOException
   *           error creating writer
   */
  HoplogWriter createWriter(int keys) throws IOException;

  /**
   * @param listener listener of reader's activity
   */
  void setReaderActivityListener(HoplogReaderActivityListener listener);
  
  /**
   * @return file name
   */
  String getFileName();

  /**
   * @return Entry count estimate for this hoplog
   */
  public ICardinality getEntryCountEstimate() throws IOException;

  /**
   * renames the file to the input name
   * 
   * @throws IOException
   */
  void rename(String name) throws IOException;

  /**
   * Deletes the sorted oplog file
   */
  void delete() throws IOException;
  
  /**
   * Returns true if the hoplog is closed for reads.
   * @return true if closed
   */
  boolean isClosed();
  
  /**
   * @param clearCache clear this sorted oplog's cache if true
   * @throws IOException 
   */
  void close(boolean clearCache) throws IOException;
  
  /**
   * @return the modification timestamp of the file
   */
  long getModificationTimeStamp();
  
  /**
   * @return the size of file
   */
  long getSize();

  /**
   * Reads sorted oplog file.
   */
  public interface HoplogReader extends HoplogSetReader<byte[], byte[]> {
    /**
     * Returns a byte buffer based view of the value linked to the key
     */
    ByteBuffer get(byte[] key) throws IOException;

    /**
     * @return Returns the bloom filter associated with this sorted oplog file.
     */
    BloomFilter getBloomFilter() throws IOException;

    /**
     * @return number of KV pairs in the file, including tombstone entries
     */
    long getEntryCount();

    /**
     * Returns the {@link ICardinality} implementation that is useful for
     * estimating the size of this Hoplog.
     * 
     * @return the cardinality estimator
     */
    ICardinality getCardinalityEstimator();
  }

  /**
   * Provides hoplog's reader's activity related events to owners
   * 
   * @author ashvina
   */
  public interface HoplogReaderActivityListener {
    /**
     * Invoked when a reader is created and an active reader did not exist
     * earlier
     */
    public void readerCreated();
    
    /**
     * Invoked when an active reader is closed
     */
    public void readerClosed();
  }

  /**
   * Writes key/value pairs in a sorted oplog file. Each entry that is appended must have a key that
   * is greater than or equal to the previous key.
   */
  public interface HoplogWriter extends Closeable {
    /**
     * Appends another key and value. The key is expected to be greater than or equal to the last
     * key that was appended.
     * @param key
     * @param value
     */
    void append(byte[] key, byte[] value) throws IOException;

    /**
     * Appends another key and value. The key is expected to be greater than or equal to the last
     * key that was appended.
     */
    void append(ByteBuffer key, ByteBuffer value) throws IOException;

    void close(EnumMap<Meta, byte[]> metadata) throws IOException;
    
    /**
     * flushes all outstanding data into the OS buffers on all DN replicas 
     * @throws IOException
     */
    void hsync() throws IOException;
    
    /**
     * Gets the size of the data that has already been written
     * to the writer.  
     * 
     * @return number of bytes already written to the writer
     */
    public long getCurrentSize() throws IOException; 
  }

  /**
   * Identifies the gemfire sorted oplog versions.
   */
  public enum HoplogVersion {
    V1;

    /**
     * Returns the version string as bytes.
     * 
     * @return the byte form
     */
    public byte[] toBytes() {
      return name().getBytes();
    }

    /**
     * Constructs the version from a byte array.
     * 
     * @param version
     *          the byte form of the version
     * @return the version enum
     */
    public static HoplogVersion fromBytes(byte[] version) {
      return HoplogVersion.valueOf(new String(version));
    }
  }

  /**
   * Names the available metadata keys that will be stored in the sorted oplog.
   */
  public enum Meta {
    /** identifies the soplog as a gemfire file, required */
    GEMFIRE_MAGIC,

    /** identifies the soplog version, required */
    SORTED_OPLOG_VERSION,
    
    /** identifies the gemfire version the soplog was created with */
    GEMFIRE_VERSION,

    /** identifies the statistics data */
    STATISTICS,

    /** identifies the embedded comparator types */
    COMPARATORS,
    
    /** identifies the pdx type data, optional */
    PDX,

    /**
     * identifies the hyperLogLog byte[] which estimates the cardinality for
     * only one hoplog
     */
    LOCAL_CARDINALITY_ESTIMATE,

    /**
     * represents the hyperLogLog byte[] after upgrading the constant from
     * 0.1 to 0.03 (in gfxd 1.4)
     */
    LOCAL_CARDINALITY_ESTIMATE_V2
    ;

    /**
     * Converts the metadata name to bytes.
     */
    public byte[] toBytes() {
      return ("gemfire." + name()).getBytes();
    }

    /**
     * Converts the byte form of the name to an enum.
     * 
     * @param key
     *          the key as bytes
     * @return the enum form
     */
    public static Meta fromBytes(byte[] key) {
      return Meta.valueOf(new String(key).substring("gemfire.".length()));
    }
  }
}
