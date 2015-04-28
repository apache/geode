/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence.soplog;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumMap;

import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedReader.Metadata;

/**
 * Defines the API for reading and writing sorted key/value pairs.  The keys
 * are expected to be lexicographically comparable {@code byte[]} arrays.
 * 
 * @author bakera
 */
public interface SortedOplog {
  /**
   * Checks if a key may be present in a set.
   */
  public interface BloomFilter {
    /**
     * Returns true if the bloom filter might contain the supplied key.  The 
     * nature of the bloom filter is such that false positives are allowed, but
     * false negatives cannot occur.
     * 
     * @param key the key to test
     * @return true if the key might be present
     */
    boolean mightContain(byte[] key);
  }
  
  /**
   * Reads key/value pairs from the sorted file.
   */
  public interface SortedOplogReader extends SortedReader<ByteBuffer> {
    /**
     * Returns the bloom filter associated with this reader.
     * @return the bloom filter
     */
    BloomFilter getBloomFilter();
    
    /**
     * Returns the metadata value for the given key.
     * 
     * @param name the metadata name
     * @return the requested metadata
     * @throws IOException error reading metadata
     */
    byte[] getMetadata(Metadata name) throws IOException;
    
    /**
     * Returns the file used to persist the soplog contents.
     * @return the file
     */
    File getFile();
    
    /**
     * @return file name
     */
    String getFileName();
    
    /**
     * renames the file to the input name
     * 
     * @throws IOException
     */
    void rename(String name) throws IOException;
    
    /**
     * @return the modification timestamp of the file
     * @throws IOException 
     */
    long getModificationTimeStamp() throws IOException;
    
    /**
     * Deletes the sorted oplog file
     */
    public void delete() throws IOException;

    /**
     * Returns true if the reader is closed.
     * @return true if closed
     */
    boolean isClosed();
  }
  
  /**
   * Writes key/value pairs in a sorted manner.  Each entry that is appended
   * must have a key that is greater than or equal to the previous key.
   */
  public interface SortedOplogWriter {
    /**
     * Appends another key and value.  The key is expected to be greater than
     * or equal to the last key that was appended.
     * 
     * @param key the key
     * @param value the value
     * @throws IOException write error
     */
    void append(ByteBuffer key, ByteBuffer value) throws IOException;

    /**
     * Appends another key and value.  The key is expected to be greater than
     * or equal to the last key that was appended.
     * 
     * @param key the key
     * @param value the value
     * @throws IOException write error
     */
    void append(byte[] key, byte[] value) throws IOException;

    /**
     * Closes the file, first writing optional user and system metadata. 
     * 
     * @param metadata the metadata to include
     * @throws IOException unable to close file
     */
    void close(EnumMap<Metadata, byte[]> metadata) throws IOException;
    
    /**
     * Invoked to close and remove the file to clean up after an error.
     * @throws IOException error closing
     */
    void closeAndDelete() throws IOException;
  }
  
  /**
   * Creates a new sorted reader.
   * 
   * @return the reader
   * @throws IOException error creating reader
   */
  SortedOplogReader createReader() throws IOException;
  
  /**
   * Creates a new sorted writer.
   * 
   * @return the writer
   * @throws IOException error creating writer
   */
  SortedOplogWriter createWriter() throws IOException;
}
