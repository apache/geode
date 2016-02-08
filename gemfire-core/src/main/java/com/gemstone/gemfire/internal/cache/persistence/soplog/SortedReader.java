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
package com.gemstone.gemfire.internal.cache.persistence.soplog;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import org.apache.hadoop.io.RawComparator;

/**
 * Defines a means to read sorted data including performing range scans.
 * 
 * @param <V> type of value returned by the sorted reader
 * 
 * @author bakera
 */
public interface SortedReader<V> extends Closeable {
  /**
   * Defines the names of additional data that may be associated with a sorted
   * reader.
   */
  public enum Metadata {
    /** identifies the disk store associated with the soplog, optional */
    DISK_STORE,
    
    /** identifies the RVV data, optional */
    RVV;

    /**
     * Converts the metadata name to bytes.
     * @return the bytes
     */
    public byte[] bytes() {
      return ("gemfire." + name()).getBytes();
    }
  }
  
  /**
   * Filters data based on metadata values.
   */
  public interface MetadataFilter {
    /**
     * Returns the name this filter acts upon.
     * @return the name
     */
    Metadata getName();
    
    /**
     * Returns true if the metadata value passes the filter.
     * @param value the value to check; may be null if the metadata value does
     *              not exist or has not been assigned yet
     * @return true if accepted
     */
    boolean accept(byte[] value);
  }
  
  /**
   * Allows comparisons between serialized objects.
   */
  public interface SerializedComparator extends RawComparator<byte[]> {
  }
  
  /**
   * Allows sorted iteration through a set of keys and values.
   */
  public interface SortedIterator<V> extends KeyValueIterator<ByteBuffer, V> {
    /**
     * Closes the iterator and frees any retained resources.
     */
    public abstract void close();
  }

  /**
   * Defines the statistics available on a sorted file.
   */
  public interface SortedStatistics {
    /**
     * Returns the number of keys in the file.
     * @return the key count
     */
    long keyCount();
    
    /**
     * Returns the first key in the file.
     * @return the first key
     */
    byte[] firstKey();
    
    /**
     * Returns the last key in the file.
     * @return the last key
     */
    byte[] lastKey();
    
    /**
     * Returns the average key size in bytes.
     * @return the average key size
     */
    double avgKeySize();
    
    /**
     * Returns the average value size in bytes.
     * @return the average value size
     */
    double avgValueSize();
    
    /**
     * Frees any resources held by for statistics generation.
     */
    void close();
  }
  
  /**
   * Returns true if the bloom filter might contain the supplied key.  The 
   * nature of the bloom filter is such that false positives are allowed, but
   * false negatives cannot occur.
   * 
   * @param key the key to test
   * @return true if the key might be present
   * @throws IOException read error
   */
  boolean mightContain(byte[] key) throws IOException;

  /**
   * Returns the value associated with the given key.
   * 
   * @param key the key
   * @return the value, or null if the key is not found
   * @throws IOException read error
   */
  V read(byte[] key) throws IOException;

  /**
   * Iterates from the first key in the file to the requested key.
   * @param to the ending key
   * @param inclusive true if the ending key is included in the iteration
   * @return the sorted iterator
   * @throws IOException scan error
   */
  SortedIterator<V> head(byte[] to, boolean inclusive) throws IOException;
  
  /**
   * Iterates from the requested key to the last key in the file.
   * @param from the starting key
   * @param inclusive true if the starting key should be included in the iteration
   * @return the sorted iterator
   * @throws IOException scan error
   */
  SortedIterator<V> tail(byte[] from, boolean inclusive) throws IOException;

  /**
   * Iterators over the entire contents of the sorted file.
   * 
   * @return the sorted iterator
   * @throws IOException scan error
   */
  SortedIterator<V> scan() throws IOException;
  
  /**
   * Scans the available keys and allows iteration over the interval [from, to) 
   * where the starting key is included and the ending key is excluded from 
   * the results.
   * 
   * @param from the start key
   * @param to the end key
   * @return the sorted iterator
   * @throws IOException scan error
   */
  SortedIterator<V> scan(byte[] from, byte[] to) throws IOException;

  /**
   * Scans the keys and returns an iterator over the interval [equalTo, equalTo].
   * 
   * @param equalTo the key to match
   * @return the sorted iterator
   * @throws IOException scan error
   */
  SortedIterator<V> scan(byte[] equalTo) throws IOException;
  
  /**
   * Scans the keys and allows iteration between the given keys.
   * 
   * @param from the start key
   * @param fromInclusive true if the start key is included in the scan
   * @param to the end key
   * @param toInclusive true if the end key is included in the scan
   * @return the sorted iterator
   * @throws IOException scan error
   */
  SortedIterator<V> scan(byte[] from, boolean fromInclusive, 
      byte[] to, boolean toInclusive) throws IOException;

  /**
   * Scans the keys and allows iteration between the given keys after applying
   * the metdata filter and the order flag.  These parameters override values
   * configured using <code>withAscending</code> or <code>withFilter</code>.
   * 
   * @param from the start key
   * @param fromInclusive true if the start key is included in the scan
   * @param to the end key
   * @param toInclusive true if the end key is included in the scan
   * @param ascending true if ascending
   * @param filter filters data based on metadata values
   * @return the sorted iterator
   * @throws IOException scan error
   */
  SortedIterator<V> scan(
      byte[] from, boolean fromInclusive, 
      byte[] to, boolean toInclusive,
      boolean ascending,
      MetadataFilter filter) throws IOException;

  /**
   * Changes the iteration order of subsequent operations.
   * 
   * @param ascending true if ascending order (default)
   * @return the reader
   */
  SortedReader<V> withAscending(boolean ascending);
  
  /**
   * Applies a metadata filter to subsequent operations.
   * 
   * @param filter the filter to apply
   * @return the reader
   */
  SortedReader<V> withFilter(MetadataFilter filter);
  
  /**
   * Returns the comparator used for sorting keys.
   * @return the comparator
   */
  SerializedComparator getComparator();
  
  /**
   * Returns the statistics regarding the keys present in the sorted file.
   * @return the statistics
   * @throws IOException unable retrieve statistics
   */
  SortedStatistics getStatistics() throws IOException;
}
