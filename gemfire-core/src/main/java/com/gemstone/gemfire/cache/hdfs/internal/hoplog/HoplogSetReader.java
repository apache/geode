/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.hdfs.internal.hoplog;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

/**
 * Reads a sorted oplog file or a merged set of sorted oplogs.
 */
public interface HoplogSetReader<K, V> {
  /**
   * Returns the value associated with the given key.
   */
  V read(K key) throws IOException;

  /**
   * Iterators over the entire contents of the sorted file.
   * 
   * @return the sorted iterator
   * @throws IOException
   */
  HoplogIterator<K, V> scan() throws IOException;

  /**
   * Scans the available keys and allows iteration over the interval [from, to) where the starting
   * key is included and the ending key is excluded from the results.
   * 
   * @param from
   *          the start key
   * @param to
   *          the end key
   * @return the sorted iterator
   * @throws IOException
   */
  HoplogIterator<K, V> scan(K from, K to) throws IOException;

  /**
   * Scans the keys and allows iteration between the given keys.
   * 
   * @param from
   *          the start key
   * @param fromInclusive
   *          true if the start key is included in the scan
   * @param to
   *          the end key
   * @param toInclusive
   *          true if the end key is included in the scan
   * @return the sorted iterator
   * @throws IOException
   */
  HoplogIterator<K, V> scan(K from, boolean fromInclusive, K to, boolean toInclusive) throws IOException;
  
  
  /**
   * Scans the available keys and allows iteration over the offset 
   * specified as parameters
   * 
   * 
   * @param startOffset
   *          the start offset
   * @param length
   *          bytes to read
   * @return the sorted iterator
   * @throws IOException
   */
  HoplogIterator<K, V> scan(long startOffset, long length) throws IOException;

  /**
   * Using Cardinality estimator provides an approximate number of entries
   * 
   * @return the number of entries
   */
  long sizeEstimate();

  /**
   * Returns true if the reader has been closed.
   * @return true if closed
   */
  boolean isClosed();

  /**
   * Allows sorted iteration through a set of keys and values.
   */
  public interface HoplogIterator<K, V> {
    K getKey();

    V getValue();

    /** moves to next element and returns the key object */
    K next() throws IOException;
    
    boolean hasNext();
    
    void close();
    
    void remove();
  }
}
