/*=========================================================================
 * Copyright (c) 2012 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.snapshot;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Map.Entry;

/**
 * Iterates over the entries in a region snapshot.  Holds resources that must
 * be freed via {@link #close()}.
 * 
 * @param <K> the key type of the snapshot region
 * @param <V> the value type the snapshot region
 * 
 * @see SnapshotReader
 * 
 * @author bakera
 * @since 7.0
 */
public interface SnapshotIterator<K, V> {
  /**
   * Returns true if there are more elements in the iteration.
   * 
   * @return true if the iterator has more elements.
   * 
   * @throws IOException error reading the snapshot
   * @throws ClassNotFoundException error deserializing the snapshot element
   */
  boolean hasNext() throws IOException, ClassNotFoundException;
  
  /**
   * Returns the next element in the iteration.
   * 
   * @return the next element
   * 
   * @throws NoSuchElementException there are no further elements
   * @throws IOException error reading the snapshot
   * @throws ClassNotFoundException error deserializing the snapshot element
   */
  Entry<K, V> next() throws IOException, ClassNotFoundException;

  /**
   * Closes the iterator and its underlying resources.
   * @throws IOException error closing the iterator
   */
  void close() throws IOException;
}
