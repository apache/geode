/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence.soplog;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Provides an {@link Iterator} view over a collection of keys and values.  The
 * implementor must provide access to the current key/value as well as a means
 * to move to the next pair.
 * 
 * @author bakera
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public abstract class AbstractKeyValueIterator<K, V> implements KeyValueIterator<K, V> {
  /** true if the iterator has been advanced to the next element */
  private boolean foundNext = false;
  
  @Override
  public boolean hasNext() {
    if (!foundNext) {
      foundNext = step();
    }
    return foundNext;
  }

  @Override
  public K next() {
    if (!foundNext && !step()) {
      throw new NoSuchElementException();
    }

    foundNext = false;
    return key();
  }
  
  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
  
  /**
   * Returns the key at the current position.
   * @return the key
   */
  public abstract K key();
  
  /**
   * Returns the value at the current position.
   * @return the value
   */
  public abstract V value();
  
  /**
   * Steps the iteration to the next position.
   * @return true if the step succeeded
   */
  protected abstract boolean step();
}
