/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence.soplog;

import java.util.Iterator;

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
public interface KeyValueIterator<K, V> extends Iterator<K> {
  /**
   * Returns the key at the current position.
   * @return the key
   */
  public K key();
  
  /**
   * Returns the value at the current position.
   * @return the value
   */
  public abstract V value();
}
