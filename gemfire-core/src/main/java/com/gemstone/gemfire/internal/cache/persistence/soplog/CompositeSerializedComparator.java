/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence.soplog;

import java.nio.ByteBuffer;

import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedReader.SerializedComparator;

/**
 * Creates and compares composite keys.
 * 
 * @author bakera
 */
public interface CompositeSerializedComparator extends SerializedComparator  {
  /**
   * Constructs a composite key consisting of a primary key and a secondary key.
   * 
   * @param key1 the primary key
   * @param key2 the secondary key
   * @return the composite key
   */
  public byte[] createCompositeKey(byte[] key1, byte[] key2);
  
  /**
   * Constructs a composite key by combining the supplied keys.  The number of
   * keys and their order must match the comparator set.
   * <p>
   * The <code>WILDCARD_KEY</code> token may be used to match all subkeys in the
   * given ordinal position.  This is useful when constructing a search key to
   * retrieve all keys for a given primary key, ignoring the remaining subkeys.
   * 
   * @param keys the keys, ordered by sort priority
   * @return the composite key
   */
  public byte[] createCompositeKey(byte[]... keys);
  
  /**
   * Returns subkey for the given ordinal position.
   * @param key the composite key
   * @return the subkey
   */
  public ByteBuffer getKey(ByteBuffer key, int ordinal);
}
