/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.hdfs.internal.hoplog;

public interface BloomFilter {
  /**
   * Returns true if the bloom filter might contain the supplied key. The nature of the bloom filter
   * is such that false positives are allowed, but false negatives cannot occur.
   */
  boolean mightContain(byte[] key);

  /**
   * Returns true if the bloom filter might contain the supplied key. The nature of the bloom filter
   * is such that false positives are allowed, but false negatives cannot occur.
   */
  boolean mightContain(byte[] key, int keyOffset, int keyLength);

  /**
   * @return Size of the bloom, in bytes
   */
  long getBloomSize();
}