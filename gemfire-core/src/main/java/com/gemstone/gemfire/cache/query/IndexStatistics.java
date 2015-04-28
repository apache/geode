/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.query;

//import java.util.*;

/**
 * Provides statistics about a GemFire cache {@link Index}.
 *
 * @since 4.0
 */
public interface IndexStatistics {

  /** Return the total number of times this index has been updated
   */
  public long getNumUpdates();
  
  /** Returns the total amount of time (in nanoseconds) spent updating
   *  this index.
   */
  public long getTotalUpdateTime();
  
  /**
   * Returns the total number of times this index has been accessed by
   * a query.
   */
  public long getTotalUses();

  /**
   * Returns the number of keys in this index.
   */
  public long getNumberOfKeys();

  /**
   * Returns the number of values in this index.
   */
  public long getNumberOfValues();


  /** Return the number of values for the specified key
   *  in this index.
   */
  public long getNumberOfValues(Object key);

  
  /**
   * Return number of read locks taken on this index
   */
  public int getReadLockCount();

  /**
   * Returns the number of keys in this index
   * at the highest level
   */
  public long getNumberOfMapIndexKeys();
  
  /**
   * Returns the number of bucket indexes created
   * in the Partitioned Region
   */
  public int getNumberOfBucketIndexes();
}
