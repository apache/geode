/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.cache.query.internal.index;

import java.util.Collection;

import com.gemstone.gemfire.internal.concurrent.CompactConcurrentHashSet2;

/**
 * This class overrides the size method to make it non-blocking for our query
 * engine. size() is only called index size estimation for selecting best index
 * for a query which can be approximate so it does NOT have to lock internal
 * segments for accurate count.
 * 
 * @author shobhit
 * @param <E>
 * @since 7.0 
 */
public class IndexConcurrentHashSet<E> extends CompactConcurrentHashSet2<E> {

  
  public IndexConcurrentHashSet() {
    super();
  }

  public IndexConcurrentHashSet(Collection<? extends E> m) {
    super(m);
  }

  public IndexConcurrentHashSet(int initialCapacity, float loadFactor,
      int concurrencyLevel) {
    super(initialCapacity, loadFactor, concurrencyLevel);
  }

  public IndexConcurrentHashSet(int initialCapacity, float loadFactor) {
    super(initialCapacity, loadFactor);
  }

  public IndexConcurrentHashSet(int initialCapacity) {
    super(initialCapacity);
  }

  /*
   * Returns the number of values in this set. If the set contains
   * more than <tt>Integer.MAX_VALUE</tt> elements, returns
   * <tt>Integer.MAX_VALUE</tt>.
   * 
   * @return the number of values in this set
   * 
   * Note: This has been modified for GemFire Indexes.
   */
//  @Override
//  public int size() {
//    long trueSize = mappingCount();
//    if (trueSize > Integer.MAX_VALUE) {
//      return Integer.MAX_VALUE;
//    } else {
//      return (int)trueSize;
//    }
//  }
}
