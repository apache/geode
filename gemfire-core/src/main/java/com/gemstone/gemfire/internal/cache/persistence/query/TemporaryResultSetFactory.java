/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence.query;

import com.gemstone.gemfire.internal.cache.persistence.query.mock.ResultListImpl;
import com.gemstone.gemfire.internal.cache.persistence.query.mock.SortedResultBagImpl;
import com.gemstone.gemfire.internal.cache.persistence.query.mock.SortedResultSetImpl;

/**
 * This is a factory for temporary result sets that overflow to disk.
 * 
 * The result sets will not be recovered when the member restarts.
 * Any temporary results still on disk when a member restarts will be deleted.
 * @author dsmith
 *
 */
public class TemporaryResultSetFactory {
  
  
  /**
   * Get a result set that is sorted. The result set will be overflowed
   * on to disk as necessary, but it will not be recovered from disk.
   * 
   * @param extractor a callback to extract the index sort key from
   * the object. The sort key is expected to be comparable.
   * @param reverse - true to reverse the natural order of the keys
   */
  public ResultSet getSortedResultSet(SortKeyExtractor extractor, boolean reverse) {
    return new SortedResultSetImpl(extractor, reverse);
  }
  
  /**
   * Get a result bag that is sorted. The result set will be overflowed
   * on to disk as necessary, but it will not be recovered from disk.
   * 
   * @param extractor a callback to extract the index sort key from
   * the object. The sort key is expected to be comparable.
   * @param reverse - true to reverse the natural order of the keys
   */
  public ResultBag getSortedResultBag(SortKeyExtractor extractor, boolean reverse) {
    return new SortedResultBagImpl(extractor, reverse);
  }
  
  /**
   * Get a result set that is not sorted. The result set will be overflowed
   * on to disk as necessary, but it will not be recovered from disk.
   * 
   * This is useful for cases where the ordering is not important,
   * but the set semantics are. For example, a distinct query.
   * 
   * @param reverse - true to reverse the natural order of the keys
   */
  public ResultSet getUnsortedResultSet(boolean reverse) {
    return new SortedResultSetImpl(null, reverse);
  }
  
  /**
   * Get a list to store temporary results. The list will be overflowed
   * on to disk as necessary, but it will not be recovered from disk.
   * 
   */
  public ResultList getResultList() {
    return new ResultListImpl();
  }
}
