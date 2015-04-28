/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence.query;

import com.gemstone.gemfire.internal.cache.CachedDeserializable;

/**
 * The contract for a sorted set of temporary results for a query.
 * This set may be persisted on disk.
 * 
 * This class is threadsafe. Iterators will reflect all entries added to
 * the set up until the time that the iterator was obtained. After that they
 * may or may not reflect modifications to the set while the iteration is in progress.
 * They will guarantee that entries will be returned in the correct order.
 * 
 * The element is allowed to be an object, or a CachedDeserializable.
 * 
 * If multiple instances of the same element are required, use SortedResultBag
 * instead.
 * 
 * The result set should be closed using the close method to free up resources
 * when it is no longer in use.
 * 
 * @author dsmith
 * @since cedar
 */
public interface ResultSet {

  /**
   * Add an element to the set. If the same element exists in the 
   * set it is replaced with the new element
   * @param e the element. e may be NULL.
   */
  void add(Object e);

  /**
   * Return all of the elements in the set.
   */
  CloseableIterator<CachedDeserializable> iterator();
  
  /**
   * Close the result set and free up any resources on disk
   * associated with the result set.
   */
  public void close();
}
