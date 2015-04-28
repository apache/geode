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
 * The contract for a list of temporary results for a query.
 * This set may be persisted on disk.
 * 
 * This class is threadsafe. Iterators will reflect all entries added to
 * the set up until the time that the iterator was obtained. After that they
 * may or may not reflect modifications to the set while the iteration is in progress.
 * They will guarantee that entries will be returned in the correct order.
 * 
 * @author dsmith
 * @since cedar
 */
public interface ResultList {

  /**
   * Add an element to the list.
   */
  void add(Object e);

  /**
   * Return all of the elements in the list, starting at a given element
   * number
   */
  CloseableIterator<CachedDeserializable> iterator();
  
  /**
   * Return all of the elements in the list, starting at a given element
   * number
   */
  CloseableIterator<CachedDeserializable> iterator(long start);
  
  /**
   * Close the result list and free up any resources on disk
   * associated with the result set.
   */
  public void close();

}
