/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence.query.mock;

import java.util.concurrent.atomic.AtomicLong;

import com.gemstone.gemfire.internal.cache.CachedDeserializable;
import com.gemstone.gemfire.internal.cache.persistence.query.CloseableIterator;
import com.gemstone.gemfire.internal.cache.persistence.query.ResultList;

public class ResultListImpl implements ResultList {
  private final SortedResultMapImpl map;
  private AtomicLong counter = new AtomicLong();
  public ResultListImpl() {
    map = new SortedResultMapImpl(false);
  }

  @Override
  public void add(Object e) {
    map.put(counter.getAndIncrement(), e);
    
  }

  @Override
  public CloseableIterator<CachedDeserializable> iterator() {
    return map.valueIterator();
  }
  
  @Override
  public CloseableIterator<CachedDeserializable> iterator(long start) {
    return map.valueIterator(start, true);
  }

  @Override
  public void close() {
    map.close();
  }
  
  

}
