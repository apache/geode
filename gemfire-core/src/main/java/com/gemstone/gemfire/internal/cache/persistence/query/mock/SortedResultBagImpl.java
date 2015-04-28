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
import com.gemstone.gemfire.internal.cache.persistence.query.IdentityExtractor;
import com.gemstone.gemfire.internal.cache.persistence.query.ResultBag;
import com.gemstone.gemfire.internal.cache.persistence.query.SortKeyExtractor;

/**
 * Mock sorted bag implementation. Uses the mock index map
 * internally, for convenience.
 * @author dsmith
 *
 */
public class SortedResultBagImpl implements ResultBag {
  private final IndexMapImpl map;
  private AtomicLong counter = new AtomicLong();
  private SortKeyExtractor extractor;
  private boolean reverse;
  
  public SortedResultBagImpl(SortKeyExtractor extractor, boolean reverse) {
    this.extractor = extractor == null ? new IdentityExtractor() : extractor;
    map = new IndexMapImpl();
    this.reverse =reverse;
  }

  @Override
  public void add(Object e) {
    map.put(extractor.getSortKey(e), counter.incrementAndGet(), e);
    
  }

  @Override
  public CloseableIterator<CachedDeserializable> iterator() {
    if(reverse) {
      return map.descendingValueIterator();
    } else {
      return map.valueIterator();
    }
  }

  @Override
  public void close() {
    map.destroy();
  }
}
