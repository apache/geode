/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence.query.mock;

import com.gemstone.gemfire.internal.cache.CachedDeserializable;
import com.gemstone.gemfire.internal.cache.persistence.query.CloseableIterator;
import com.gemstone.gemfire.internal.cache.persistence.query.IdentityExtractor;
import com.gemstone.gemfire.internal.cache.persistence.query.ResultSet;
import com.gemstone.gemfire.internal.cache.persistence.query.SortKeyExtractor;

public class SortedResultSetImpl implements ResultSet {
  private final SortedResultMapImpl map;
  private SortKeyExtractor extractor;
  public SortedResultSetImpl(SortKeyExtractor extractor, boolean reverse) {
    this.extractor = extractor == null ? new IdentityExtractor() : extractor;
    
    map = new SortedResultMapImpl(reverse);
  }

  @Override
  public void add(Object e) {
    map.put(extractor.getSortKey(e), e);
    
  }

  @Override
  public CloseableIterator<CachedDeserializable> iterator() {
    return map.valueIterator();
  }

  @Override
  public void close() {
    map.close();
  }
  
  

}
