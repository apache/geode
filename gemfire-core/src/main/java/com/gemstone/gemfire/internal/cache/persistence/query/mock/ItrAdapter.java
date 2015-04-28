/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence.query.mock;

import java.util.Iterator;

import com.gemstone.gemfire.internal.cache.CachedDeserializable;
import com.gemstone.gemfire.internal.cache.persistence.query.CloseableIterator;

/**
 * An adapter from a java.util.iterator to a closable iterator.
 * 
 * The java.util.iterator is expected to be iterating over a set of 
 * CachedDeserializable objects.
 * @author dsmith
 *
 */
class ItrAdapter implements CloseableIterator<CachedDeserializable> {
  
  private Iterator<?> iterator;

  public ItrAdapter(Iterator<?> iterator) {
    this.iterator = iterator;
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public CachedDeserializable next() {
    return (CachedDeserializable) iterator.next();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
    
  }

  @Override
  public void close() {
    //do nothing
    
  }
  
}