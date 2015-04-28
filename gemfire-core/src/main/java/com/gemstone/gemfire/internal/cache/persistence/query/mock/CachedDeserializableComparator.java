/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence.query.mock;

import java.util.Comparator;

import com.gemstone.gemfire.internal.cache.CachedDeserializable;

/**
 * Compare two cached deserializable objects by unwrapping
 * the underlying object.
 * 
 * If either object is not a cached deserializable, just use
 * the object directly.
 * @author dsmith
 *
 */
class CachedDeserializableComparator implements Comparator<Object> {

  private Comparator comparator;

  public CachedDeserializableComparator(Comparator<?> comparator) {
    this.comparator = comparator;
  }

  @Override
  public int compare(Object o1, Object o2) {
    if(o1 instanceof CachedDeserializable) {
      o1 = ((CachedDeserializable) o1).getDeserializedForReading();
    }
    
    if(o2 instanceof CachedDeserializable) {
      o2 = ((CachedDeserializable) o2).getDeserializedForReading();
    }
    
    return comparator.compare(o1, o2);
    
  }
  
}