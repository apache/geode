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
import com.gemstone.gemfire.internal.cache.EntryEventImpl;

/**
 * Compare the serialized form of two objects.
 * 
 * Note that this does not do lexigraphic order, or really
 * any useful order. It's only guaranteed to return 0
 * if the bytes are equal, and satisfy all the transitivity
 * and communitivity properties of compare.
 * @author dsmith
 *
 */
public class ByteComparator implements Comparator<Object> {

  public static final byte[] MIN_BYTES = new byte[0];
  public static final byte[] MAX_BYTES = new byte[0];

  @Override
  public int compare(Object o1, Object o2) {
    byte[] o1Bytes = getBytes(o1);
    byte[] o2Bytes = getBytes(o2);

    if(o1Bytes == o2Bytes) {
      return 0;
    }
    
    if(o1Bytes == MIN_BYTES) {
      return -1;
    }
    if(o2Bytes == MIN_BYTES) {
      return 1;
    }
    if(o1Bytes == MAX_BYTES) {
      return 1;
    }
    if(o2Bytes == MAX_BYTES) {
      return -1;
    }
    
    for(int i =0; i < o1Bytes.length; i++) {
      if(i > o2Bytes.length) {
        return 1;
      } else {
        int result = o1Bytes[i] - o2Bytes[i];
        if(result != 0 ) {
          return result;
        }
      }
    }
    
    return o2Bytes.length > o1Bytes.length ? 1 : 0;
  }

  private byte[] getBytes(Object o) {
    if(o instanceof byte[]) {
      return (byte[]) o;
    }
    if(o instanceof CachedDeserializable) {
      return ((CachedDeserializable) o).getSerializedValue();
    } else {
      return EntryEventImpl.serialize(o);
    }
  }

}
