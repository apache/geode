/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.cache.persistence.query.mock;

import java.util.Comparator;

import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.EntryEventImpl;

/**
 * Compare the serialized form of two objects.
 * 
 * Note that this does not do lexigraphic order, or really
 * any useful order. It's only guaranteed to return 0
 * if the bytes are equal, and satisfy all the transitivity
 * and communitivity properties of compare.
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
