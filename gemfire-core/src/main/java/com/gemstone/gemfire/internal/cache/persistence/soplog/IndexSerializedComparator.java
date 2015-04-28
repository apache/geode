/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence.soplog;

import java.nio.ByteBuffer;

import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedReader.SerializedComparator;
import com.gemstone.gemfire.internal.util.Bytes;

/**
 * Provides a comparator for composite keys of the form (k0, k1).  The primary 
 * keys are compared lexicographically while the secondary keys are compared 
 * bitwise.  The key format includes the primary key length to avoid deserialization 
 * the secondary key when reading:
 * <pre>
 * | varint | primary key | secondary key |
 * </pre>
 * The key length is encoded using a protobuf-style varint.
 * <p>
 * 
 * @author bakera
 */
public class IndexSerializedComparator implements CompositeSerializedComparator, 
DelegatingSerializedComparator {
  
  private volatile SerializedComparator primary;
  private volatile SerializedComparator secondary;
  
  public IndexSerializedComparator() {
    primary = new LexicographicalComparator();
    secondary = new ByteComparator();
  }
  
  @Override
  public void setComparators(SerializedComparator[] comparators) {
    assert comparators.length == 2;
    
    primary = comparators[0];
    secondary = comparators[1];
  }

  @Override
  public SerializedComparator[] getComparators() {
    return new SerializedComparator[] { primary, secondary };
  }

  @Override
  public int compare(byte[] o1, byte[] o2) {
    return compare(o1, 0, o1.length, o2, 0, o2.length);
  }
  
  @Override
  public int compare(byte[] b1, int o1, int l1, byte[] b2, int o2, int l2) {
    int klen1 = Bytes.getVarInt(b1, o1);
    int klen2 = Bytes.getVarInt(b2, o2);
    
    int off1 = o1 + Bytes.sizeofVarInt(klen1);
    int off2 = o2 + Bytes.sizeofVarInt(klen2);
    
    // skip the comparison operation if there is a SearchToken.WILDCARD
    if (!SoplogToken.isWildcard(b1, off1, b2, off2)) {
      int diff = primary.compare(b1, off1, klen1, b2, off2, klen2);
      if (diff != 0) {
        return diff;
      }
    }
    off1 += klen1;
    off2 += klen2;

    if (!SoplogToken.isWildcard(b1, off1, b2, off2)) {
      l1 -= (off1 - o1);
      l2 -= (off2 - o2);
      return secondary.compare(b1, off1, l1, b2, off2, l2);
    }
    return 0;
  }

  @Override
  public ByteBuffer getKey(ByteBuffer key, int ordinal) {
    assert ordinal < 2;
    
    ByteBuffer subkey;
    int klen = Bytes.getVarInt(key);
    if (ordinal == 0) {
      subkey = (ByteBuffer) key.slice().limit(klen);
      
    } else {
      subkey = ((ByteBuffer) key.position(key.position() + klen)).slice();
    }
    
    key.rewind();
    return subkey;
  }

  @Override
  public byte[] createCompositeKey(byte[] key1, byte[] key2) {
    int vlen = Bytes.sizeofVarInt(key1.length);
    byte[] buf = new byte[vlen + key1.length + key2.length];
    
    Bytes.putVarInt(key1.length, buf, 0);
    System.arraycopy(key1, 0, buf, vlen, key1.length);
    System.arraycopy(key2, 0, buf, vlen + key1.length, key2.length);
    
    return buf;
  }

  @Override
  public byte[] createCompositeKey(byte[]... keys) {
    assert keys.length == 2;
    
    return createCompositeKey(keys[0], keys[1]);
  }
}
