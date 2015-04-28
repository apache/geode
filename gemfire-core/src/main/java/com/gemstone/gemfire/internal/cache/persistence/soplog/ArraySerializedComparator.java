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
 * Provides comparisons of composite keys by comparing each of the constituent
 * parts of the key in order.  A subkey will only be evaluated if the preceeding
 * keys have compared as equal.
 * <p>
 * Prior to use, an instance must be configured with the ordered list of
 * comparators to apply.
 * <p>
 * The keys for an N-composite are stored as follows:
 * <pre>
 * | len[0] | key[0] | len[1] | key[1] | ... | len[N-2] | key[N-2] | key[N-1] |
 * </pre>
 * where the key length is stored as a protobuf varint.
 * 
 * @author bakera
 */
public class ArraySerializedComparator implements CompositeSerializedComparator, 
DelegatingSerializedComparator {

  /** the comparators */
  private volatile SerializedComparator[] comparators;
  
  /**
   * Injects the comparators to be used on composite keys.  The number and order
   * must match the key.
   * 
   * @param comparators the comparators
   */
  public void setComparators(SerializedComparator[] comparators) {
    this.comparators = comparators;
  }
  
  @Override
  public int compare(byte[] o1, byte[] o2) {
    return compare(o1, 0, o1.length, o2, 0, o2.length);
  }

  @Override
  public int compare(byte[] b1, int o1, int l1, byte[] b2, int o2, int l2) {
    SerializedComparator[] sc = comparators;
    
    int off1 = o1;
    int off2 = o2;
    for (int i = 0; i < sc.length - 1; i++) {
      int klen1 = Bytes.getVarInt(b1, off1);
      int klen2 = Bytes.getVarInt(b2, off2);

      off1 += Bytes.sizeofVarInt(klen1);
      off2 += Bytes.sizeofVarInt(klen2);
      
      if (!SoplogToken.isWildcard(b1, off1, b2, off2)) {
        int diff = sc[i].compare(b1, off1, klen1, b2, off2, klen2);
        if (diff != 0) {
          return diff;
        }
      }
      off1 += klen1;
      off2 += klen2;
    }
    
    if (!SoplogToken.isWildcard(b1, off1, b2, off2)) {
      l1 -= (off1 - o1);
      l2 -= (off2 - o2);
      return sc[sc.length - 1].compare(b1, off1, l1, b2, off2, l2);
    }
    return 0;
  }

  @Override
  public SerializedComparator[] getComparators() {
    return comparators;
  }

  @Override
  public byte[] createCompositeKey(byte[] key1, byte[] key2) {
    return createCompositeKey(new byte[][] { key1, key2 });
  }
  
  @Override
  public byte[] createCompositeKey(byte[]... keys) {
    assert comparators.length == keys.length;
    
    int size = 0;
    for (int i = 0; i < keys.length - 1; i++) {
      size += keys[i].length + Bytes.sizeofVarInt(keys[i].length);
    }
    size += keys[keys.length - 1].length;
    
    // TODO do we have to do a copy here or can we delay until the disk write?
    int off = 0;
    byte[] buf = new byte[size];
    for (int i = 0; i < keys.length - 1; i++) {
      off = Bytes.putVarInt(keys[i].length, buf, off);
      System.arraycopy(keys[i], 0, buf, off, keys[i].length);
      off += keys[i].length;
    }
    System.arraycopy(keys[keys.length - 1], 0, buf, off, keys[keys.length - 1].length);
    return buf;
  }

  @Override
  public ByteBuffer getKey(ByteBuffer key, int ordinal) {
    assert ordinal < comparators.length;
    
    for (int i = 0; i < comparators.length - 1; i++) {
      int klen = Bytes.getVarInt(key);
      if (i == ordinal) {
        ByteBuffer subkey = (ByteBuffer) key.slice().limit(klen);
        key.rewind();
        
        return subkey;
      }
      key.position(key.position() + klen);
    }
    
    ByteBuffer subkey = key.slice();
    key.rewind();
    
    return subkey;
  }
}
