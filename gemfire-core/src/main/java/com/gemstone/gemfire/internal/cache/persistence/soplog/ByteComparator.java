/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence.soplog;

import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedReader.SerializedComparator;

/**
 * Compares objects byte-by-byte.  This is fast and sufficient for cases when
 * lexicographic ordering is not important or the serialization is order-
 * preserving. 
 * 
 * @author bakera
 */
public class ByteComparator implements SerializedComparator {
  @Override
  public int compare(byte[] rhs, byte[] lhs) {
    return compare(rhs, 0, rhs.length, lhs, 0, lhs.length);
  }

  @Override
  public int compare(byte[] r, int rOff, int rLen, byte[] l, int lOff, int lLen) {
    return compareBytes(r, rOff, rLen, l, lOff, lLen);
  }
  
  /**
   * Compares two byte arrays element-by-element.
   * 
   * @param r the right array
   * @param rOff the offset of r
   * @param rLen the length of r to compare
   * @param l the left array
   * @param lOff the offset of l
   * @param lLen the length of l to compare
   * @return -1 if r < l; 0 if r == l; 1 if r > 1
   */
  
  public static int compareBytes(byte[] r, int rOff, int rLen, byte[] l, int lOff, int lLen) {
    int rLast = rOff + rLen;
    int lLast = lOff + lLen;
    for (int i = rOff, j = lOff; i < rLast && j < lLast; i++, j++) {
      int diff = (r[i] & 0xff) - (l[j] & 0xff);
      if (diff != 0) {
        return diff;
      }
    }
    return rLen - lLen;
  }
}
