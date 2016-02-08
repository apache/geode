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
package com.gemstone.gemfire.internal.cache.persistence.soplog;

import org.apache.hadoop.hbase.util.Bytes;

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
    return Bytes.compareTo(r, rOff, rLen, l, lOff, lLen);
  }
}
