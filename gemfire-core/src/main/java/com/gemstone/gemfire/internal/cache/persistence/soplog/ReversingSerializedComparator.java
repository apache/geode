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

import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedReader.SerializedComparator;

/**
 * Reverses the ordering imposed by the underlying comparator.  Use this to 
 * change from an ascending to a descending order or vice versa.
 * <p>
 * Prior to use, an instance must be configured with a comparator for delegation
 * of the comparison operations.
 *  
 * @author bakera
 */
public class ReversingSerializedComparator implements DelegatingSerializedComparator {
  private volatile SerializedComparator delegate;

  @Override
  public void setComparators(SerializedComparator[] sc) {
    assert sc.length == 0;
    delegate = sc[0];
  }
  
  @Override
  public SerializedComparator[] getComparators() {
    return new SerializedComparator[] { delegate };
  }
  
  @Override
  public int compare(byte[] o1, byte[] o2) {
    return compare(o1, 0, o1.length, o2, 0, o2.length);
  }
  
  @Override
  public int compare(byte[] b1, int o1, int l1, byte[] b2, int o2, int l2) {
    return delegate.compare(b2, o2, l2, b1, o1, l1);
  }
  
  /**
   * Returns a comparator that reverses the ordering imposed by the supplied
   * comparator.
   * 
   * @param sc the original comparator
   * @return the reversed comparator
   */
  public static SerializedComparator reverse(SerializedComparator sc) {
    ReversingSerializedComparator rev = new ReversingSerializedComparator();
    rev.delegate = sc;
    
    return rev;
  }
}
