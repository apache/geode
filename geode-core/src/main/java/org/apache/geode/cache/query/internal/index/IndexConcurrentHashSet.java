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
/**
 * 
 */
package org.apache.geode.cache.query.internal.index;

import java.util.Collection;

import org.apache.geode.internal.concurrent.CompactConcurrentHashSet2;

/**
 * This class overrides the size method to make it non-blocking for our query
 * engine. size() is only called index size estimation for selecting best index
 * for a query which can be approximate so it does NOT have to lock internal
 * segments for accurate count.
 * 
 * @param <E>
 * @since GemFire 7.0
 */
public class IndexConcurrentHashSet<E> extends CompactConcurrentHashSet2<E> {

  
  public IndexConcurrentHashSet() {
    super();
  }

  public IndexConcurrentHashSet(Collection<? extends E> m) {
    super(m);
  }

  public IndexConcurrentHashSet(int initialCapacity, float loadFactor,
      int concurrencyLevel) {
    super(initialCapacity, loadFactor, concurrencyLevel);
  }

  public IndexConcurrentHashSet(int initialCapacity, float loadFactor) {
    super(initialCapacity, loadFactor);
  }

  public IndexConcurrentHashSet(int initialCapacity) {
    super(initialCapacity);
  }

  /*
   * Returns the number of values in this set. If the set contains
   * more than <tt>Integer.MAX_VALUE</tt> elements, returns
   * <tt>Integer.MAX_VALUE</tt>.
   * 
   * @return the number of values in this set
   * 
   * Note: This has been modified for GemFire Indexes.
   */
//  @Override
//  public int size() {
//    long trueSize = mappingCount();
//    if (trueSize > Integer.MAX_VALUE) {
//      return Integer.MAX_VALUE;
//    } else {
//      return (int)trueSize;
//    }
//  }
}
