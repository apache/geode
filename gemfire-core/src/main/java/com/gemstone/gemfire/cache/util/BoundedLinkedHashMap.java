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
package com.gemstone.gemfire.cache.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Class <code>BoundedLinkedHashMap</code> is a bounded
 * <code>LinkedHashMap</code>. The bound is the maximum
 * number of entries the <code>BoundedLinkedHashMap</code>
 * can contain.
 *
 * @since 4.2
 * @deprecated as of 5.7 create your own class that extends {@link LinkedHashMap}
 * and implement {@link LinkedHashMap#removeEldestEntry}
 * to enforce a maximum number of entries.
 */
public class BoundedLinkedHashMap extends LinkedHashMap
{
  private static final long serialVersionUID = -3419897166186852692L;

  /**
   * The maximum number of entries allowed in this
   * <code>BoundedLinkedHashMap</code>
   */
  protected int _maximumNumberOfEntries;

  /**
   * Constructor.
   *
   * @param initialCapacity The initial capacity.
   * @param loadFactor The load factor
   * @param maximumNumberOfEntries The maximum number of allowed entries
   */
  public BoundedLinkedHashMap(int initialCapacity, float loadFactor, int maximumNumberOfEntries) {
    super(initialCapacity, loadFactor);
    this._maximumNumberOfEntries = maximumNumberOfEntries;
  }

  /**
   * Constructor.
   *
   * @param initialCapacity The initial capacity.
   * @param maximumNumberOfEntries The maximum number of allowed entries
   */
  public BoundedLinkedHashMap(int initialCapacity, int maximumNumberOfEntries) {
    super(initialCapacity);
    this._maximumNumberOfEntries = maximumNumberOfEntries;
  }

  /**
   * Constructor.
   *
   * @param maximumNumberOfEntries The maximum number of allowed entries
   */
  public BoundedLinkedHashMap(int maximumNumberOfEntries) {
    super();
    this._maximumNumberOfEntries = maximumNumberOfEntries;
  }

  /**
   * Returns the maximum number of entries.
   * @return the maximum number of entries
   */
  public int getMaximumNumberOfEntries(){
    return this._maximumNumberOfEntries;
  }

  @Override
  protected boolean removeEldestEntry(Map.Entry entry) {
    return size() > this._maximumNumberOfEntries;
  }
}
