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

import java.util.Iterator;

/**
 * Provides an {@link Iterator} view over a collection of keys and values.  The
 * implementor must provide access to the current key/value as well as a means
 * to move to the next pair.
 * 
 * @author bakera
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public interface KeyValueIterator<K, V> extends Iterator<K> {
  /**
   * Returns the key at the current position.
   * @return the key
   */
  public K key();
  
  /**
   * Returns the value at the current position.
   * @return the value
   */
  public abstract V value();
}
