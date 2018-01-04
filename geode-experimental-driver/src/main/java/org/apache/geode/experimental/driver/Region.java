/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.experimental.driver;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.geode.annotations.Experimental;

/**
 * Defines the behaviors of a GemFire region. A region is an associative array from unique keys to
 * values. For each key, the region will contain zero or one value.
 *
 * <strong>This code is an experimental prototype and is presented "as is" with no warranty,
 * suitability, or fitness of purpose implied.</strong>
 *
 * @param <K> Type of region keys.
 * @param <V> Type of region values.
 * @see org.apache.geode.experimental.driver.JSONWrapper
 */
@Experimental
public interface Region<K, V> {
  /**
   * Captures a snapshot of the attributes (e.g., size) of this region.
   *
   * @return Attributes associated with this region.
   * @throws IOException
   */
  RegionAttributes getRegionAttributes() throws IOException;

  /**
   * Gets the value, if any, contained in this region for the <code>key</code>.
   *
   * @param key Unique key associated with a value.
   * @return Value, if any, associated with <code>key</code>.
   * @throws IOException
   */
  V get(K key) throws IOException;

  /**
   * Gets the values, if any, contained in this region for the collection of <code>keys</code>.
   *
   * @param keys Collection of unique keys associated with values.
   * @return Map from <code>keys</code> to their associated values.
   * @throws IOException
   */
  Map<K, V> getAll(Collection<K> keys) throws IOException;

  /**
   * Puts the <code>value</code> into this region for the <code>key</code>.
   *
   * @param key Unique key to associate with the <code>value</code>.
   * @param value Value to associate with the <code>key</code>.
   * @throws IOException
   */
  void put(K key, V value) throws IOException;

  /**
   * Puts the map from keys to <code>values</code> into this region. If any one key/value pair can
   * not be inserted, the remaining pair insertions will be attempted.
   *
   * @param values Map from <code>keys</code> to their associated values.
   * @throws IOException
   */
  void putAll(Map<K, V> values) throws IOException;


  /**
   * Removes any value associated with the <code>key</code> from this region.
   *
   * @param key Unique key associated with a value.
   * @throws IOException
   */
  void remove(K key) throws IOException;
}
