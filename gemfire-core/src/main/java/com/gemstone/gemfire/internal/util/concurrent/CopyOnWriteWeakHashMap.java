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
package com.gemstone.gemfire.internal.util.concurrent;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

/**
 * A copy on write hash map that uses weak references for keys.
 * 
 * @author dsmith
 *
 */
public class CopyOnWriteWeakHashMap<K,V> extends AbstractMap<K, V> {
  private volatile Map<K,V> map = Collections.emptyMap();

  @Override
  public V get(Object key) {
    return map.get(key);
  }
  @Override
  public boolean containsKey(Object key) {
    return this.map.containsKey(key);
  }
  @Override
  public boolean containsValue(Object value) {
    return this.map.containsValue(value);
  }



  @Override
  public synchronized V put(K key, V value) {
    WeakHashMap<K, V> tmp = new WeakHashMap<K, V>(map);
    V result = tmp.put(key, value);
    map = Collections.unmodifiableMap(tmp);
    return result;
  }
  
  public synchronized V putIfAbsent(K key, V value) {
    V oldValue = map.get(key);
    if(oldValue != null) {
      return oldValue;
    }
    
    WeakHashMap<K, V> tmp = new WeakHashMap<K, V>(map);
    V result = tmp.put(key, value);
    map = Collections.unmodifiableMap(tmp);
    return result;
  }



  @Override
  public synchronized void putAll(Map<? extends K, ? extends V> m) {
    WeakHashMap<K, V> tmp = new WeakHashMap<K, V>(map);
    tmp.putAll(m);
    map = Collections.unmodifiableMap(tmp);
  }



  @Override
  public synchronized V remove(Object key) {
    WeakHashMap<K, V> tmp = new WeakHashMap<K, V>(map);
    V result = tmp.remove(key);
    map = Collections.unmodifiableMap(tmp);
    return result;
  }

  @Override
  public synchronized void clear() {
    map = Collections.emptyMap();
  }


  @Override
  public Set<java.util.Map.Entry<K, V>> entrySet() {
    return map.entrySet();
  }
}
