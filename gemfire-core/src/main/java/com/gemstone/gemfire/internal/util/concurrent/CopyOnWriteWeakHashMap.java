/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
