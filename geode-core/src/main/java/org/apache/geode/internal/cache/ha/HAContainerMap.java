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
package org.apache.geode.internal.cache.ha;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;

/**
 * @since GemFire 5.7
 */
public class HAContainerMap implements HAContainerWrapper {

  /**
   * TODO: Amogh: Using ConcurrentHashMap may be beneficial. It gives us putEntryIfAbsent()!
   */
  private ConcurrentHashMap map = null;

  /**
   * This map helps us retrieve the proxy id at the receiver side during GII so that we can retain
   * the cqlist of a client for an event which already existed at the receiver side.
   */
  private final Map<String, CacheClientProxy> haRegionNameToProxy;

  public HAContainerMap(ConcurrentHashMap containerMap) {
    map = containerMap;
    haRegionNameToProxy = new ConcurrentHashMap<String, CacheClientProxy>();
  }

  public ClientProxyMembershipID getProxyID(String haRegionName) {
    CacheClientProxy proxy = haRegionNameToProxy.get(haRegionName);
    if (proxy != null) {
      return proxy.getProxyID();
    } else {
      return null;
    }
  }

  public Object putProxy(String haName, CacheClientProxy proxy) {
    return haRegionNameToProxy.put(haName, proxy);
  }

  public CacheClientProxy getProxy(String haRegionName) {
    return haRegionNameToProxy.get(haRegionName);
  }

  public Object removeProxy(String haName) {
    return haRegionNameToProxy.remove(haName);
  }

  public Object getKey(Object key) {
    Entry entry = (Entry) map.get(key);
    return (entry == null) ? null : entry.getKey();
  }

  public String getName() {
    return "HashMap";
  }

  public void cleanUp() {
    // TODO: Amogh: Should we make the map instance null?
    clear();
  }

  public void clear() {
    map.clear();
  }

  public boolean containsKey(Object key) {
    return map.containsKey(key);
  }

  public boolean containsValue(Object value) {
    throw new UnsupportedOperationException("containsValue() not supported.");
  }

  public Set entrySet() {
    throw new UnsupportedOperationException("entrySet() not supported.");
  }

  public Object get(Object key) {
    Entry entry = (Entry) map.get(key);
    return (entry == null) ? null : entry.getValue();
  }

  public Object getEntry(Object key) {
    return map.get(key);
  }

  public boolean isEmpty() {
    return map.isEmpty();
  }

  public Set keySet() {
    return map.keySet();
  }

  public Object put(Object key, Object value) {
    Entry old = (Entry) map.put(key, new Entry(key, value));
    return old != null ? old.getValue() : null;
  }

  @SuppressWarnings("unchecked")
  public Object putIfAbsent(Object key, Object value) {
    Entry old = (Entry) map.putIfAbsent(key, new Entry(key, value));
    return old != null ? old.getValue() : null;
  }

  public void putAll(Map t) {
    throw new UnsupportedOperationException("putAll() not supported.");
  }

  public Object remove(Object key) {
    Entry entry = (Entry) map.remove(key);
    return (entry == null) ? null : entry.getValue();
  }

  public int size() {
    return map.size();
  }

  public Collection values() {
    // return map.values();
    throw new UnsupportedOperationException("values() not supported.");
  }

  protected static class Entry implements Map.Entry {
    private Object key = null;

    private Object value = null;

    public Entry(Object key, Object val) {
      if (key == null || val == null) {
        throw new IllegalArgumentException("key or value cannot be null.");
      }
      this.key = key;
      this.value = val;
    }

    public Object getKey() {
      return this.key;
    }

    public Object getValue() {
      return this.value;
    }

    public Object setValue(Object val) {
      throw new UnsupportedOperationException("setValue() not supported.");
    }
  }

}
