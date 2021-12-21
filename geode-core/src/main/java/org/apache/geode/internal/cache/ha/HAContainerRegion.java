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

import org.apache.geode.CancelException;
import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.ClientUpdateMessageImpl;
import org.apache.geode.internal.cache.tier.sockets.HAEventWrapper;

/**
 * @since GemFire 5.7
 */
public class HAContainerRegion implements HAContainerWrapper {

  private final Region map;

  private final Map<String, CacheClientProxy> haRegionNameToProxy;

  public HAContainerRegion(Region region) {
    map = region;
    haRegionNameToProxy = new ConcurrentHashMap<String, CacheClientProxy>();
  }

  @Override
  public ClientProxyMembershipID getProxyID(String haRegionName) {
    CacheClientProxy proxy = haRegionNameToProxy.get(haRegionName);
    if (proxy != null) {
      return proxy.getProxyID();
    } else {
      return null;
    }
  }

  public Region getMapForTest() {
    Region region = map;
    return region;
  }

  @Override
  public Object putProxy(String haName, CacheClientProxy proxy) {
    return haRegionNameToProxy.put(haName, proxy);
  }

  @Override
  public CacheClientProxy getProxy(String haName) {
    return haRegionNameToProxy.get(haName);
  }

  @Override
  public Object removeProxy(String haName) {
    return haRegionNameToProxy.remove(haName);
  }

  @Override
  public Object getKey(Object key) {
    Map.Entry entry = map.getEntry(key);
    if (entry != null) {
      try {
        return entry.getKey();
      }
      // Is this catch block needed?
      catch (EntryDestroyedException ede) {
        return null;
      }
    }
    return null;
  }

  @Override
  public String getName() {
    return map.getName();
  }

  @Override
  public void cleanUp() {
    try {
      map.destroyRegion();
    } catch (CancelException e) {
      // ignore
    } catch (RegionDestroyedException e) {
      // ignore
    }
  }

  @Override
  public void clear() {
    map.clear();
  }

  @Override
  public boolean containsKey(Object key) {
    return map.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return map.containsValue(value);
  }

  @Override
  public Set entrySet() {
    return map.entrySet();
  }

  @Override
  public Object get(Object key) {
    ClientUpdateMessageImpl msg = (ClientUpdateMessageImpl) map.get(key);
    if (msg != null) {
      msg.setEventIdentifier(((HAEventWrapper) key).getEventId());
      if (msg.hasCqs()) {
        msg.setClientCqs(((HAEventWrapper) key).getClientCqs());
      }
    }
    return msg;
  }

  @Override
  public Object getEntry(Object key) {
    Region.Entry entry = map.getEntry(key);
    if (entry != null) {
      ClientUpdateMessageImpl msg = (ClientUpdateMessageImpl) entry.getValue();
      msg.setEventIdentifier(((HAEventWrapper) key).getEventId());
      if (msg.hasCqs()) {
        msg.setClientCqs(((HAEventWrapper) key).getClientCqs());
      }
    }
    return entry;
  }

  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }

  @Override
  public Set keySet() {
    return map.keySet();
  }

  @Override
  public Object put(Object key, Object value) {
    return map.put(key, value);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object putIfAbsent(Object key, Object value) {
    return map.putIfAbsent(key, value);
  }

  @Override
  public void putAll(Map t) {
    map.putAll(t);
  }

  @Override
  public Object remove(Object key) {
    return map.remove(key);
  }

  @Override
  public int size() {
    return map.size();
  }

  @Override
  public Collection values() {
    return map.values();
  }

}
