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
package com.gemstone.gemfire.internal.cache.ha;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.EntryDestroyedException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientUpdateMessageImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.HAEventWrapper;

/**
 * @since GemFire 5.7
 */
public class HAContainerRegion implements HAContainerWrapper {

  private Region map;
  
  private final Map<String, CacheClientProxy> haRegionNameToProxy;
  
  public HAContainerRegion (Region region) {
    map = region;
    haRegionNameToProxy = new ConcurrentHashMap<String, CacheClientProxy>();
  }

  public ClientProxyMembershipID getProxyID(String haRegionName) {
    CacheClientProxy proxy = haRegionNameToProxy.get(haRegionName);
    if (proxy != null){
      return proxy.getProxyID();
    } else {
      return null;
    }
  }

  public Region getMapForTest() {
    Region region = (Region)map;
    return region;
  }

  public Object putProxy(String haName, CacheClientProxy proxy) {
    return haRegionNameToProxy.put(haName, proxy);
  }

  public CacheClientProxy getProxy(String haName) {
    return haRegionNameToProxy.get(haName);
  }
  
  public Object removeProxy(String haName) {
    return haRegionNameToProxy.remove(haName);
  }
    
  public Object getKey(Object key) {
    Map.Entry entry = ((Region)map).getEntry(key);
    if(entry != null) {
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

  public String getName() {
    return ((Region)map).getName();
  }

  public void cleanUp() {
    try {
      ((Region)map).destroyRegion();
    }
    catch (CancelException e) {
      // ignore
    }
    catch (RegionDestroyedException e) {
      // ignore
    }
  }

  public void clear() {
    map.clear();
  }

  public boolean containsKey(Object key) {
    return map.containsKey(key);
  }

  public boolean containsValue(Object value) {
    return map.containsValue(value);
  }

  public Set entrySet() {
    return map.entrySet();
  }

  public Object get(Object key) {
    ClientUpdateMessageImpl msg = (ClientUpdateMessageImpl)map.get(key);
    if (msg != null) {
      msg.setEventIdentifier(((HAEventWrapper)key).getEventId());
      if (msg.hasCqs()) {
        msg.setClientCqs(((HAEventWrapper)key).getClientCqs());
      }
    }
    return msg;
  }
  
  public Object getEntry(Object key) {
    Region.Entry entry = ((Region)map).getEntry(key);
    if(entry != null) {
      ClientUpdateMessageImpl msg = (ClientUpdateMessageImpl)entry.getValue();
      msg.setEventIdentifier(((HAEventWrapper)key).getEventId());
      if(msg.hasCqs()) {
        msg.setClientCqs(((HAEventWrapper)key).getClientCqs());
      }      
    }
    return entry;
  }

  public boolean isEmpty() {
    return map.isEmpty();
  }

  public Set keySet() {
    return map.keySet();
  }

  public Object put(Object key, Object value) {
    return map.put(key, value);
  }

  @SuppressWarnings("unchecked")
  public Object putIfAbsent(Object key, Object value) {
    return map.putIfAbsent(key, value);
  }
  
  public void putAll(Map t) {
    map.putAll(t);
  }

  public Object remove(Object key) {
    return map.remove(key);
  }

  public int size() {
    return map.size();
  }

  public Collection values() {
    return map.values();
  }

}
