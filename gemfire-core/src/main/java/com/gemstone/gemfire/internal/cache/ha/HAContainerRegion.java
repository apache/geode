/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
 * @author ashetkar
 * @since 5.7
 */
public class HAContainerRegion implements HAContainerWrapper {

  private Map map;
  
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
    else {
      return null;
    }
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
    ClientUpdateMessageImpl cum = (ClientUpdateMessageImpl)map.get(key);
    if (cum != null) {
      cum.setEventIdentifier(((HAEventWrapper)key).getEventId());
      if (cum.hasCqs()) {
        cum.setClientCqs(((HAEventWrapper)key).getClientCqs());
      }
    }
    return cum;
  }
  
  public Object getEntry(Object key) {
    Region.Entry entry = ((Region)map).getEntry(key);
    if(entry != null) {
      ClientUpdateMessageImpl cum = (ClientUpdateMessageImpl)entry.getValue();
      cum.setEventIdentifier(((HAEventWrapper)key).getEventId());
      if(cum.hasCqs()) {
        cum.setClientCqs(((HAEventWrapper)key).getClientCqs());
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
