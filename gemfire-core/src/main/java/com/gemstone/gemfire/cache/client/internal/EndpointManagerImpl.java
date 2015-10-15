/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.DummyStatisticsFactory;
import com.gemstone.gemfire.internal.cache.PoolStats;
import com.gemstone.gemfire.internal.cache.execute.TransactionFunctionService;
import com.gemstone.gemfire.internal.cache.tier.InternalClientMembership;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * @author dsmith
 *
 */
public class EndpointManagerImpl implements EndpointManager {
  private static final Logger logger = LogService.getLogger();
  
  private volatile Map<ServerLocation, Endpoint> endpointMap = Collections.emptyMap();
  private final Map/*<ServerLocation, ConnectionStats>*/<ServerLocation, ConnectionStats> statMap = new HashMap<ServerLocation, ConnectionStats>();
  private final DistributedSystem ds;
  private final String poolName;
  private final EndpointListenerBroadcaster listener = new EndpointListenerBroadcaster();
  protected final CancelCriterion cancelCriterion;
  private final PoolStats poolStats;
  
  public EndpointManagerImpl(String poolName, DistributedSystem ds,CancelCriterion cancelCriterion, PoolStats poolStats) {
    this.ds = ds;
    this.poolName = poolName;
    this.cancelCriterion = cancelCriterion;
    this.poolStats = poolStats;
    listener.addListener(new EndpointListenerForBridgeMembership());
    listener.addListener(new TransactionFunctionService.ListenerForTransactionFunctionService());
  }
  
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.client.internal.EndpointManager#referenceEndpoint(com.gemstone.gemfire.distributed.internal.ServerLocation)
   */
  public Endpoint referenceEndpoint(ServerLocation server, DistributedMember memberId) {
    //logger.warn("REFENDPOINT server:"+server+" memberId:"+memberId);
    Endpoint endpoint = endpointMap.get(server);
    boolean addedEndpoint = false;
    if(endpoint == null || endpoint.isClosed()) {
      synchronized(this) {
        endpoint = endpointMap.get(server);
        if(endpoint == null || endpoint.isClosed()) {
          ConnectionStats stats  = getStats(server);
          Map<ServerLocation, Endpoint> endpointMapTemp = new HashMap<ServerLocation, Endpoint>(endpointMap);
          endpoint = new Endpoint(this, ds, server, stats, memberId);
          endpointMapTemp.put(server, endpoint);
          endpointMap = Collections.unmodifiableMap(endpointMapTemp);
          addedEndpoint = true;
          poolStats.setServerCount(endpointMap.size());
        }
      }
    }
    
    endpoint.addReference();
    
    if(addedEndpoint) {
      //logger.warn("EMANFIRE2:JOIN:"+endpoint.getLocation()+" mid:"+endpoint.getMemberId());
      listener.endpointNowInUse(endpoint);
    } else {
      //logger.warn("EMANFIRE33:NOJOIN:"+endpoint.getLocation()+" mid:"+endpoint.getMemberId());
    }
    
    return endpoint;
  }
  
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.client.internal.EndpointManager#serverCrashed(com.gemstone.gemfire.cache.client.internal.Endpoint)
   */
  public void serverCrashed(Endpoint endpoint) {
    removeEndpoint(endpoint, true);
  }
  
  void endpointNotInUse(Endpoint endpoint) {
    removeEndpoint(endpoint, false);
  }
  
  /** Used by Endpoint only, when the reference count for this endpoint reaches 0 */
  private void removeEndpoint(Endpoint endpoint, boolean crashed) {
    endpoint.close();
    boolean removedEndpoint = false;
    synchronized(this) {
      Map<ServerLocation, Endpoint> endpointMapTemp = new HashMap<ServerLocation, Endpoint>(endpointMap);
      endpoint = endpointMapTemp.remove(endpoint.getLocation());
      if(endpoint != null) {
        endpointMap = Collections.unmodifiableMap(endpointMapTemp);
        removedEndpoint = true;
      }
      poolStats.setServerCount(endpointMap.size());
    }
    if(removedEndpoint) {
      PoolImpl pool = (PoolImpl)PoolManager.find(this.poolName);
      if (pool != null && pool.getMultiuserAuthentication()) {
        int size = 0;
        ArrayList<ProxyCache> proxyCaches = pool.getProxyCacheList();
        synchronized (proxyCaches) {
        for (ProxyCache proxyCache : proxyCaches) {
          try {
            Long userId = proxyCache.getUserAttributes().getServerToId().remove(
                endpoint.getLocation());
            if (userId != null) {
              ++size;
            }
          } catch (CacheClosedException cce) {
            // If this call is triggered by a Cache.close(), then this can be
            // expected.
          }
        }
        if (logger.isDebugEnabled()) {
          logger.debug("EndpointManagerImpl.removeEndpoint() Removed server {} from {} user's ProxyCache", endpoint.getLocation(), size);
        }
        }
        UserAttributes ua = UserAttributes.userAttributes.get();
        if (ua != null) {
          Long userId = ua.getServerToId().remove(endpoint.getLocation());
          if (userId != null && logger.isDebugEnabled()) {
            logger.debug("EndpointManagerImpl.removeEndpoint() Removed server {} from thread local variable", endpoint.getLocation());
          }
        }
      } else if (pool != null && !pool.getMultiuserAuthentication()) {
        endpoint.getLocation().setUserId(-1);
      }
      if(crashed) {
        listener.endpointCrashed(endpoint);
      }
      else {
        listener.endpointNoLongerInUse(endpoint);
      }
    }
  }
  
  

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.client.internal.EndpointManager#getEndpointMap()
   */
  public Map<ServerLocation, Endpoint> getEndpointMap() {
    return endpointMap;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.client.internal.EndpointManager#close()
   */
  public synchronized void close() {
    for(Iterator<ConnectionStats> itr = statMap.values().iterator(); itr.hasNext(); ) {
      ConnectionStats stats = itr.next();
      stats.close();
    }
    
    statMap.clear();
    endpointMap = Collections.emptyMap();
    listener.clear();
  }
  
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.client.internal.EndpointManager#addListener(com.gemstone.gemfire.cache.client.internal.EndpointManagerImpl.EndpointListener)
   */
  public void addListener(EndpointManager.EndpointListener listener) {
    this.listener.addListener(listener);
  }
  
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.client.internal.EndpointManager#removeListener(com.gemstone.gemfire.cache.client.internal.EndpointManagerImpl.EndpointListener)
   */
  public void removeListener(EndpointManager.EndpointListener listener) {
    this.listener.removeListener(listener);
  }
  
  private synchronized ConnectionStats getStats(ServerLocation location) {
    ConnectionStats stats = statMap.get(location);
    if(stats == null) {
      String statName = poolName + "-" + location.toString();
      PoolImpl pool = (PoolImpl)PoolManager.find(this.poolName);
      if (pool != null) {
        if (pool.getGatewaySender() != null) {
          stats = new ConnectionStats(new DummyStatisticsFactory(), statName,
              this.poolStats/*, this.gatewayStats*/);
        }
      }
      if (stats == null) {
        stats = new ConnectionStats(ds, statName, this.poolStats/*,
            this.gatewayStats*/);
      }
      statMap.put(location, stats);
    }
    
    return stats;
  }
  
  public synchronized Map<ServerLocation, ConnectionStats> getAllStats() {
    return new HashMap<ServerLocation, ConnectionStats>(statMap);
  }

  public int getConnectedServerCount() {
    return getEndpointMap().size();
  }
  
  public static void loadEmergencyClasses() {
    //do nothing
  }
  
  protected static class EndpointListenerBroadcaster implements EndpointManager.EndpointListener {
  
    private volatile Set/*<EndpointListener>*/<EndpointListener> endpointListeners = Collections.emptySet();
    
    public synchronized void addListener(EndpointManager.EndpointListener listener) {
      HashSet<EndpointListener> tmpListeners = new HashSet<EndpointListener>(endpointListeners);
      tmpListeners.add(listener);
      endpointListeners = Collections.unmodifiableSet(tmpListeners);
    }

    public synchronized void clear() {
      endpointListeners = Collections.emptySet();
    }

    public void removeListener(EndpointManager.EndpointListener listener) {
      HashSet<EndpointListener> tmpListeners = new HashSet<EndpointListener>(endpointListeners);
      tmpListeners.remove(listener);
      endpointListeners = Collections.unmodifiableSet(tmpListeners);
    }

    public void endpointCrashed(Endpoint endpoint) {
      for(Iterator<EndpointListener> itr = endpointListeners.iterator(); itr.hasNext(); ) {
        EndpointManager.EndpointListener listener = itr.next();
        listener.endpointCrashed(endpoint);
      }
    }

    public void endpointNoLongerInUse(Endpoint endpoint) {
      for(Iterator<EndpointListener> itr = endpointListeners.iterator(); itr.hasNext(); ) {
        EndpointManager.EndpointListener listener = itr.next();
        listener.endpointNoLongerInUse(endpoint);
      }
    }

    public void endpointNowInUse(Endpoint endpoint) {
      //logger.warn("HIGHUP:JOIN:"+endpoint.getLocation());
      for(Iterator<EndpointListener> itr = endpointListeners.iterator(); itr.hasNext(); ) {
        EndpointManager.EndpointListener listener = itr.next();
        listener.endpointNowInUse(endpoint);
      }
    }
  }
  
  
  
  public class EndpointListenerForBridgeMembership implements EndpointManager.EndpointListener {
    
    public void endpointCrashed(Endpoint endpoint) {
      if(endpoint.getMemberId()==null || cancelCriterion.cancelInProgress()!=null) {
        return;
      }
      //logger.warn("EMANFIRE:CRASH:"+endpoint.getLocation());
      InternalClientMembership.notifyCrashed(endpoint.getMemberId(), false);
    }

    public void endpointNoLongerInUse(Endpoint endpoint) {
      if(endpoint.getMemberId()==null || cancelCriterion.cancelInProgress()!=null) {
        return;
      }
      //logger.warn("EMANFIRE:LEFT:"+endpoint.getLocation());
      InternalClientMembership.notifyLeft(endpoint.getMemberId(), false);
    }

    public void endpointNowInUse(Endpoint endpoint) {
      if(cancelCriterion.cancelInProgress()!=null) {
        return;
      }
      //logger.warn("EMANFIRE:JOIN:"+endpoint.getLocation()+" mid:"+endpoint.getMemberId(),new Exception());
      InternalClientMembership.notifyJoined(endpoint.getMemberId(), false);
    }
  }

  public String getPoolName() {
    return poolName;
  }  
  
}
