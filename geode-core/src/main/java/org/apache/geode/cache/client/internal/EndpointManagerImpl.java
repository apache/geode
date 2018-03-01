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
package org.apache.geode.cache.client.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.PoolStats;
import org.apache.geode.internal.cache.tier.InternalClientMembership;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.statistics.DummyStatisticsFactory;

/**
 *
 */
public class EndpointManagerImpl implements EndpointManager {
  private static final Logger logger = LogService.getLogger();

  private volatile Map<ServerLocation, Endpoint> endpointMap = Collections.emptyMap();
  private final Map/* <ServerLocation, ConnectionStats> */<ServerLocation, ConnectionStats> statMap =
      new HashMap<ServerLocation, ConnectionStats>();
  private final DistributedSystem ds;
  private final String poolName;
  private final EndpointListenerBroadcaster listener = new EndpointListenerBroadcaster();
  protected final CancelCriterion cancelCriterion;
  private final PoolStats poolStats;

  public EndpointManagerImpl(String poolName, DistributedSystem ds, CancelCriterion cancelCriterion,
      PoolStats poolStats) {
    this.ds = ds;
    this.poolName = poolName;
    this.cancelCriterion = cancelCriterion;
    this.poolStats = poolStats;
    listener.addListener(new EndpointListenerForBridgeMembership());
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.client.internal.EndpointManager#referenceEndpoint(org.apache.geode.
   * distributed.internal.ServerLocation)
   */
  public Endpoint referenceEndpoint(ServerLocation server, DistributedMember memberId) {
    Endpoint endpoint = endpointMap.get(server);
    boolean addedEndpoint = false;
    if (endpoint == null || endpoint.isClosed()) {
      synchronized (this) {
        endpoint = endpointMap.get(server);
        if (endpoint == null || endpoint.isClosed()) {
          ConnectionStats stats = getStats(server);
          Map<ServerLocation, Endpoint> endpointMapTemp =
              new HashMap<ServerLocation, Endpoint>(endpointMap);
          endpoint = new Endpoint(this, ds, server, stats, memberId);
          listener.clearPdxRegistry(endpoint);
          endpointMapTemp.put(server, endpoint);
          endpointMap = Collections.unmodifiableMap(endpointMapTemp);
          addedEndpoint = true;
          poolStats.setServerCount(endpointMap.size());
        }
      }
    }

    endpoint.addReference();

    if (addedEndpoint) {
      listener.endpointNowInUse(endpoint);
    }

    return endpoint;
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.cache.client.internal.EndpointManager#serverCrashed(org.apache.geode.cache.
   * client.internal.Endpoint)
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
    synchronized (this) {
      Map<ServerLocation, Endpoint> endpointMapTemp =
          new HashMap<ServerLocation, Endpoint>(endpointMap);
      endpoint = endpointMapTemp.remove(endpoint.getLocation());
      if (endpoint != null) {
        endpointMap = Collections.unmodifiableMap(endpointMapTemp);
        removedEndpoint = true;
      }
      poolStats.setServerCount(endpointMap.size());
    }
    if (removedEndpoint) {
      PoolImpl pool = (PoolImpl) PoolManager.find(this.poolName);
      if (pool != null && pool.getMultiuserAuthentication()) {
        int size = 0;
        ArrayList<ProxyCache> proxyCaches = pool.getProxyCacheList();
        synchronized (proxyCaches) {
          for (ProxyCache proxyCache : proxyCaches) {
            try {
              Long userId =
                  proxyCache.getUserAttributes().getServerToId().remove(endpoint.getLocation());
              if (userId != null) {
                ++size;
              }
            } catch (CacheClosedException cce) {
              // If this call is triggered by a Cache.close(), then this can be
              // expected.
            }
          }
          if (logger.isDebugEnabled()) {
            logger.debug(
                "EndpointManagerImpl.removeEndpoint() Removed server {} from {} user's ProxyCache",
                endpoint.getLocation(), size);
          }
        }
        UserAttributes ua = UserAttributes.userAttributes.get();
        if (ua != null) {
          Long userId = ua.getServerToId().remove(endpoint.getLocation());
          if (userId != null && logger.isDebugEnabled()) {
            logger.debug(
                "EndpointManagerImpl.removeEndpoint() Removed server {} from thread local variable",
                endpoint.getLocation());
          }
        }
      } else if (pool != null && !pool.getMultiuserAuthentication()) {
        endpoint.getLocation().setUserId(-1);
      }
      if (crashed) {
        listener.endpointCrashed(endpoint);
      } else {
        listener.endpointNoLongerInUse(endpoint);
      }
    }
  }



  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.client.internal.EndpointManager#getEndpointMap()
   */
  public Map<ServerLocation, Endpoint> getEndpointMap() {
    return endpointMap;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.client.internal.EndpointManager#close()
   */
  public synchronized void close() {
    for (Iterator<ConnectionStats> itr = statMap.values().iterator(); itr.hasNext();) {
      ConnectionStats stats = itr.next();
      stats.close();
    }

    statMap.clear();
    endpointMap = Collections.emptyMap();
    listener.clear();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.client.internal.EndpointManager#addListener(org.apache.geode.cache.
   * client.internal.EndpointManagerImpl.EndpointListener)
   */
  public void addListener(EndpointManager.EndpointListener listener) {
    this.listener.addListener(listener);
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.cache.client.internal.EndpointManager#removeListener(org.apache.geode.cache.
   * client.internal.EndpointManagerImpl.EndpointListener)
   */
  public void removeListener(EndpointManager.EndpointListener listener) {
    this.listener.removeListener(listener);
  }

  private synchronized ConnectionStats getStats(ServerLocation location) {
    ConnectionStats stats = statMap.get(location);
    if (stats == null) {
      String statName = poolName + "-" + location.toString();
      PoolImpl pool = (PoolImpl) PoolManager.find(this.poolName);
      if (pool != null) {
        if (pool.getGatewaySender() != null) {
          stats = new ConnectionStats(new DummyStatisticsFactory(), statName,
              this.poolStats/* , this.gatewayStats */);
        }
      }
      if (stats == null) {
        stats = new ConnectionStats(ds, statName, this.poolStats/*
                                                                 * , this.gatewayStats
                                                                 */);
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
    // do nothing
  }

  protected static class EndpointListenerBroadcaster implements EndpointManager.EndpointListener {

    private volatile Set/* <EndpointListener> */<EndpointListener> endpointListeners =
        Collections.emptySet();

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
      for (Iterator<EndpointListener> itr = endpointListeners.iterator(); itr.hasNext();) {
        EndpointManager.EndpointListener listener = itr.next();
        listener.endpointCrashed(endpoint);
      }
    }

    public void endpointNoLongerInUse(Endpoint endpoint) {
      for (Iterator<EndpointListener> itr = endpointListeners.iterator(); itr.hasNext();) {
        EndpointManager.EndpointListener listener = itr.next();
        listener.endpointNoLongerInUse(endpoint);
      }
    }

    public void endpointNowInUse(Endpoint endpoint) {
      for (Iterator<EndpointListener> itr = endpointListeners.iterator(); itr.hasNext();) {
        EndpointManager.EndpointListener listener = itr.next();
        if (!(listener instanceof PdxRegistryRecoveryListener)) {
          listener.endpointNowInUse(endpoint);
        }
      }
    }

    public void clearPdxRegistry(Endpoint endpoint) {
      for (Iterator<EndpointListener> itr = endpointListeners.iterator(); itr.hasNext();) {
        EndpointManager.EndpointListener listener = itr.next();
        if (listener instanceof PdxRegistryRecoveryListener) {
          listener.endpointNowInUse(endpoint);
        }
      }
    }

  }



  public class EndpointListenerForBridgeMembership implements EndpointManager.EndpointListener {

    public void endpointCrashed(Endpoint endpoint) {
      if (cancelCriterion.isCancelInProgress()) {
        return;
      }
      InternalClientMembership.notifyServerCrashed(endpoint.getLocation());
    }

    public void endpointNoLongerInUse(Endpoint endpoint) {
      if (cancelCriterion.isCancelInProgress()) {
        return;
      }
      InternalClientMembership.notifyServerLeft(endpoint.getLocation());
    }

    public void endpointNowInUse(Endpoint endpoint) {
      if (cancelCriterion.isCancelInProgress()) {
        return;
      }
      InternalClientMembership.notifyServerJoined(endpoint.getLocation());
    }
  }

  public String getPoolName() {
    return poolName;
  }

}
