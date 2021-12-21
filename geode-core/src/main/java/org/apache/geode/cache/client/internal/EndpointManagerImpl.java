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

import static org.apache.geode.logging.internal.spi.LoggingProvider.SECURITY_LOGGER_NAME;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.ServerLocationAndMemberId;
import org.apache.geode.internal.cache.PoolStats;
import org.apache.geode.internal.cache.tier.InternalClientMembership;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class EndpointManagerImpl implements EndpointManager {
  private static final Logger logger = LogService.getLogger();
  private static final Logger secureLogger = LogService.getLogger(SECURITY_LOGGER_NAME);

  private volatile Map<ServerLocationAndMemberId, Endpoint> endpointMap = Collections.emptyMap();
  private final Map<ServerLocationAndMemberId, ConnectionStats> statMap = new HashMap<>();
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

  @Override
  public Endpoint referenceEndpoint(ServerLocation server, DistributedMember memberId) {
    ServerLocationAndMemberId serverLocationAndMemberId =
        new ServerLocationAndMemberId(server, memberId.getUniqueId());
    Endpoint endpoint = endpointMap.get(serverLocationAndMemberId);
    boolean addedEndpoint = false;
    if (endpoint == null || endpoint.isClosed()) {
      synchronized (this) {
        endpoint = endpointMap.get(serverLocationAndMemberId);
        if (endpoint == null || endpoint.isClosed()) {
          ConnectionStats stats = getStats(serverLocationAndMemberId);
          Map<ServerLocationAndMemberId, Endpoint> endpointMapTemp = new HashMap<>(endpointMap);
          endpoint = new Endpoint(this, ds, server, stats, memberId);
          listener.clearPdxRegistry(endpoint);
          endpointMapTemp.put(serverLocationAndMemberId, endpoint);
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

  @Override
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
      Map<ServerLocationAndMemberId, Endpoint> endpointMapTemp = new HashMap<>(endpointMap);
      endpoint = endpointMapTemp.remove(new ServerLocationAndMemberId(endpoint.getLocation(),
          endpoint.getMemberId().getUniqueId()));
      if (endpoint != null) {
        endpointMap = Collections.unmodifiableMap(endpointMapTemp);
        removedEndpoint = true;
      }
      poolStats.setServerCount(endpointMap.size());
    }

    if (removedEndpoint) {
      PoolImpl pool = (PoolImpl) PoolManager.find(poolName);
      ServerLocation location = endpoint.getLocation();
      if (pool != null && pool.getMultiuserAuthentication()) {
        int size = 0;
        ArrayList<ProxyCache> proxyCaches = pool.getProxyCacheList();
        synchronized (proxyCaches) {
          for (ProxyCache proxyCache : proxyCaches) {
            try {
              Long userId =
                  proxyCache.getUserAttributes().getServerToId().remove(location);
              if (userId != null) {
                ++size;
              }
            } catch (CacheClosedException cce) {
              // If this call is triggered by a Cache.close(), then this can be
              // expected.
            }
          }
          secureLogger.debug(
              "EndpointManagerImpl.removeEndpoint() Removed server {} from {} user's ProxyCache",
              location, size);
        }
        UserAttributes ua = UserAttributes.userAttributes.get();
        if (ua != null) {
          Long userId = ua.getServerToId().remove(location);
          if (userId != null) {
            secureLogger.debug(
                "EndpointManagerImpl.removeEndpoint() Removed server {} from thread local variable",
                location);
          }
        }
      } else if (pool != null && !pool.getMultiuserAuthentication()) {
        secureLogger.debug("set the userId of {} to -1", location);
        location.setUserId(-1);
      }

      if (crashed) {
        listener.endpointCrashed(endpoint);
      } else {
        listener.endpointNoLongerInUse(endpoint);
      }
    }
  }



  @Override
  public Map<ServerLocationAndMemberId, Endpoint> getEndpointMap() {
    return endpointMap;
  }

  @Override
  public synchronized void close() {
    for (ConnectionStats stats : statMap.values()) {
      stats.close();
    }

    statMap.clear();
    endpointMap = Collections.emptyMap();
    listener.clear();
  }

  @Override
  public void addListener(EndpointManager.EndpointListener listener) {
    this.listener.addListener(listener);
  }

  @Override
  public void removeListener(EndpointManager.EndpointListener listener) {
    this.listener.removeListener(listener);
  }

  @VisibleForTesting
  public Set<EndpointListener> getListeners() {
    return listener.endpointListeners;
  }

  private synchronized ConnectionStats getStats(ServerLocationAndMemberId location) {
    ConnectionStats stats = statMap.get(location);
    if (stats == null) {
      PoolImpl pool = (PoolImpl) PoolManager.find(poolName);
      if (pool != null) {
        if (pool.getGatewaySender() != null) {
          String statName = pool.getGatewaySender().getId() + "-" + location.toString();
          stats = new ConnectionStats(ds, "GatewaySender", statName, poolStats);
        }
      }
      if (stats == null) {
        String statName = poolName + "-" + location.toString();
        stats = new ConnectionStats(ds, "Client", statName, poolStats);
      }
      statMap.put(location, stats);
    }

    return stats;
  }

  @Override
  public synchronized Map<ServerLocationAndMemberId, ConnectionStats> getAllStats() {
    return new HashMap<>(statMap);
  }

  @Override
  public int getConnectedServerCount() {
    return getEndpointMap().size();
  }

  protected static class EndpointListenerBroadcaster implements EndpointManager.EndpointListener {

    private volatile Set<EndpointListener> endpointListeners = Collections.emptySet();

    public synchronized void addListener(EndpointManager.EndpointListener listener) {
      HashSet<EndpointListener> tmpListeners = new HashSet<>(endpointListeners);
      tmpListeners.add(listener);
      endpointListeners = Collections.unmodifiableSet(tmpListeners);
    }

    public synchronized void clear() {
      endpointListeners = Collections.emptySet();
    }

    public void removeListener(EndpointManager.EndpointListener listener) {
      HashSet<EndpointListener> tmpListeners = new HashSet<>(endpointListeners);
      tmpListeners.remove(listener);
      endpointListeners = Collections.unmodifiableSet(tmpListeners);
    }

    @Override
    public void endpointCrashed(Endpoint endpoint) {
      for (EndpointListener listener : endpointListeners) {
        listener.endpointCrashed(endpoint);
      }
    }

    @Override
    public void endpointNoLongerInUse(Endpoint endpoint) {
      for (EndpointListener listener : endpointListeners) {
        listener.endpointNoLongerInUse(endpoint);
      }
    }

    @Override
    public void endpointNowInUse(Endpoint endpoint) {
      for (EndpointListener listener : endpointListeners) {
        if (!(listener instanceof PdxRegistryRecoveryListener)) {
          listener.endpointNowInUse(endpoint);
        }
      }
    }

    void clearPdxRegistry(Endpoint endpoint) {
      for (EndpointListener listener : endpointListeners) {
        if (listener instanceof PdxRegistryRecoveryListener) {
          listener.endpointNowInUse(endpoint);
        }
      }
    }

  }



  public class EndpointListenerForBridgeMembership implements EndpointManager.EndpointListener {

    @Override
    public void endpointCrashed(Endpoint endpoint) {
      if (cancelCriterion.isCancelInProgress()) {
        return;
      }
      InternalClientMembership.notifyServerCrashed(endpoint.getLocation());
    }

    @Override
    public void endpointNoLongerInUse(Endpoint endpoint) {
      if (cancelCriterion.isCancelInProgress()) {
        return;
      }
      InternalClientMembership.notifyServerLeft(endpoint.getLocation());
    }

    @Override
    public void endpointNowInUse(Endpoint endpoint) {
      if (cancelCriterion.isCancelInProgress()) {
        return;
      }
      InternalClientMembership.notifyServerJoined(endpoint.getLocation());
    }
  }

  @Override
  public String getPoolName() {
    return poolName;
  }

}
