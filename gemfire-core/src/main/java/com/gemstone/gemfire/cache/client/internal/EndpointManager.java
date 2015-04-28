/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

import java.util.Map;

import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.ServerLocation;

/**
 * The endpoint manager keeps track of which servers we 
 * are connected to. Other parts of the client code can register
 * listeners that will be notified about when endpoints are created
 * or died. For example the connection manager registers a listener
 * to be notified if a server dies and closes all of it's connections.
 * @author dsmith
 *
 */
public interface EndpointManager {
  /**
   * Get the endpoint for this server and create it if it does not already exist.
   * This increments the reference count for the endpoint,
   *  so you should call {@link Endpoint#removeReference()} after
   *  the endpoint is no longer in use.
   */
  Endpoint referenceEndpoint(ServerLocation server, DistributedMember memberId);

  /**
   * Indicate that a particular server has crashed. All of the listeners will be notified
   * that the server has crashed.
   * @param endpoint
   */
  void serverCrashed(Endpoint endpoint);

  /**
   * Get the map of all endpoints currently in use.
   * @return a map for ServerLocation->Endpoint
   */
  Map<ServerLocation, Endpoint> getEndpointMap();

  void close();

  /** Add a listener which will be notified when the state of
   * an endpoint changes.
   */
  void addListener(EndpointManager.EndpointListener listener);

  /**
   * Remove a listener. 
   */
  void removeListener(EndpointManager.EndpointListener listener);
  
  /**
   * Get the stats for all of the servers we ever connected too.
   * @return a map of ServerLocation-> ConnectionStats
   */
  public Map getAllStats();

  /**
   * Test hook that returns the number of servers we currently have connections to.
   */
  public int getConnectedServerCount();

  public static interface EndpointListener {
    
    void endpointNoLongerInUse(Endpoint endpoint);
    
    void endpointCrashed(Endpoint endpoint);
    
    void endpointNowInUse(Endpoint endpoint);
  }

  public static class EndpointListenerAdapter implements EndpointListener {
  
    public void endpointCrashed(Endpoint endpoint) {
    }
  
    public void endpointNoLongerInUse(Endpoint endpoint) {
    }
  
    public void endpointNowInUse(Endpoint endpoint) {
    }
  }

  public String getPoolName();
}
