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

import java.util.Map;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ServerLocation;

/**
 * The endpoint manager keeps track of which servers we are connected to. Other parts of the client
 * code can register listeners that will be notified about when endpoints are created or died. For
 * example the connection manager registers a listener to be notified if a server dies and closes
 * all of it's connections.
 *
 */
public interface EndpointManager {
  /**
   * Get the endpoint for this server and create it if it does not already exist. This increments
   * the reference count for the endpoint, so you should call {@link Endpoint#removeReference()}
   * after the endpoint is no longer in use.
   */
  Endpoint referenceEndpoint(ServerLocation server, DistributedMember memberId);

  /**
   * Indicate that a particular server has crashed. All of the listeners will be notified that the
   * server has crashed.
   *
   */
  void serverCrashed(Endpoint endpoint);

  /**
   * Get the map of all endpoints currently in use.
   *
   * @return a map for ServerLocation->Endpoint
   */
  Map<ServerLocation, Endpoint> getEndpointMap();

  void close();

  /**
   * Add a listener which will be notified when the state of an endpoint changes.
   */
  void addListener(EndpointManager.EndpointListener listener);

  /**
   * Remove a listener.
   */
  void removeListener(EndpointManager.EndpointListener listener);

  /**
   * Get the stats for all of the servers we ever connected too.
   *
   * @return a map of ServerLocation-> ConnectionStats
   */
  Map<ServerLocation, ConnectionStats> getAllStats();

  /**
   * Test hook that returns the number of servers we currently have connections to.
   */
  int getConnectedServerCount();

  interface EndpointListener {

    void endpointNoLongerInUse(Endpoint endpoint);

    void endpointCrashed(Endpoint endpoint);

    void endpointNowInUse(Endpoint endpoint);
  }

  class EndpointListenerAdapter implements EndpointListener {

    public void endpointCrashed(Endpoint endpoint) {}

    public void endpointNoLongerInUse(Endpoint endpoint) {}

    public void endpointNowInUse(Endpoint endpoint) {}
  }

  String getPoolName();
}
