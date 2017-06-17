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
package org.apache.geode.internal.cache.tier.sockets;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.cache.CacheServerImpl;

import java.util.List;
import java.util.Set;

/**
 * Provides tests a way to access CacheServer, AcceptorImpl and ServerConnection
 */
public class CacheServerUtils {

  /**
   * Returns single CacheServer for the specified Cache instance
   */
  public static CacheServer getCacheServer(final Cache cache) {
    List<CacheServer> cacheServers = cache.getCacheServers();
    CacheServer cacheServer = cacheServers.get(0);
    return cacheServer;
  }

  /**
   * Returns AcceptorImpl for the specified CacheServer instance
   */
  public static AcceptorImpl getAcceptorImpl(final CacheServer cacheServer) {
    AcceptorImpl acceptor = ((CacheServerImpl) cacheServer).getAcceptor();
    return acceptor;
  }

  /**
   * Returns single ServerConnection for the specified CacheServer instance
   */
  public static ServerConnection getServerConnection(final CacheServer cacheServer) {
    AcceptorImpl acceptor = ((CacheServerImpl) cacheServer).getAcceptor();
    Set<ServerConnection> serverConnections = acceptor.getAllServerConnections();
    ServerConnection serverConnection = serverConnections.iterator().next(); // null
    return serverConnection;
  }
}
