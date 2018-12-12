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

import java.util.Set;

import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.security.GemFireSecurityException;

/**
 * A factory for creating new connections.
 *
 * @since GemFire 5.7
 *
 */
public interface ConnectionFactory {

  /**
   * Create a client to server connection to the given server
   *
   * @param location the server to connection
   * @return a connection to that server, or null if a connection could not be established.
   * @throws GemFireSecurityException if there was a security exception while trying to establish a
   *         connections.
   */
  Connection createClientToServerConnection(ServerLocation location, boolean forQueue)
      throws GemFireSecurityException;

  /**
   * Returns the best server for this client to connect to. Returns null if no servers exist.
   *
   * @param currentServer if non-null then we are trying to replace a connection that we have to
   *        this server.
   * @param excludedServers the list of servers to skip over when finding a server to connect to
   */
  ServerLocation findBestServer(ServerLocation currentServer, Set excludedServers);

  /**
   * Create a client to server connection to any server that is not in the excluded list.
   *
   * @param excludedServers the list of servers to skip over when finding a server to connect to
   * @return a connection or null if a connection could not be established.
   * @throws GemFireSecurityException if there was a security exception trying to establish a
   *         connection.
   */
  Connection createClientToServerConnection(Set excludedServers) throws GemFireSecurityException;

  ClientUpdater createServerToClientConnection(Endpoint endpoint, QueueManager qManager,
      boolean isPrimary, ClientUpdater failedUpdater);

  ServerDenyList getDenyList();
}
