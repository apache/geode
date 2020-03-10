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
package org.apache.geode.cache.client.internal.pooling;

import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.cache.client.AllConnectionsInUseException;
import org.apache.geode.cache.client.NoAvailableServersException;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.client.internal.Connection;
import org.apache.geode.distributed.PoolCancelledException;
import org.apache.geode.distributed.internal.ServerLocation;

/**
 * A pool for managing client to server connections. This interface allows connections to be checked
 * out and checked in, and keeps the number of connections within the min and max boundaries.
 *
 * @since GemFire 5.7
 *
 */
public interface ConnectionManager {

  /**
   * Borrow an existing idle connection or create a new one. Fails after one failed attempt to
   * create a new connection.
   *
   * @param aquireTimeout The amount of time to wait for a connection to become available.
   * @return A connection to use.
   * @throws AllConnectionsInUseException If the maximum number of connections are already, in use
   *         and no connection becomes free within the aquireTimeout.
   * @throws NoAvailableServersException if we can't connect to any server
   * @throws ServerOperationException if there is an issue with security or connecting to a gateway
   * @throws PoolCancelledException if the pool is being shut down
   */
  Connection borrowConnection(long aquireTimeout)
      throws AllConnectionsInUseException, NoAvailableServersException;

  /**
   * Borrow an existing idle connection or create a new one to a specific server. Fails after one
   * failed attempt to create a new connection. May cause pool to exceed maxConnections by one, if
   * no connection is available.
   *
   * @param server The server the connection needs to be to.
   * @param aquireTimeout The amount of time to wait for a connection to become available, if
   *        onlyUseExistingCnx is set to true.
   * @param onlyUseExistingCnx if true, will not create a new connection if none are available.
   * @return A connection to use.
   * @throws AllConnectionsInUseException If there is no available connection on the desired server,
   *         and onlyUseExistingCnx is set.
   * @throws NoAvailableServersException If we can't connect to any server
   * @throws ServerConnectivityException If finding a connection and creating a connection both fail
   *         to return a connection
   *
   */
  Connection borrowConnection(ServerLocation server, long aquireTimeout, boolean onlyUseExistingCnx)
      throws AllConnectionsInUseException, NoAvailableServersException;

  /**
   * Return a connection to the pool. The connection should not be used after it is returned.
   *
   * @param connection the connection to return
   */
  void returnConnection(Connection connection);

  /**
   * Return a connection to the pool. The connection should not be used after it is returned.
   *
   * @param connection the connection to return
   * @param accessed if true then the connection was accessed
   */
  void returnConnection(Connection connection, boolean accessed);

  /**
   * Start the idle expiration for the pool and prefill the pool.
   */
  void start(ScheduledExecutorService backgroundProcessor);

  /**
   * Shutdown the pool.
   *
   * @param keepAlive whether to signal to servers to keep the connection alive
   */
  void close(boolean keepAlive);

  /**
   * Exchange one connection for a new connection to a different server. This method can break the
   * max connection contract if there is no available connection and maxConnections has already been
   * reached.
   *
   * @param conn connection to exchange. It will be returned to the pool (or destroyed if it has
   *        been invalidated).
   * @param excludedServers servers to exclude when looking for a new connection
   * @throws InternalGemFireException if the found connection is already active
   * @throws NoAvailableServersException if no servers are available to connect to
   * @throws ServerOperationException if creating a connection fails due to authentication issues
   * @return a new connection to the pool to a server that is not in the list of excluded servers
   */
  Connection exchangeConnection(Connection conn, Set<ServerLocation> excludedServers)
      throws AllConnectionsInUseException;

  /**
   * Test hook to find out current number of connections this manager has.
   */
  int getConnectionCount();

  void emergencyClose();
}
