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
package org.apache.geode.cache.client.internal.pooling;

import java.util.Set;

import java.util.concurrent.ScheduledExecutorService;
import org.apache.geode.cache.client.AllConnectionsInUseException;
import org.apache.geode.cache.client.NoAvailableServersException;
import org.apache.geode.cache.client.internal.Connection;
import org.apache.geode.distributed.internal.ServerLocation;

/**
 * A pool for managing client to server connections. This interface
 * allows connections to be checked out and checked in, and keeps
 * the number of connections within the min and max boundaries. 
 * @since GemFire 5.7
 *
 */
public interface ConnectionManager {

  /**
   * Borrow an existing idle connection or create a new one.
   * 
   * @param aquireTimeout
   *                The amount of time to wait for a connection to become
   *                available.
   * @return A connection to use.
   * @throws AllConnectionsInUseException
   *                 If the maximum number of connections are already in use and
   *                 no connection becomes free within the aquireTimeout.
   * @throws NoAvailableServersException
   *                 if we can't connect to any server
   */
  Connection borrowConnection(long aquireTimeout)
      throws AllConnectionsInUseException, NoAvailableServersException;

  /**
   * Borrow an existing idle connection or create a new one to a specific server.
   *
   * @param server  The server the connection needs to be to.
   * @param aquireTimeout
   *                The amount of time to wait for a connection to become
   *                available.
   * @return A connection to use.
   * @throws AllConnectionsInUseException
   *                 If the maximum number of connections are already in use and
   *                 no connection becomes free within the aquireTimeout.
   * @throws NoAvailableServersException
   *                 if we can't connect to any server
   * 
   */
  Connection borrowConnection(ServerLocation server, long aquireTimeout,boolean onlyUseExistingCnx)
      throws AllConnectionsInUseException, NoAvailableServersException;

  /**
   * Return a connection to the pool. The connection should not be
   * used after it is returned.
   * @param connection the connection to return
   */
  void returnConnection(Connection connection);

  /**
   * Return a connection to the pool. The connection should not be
   * used after it is returned.
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
   * @param keepAlive
   *                whether to signal to servers to keep the connection alive
   */
  void close(boolean keepAlive);

  /**
   * Exchange one connection for a new connection to a different server.
   * 
   * @param conn
   *                connection to exchange. It will be returned to the pool (or
   *                destroyed if it has been invalidated).
   * @param excludedServers
   *                servers to exclude when looking for a new connection
   * @return a new connection to the pool to a server that is not in the list of
   *         excluded servers
   * @throws AllConnectionsInUseException
   */
  Connection exchangeConnection(Connection conn,
      Set/* <ServerLocation> */excludedServers, long aquireTimeout)
      throws AllConnectionsInUseException;

  /**
   * Test hook to find out current number of connections this manager has.
   */
  public int getConnectionCount();

  void emergencyClose();

  /**
   * Used to active a thread local connection
   */
  public void activate(Connection conn);
  /**
   * Used to passivate a thread local connection
   */
  public void passivate(Connection conn, boolean accessed);

  public Connection getConnection(Connection conn);
}
