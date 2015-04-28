/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal.pooling;

import java.util.Set;

import java.util.concurrent.ScheduledExecutorService;
import com.gemstone.gemfire.cache.client.AllConnectionsInUseException;
import com.gemstone.gemfire.cache.client.NoAvailableServersException;
import com.gemstone.gemfire.cache.client.internal.Connection;
import com.gemstone.gemfire.distributed.internal.ServerLocation;

/**
 * A pool for managing client to server connections. This interface
 * allows connections to be checked out and checked in, and keeps
 * the number of connections within the min and max boundaries. 
 * @author dsmith
 * @since 5.7
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
