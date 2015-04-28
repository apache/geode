/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

import java.util.Set;

import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.security.GemFireSecurityException;

/**
 * A factory for creating new connections.
 * @author dsmith
 * @since 5.7
 *
 */
public interface ConnectionFactory {

  /**
   * Create a client to server connection to the given server
   * @param location the server to connection
   * @return a connection to that server, or null if 
   * a connection could not be established.
   * @throws GemFireSecurityException if there was a security exception 
   * while trying to establish a connections.
   */
  Connection createClientToServerConnection(ServerLocation location, boolean forQueue)
    throws GemFireSecurityException;

  /**
   * Returns the best server for this client to connect to.
   * Returns null if no servers exist.
   * @param currentServer if non-null then we are trying to replace a connection
   *                      that we have to this server.
   * @param excludedServers the list of servers
   * to skip over when finding a server to connect to
   */
  ServerLocation findBestServer(ServerLocation currentServer, Set excludedServers);

  /**
   * Create a client to server connection to any server
   * that is not in the excluded list.
   * @param excludedServers the list of servers
   * to skip over when finding a server to connect to
   * @return a connection or null if 
   * a connection could not be established.
   * @throws GemFireSecurityException if there was a security exception
   * trying to establish a connection.
   */
  Connection createClientToServerConnection(Set excludedServers) throws GemFireSecurityException;
  
  ClientUpdater createServerToClientConnection(Endpoint endpoint, QueueManager qManager, boolean isPrimary, ClientUpdater failedUpdater);
  
  ServerBlackList getBlackList();
}
