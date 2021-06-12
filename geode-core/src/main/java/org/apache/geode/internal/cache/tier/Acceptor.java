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
package org.apache.geode.internal.cache.tier;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Set;

import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.internal.cache.tier.sockets.ClientHealthMonitor;
import org.apache.geode.internal.cache.tier.sockets.CommBufferPool;
import org.apache.geode.internal.cache.tier.sockets.ConnectionListener;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.net.NioFilter;
import org.apache.geode.internal.net.SocketCloser;

/**
 * Defines the message listener/acceptor interface which is the GemFire cache server. Multiple
 * communication stacks may provide implementations for the interfaces defined in this package
 *
 * @since GemFire 2.0.2
 */
public interface Acceptor extends CommBufferPool {

  /**
   * Listens for a client to connect and establishes a connection to that client.
   */
  void accept() throws Exception;

  /**
   * Starts this acceptor thread
   */
  void start() throws IOException;

  /**
   * Returns the port on which this acceptor listens for connections from clients.
   */
  int getPort();

  /**
   * returns the server's name string, including the inet address and port that the server is
   * listening on
   */
  String getServerName();

  /**
   * Closes this acceptor thread
   */
  void close();

  /**
   * Is this acceptor running (handling connections)?
   */
  boolean isRunning();

  /**
   * Returns the CacheClientNotifier used by this Acceptor.
   */
  CacheClientNotifier getCacheClientNotifier();

  CacheServerStats getStats();

  String getExternalAddress();

  void emergencyClose();

  ServerConnection[] getAllServerConnectionList();

  int getClientServerConnectionCount();

  Set<ServerConnection> getAllServerConnections();

  CachedRegionHelper getCachedRegionHelper();

  long getAcceptorId();

  boolean isGatewayReceiver();

  boolean isSelector();

  InetAddress getServerInetAddress();

  void notifyCacheMembersOfClose();

  ClientHealthMonitor getClientHealthMonitor();

  ConnectionListener getConnectionListener();

  void refuseHandshake(OutputStream out, String message, byte exception) throws IOException;

  void refuseHandshake(String message, byte exception, NioFilter ioFilter,
      Socket socket) throws IOException;

  void registerServerConnection(ServerConnection serverConnection);

  void unregisterServerConnection(ServerConnection serverConnection);

  void decClientServerConnectionCount();

  int getMaximumTimeBetweenPings();

  SocketCloser getSocketCloser();
}
