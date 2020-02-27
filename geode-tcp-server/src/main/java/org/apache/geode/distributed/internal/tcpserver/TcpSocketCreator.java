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

package org.apache.geode.distributed.internal.tcpserver;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;


/**
 * Create sockets for TcpServer (and TcpClient).
 */
public interface TcpSocketCreator {
  boolean useSSL();

  /**
   * After creating a socket connection use this method to initiate the SSL
   * handshake. If SSL is not enabled for the socket this method does nothing.
   */
  void handshakeIfSocketIsSSL(Socket socket, int timeout) throws IOException;

  /**
   * Create a server socket that listens on all interfaces
   */
  ServerSocket createServerSocket(int nport, int backlog) throws IOException;

  /**
   * Create a server socket that is bound to the given address
   */
  ServerSocket createServerSocket(int nport, int backlog, InetAddress bindAddr)
      throws IOException;

  /**
   * Create a connection to the given host/port using client-cache defaults for things
   * like socket buffer size
   */
  Socket connectForClient(HostAndPort addr, int connectTimeout) throws IOException;

  /**
   * Creates a connection to the given host/port
   */
  Socket connectForClient(HostAndPort addr, int connectTimeout, int socketBufferSize)
      throws IOException;

  /**
   * Creates a connection to the given host/port. This method ignores any
   * custom client-side socket factory that may be installed.
   */
  Socket connectForServer(HostAndPort addr) throws IOException;

  /**
   * Creates a connection to the given host/port. The ConnectionWatcher may be null.
   * If it is not null the watcher is notified before and after the connection is created.
   * This is typically used by a timer task in order to take action should connection-formation
   * take too long.
   * <p>
   * This method ignores any custom client-side socket factory that may be installed.
   */
  Socket connectForServer(HostAndPort addr, int timeout,
      ConnectionWatcher optionalWatcher) throws IOException;


  /**
   * This method gives you pretty much full control over creating a server socket.
   * Use it with care.
   */
  ServerSocket createServerSocketUsingPortRange(InetAddress ba, int backlog,
      boolean isBindAddress, boolean useNIO, int tcpBufferSize, int[] tcpPortRange,
      boolean sslConnection) throws IOException;

  /**
   * This method gives you pretty much full control over creating a connection
   * to the given host/port. Use it with care.
   */
  Socket connect(HostAndPort addr, int timeout,
      ConnectionWatcher optionalWatcher, boolean allowClientSocketFactory, int socketBufferSize,
      boolean useSSL) throws IOException;

}
