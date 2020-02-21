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

  ServerSocket createServerSocket(int nport, int backlog) throws IOException;

  ServerSocket createServerSocket(int nport, int backlog, InetAddress bindAddr)
      throws IOException;

  ServerSocket createServerSocketUsingPortRange(InetAddress ba, int backlog,
      boolean isBindAddress, boolean useNIO, int tcpBufferSize, int[] tcpPortRange,
      boolean sslConnection) throws IOException;

  Socket connect(HostAndPort addr, int timeout,
      ConnectionWatcher optionalWatcher, boolean clientSide) throws IOException;

  Socket connect(HostAndPort addr, int timeout,
      ConnectionWatcher optionalWatcher, boolean clientSide, int socketBufferSize,
      boolean sslConnection) throws IOException;

  void handshakeIfSocketIsSSL(Socket socket, int timeout) throws IOException;

}
