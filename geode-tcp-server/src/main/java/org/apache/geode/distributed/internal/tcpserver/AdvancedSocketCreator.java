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

public interface AdvancedSocketCreator {
  /**
   * Returns true if this socket creator is configured to use SSL by default
   */
  boolean useSSL();

  /**
   * After creating a socket connection use this method to initiate the SSL
   * handshake. If SSL is not enabled for the socket this method does nothing.
   */
  void handshakeIfSocketIsSSL(Socket socket, int timeout) throws IOException;

  /**
   * This method gives you pretty much full control over creating a server socket.
   * Use it with care.
   */
  ServerSocket createServerSocketUsingPortRange(InetAddress ba, int backlog,
      boolean isBindAddress, boolean useNIO,
      int tcpBufferSize, int[] tcpPortRange,
      boolean sslConnection) throws IOException;

  /**
   * This method gives you pretty much full control over creating a connection
   * to the given host/port. Use it with care.
   * <p>
   * Return a client socket, timing out if unable to connect and timeout > 0 (millis). The parameter
   * <i>timeout</i> is ignored if SSL is being used, as there is no timeout argument in the ssl
   * socket factory
   */
  Socket connect(HostAndPort addr, int timeout,
      ConnectionWatcher optionalWatcher, boolean allowClientSocketFactory,
      int socketBufferSize,
      boolean useSSL) throws IOException;
}
