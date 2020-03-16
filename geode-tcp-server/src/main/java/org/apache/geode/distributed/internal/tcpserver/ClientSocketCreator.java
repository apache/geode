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
import java.net.Socket;

/**
 * ClientSocketCreator should be used, in most cases, in client caches and for WAN
 * connections. It allows the use of a socket factory in creating connections to servers.
 */
public interface ClientSocketCreator {

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
   * Create a connection to the given host/port using client-cache defaults for things
   * like socket buffer size
   */
  Socket connect(HostAndPort addr, int connectTimeout) throws IOException;

  Socket connect(HostAndPort addr, int connectTimeout, int socketBufferSize,
      TcpSocketFactory socketFactory)
      throws IOException;

}
