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

package org.apache.geode.distributed.internal.membership.adapter;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.geode.distributed.internal.tcpserver.ConnectionWatcher;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketCreator;

/**
 * Adapt a SocketCreator from geode-core to function as a TcpSocketAdapter
 * in geode-tcp-server
 */
public class SocketCreatorAdapter implements TcpSocketCreator {
  final org.apache.geode.internal.net.SocketCreator socketCreator;

  private SocketCreatorAdapter(final org.apache.geode.internal.net.SocketCreator socketCreator) {
    this.socketCreator = socketCreator;
  }

  public static TcpSocketCreator asTcpSocketCreator(
      final org.apache.geode.internal.net.SocketCreator socketCreator) {
    return new SocketCreatorAdapter(socketCreator);
  }

  @Override
  public boolean useSSL() {
    return socketCreator.useSSL();
  }

  @Override
  public ServerSocket createServerSocket(final int nport, final int backlog) throws IOException {
    return socketCreator.createServerSocket(nport, backlog);
  }

  @Override
  public ServerSocket createServerSocket(final int nport, final int backlog,
      final InetAddress bindAddr)
      throws IOException {
    return socketCreator.createServerSocket(nport, backlog, bindAddr);
  }

  @Override
  public Socket connect(final InetAddress inetadd, final int port, final int timeout,
      final ConnectionWatcher optionalWatcher, final boolean clientSide)
      throws IOException {
    return socketCreator.connect(inetadd, port, timeout, optionalWatcher, clientSide);
  }

  @Override
  public void handshakeIfSocketIsSSL(final Socket socket, final int timeout) throws IOException {
    socketCreator.handshakeIfSocketIsSSL(socket, timeout);
  }
}
