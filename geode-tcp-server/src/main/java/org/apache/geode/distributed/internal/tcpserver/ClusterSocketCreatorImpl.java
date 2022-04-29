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
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * ClusterSocketCreatorImpl is constructed and held by a TcpSocketCreator. It is
 * accessed through the method {@link TcpSocketCreator#forCluster()}.
 */
public class ClusterSocketCreatorImpl implements ClusterSocketCreator {
  private final TcpSocketCreatorImpl socketCreator;

  protected ClusterSocketCreatorImpl(TcpSocketCreatorImpl socketCreator) {

    this.socketCreator = socketCreator;
  }

  public boolean useSSL() {
    return socketCreator.useSSL();
  }

  public void handshakeIfSocketIsSSL(Socket socket, int timeout) throws IOException {
    if (useSSL()) {
      throw new IllegalStateException("Handshake on SSL connections is not supported");
    }
  }

  @Override
  public final ServerSocket createServerSocket(int nport, int backlog) throws IOException {
    return createServerSocket(nport, backlog, null, -1, useSSL());
  }

  @Override
  public final ServerSocket createServerSocket(int nport, int backlog, InetAddress bindAddr)
      throws IOException {
    return createServerSocket(nport, backlog, bindAddr, -1, useSSL());
  }

  /**
   * Overridable method for creating a server socket. Override this if you are implementing
   * SSL communications or otherwise need to customize server socket creation.
   */
  protected ServerSocket createServerSocket(int nport, int backlog, InetAddress bindAddr,
      int socketBufferSize, boolean sslConnection) throws IOException {
    if (sslConnection) {
      throw new UnsupportedOperationException();
    }
    ServerSocket result = new ServerSocket();
    result.setReuseAddress(true);
    if (socketBufferSize != -1) {
      result.setReceiveBufferSize(socketBufferSize);
    }
    try {
      result.bind(new InetSocketAddress(bindAddr, nport), backlog);
    } catch (BindException e) {
      BindException throwMe =
          new BindException(String.format("Failed to create server socket on %s[%s]",
              bindAddr == null ? InetAddress.getLocalHost().getHostAddress() : bindAddr,
              nport));
      throwMe.initCause(e);
      throw throwMe;
    }
    return result;
  }

  /**
   * Return a client socket. This method is used by peers.
   */
  public Socket connect(HostAndPort addr) throws IOException {
    return socketCreator.connect(addr, 0, null, false, -1);
  }

  @Override
  public final Socket connect(HostAndPort addr, int timeout,
      ConnectionWatcher optionalWatcher,
      TcpSocketFactory socketFactory)
      throws IOException {
    return socketCreator.advancedSocketCreator.connect(addr, timeout, optionalWatcher, false, -1,
        useSSL(), socketFactory);
  }


}
