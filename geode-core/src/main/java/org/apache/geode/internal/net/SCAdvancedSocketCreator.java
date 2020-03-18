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
package org.apache.geode.internal.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import javax.net.ssl.SSLSocketFactory;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.SystemConnectException;
import org.apache.geode.distributed.internal.tcpserver.AdvancedSocketCreatorImpl;
import org.apache.geode.distributed.internal.tcpserver.ConnectionWatcher;
import org.apache.geode.distributed.internal.tcpserver.HostAndPort;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketFactory;

class SCAdvancedSocketCreator extends AdvancedSocketCreatorImpl {
  final SocketCreator coreSocketCreator;

  protected SCAdvancedSocketCreator(SocketCreator socketCreator) {
    super(socketCreator);
    coreSocketCreator = socketCreator;
  }

  @Override
  public void handshakeIfSocketIsSSL(Socket socket, int timeout) throws IOException {
    coreSocketCreator.handshakeIfSocketIsSSL(socket, timeout);
  }

  @Override
  public Socket connect(HostAndPort addr, int timeout,
      ConnectionWatcher optionalWatcher, boolean allowClientSocketFactory, int socketBufferSize,
      boolean useSSL, TcpSocketFactory socketFactory) throws IOException {

    coreSocketCreator.printConfig();

    if (!useSSL) {
      return super.connect(addr, timeout, optionalWatcher, allowClientSocketFactory,
          socketBufferSize,
          useSSL, socketFactory);
    }

    // create an SSL connection

    InetSocketAddress sockaddr = addr.getSocketInetAddress();

    if (coreSocketCreator.getSslContext() == null) {
      throw new GemFireConfigException(
          "SSL not configured correctly, Please look at previous error");
    }
    Socket socket = socketFactory.createSocket();

    // Optionally enable SO_KEEPALIVE in the OS network protocol.
    socket.setKeepAlive(ENABLE_TCP_KEEP_ALIVE);

    // If necessary, set the receive buffer size before connecting the
    // socket so that large buffers will be allocated on accepted sockets
    // (see java.net.Socket.setReceiverBufferSize javadocs for details)
    if (socketBufferSize != -1) {
      socket.setReceiveBufferSize(socketBufferSize);
    }

    try {
      if (optionalWatcher != null) {
        optionalWatcher.beforeConnect(socket);
      }
      socket.connect(sockaddr, Math.max(timeout, 0));
      SSLSocketFactory sf = coreSocketCreator.getSslContext().getSocketFactory();
      socket = sf.createSocket(socket, addr.getHostName(), addr.getPort(), true);
      coreSocketCreator.configureClientSSLSocket(socket, addr, timeout);
      return socket;

    } finally {
      if (optionalWatcher != null) {
        optionalWatcher.afterConnect(socket);
      }
    }
  }

  @Override
  protected RuntimeException problemCreatingSocketInPortRangeException(String s, IOException e) {
    return new GemFireConfigException(s, e);
  }

  @Override
  protected RuntimeException noFreePortException(String reason) {
    return new SystemConnectException(reason);
  }

  @Override
  protected Socket createCustomClientSocket(HostAndPort addr) throws IOException {
    if (coreSocketCreator.getClientSocketFactory() != null) {
      InetSocketAddress inetSocketAddress = addr.getSocketInetAddress();
      return coreSocketCreator.getClientSocketFactory().createSocket(inetSocketAddress.getAddress(),
          inetSocketAddress.getPort());
    }
    return null;
  }



}
