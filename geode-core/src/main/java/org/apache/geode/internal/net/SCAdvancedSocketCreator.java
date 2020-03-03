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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import javax.net.SocketFactory;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIServerName;

import org.apache.logging.log4j.Logger;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.SystemConnectException;
import org.apache.geode.distributed.internal.tcpserver.AdvancedSocketCreatorImpl;
import org.apache.geode.distributed.internal.tcpserver.ConnectionWatcher;
import org.apache.geode.distributed.internal.tcpserver.HostAndPort;
import org.apache.geode.logging.internal.log4j.api.LogService;

class SCAdvancedSocketCreator extends AdvancedSocketCreatorImpl {
  private static final Logger logger = LogService.getLogger();

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
      boolean useSSL) throws IOException {

    coreSocketCreator.printConfig();

    if (!useSSL) {
      return super.connect(addr, timeout, optionalWatcher, allowClientSocketFactory,
          socketBufferSize,
          useSSL);
    }

    // create an SSL connection

    if (coreSocketCreator.getSslContext() == null) {
      throw new GemFireConfigException(
          "SSL not configured correctly, Please look at previous error");
    }
    SocketFactory sf = coreSocketCreator.getSslContext().getSocketFactory();
    final Socket socket;
    socket = sf.createSocket();

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

      final InetSocketAddress sniProxyAddress =
          coreSocketCreator.getSslConfig().getSniProxyAddress();
      final boolean provideSNI = sniProxyAddress != null;

      if (provideSNI) {
        socket.connect(sniProxyAddress, Math.max(timeout, 0));
        coreSocketCreator.configureClientSSLSocket(socket, timeout, sslParameters -> {
          final List<SNIServerName> sniHostNames = new ArrayList<>(1);
          sniHostNames.add(new SNIHostName(addr.getHostName()));
          sslParameters.setServerNames(sniHostNames);
        });
      } else {
        final InetSocketAddress sockaddr;
        InetSocketAddress sockaddrTemp = addr.getSocketInetAddress();
        if (sockaddrTemp.getAddress() == null) {
          final InetAddress address = InetAddress.getByName(sockaddrTemp.getHostString());
          sockaddr = new InetSocketAddress(address, sockaddrTemp.getPort());
        } else {
          sockaddr = sockaddrTemp;
        }
        socket.connect(sockaddr, Math.max(timeout, 0));
        coreSocketCreator.configureClientSSLSocket(socket, timeout, _ignored -> {
        });
      }

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
