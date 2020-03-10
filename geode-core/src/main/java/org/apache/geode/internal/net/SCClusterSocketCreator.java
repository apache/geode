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
import java.net.ServerSocket;
import java.net.Socket;

import javax.net.ServerSocketFactory;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.distributed.internal.tcpserver.ClusterSocketCreatorImpl;
import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.net.SSLParameterExtension;

class SCClusterSocketCreator extends ClusterSocketCreatorImpl {
  private final SocketCreator coreSocketCreator;

  protected SCClusterSocketCreator(SocketCreator socketCreator) {
    super(socketCreator);
    coreSocketCreator = socketCreator;
  }

  @Override
  public void handshakeIfSocketIsSSL(Socket socket, int timeout) throws IOException {
    coreSocketCreator.handshakeIfSocketIsSSL(socket, timeout);
  }

  public ServerSocket createServerSocket(int nport, int backlog, InetAddress bindAddr,
      int socketBufferSize) throws IOException {
    return createServerSocket(nport, backlog, bindAddr, socketBufferSize,
        coreSocketCreator.useSSL());
  }

  @Override
  protected ServerSocket createServerSocket(int nport, int backlog, InetAddress bindAddr,
      int socketBufferSize, boolean sslConnection) throws IOException {
    coreSocketCreator.printConfig();
    if (!sslConnection) {
      return super.createServerSocket(nport, backlog, bindAddr, socketBufferSize, sslConnection);
    }
    if (coreSocketCreator.getSslContext() == null) {
      throw new GemFireConfigException(
          "SSL not configured correctly, Please look at previous error");
    }
    ServerSocketFactory ssf = coreSocketCreator.getSslContext().getServerSocketFactory();
    SSLServerSocket serverSocket = (SSLServerSocket) ssf.createServerSocket();
    serverSocket.setReuseAddress(true);
    // If necessary, set the receive buffer size before binding the socket so
    // that large buffers will be allocated on accepted sockets (see
    // java.net.ServerSocket.setReceiverBufferSize javadocs)
    if (socketBufferSize != -1) {
      serverSocket.setReceiveBufferSize(socketBufferSize);
    }
    serverSocket.bind(new InetSocketAddress(bindAddr, nport), backlog);
    finishServerSocket(serverSocket);
    return serverSocket;
  }

  /**
   * Configure the SSLServerSocket based on this SocketCreator's settings.
   */
  private void finishServerSocket(SSLServerSocket serverSocket) {
    SSLConfig sslConfig = coreSocketCreator.getSslConfig();
    serverSocket.setUseClientMode(false);
    if (sslConfig.isRequireAuth()) {
      // serverSocket.setWantClientAuth( true );
      serverSocket.setNeedClientAuth(true);
    }
    serverSocket.setEnableSessionCreation(true);

    // restrict protocols
    String[] protocols = sslConfig.getProtocolsAsStringArray();
    if (!"any".equalsIgnoreCase(protocols[0])) {
      serverSocket.setEnabledProtocols(protocols);
    }
    // restrict ciphers
    String[] ciphers = sslConfig.getCiphersAsStringArray();
    if (!"any".equalsIgnoreCase(ciphers[0])) {
      serverSocket.setEnabledCipherSuites(ciphers);
    }

    SSLParameterExtension sslParameterExtension = sslConfig.getSSLParameterExtension();
    if (sslParameterExtension != null) {
      SSLParameters modifiedParams =
          sslParameterExtension.modifySSLServerSocketParameters(serverSocket.getSSLParameters());
      serverSocket.setSSLParameters(modifiedParams);
    }

  }


}
