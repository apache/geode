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

import static org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import javax.net.ServerSocketFactory;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;

import org.apache.logging.log4j.Logger;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.distributed.internal.tcpserver.ClusterSocketCreatorImpl;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.net.SSLParameterExtension;

class SCClusterSocketCreator extends ClusterSocketCreatorImpl {

  private static final Logger logger = LogService.getLogger();

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
    logger.info("BGB bound (TLS) address: {}, port: {}, at {}",
        bindAddr, nport, getStackTrace(new Throwable()));
    finishServerSocket(serverSocket);
    return serverSocket;
  }

  /**
   * Configure the SSLServerSocket based on this SocketCreator's settings.
   */
  private void finishServerSocket(SSLServerSocket serverSocket) {
    configureServerSocket(coreSocketCreator.getSslConfig(), serverSocket);
  }

  static void configureServerSocket(final SSLConfig sslConfig, final SSLServerSocket serverSocket) {
    serverSocket.setUseClientMode(false);
    if (sslConfig.isRequireAuth()) {
      serverSocket.setNeedClientAuth(true);
    }
    serverSocket.setEnableSessionCreation(true);

    final String[] protocols = sslConfig.getServerProtocolsAsStringArray();
    if (!SSLConfig.isAnyProtocols(protocols)) {
      serverSocket.setEnabledProtocols(protocols);
    }

    if (!sslConfig.isAnyCiphers()) {
      serverSocket.setEnabledCipherSuites(sslConfig.getCiphersAsStringArray());
    }

    final SSLParameterExtension sslParameterExtension = sslConfig.getSSLParameterExtension();
    if (sslParameterExtension != null) {
      final SSLParameters modifiedParams =
          sslParameterExtension.modifySSLServerSocketParameters(serverSocket.getSSLParameters());
      serverSocket.setSSLParameters(modifiedParams);
    }
  }


}
