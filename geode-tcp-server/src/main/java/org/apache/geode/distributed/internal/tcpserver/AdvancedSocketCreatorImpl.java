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
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.geode.util.internal.GeodeGlossary;

/**
 * AdvancedSocketCreatorImpl is constructed and held by a TcpSocketCreator. It is
 * accessed through the method {@link TcpSocketCreator#forAdvancedUse()}.
 */
public class AdvancedSocketCreatorImpl implements AdvancedSocketCreator {

  public static final boolean ENABLE_TCP_KEEP_ALIVE;

  static {
    // customers want tcp/ip keep-alive turned on by default
    // to avoid dropped connections. It can be turned off by setting this
    // property to false
    String str = System.getProperty(GeodeGlossary.GEMFIRE_PREFIX + "setTcpKeepAlive");
    if (str != null) {
      ENABLE_TCP_KEEP_ALIVE = Boolean.valueOf(str);
    } else {
      ENABLE_TCP_KEEP_ALIVE = true;
    }
  }

  protected final TcpSocketCreatorImpl socketCreator;

  protected AdvancedSocketCreatorImpl(TcpSocketCreatorImpl socketCreator) {
    this.socketCreator = socketCreator;
  }

  @Override
  public boolean useSSL() {
    return socketCreator.useSSL();
  }

  @Override
  public void handshakeIfSocketIsSSL(Socket socket, int timeout) throws IOException {
    if (useSSL()) {
      throw new IllegalStateException("Handshake on SSL connections is not supported");
    }
  }

  @Override
  public Socket connect(HostAndPort addr, int timeout,
      ConnectionWatcher optionalWatcher, boolean allowClientSocketFactory,
      int socketBufferSize, boolean useSSL) throws IOException {
    if (useSSL) {
      throw new IllegalArgumentException();
    }
    Socket socket = null;
    if (allowClientSocketFactory) {
      socket = createCustomClientSocket(addr);
    }
    if (socket == null) {
      socket = new Socket();

      // Optionally enable SO_KEEPALIVE in the OS network protocol.
      socket.setKeepAlive(ENABLE_TCP_KEEP_ALIVE);

      if (socketBufferSize != -1) {
        socket.setReceiveBufferSize(socketBufferSize);
      }
      if (optionalWatcher != null) {
        optionalWatcher.beforeConnect(socket);
      }
      InetSocketAddress inetSocketAddress = addr.getSocketInetAddress();
      try {
        InetAddress serverAddress = inetSocketAddress.getAddress();
        if (serverAddress == null) {
          serverAddress = InetAddress.getByName(inetSocketAddress.getHostString());
        }
        socket.connect(
            new InetSocketAddress(serverAddress, inetSocketAddress.getPort()),
            Math.max(timeout, 0));
      } finally {
        if (optionalWatcher != null) {
          optionalWatcher.afterConnect(socket);
        }
      }
    }
    return socket;
  }

  @Override
  public final ServerSocket createServerSocketUsingPortRange(InetAddress ba, int backlog,
      boolean isBindAddress, boolean useNIO,
      int tcpBufferSize, int[] tcpPortRange,
      boolean sslConnection) throws IOException {
    try {
      // Get a random port from range.
      int startingPort = tcpPortRange[0]
          + ThreadLocalRandom.current().nextInt(tcpPortRange[1] - tcpPortRange[0] + 1);
      int localPort = startingPort;
      int portLimit = tcpPortRange[1];

      while (true) {
        if (localPort > portLimit) {
          if (startingPort != 0) {
            localPort = tcpPortRange[0];
            portLimit = startingPort - 1;
            startingPort = 0;
          } else {
            throw noFreePortException(
                String.format("Unable to find a free port in the membership-port-range: [%d,%d]",
                    tcpPortRange[0], tcpPortRange[1]));
          }
        }
        ServerSocket socket = null;
        try {
          if (useNIO) {
            ServerSocketChannel channel = ServerSocketChannel.open();
            socket = channel.socket();

            InetSocketAddress address =
                new InetSocketAddress(isBindAddress ? ba : null, localPort);
            socket.bind(address, backlog);
          } else {
            socket = socketCreator.clusterSocketCreator.createServerSocket(localPort,
                backlog, isBindAddress ? ba : null,
                tcpBufferSize, sslConnection);
          }
          return socket;
        } catch (java.net.SocketException ex) {
          if (socket != null && !socket.isClosed()) {
            socket.close();
          }
          localPort++;
        }
      }
    } catch (IOException e) {
      throw problemCreatingSocketInPortRangeException(
          "unable to create a socket in the membership-port range", e);
    }
  }

  /**
   * Overridable method for creating an exception during search of port-range
   */
  protected RuntimeException problemCreatingSocketInPortRangeException(String s, IOException e) {
    return new RuntimeException(s, e);
  }

  /**
   * Overridable method for creating an exception during search of port-range
   */
  protected RuntimeException noFreePortException(String reason) {
    return new RuntimeException(reason);
  }

  /**
   * reimplement this method to use a custom socket factory to create and configure a new
   * client-side socket
   *
   * @return the socket, or null if no custom client socket factory is available
   */
  protected Socket createCustomClientSocket(HostAndPort addr) throws IOException {
    throw new UnsupportedOperationException(
        "custom client socket factory is not supported by this socket creator");
  }


}
