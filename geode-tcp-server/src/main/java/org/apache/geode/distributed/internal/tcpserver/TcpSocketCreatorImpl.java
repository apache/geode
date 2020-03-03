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
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.geode.util.internal.GeodeGlossary;

/**
 * TcpSocketCreatorImpl is a simple implementation of TcpSocketCreator for use in the
 * tcp-server and membership subprojects. It does not support SSL - see SocketCreator
 * in geode-core for a more robust and feature-filled TcpSocketCreator implementation.
 */
public class TcpSocketCreatorImpl implements TcpSocketCreator {


  public static final boolean ENABLE_TCP_KEEP_ALIVE;

  static {
    // bug #49484 - customers want tcp/ip keep-alive turned on by default
    // to avoid dropped connections. It can be turned off by setting this
    // property to false
    String str = System.getProperty(GeodeGlossary.GEMFIRE_PREFIX + "setTcpKeepAlive");
    if (str != null) {
      ENABLE_TCP_KEEP_ALIVE = Boolean.valueOf(str);
    } else {
      ENABLE_TCP_KEEP_ALIVE = true;
    }
  }

  public TcpSocketCreatorImpl() {}


  @Override
  public boolean useSSL() {
    return false;
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

            InetSocketAddress address = new InetSocketAddress(isBindAddress ? ba : null, localPort);
            socket.bind(address, backlog);
          } else {
            socket = this.createServerSocket(localPort, backlog, isBindAddress ? ba : null,
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
              String.valueOf(nport)));
      throwMe.initCause(e);
      throw throwMe;
    }
    return result;
  }


  @Override
  public final Socket connect(InetAddress inetadd, int port, int timeout,
      ConnectionWatcher optionalWatcher, boolean clientSide)
      throws IOException {
    return connect(inetadd, port, timeout, optionalWatcher, clientSide, -1, useSSL());
  }

  @Override
  public Socket connect(InetAddress inetadd, int port, int timeout,
      ConnectionWatcher optionalWatcher, boolean clientSide,
      int socketBufferSize, boolean sslConnection) throws IOException {
    if (sslConnection) {
      throw new IllegalArgumentException();
    }
    Socket socket = null;
    if (clientSide) {
      socket = createCustomClientSocket(inetadd, port);
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
      try {
        socket.connect(new InetSocketAddress(inetadd, port), Math.max(timeout, 0));
      } finally {
        if (optionalWatcher != null) {
          optionalWatcher.afterConnect(socket);
        }
      }
    }
    return socket;
  }

  /**
   * reimplement this method to use a custom socket factory to create and configure a new
   * client-side socket
   *
   * @return the socket, or null if no custom client socket factory is available
   */
  protected Socket createCustomClientSocket(InetAddress inetaddr, int port) throws IOException {
    throw new UnsupportedOperationException(
        "custom client socket factory is not supported by this socket creator");
  }

  @Override
  public void handshakeIfSocketIsSSL(Socket socket, int timeout) throws IOException {
    if (useSSL()) {
      throw new IllegalStateException("Handshake on SSL connections is not supported");
    }
  }

}
