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
package org.apache.geode.internal;


import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketException;


/**
 * Provides helper methods for acquiring a set of unique available ports. It is not safe to simply
 * call AvailablePort.getRandomAvailablePort several times in a row without doing something to
 * ensure that they are unique. Although they are random, it is possible for subsequent calls to
 * getRandomAvailablePort to return the same integer, unless that port is put into use before
 * further calls to getRandomAvailablePort.
 */
public abstract class AvailablePortHelper {

  /**
   * Returns array of unique randomly available tcp ports of specified count.
   *
   * @param count number of desired ports
   * @return the ports
   */
  public static int[] getRandomAvailableTCPPorts(final int count) {
    final int[] ports = new int[count];
    for (int i = 0; i < count; ++i) {
      ports[i] = getRandomAvailableTCPPort();
    }
    return ports;
  }

  /**
   * Returns available ephemeral TCP port.
   */
  public static int getRandomAvailableTCPPort() {
    try (final ServerSocket socket = bindEphemeralTcpSocket()) {
      System.err.println("AvailablePortHelper.getRandomAvailableTCPPort(): " + socket);
      return socket.getLocalPort();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Returns available ephemeral UDP port.
   */
  public static int getRandomAvailableUDPPort() {
    try (final DatagramSocket socket = bindEphemeralUdpSocket()) {
      System.err.println("AvailablePortHelper.getRandomAvailableUDPPort(): " + socket);
      return socket.getLocalPort();
    } catch (SocketException e) {
      throw new IllegalStateException(e);
    }
  }

  private static DatagramSocket bindEphemeralUdpSocket() throws SocketException {
    return new DatagramSocket();
  }

  private static ServerSocket bindEphemeralTcpSocket() throws IOException {
    ServerSocket socket = new ServerSocket();
    socket.bind(new InetSocketAddress(0));
    return socket;
  }


}
