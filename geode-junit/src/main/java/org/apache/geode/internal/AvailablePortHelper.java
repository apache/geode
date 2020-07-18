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
import java.util.ArrayList;
import java.util.List;

import org.apache.geode.internal.AvailablePort.Keeper;

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
   */
  public static int[] getRandomAvailableTCPPorts(int count) {
    return getRandomAvailableTCPPortRange(count);
  }

  public static List<Keeper<?>> getRandomAvailableTCPPortKeepers(int count) {
    List<Keeper<?>> result = new ArrayList<>();
    while (result.size() < count) {
      result.add(getUniquePortKeeper(AvailablePort.SOCKET));
    }
    return result;
  }

  /**
   * Returns an array of unique randomly available tcp ports
   *
   * @param count number of desired ports
   * @return the ports
   */
  public static int[] getRandomAvailableTCPPortRange(final int count) {
    List<Keeper> list = getUniquePortRangeKeepers(AvailablePort.SOCKET, count);
    int[] ports = new int[list.size()];
    int i = 0;
    for (Keeper k : list) {
      ports[i] = k.getPort();
      k.release();
      i++;
    }
    return ports;
  }

  public static List<Keeper> getRandomAvailableTCPPortRangeKeepers(final int count) {
    return getRandomAvailableTCPPortRangeKeepers(count, false);
  }

  public static List<Keeper> getRandomAvailableTCPPortRangeKeepers(final int count,
      final boolean useMembershipPortRange) {
    return getUniquePortRangeKeepers(AvailablePort.SOCKET,
        count);
  }

  private static void releaseKeepers(Iterable<Keeper> keepers) {
    for (Keeper keeper : keepers) {
      keeper.release();
    }
  }

  /**
   * Returns array of unique randomly available tcp ports of specified count.
   */
  public static int[] getRandomAvailableTCPPortsForDUnitSite(final int count) {
    int[] ports = new int[count];
    for (int i = 0; i < count; ++i) {
      ports[i] = getRandomAvailablePortForDUnitSite();
    }
    return ports;
  }


  /**
   * Returns array of unique randomly available tcp ports of specified count.
   */
  public static int getRandomAvailablePortForDUnitSite() {
    return getRandomAvailableTCPPort();
  }

  /**
   * Returns available ephemeral TCP port.
   */
  public static int getRandomAvailableTCPPort() {
    try (final ServerSocket socket = bindEphemeralTcpSocket()) {
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
      return socket.getLocalPort();
    } catch (SocketException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Get keeper objects for the next unused, consecutive 'rangeSize' ports on this machine.
   *
   * @param protocol - either AvailablePort.SOCKET (TCP) or AvailablePort.MULTICAST (UDP)
   * @param rangeSize - number of contiguous ports needed
   * @return Keeper objects associated with a range of ports satisfying the request
   */
  private static List<Keeper> getUniquePortRangeKeepers(final int protocol, final int rangeSize) {

    final List<Keeper> keepers = new ArrayList<>(rangeSize);
    for (int i = 0; i < rangeSize; ++i) {
      try {
        if (AvailablePort.SOCKET == protocol) {
          final ServerSocket socket = bindEphemeralTcpSocket();
          keepers.add(new Keeper<>(socket, socket.getLocalPort()));
        } else {
          final DatagramSocket socket = bindEphemeralUdpSocket();
          keepers.add(new Keeper<>(socket, socket.getLocalPort()));
        }
      } catch (IOException e) {
        releaseKeepers(keepers);
        throw new IllegalStateException(e);
      }
    }

    return keepers;
  }

  private static DatagramSocket bindEphemeralUdpSocket() throws SocketException {
    return new DatagramSocket();
  }

  private static ServerSocket bindEphemeralTcpSocket() throws IOException {
    ServerSocket socket = new ServerSocket();
    socket.bind(new InetSocketAddress(0));
    return socket;
  }

  private static Keeper<?> getUniquePortKeeper(int protocol) {
    return getUniquePortRangeKeepers(protocol, 1).get(0);
  }

}
