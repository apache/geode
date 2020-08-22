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
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Provides helper methods for acquiring a set of unique available ports. It is not safe to simply
 * call AvailablePort.getRandomAvailablePort several times in a row without doing something to
 * ensure that they are unique. Although they are random, it is possible for subsequent calls to
 * getRandomAvailablePort to return the same integer, unless that port is put into use before
 * further calls to getRandomAvailablePort.
 */
public class AvailablePortHelper extends AvailablePort {

  /**
   * Returns an array of unique randomly available tcp ports
   *
   * @param count number of desired ports
   * @return the ports
   */
  public static int[] getRandomAvailableTCPPorts(final int count) {
    final List<TcpPortKeeper> list = getRandomAvailableTCPPortKeepers(count);
    final int[] ports = new int[list.size()];
    int i = 0;
    for (final TcpPortKeeper k : list) {
      ports[i++] = k.getPort();
      k.release();
    }
    return ports;
  }

  public static List<TcpPortKeeper> getRandomAvailableTCPPortKeepers(final int count) {
    final List<TcpPortKeeper> result = new ArrayList<>();
    while (result.size() < count) {
      result.add(getTcpPortKeeper());
    }
    return result;
  }

  private static void releaseKeepers(final List<TcpPortKeeper> keepers) {
    for (TcpPortKeeper keeper : keepers) {
      keeper.release();
    }
  }

  /**
   * Returns randomly available tcp port.
   */
  public static int getRandomAvailableTCPPort() {
    return getTcpPortKeeper().release();
  }

  /**
   * Returns array of unique randomly available udp ports of specified count.
   */
  private static int[] getRandomAvailableMulticastPorts(int count) {
    int[] ports = new int[count];
    int i = 0;
    while (i < count) {
      ports[i] = getRandomAvailableUDPPort();
      ++i;
    }
    return ports;
  }

  /**
   * Returns randomly available udp port.
   */
  public static int getRandomAvailableUDPPort() {
    // TODO jbarrett - multicast
    throw new UnsupportedOperationException("multicast");
  }

  private static TcpPortKeeper getTcpPortKeeper() {
    return getEphemeralTcpPort(getAddress(SOCKET));
  }

  public static int getRandomAvailablePort(int protocol) {
    return AvailablePort.getRandomAvailablePort(protocol, getAddress(protocol));
  }

  public static boolean isPortAvailable(int port, int protocol) {
    return AvailablePort.isPortAvailable(port, protocol, AvailablePort.getAddress(protocol));
  }

}
