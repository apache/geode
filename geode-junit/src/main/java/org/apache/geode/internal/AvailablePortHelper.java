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

import static org.apache.geode.internal.AvailablePort.getAddress;

import java.io.Closeable;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.inet.LocalHostUtil;

/**
 * Provides helper methods for acquiring a set of unique available ports. It is not safe to simply
 * call AvailablePort.getRandomAvailablePort several times in a row without doing something to
 * ensure that they are unique. Although they are random, it is possible for subsequent calls to
 * getRandomAvailablePort to return the same integer, unless that port is put into use before
 * further calls to getRandomAvailablePort.
 */
public class AvailablePortHelper {

  public static final int SOCKET = AvailablePort.SOCKET;
  public static final int MULTICAST = AvailablePort.MULTICAST;

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

  public static int getRandomAvailableTCPPort(final InetAddress inetAddress) {
    return getTcpPortKeeper(inetAddress).release();
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
    return getEphemeralMulticastPort(getAddress(MULTICAST)).release();
  }

  private static TcpPortKeeper getTcpPortKeeper(final InetAddress inetAddress) {
    return getEphemeralTcpPort(inetAddress);
  }

  private static TcpPortKeeper getTcpPortKeeper() {
    return getTcpPortKeeper(getAddress(SOCKET));
  }

  public static int getRandomAvailablePort(int protocol) {
    return getTcpPortKeeper(getAddress(protocol)).release();
  }

  public static boolean isPortAvailable(int port, int protocol) {
    return AvailablePort.isPortAvailable(port, protocol, getAddress(protocol));
  }

  /**
   * Must be copied from {@link AvailablePort} because of rolling upgrades. If dependent on
   * AvailablePort then previous bugs in AvailablePort will cause problems during rolling upgrade
   * tests.
   */
  private static TcpPortKeeper getEphemeralTcpPort(final InetAddress inetAddress) {
    // TODO jbarrett - reasonable timeout
    while (true) {
      try {
        return new TcpPortKeeper(inetAddress);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Must be copied from {@link AvailablePort} because of rolling upgrades. If dependent on
   * AvailablePort then previous bugs in AvailablePort will cause problems during rolling upgrade
   * tests.
   */
  private static MulticastPortKeeper getEphemeralMulticastPort(final InetAddress inetAddress) {
    // TODO jbarrett - reasonable timeout
    while (true) {
      try {
        return new MulticastPortKeeper(inetAddress);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Must be copied from {@link AvailablePort} because of rolling upgrades. If dependent on
   * AvailablePort then previous bugs in AvailablePort will cause problems during rolling upgrade
   * tests.
   */
  public static abstract class PortKeeper implements Closeable {
    public abstract int getPort();

    public int release() {
      try {
        return getPort();
      } finally {
        safeCloseAndLog(this);
      }
    }

    static void safeCloseAndLog(final Closeable closeable) {
      try {
        safeClose(closeable);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    static void safeClose(final Closeable closeable) throws IOException {
      if (null != closeable) {
        closeable.close();
      }
    }
  }

  public static class MulticastPortKeeper extends PortKeeper {
    private final MulticastSocket multicastSocket;
    private MulticastPortKeeper(final InetAddress inetAddress) throws IOException {
      multicastSocket = new MulticastSocket();
      try {
        final InetAddress localHost = LocalHostUtil.getLocalHost();
        multicastSocket.setInterface(localHost);
        multicastSocket.setSoTimeout(Integer.getInteger("AvailablePort.timeout", 2000));
        multicastSocket.setReuseAddress(true);
        final byte[] buffer = new byte[] {'p', 'i', 'n', 'g'};
        final InetAddress multicastAddress = inetAddress == null ? DistributionConfig.DEFAULT_MCAST_ADDRESS : inetAddress;
        final InetSocketAddress multicastSocketAddress = new InetSocketAddress(multicastAddress, getPort());
        multicastSocket.joinGroup(multicastAddress);
        final DatagramPacket
            packet = new DatagramPacket(buffer, 0, buffer.length, multicastSocketAddress);
        multicastSocket.send(packet);
        multicastSocket.receive(packet);
      } catch (IOException e) {
        close();
      }
    }

    @Override
    public int getPort() {
      return multicastSocket.getLocalPort();
    }

    @Override
    public void close() throws IOException {
      safeClose(multicastSocket);
    }
  }

  public static class TcpPortKeeper extends PortKeeper {
    private final ServerSocket serverSocket;
    private final Socket client;
    private final Socket server;

    private TcpPortKeeper(final InetAddress inetAddress) throws IOException {
      try {
        serverSocket = new ServerSocket();
        serverSocket.setReuseAddress(true);
        serverSocket.bind(new InetSocketAddress(inetAddress, 0));
        client = new Socket();
        client.connect(new InetSocketAddress(inetAddress, getPort()));
        server = serverSocket.accept();
      } catch (IOException e) {
        safeCloseAndLog(this);
        throw e;
      }
    }

    @Override
    public int getPort() {
      return serverSocket.getLocalPort();
    }

    @Override
    public void close() throws IOException {
      try {
        safeClose(server);
      } finally {
        try {
          safeClose(client);
        } finally {
          safeClose(serverSocket);
        }
      }
    }

  }


}
