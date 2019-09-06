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

import static org.apache.geode.distributed.internal.DistributionConfig.DEFAULT_MCAST_ADDRESS;
import static org.apache.geode.distributed.internal.DistributionConfig.DEFAULT_MCAST_PORT;
import static org.apache.geode.distributed.internal.DistributionConfig.DEFAULT_MEMBERSHIP_PORT_RANGE;
import static org.apache.geode.distributed.internal.DistributionConfig.GEMFIRE_PREFIX;
import static org.apache.geode.internal.net.AvailablePort.Protocol.MULTICAST;
import static org.apache.geode.internal.net.AvailablePort.Protocol.SOCKET;
import static org.apache.geode.internal.net.AvailablePort.Range.LOWER_BOUND;
import static org.apache.geode.internal.net.AvailablePort.Range.UPPER_BOUND;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.DatagramPacket;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.util.Enumeration;
import java.util.Random;

public class AvailablePortImpl implements AvailablePort {

  private static final Random random =
      Boolean.getBoolean("AvailablePort.fastRandom") ? new Random() : new SecureRandom();

  @Override
  public InetAddress getAddress(int protocol) {
    String name = null;
    try {
      if (protocol == SOCKET.value()) {
        name = System.getProperty(GEMFIRE_PREFIX + "bind-address");
      } else if (protocol == MULTICAST.value()) {
        name = System.getProperty(GEMFIRE_PREFIX + "mcast-address");
      }
      if (name != null) {
        return InetAddress.getByName(name);
      }
    } catch (UnknownHostException e) {
      throw new RuntimeException("Unable to resolve address " + name, e);
    }
    return null;
  }

  @Override
  public boolean isPortAvailable(final int port, int protocol) {
    return isPortAvailable(port, protocol, getAddress(protocol));
  }

  @Override
  public boolean isPortAvailable(final int port, int protocol, InetAddress addr) {
    if (protocol == SOCKET.value()) {
      // Try to create a ServerSocket
      if (addr == null) {
        return checkAllInterfaces(port);
      }

      return checkOneInterface(addr, port);
    }

    if (protocol == MULTICAST.value()) {
      MulticastSocket socket = null;
      try {
        socket = new MulticastSocket();
        InetAddress localHost = SocketCreator.getLocalHost();
        socket.setInterface(localHost);
        socket.setSoTimeout(Integer.getInteger("AvailablePort.timeout", 2000));
        socket.setReuseAddress(true);
        byte[] buffer = new byte[4];
        buffer[0] = (byte) 'p';
        buffer[1] = (byte) 'i';
        buffer[2] = (byte) 'n';
        buffer[3] = (byte) 'g';
        InetAddress mcid = addr == null ? DEFAULT_MCAST_ADDRESS : addr;
        SocketAddress mcaddr = new InetSocketAddress(mcid, port);
        socket.joinGroup(mcid);
        DatagramPacket packet = new DatagramPacket(buffer, 0, buffer.length, mcaddr);
        socket.send(packet);

        try {
          socket.receive(packet);
          packet.getData(); // make sure there's data, but no need to process it
          return false;
        } catch (SocketTimeoutException ignored) {
          return true;
        } catch (Exception ignored) {
          return false;
        }

      } catch (IOException e) {
        if (e.getMessage().equals("Network is unreachable")) {
          throw new UncheckedIOException("Network is unreachable", e);
        }
        return false;
      } catch (Exception ignored) {
        return false;
      } finally {
        if (socket != null) {
          socket.close();
        }
      }
    }

    throw new IllegalArgumentException(String.format("Unknown protocol: %s", protocol));
  }

  @Override
  public Keeper isPortKeepable(final int port, int protocol, InetAddress addr) {
    if (protocol == SOCKET.value()) {
      // Try to create a ServerSocket
      if (addr == null) {
        return keepAllInterfaces(port);
      }

      return keepOneInterface(addr, port);
    }

    if (protocol == MULTICAST.value()) {
      throw new IllegalArgumentException("You can not keep the JGROUPS protocol");
    }

    throw new IllegalArgumentException(String.format("Unknown protocol: %s", protocol));
  }

  @Override
  public int getRandomAvailablePort(int protocol) {
    return getRandomAvailablePort(protocol, getAddress(protocol));
  }

  @Override
  public Keeper getRandomAvailablePortKeeper(int protocol) {
    return getRandomAvailablePortKeeper(protocol, getAddress(protocol));
  }

  @Override
  public int getAvailablePortInRange(int rangeBase, int rangeTop, int protocol) {
    return getAvailablePortInRange(protocol, getAddress(protocol), rangeBase, rangeTop);
  }

  @Override
  public int getRandomAvailablePortWithMod(int protocol, int mod) {
    return getRandomAvailablePortWithMod(protocol, getAddress(protocol), mod);
  }

  @Override
  public int getRandomAvailablePort(int protocol, InetAddress addr) {
    return getRandomAvailablePort(protocol, addr, false);
  }

  @Override
  public int getRandomAvailablePort(int protocol, InetAddress addr,
      boolean useMembershipPortRange) {
    while (true) {
      int port = getRandomWildcardBindPortNumber(useMembershipPortRange);
      if (isPortAvailable(port, protocol, addr)) {
        // don't return the products default multicast port
        if (!(protocol == MULTICAST.value() && port == DEFAULT_MCAST_PORT)) {
          return port;
        }
      }
    }
  }

  @Override
  public Keeper getRandomAvailablePortKeeper(int protocol, InetAddress addr) {
    return getRandomAvailablePortKeeper(protocol, addr, false);
  }

  @Override
  public Keeper getRandomAvailablePortKeeper(int protocol, InetAddress addr,
      boolean useMembershipPortRange) {
    while (true) {
      int port = getRandomWildcardBindPortNumber(useMembershipPortRange);
      Keeper result = isPortKeepable(port, protocol, addr);
      if (result != null) {
        return result;
      }
    }
  }

  @Override
  public int getAvailablePortInRange(int protocol, InetAddress addr, int rangeBase, int rangeTop) {
    for (int port = rangeBase; port <= rangeTop; port++) {
      if (isPortAvailable(port, protocol, addr)) {
        return port;
      }
    }
    return -1;
  }

  @Override
  public int getRandomAvailablePortWithMod(int protocol, InetAddress addr, int mod) {
    while (true) {
      int port = getRandomWildcardBindPortNumber();
      if (isPortAvailable(port, protocol, addr) && (port % mod) == 0) {
        return port;
      }
    }
  }

  @Override
  public int getRandomAvailablePortInRange(int rangeBase, int rangeTop, int protocol) {
    int numberOfPorts = rangeTop - rangeBase;
    // do "5 times the numberOfPorts" iterations to select a port number. This will ensure that
    // each of the ports from given port range get a chance at least once
    int numberOfRetrys = numberOfPorts * 5;
    for (int i = 0; i < numberOfRetrys; i++) {
      int port = random.nextInt(numberOfPorts + 1) + rangeBase;// add 1 to numberOfPorts so that
      // rangeTop also gets included
      if (isPortAvailable(port, protocol, getAddress(protocol))) {
        return port;
      }
    }
    return -1;
  }

  private static boolean checkOneInterface(InetAddress addr, int port) {
    Keeper k = keepOneInterface(addr, port);
    if (k != null) {
      k.release();
      return true;
    }
    return false;
  }

  private static Keeper keepOneInterface(InetAddress addr, int port) {
    ServerSocket server = null;
    try {
      server = new ServerSocket();
      server.setReuseAddress(true);
      if (addr != null) {
        server.bind(new InetSocketAddress(addr, port));
      } else {
        server.bind(new InetSocketAddress(port));
      }
      Keeper result = new Keeper(server, port);
      server = null;
      return result;
    } catch (IOException e) {
      if (e.getMessage().equals("Network is unreachable")) {
        throw new UncheckedIOException("Network is unreachable", e);
      }

      if (addr instanceof Inet6Address) {
        byte[] addrBytes = addr.getAddress();
        if ((addrBytes[0] == (byte) 0xfe) && (addrBytes[1] == (byte) 0x80)) {
          // Hack, early Sun 1.5 versions (like Hitachi's JVM) cannot handle IPv6
          // link local addresses. Cannot trust InetAddress.isLinkLocalAddress()
          // see http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6558853
          // By returning true we ignore these interfaces and potentially say a
          // port is not in use when it really is.
          Keeper result = new Keeper(server, port);
          server = null;
          return result;
        }
      }

      return null;
    } catch (Exception ex) {
      return null;
    } finally {
      if (server != null) {
        try {
          server.close();
        } catch (IOException ignored) {
          // ignored
        }
      }
    }
  }

  /**
   * Check to see if a given port is available port on all interfaces on this host.
   *
   * @return true of if the port is free on all interfaces
   */
  private static boolean checkAllInterfaces(int port) {
    Keeper k = keepAllInterfaces(port);
    if (k != null) {
      k.release();
      return true;
    }
    return false;
  }

  private static Keeper keepAllInterfaces(int port) {
    // First check to see if we can bind to the wildcard address.
    if (!checkOneInterface(null, port)) {
      return null;
    }

    // Now check all of the addresses for all of the addresses
    // on this system. On some systems (solaris, aix) binding
    // to the wildcard address will successfully bind to only some
    // of the interfaces if other interfaces are in use. We want to
    // make sure this port is completely free.
    //
    // Note that we still need the check of the wildcard address, above,
    // because on some systems (aix) we can still bind to specific addresses
    // if someone else has bound to the wildcard address.
    Enumeration en;
    try {
      en = NetworkInterface.getNetworkInterfaces();
    } catch (SocketException e) {
      throw new RuntimeException(e);
    }

    while (en.hasMoreElements()) {
      NetworkInterface next = (NetworkInterface) en.nextElement();
      Enumeration en2 = next.getInetAddresses();
      while (en2.hasMoreElements()) {
        InetAddress addr = (InetAddress) en2.nextElement();
        boolean available = checkOneInterface(addr, port);
        if (!available) {
          return null;
        }
      }
    }

    // Now do it one more time but reserve the wildcard address
    return keepOneInterface(null, port);
  }

  private static int getRandomWildcardBindPortNumber() {
    return getRandomWildcardBindPortNumber(false);
  }

  private static int getRandomWildcardBindPortNumber(boolean useMembershipPortRange) {
    int rangeBase;
    int rangeTop;
    if (!useMembershipPortRange) {
      rangeBase = LOWER_BOUND.value(); // 20000/udp is securid
      rangeTop = UPPER_BOUND.value(); // 30000/tcp is spoolfax
    } else {
      rangeBase = DEFAULT_MEMBERSHIP_PORT_RANGE[0];
      rangeTop = DEFAULT_MEMBERSHIP_PORT_RANGE[1];
    }

    return random.nextInt(rangeTop - rangeBase) + rangeBase;
  }
}
