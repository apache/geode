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

import static org.apache.geode.distributed.internal.membership.api.MembershipConfig.DEFAULT_MCAST_ADDRESS;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
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
import java.util.Enumeration;
import java.util.Random;
import java.util.function.IntPredicate;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.distributed.internal.membership.api.MembershipConfig;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * AvailablePort determines whether a given port is available and can also randomly select an
 * available port.
 */
public class AvailablePort {
  public static final int AVAILABLE_PORTS_LOWER_BOUND = 20001;// 20000/udp is securid
  public static final int AVAILABLE_PORTS_UPPER_BOUND = 29999;// 30000/tcp is spoolfax

  /**
   * Randomly selects a port that is available for TCP on {@link #bindAddress() the bind address},
   * or on all addresses if the bind address is undefined. The port is selected from the range
   * {@link #AVAILABLE_PORTS_LOWER_BOUND} to {@link #AVAILABLE_PORTS_UPPER_BOUND}.
   */
  public static int getRandomAvailableTCPPort() {
    return getRandomAvailableTCPPort(bindAddress());
  }

  /**
   * Randomly selects a port that is available for TCP on the given address, or on all addresses if
   * address is null. The port is selected from the range {@link #AVAILABLE_PORTS_LOWER_BOUND} to
   * {@link #AVAILABLE_PORTS_UPPER_BOUND}.
   */
  public static int getRandomAvailableTCPPort(InetAddress address) {
    return getRandomAvailablePort(p -> isAvailableForTCP(p, address));
  }

  /**
   * Randomly selects a port in the given range that is available for TCP on
   * {@link #bindAddress() the bind address}, or on all addresses if the bind address is undefined.
   */
  public static int getRandomAvailableTCPPortInRange(int lowerBound, int upperBound) {
    int numberOfPorts = upperBound - lowerBound;
    // do "5 times the numberOfPorts" iterations to select a port number. This will ensure that
    // each of the ports from given port range get a chance at least once
    int numberOfRetrys = numberOfPorts * 5;
    InetAddress address = bindAddress();
    for (int i = 0; i < numberOfRetrys; i++) {
      int port = randomNumberInRange(lowerBound, upperBound);
      if (isAvailableForTCP(port, address)) {
        return port;
      }
    }
    return -1;
  }

  /**
   * Randomly selects a port that is available to listen for multicast messages on
   * {@link #mcastAddress() the mcast address}, or on
   * {@link MembershipConfig#DEFAULT_MCAST_ADDRESS the default mcast address} if the mcast
   * address is undefined. The port is selected from the range
   * {@link #AVAILABLE_PORTS_LOWER_BOUND} to {@link #AVAILABLE_PORTS_UPPER_BOUND}.
   */
  public static int getRandomAvailableMulticastPort() {
    return getRandomAvailableMulticastPort(mcastAddress());
  }

  /**
   * Randomly selects a port that is available to listen for multicast messages on the given
   * address, or on {@link MembershipConfig#DEFAULT_MCAST_ADDRESS the default mcast address} if
   * address is null. The port is selected from the range {@link #AVAILABLE_PORTS_LOWER_BOUND} to
   * {@link #AVAILABLE_PORTS_UPPER_BOUND}.
   */
  public static int getRandomAvailableMulticastPort(InetAddress address) {
    return getRandomAvailablePort(p -> isAvailableForMulticast(p, address));
  }

  /**
   * Returns whether the port is available for TCP on the given address, or on all addresses if
   * address is null.
   */
  public static boolean isAvailableForTCP(int port, InetAddress address) {
    if (address == null) {
      return testAllInterfaces(port);
    } else {
      return testOneInterface(address, port);
    }
  }

  /**
   * Returns whether the port is available for TCP on {@link #bindAddress() the bind address}, or on
   * all addresses if the bind address is undefined.
   */
  public static boolean isAvailableForTCP(int port) {
    return isAvailableForTCP(port, bindAddress());
  }

  /**
   * Returns whether the port is available to listen for multicast messages on the given address,
   * or on {@link MembershipConfig#DEFAULT_MCAST_ADDRESS the default mcast address} if address is
   * null.
   */
  public static boolean isAvailableForMulticast(int port, InetAddress address) {
    MulticastSocket socket = null;
    try {
      socket = new MulticastSocket();
      InetAddress localHost = LocalHostUtil.getLocalHost();
      socket.setInterface(localHost);
      socket.setSoTimeout(Integer.getInteger("AvailablePort.timeout", 2000));
      socket.setReuseAddress(true);
      byte[] buffer = new byte[4];
      buffer[0] = (byte) 'p';
      buffer[1] = (byte) 'i';
      buffer[2] = (byte) 'n';
      buffer[3] = (byte) 'g';
      InetAddress mcid =
          address == null ? InetAddress.getByName(DEFAULT_MCAST_ADDRESS) : address;
      SocketAddress mcaddr = new InetSocketAddress(mcid, port);
      socket.joinGroup(mcid);
      DatagramPacket packet = new DatagramPacket(buffer, 0, buffer.length, mcaddr);
      socket.send(packet);
      try {
        socket.receive(packet);
        return false;
      } catch (SocketTimeoutException ste) {
        return true;
      } catch (Exception e) {
        e.printStackTrace();
        return false;
      }
    } catch (java.io.IOException ioe) {
      if (ioe.getMessage().equals("Network is unreachable")) {
        throw new RuntimeException(
            "Network is unreachable", ioe);
      }
      ioe.printStackTrace();
      return false;
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    } finally {
      if (socket != null) {
        try {
          socket.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }

  /**
   * Returns whether the port is available to listen for multicast messages on on
   * {@link #mcastAddress() the mcast address}, or on
   * {@link MembershipConfig#DEFAULT_MCAST_ADDRESS the default mcast address} if the mcast
   * address is undefined.
   */
  public static boolean isAvailableForMulticast(int port) {
    return isAvailableForMulticast(port, mcastAddress());
  }

  public static Keeper keepPortForTCP(final int port) {
    return keepPortForTCP(port, bindAddress());
  }

  public static Keeper keepPortForTCP(final int port, InetAddress addr) {
    if (addr == null) {
      return keepAllInterfaces(port);
    } else {
      return keepOneInterface(addr, port);
    }
  }

  private static InetAddress getAddress(String host) {
    if (host == null) {
      return null;
    }
    try {
      return InetAddress.getByName(host);
    } catch (UnknownHostException e) {
      throw new RuntimeException("Unable to resolve address " + host);
    }
  }

  private static int getRandomAvailablePort(IntPredicate availabilityTester) {
    while (true) {
      int port = randomNumberInRange(AVAILABLE_PORTS_LOWER_BOUND, AVAILABLE_PORTS_UPPER_BOUND);
      if (availabilityTester.test(port)) {
        return port;
      }
    }
  }

  private static boolean testOneInterface(InetAddress addr, int port) {
    Keeper k = keepOneInterface(addr, port);
    if (k != null) {
      k.release();
      return true;
    } else {
      return false;
    }
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
    } catch (java.io.IOException ioe) {
      if (ioe.getMessage().equals("Network is unreachable")) {
        throw new RuntimeException("Network is unreachable");
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
        } catch (Exception ignored) {
        }
      }
    }
  }

  /**
   * Test to see if a given port is available port on all interfaces on this host.
   *
   * @return true of if the port is free on all interfaces
   */
  private static boolean testAllInterfaces(int port) {
    Keeper k = keepAllInterfaces(port);
    if (k != null) {
      k.release();
      return true;
    } else {
      return false;
    }
  }

  private static Keeper keepAllInterfaces(int port) {
    // First check to see if we can bind to the wildcard address.
    if (!testOneInterface(null, port)) {
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
    Enumeration<NetworkInterface> interfaces;
    try {
      interfaces = NetworkInterface.getNetworkInterfaces();
    } catch (SocketException e) {
      throw new RuntimeException(e);
    }
    while (interfaces.hasMoreElements()) {
      NetworkInterface next = interfaces.nextElement();
      Enumeration<InetAddress> addressesOnInterface = next.getInetAddresses();
      while (addressesOnInterface.hasMoreElements()) {
        InetAddress addr = addressesOnInterface.nextElement();
        boolean available = testOneInterface(addr, port);
        if (!available) {
          return null;
        }
      }
    }
    // Now do it one more time but reserve the wildcard address
    return keepOneInterface(null, port);
  }

  /**
   * Returns the address named by the "bind-address" system property, or null if "bind-address"
   * is undefined.
   */
  private static InetAddress bindAddress() {
    return getAddress(System.getProperty(GeodeGlossary.GEMFIRE_PREFIX + "bind-address"));
  }

  /**
   * Returns the address named by the "mcast-address" system property, or null if "mcast-address"
   * is undefined.
   */
  private static InetAddress mcastAddress() {
    return getAddress(System.getProperty(GeodeGlossary.GEMFIRE_PREFIX + "mcast-address"));
  }

  @Immutable
  private static final Random rand;

  static {
    boolean fast = Boolean.getBoolean("AvailablePort.fastRandom");
    if (fast) {
      rand = new Random();
    } else {
      rand = new java.security.SecureRandom();
    }
  }

  private static int randomNumberInRange(int lowerBound, int upperBound) {
    return rand.nextInt(upperBound + 1 - lowerBound) + lowerBound;
  }

  /**
   * This class will keep an allocated port allocated until it is used. This makes the window
   * smaller that can cause bug 46690
   */
  public static class Keeper implements Serializable {
    private final transient ServerSocket ss;
    private final int port;

    public Keeper(ServerSocket ss, int port) {
      this.ss = ss;
      this.port = port;
    }

    public Keeper(ServerSocket ss, Integer port) {
      this.ss = ss;
      this.port = port != null ? port : 0;
    }

    public int getPort() {
      return port;
    }

    /**
     * Once you call this the socket will be freed and can then be reallocated by someone else.
     */
    public void release() {
      try {
        if (ss != null) {
          ss.close();
        }
      } catch (IOException ignore) {
      }
    }
  }

  /////////////////////// Main Program ///////////////////////

  @Immutable
  private static final PrintStream out = System.out;
  @Immutable
  private static final PrintStream err = System.err;

  private static void usage(String s) {
    err.println("\n** " + s + "\n");
    err.println("usage: java AvailablePort socket|jgroups [\"addr\" network-address] [port]");
    err.println();
    err.println(
        "This program either prints whether or not a port is available for a given protocol, or "
            + "it prints out an available port for a given protocol.");
    err.println();
    System.exit(1);
  }

  public static void main(String[] args) throws UnknownHostException {
    String protocolString = null;
    String addrString = null;
    String portString = null;

    for (int i = 0; i < args.length; i++) {
      if (protocolString == null) {
        protocolString = args[i];
      } else if (args[i].equals("addr") && i < args.length - 1) {
        addrString = args[++i];
      } else if (portString == null) {
        portString = args[i];
      } else {
        usage("Spurious command line: " + args[i]);
      }
    }

    InetAddress addr = addrString == null ? null : InetAddress.getByName(addrString);

    if (protocolString == null) {
      usage("Missing protocol");
      return;
    }

    IntPredicate availabilityTester;
    if (protocolString.equalsIgnoreCase("socket")) {
      availabilityTester = p -> isAvailableForTCP(p, addr);
    } else if (protocolString.equalsIgnoreCase("javagroups")
        || protocolString.equalsIgnoreCase("jgroups")) {
      availabilityTester = p -> isAvailableForMulticast(p, addr);
    } else {
      usage("Unknown protocol: " + protocolString);
      return;
    }

    if (portString == null) {
      out.println("\nRandomly selected " + protocolString + " port: "
          + getRandomAvailablePort(availabilityTester) + "\n");
      return;
    }

    int port;
    try {
      port = Integer.parseInt(portString);
    } catch (NumberFormatException ex) {
      usage("Malformed port: " + portString);
      return;
    }
    boolean isAvailable = availabilityTester.test(port);
    out.println("\nPort " + port + " is " + (isAvailable ? "" : "not ")
        + "available for a " + protocolString + " connection\n");
  }

  /**
   * @deprecated call {@link #getRandomAvailableTCPPort(InetAddress)} or
   *             {@link #getRandomAvailableMulticastPort(InetAddress)}
   */
  @Deprecated()
  public static int getRandomAvailablePort(int protocol, InetAddress address) {
    return getRandomAvailablePort(portTester(protocol, address));
  }

  /**
   * @deprecated call {@link #getRandomAvailableTCPPort()} or
   *             {@link #getRandomAvailableMulticastPort()}
   */
  public static int getRandomAvailablePort(int protocol) {
    return getRandomAvailablePort(portTester(protocol, defaultAddress(protocol)));
  }

  /**
   * @deprecated call {@link #isAvailableForTCP(int, InetAddress)} or
   *             {@link #isAvailableForMulticast(int, InetAddress)}
   */
  public static boolean isPortAvailable(int port, int protocol, InetAddress address) {
    return portTester(protocol, address).test(port);
  }

  /**
   * @deprecated call {@link #isAvailableForTCP(int)} or {@link #isAvailableForMulticast(int)}
   */
  public static boolean isPortAvailable(int port, int protocol) {
    return portTester(protocol, defaultAddress(protocol)).test(port);
  }

  /**
   * @deprecated call the appropriate TCP method
   */
  public static final int SOCKET = 0;

  /**
   * @deprecated call the appropriate Multicast method
   */
  public static final int MULTICAST = 1;

  private static IntPredicate portTester(int protocol, InetAddress address) {
    switch (protocol) {
      case SOCKET:
        return p -> isAvailableForTCP(p, address);
      case MULTICAST:
        return p -> isAvailableForMulticast(p, address);
      default:
        throw new IllegalArgumentException(String.format("Unknown protocol: %d", protocol));
    }
  }

  private static InetAddress defaultAddress(int protocol) {
    switch (protocol) {
      case SOCKET:
        return bindAddress();
      case MULTICAST:
        return mcastAddress();
      default:
        throw new IllegalArgumentException(String.format("Unknown protocol: %d", protocol));
    }
  }
}
