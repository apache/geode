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

import java.io.PrintStream;
import java.net.InetAddress;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.net.AvailablePort.Keeper;
import org.apache.geode.internal.net.AvailablePort.Protocol;
import org.apache.geode.internal.net.AvailablePort.Range;

/**
 * This class determines whether or not a given port is available and can also provide a randomly
 * selected available port.
 *
 * @deprecated Please use {@link org.apache.geode.internal.net.AvailablePort} instead.
 */
public class AvailablePort {

  private static final org.apache.geode.internal.net.AvailablePort delegate =
      org.apache.geode.internal.net.AvailablePort.create();

  /** Is the port available for a Socket (TCP) connection? */
  public static final int SOCKET = Protocol.SOCKET.value();
  public static final int AVAILABLE_PORTS_LOWER_BOUND = Range.LOWER_BOUND.value();// 20000/udp is
                                                                                  // securid
  public static final int AVAILABLE_PORTS_UPPER_BOUND = Range.UPPER_BOUND.value();// 30000/tcp is
                                                                                  // spoolfax
  /** Is the port available for a JGroups (UDP) multicast connection */
  public static final int MULTICAST = Protocol.MULTICAST.value();

  /**
   * see if there is a gemfire system property that establishes a default address for the given
   * protocol, and return it
   */
  public static InetAddress getAddress(int protocol) {
    return delegate.getAddress(protocol);
  }

  /**
   * Returns whether or not the given port on the local host is available (that is, unused).
   *
   * @param port The port to check
   * @param protocol The protocol to check (either {@link #SOCKET} or {@link #MULTICAST}).
   *
   * @throws IllegalArgumentException {@code protocol} is unknown
   */
  public static boolean isPortAvailable(final int port, int protocol) {
    return delegate.isPortAvailable(port, protocol);
  }

  /**
   * Returns whether or not the given port on the local host is available (that is, unused).
   *
   * @param port The port to check
   * @param protocol The protocol to check (either {@link #SOCKET} or {@link #MULTICAST}).
   * @param addr the bind address (or mcast address) to use
   *
   * @throws IllegalArgumentException {@code protocol} is unknown
   */
  public static boolean isPortAvailable(final int port, int protocol, InetAddress addr) {
    return delegate.isPortAvailable(port, protocol, addr);
  }

  public static Keeper isPortKeepable(final int port, int protocol, InetAddress addr) {
    return delegate.isPortKeepable(port, protocol, addr);
  }

  /**
   * Returns a randomly selected available port in the range 5001 to 32767.
   *
   * @param protocol The protocol to check (either {@link #SOCKET} or {@link #MULTICAST}).
   *
   * @throws IllegalArgumentException {@code protocol} is unknown
   */
  public static int getRandomAvailablePort(int protocol) {
    return delegate.getRandomAvailablePort(protocol);
  }

  public static Keeper getRandomAvailablePortKeeper(int protocol) {
    return delegate.getRandomAvailablePortKeeper(protocol);
  }

  /**
   * Returns a randomly selected available port in the provided range.
   *
   * @param protocol The protocol to check (either {@link #SOCKET} or {@link #MULTICAST}).
   *
   * @throws IllegalArgumentException {@code protocol} is unknown
   */
  public static int getAvailablePortInRange(int rangeBase, int rangeTop, int protocol) {
    return delegate.getAvailablePortInRange(rangeBase, rangeTop, protocol);
  }

  /**
   * Returns a randomly selected available port in the range 5001 to 32767 that satisfies a modulus
   *
   * @param protocol The protocol to check (either {@link #SOCKET} or {@link #MULTICAST}).
   *
   * @throws IllegalArgumentException {@code protocol} is unknown
   */
  public static int getRandomAvailablePortWithMod(int protocol, int mod) {
    return delegate.getRandomAvailablePortWithMod(protocol, mod);
  }


  /**
   * Returns a randomly selected available port in the range 5001 to 32767.
   *
   * @param protocol The protocol to check (either {@link #SOCKET} or {@link #MULTICAST}).
   * @param addr the bind-address or mcast address to use
   *
   * @throws IllegalArgumentException {@code protocol} is unknown
   */
  public static int getRandomAvailablePort(int protocol, InetAddress addr) {
    return delegate.getRandomAvailablePort(protocol, addr);
  }

  /**
   * Returns a randomly selected available port in the range 5001 to 32767.
   *
   * @param protocol The protocol to check (either {@link #SOCKET} or {@link #MULTICAST}).
   * @param addr the bind-address or mcast address to use
   * @param useMembershipPortRange use true if the port will be used for membership
   *
   * @throws IllegalArgumentException {@code protocol} is unknown
   */
  public static int getRandomAvailablePort(int protocol, InetAddress addr,
      boolean useMembershipPortRange) {
    return delegate.getRandomAvailablePort(protocol, addr, useMembershipPortRange);
  }

  public static Keeper getRandomAvailablePortKeeper(int protocol, InetAddress addr) {
    return delegate.getRandomAvailablePortKeeper(protocol, addr);
  }

  public static Keeper getRandomAvailablePortKeeper(int protocol, InetAddress addr,
      boolean useMembershipPortRange) {
    return delegate.getRandomAvailablePortKeeper(protocol, addr, useMembershipPortRange);
  }

  /**
   * Returns a randomly selected available port in the provided range.
   *
   * @param protocol The protocol to check (either {@link #SOCKET} or {@link #MULTICAST}).
   * @param addr the bind-address or mcast address to use
   *
   * @throws IllegalArgumentException {@code protocol} is unknown
   */
  public static int getAvailablePortInRange(int protocol, InetAddress addr, int rangeBase,
      int rangeTop) {
    return delegate.getAvailablePortInRange(protocol, addr, rangeBase, rangeTop);
  }

  /**
   * Returns a randomly selected available port in the range 5001 to 32767 that satisfies a modulus
   * and the provided protocol
   *
   * @param protocol The protocol to check (either {@link #SOCKET} or {@link #MULTICAST}).
   * @param addr the bind-address or mcast address to use
   *
   * @throws IllegalArgumentException {@code protocol} is unknown
   */
  public static int getRandomAvailablePortWithMod(int protocol, InetAddress addr, int mod) {
    return delegate.getRandomAvailablePortWithMod(protocol, addr, mod);
  }

  public static int getRandomAvailablePortInRange(int rangeBase, int rangeTop, int protocol) {
    return delegate.getRandomAvailablePortInRange(rangeBase, rangeTop, protocol);
  }

  @Immutable
  private static final PrintStream out = System.out;
  @Immutable
  private static final PrintStream err = System.err;

  private static void usage(String s) {
    err.println("\n** " + s + "\n");
    err.println("usage: java AvailablePort socket|jgroups [\"addr\" network-address] [port]");
    err.println("");
    err.println(
        "This program either prints whether or not a port is available for a given protocol, or it prints out an available port for a given protocol.");
    err.println("");
    ExitCode.FATAL.doSystemExit();
  }

  public static void main(String[] args) {
    String protocolString = null;
    String addrString = null;
    String portString = null;

    for (int i = 0; i < args.length; i++) {
      if (protocolString == null) {
        protocolString = args[i];

      } else if (args[i].equals("addr")) {
        addrString = args[++i];
      } else if (portString == null) {
        portString = args[i];
      } else {
        usage("Spurious command line: " + args[i]);
      }
    }

    int protocol;

    if (protocolString == null) {
      usage("Missing protocol");
      return;

    } else if (protocolString.equalsIgnoreCase("socket")) {
      protocol = SOCKET;

    } else if (protocolString.equalsIgnoreCase("javagroups")
        || protocolString.equalsIgnoreCase("jgroups")) {
      protocol = MULTICAST;

    } else {
      usage("Unknown protocol: " + protocolString);
      return;
    }

    InetAddress addr = null;
    if (addrString != null) {
      try {
        addr = InetAddress.getByName(addrString);
      } catch (Exception e) {
        e.printStackTrace();
        ExitCode.FATAL.doSystemExit();
      }
    }

    if (portString != null) {
      int port;
      try {
        port = Integer.parseInt(portString);

      } catch (NumberFormatException ex) {
        usage("Malformed port: " + portString);
        return;
      }

      out.println("\nPort " + port + " is " + (isPortAvailable(port, protocol, addr) ? "" : "not ")
          + "available for a " + protocolString + " connection\n");

    } else {
      out.println("\nRandomly selected " + protocolString + " port: "
          + getRandomAvailablePort(protocol, addr) + "\n");
    }
  }
}
