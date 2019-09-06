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

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;

/**
 * This class determines whether or not a given port is available and can also provide a randomly
 * selected available port.
 *
 * <p>
 * Usage:
 *
 * <pre>
 * AvailablePort availablePort = AvailablePort.create();
 * int portToUse = availablePort.getRandomAvailablePort(AvailablePort.Protocol.SOCKET);
 * </pre>
 */
public interface AvailablePort {

  enum Protocol {
    /** Is the port available for a Socket (TCP) connection? */
    SOCKET(0),
    /** Is the port available for a JGroups (UDP) multicast connection */
    MULTICAST(1);

    private final int intLevel;

    Protocol(int intLevel) {
      this.intLevel = intLevel;
    }

    public int intLevel() {
      return intLevel;
    }

    public static Protocol valueOf(int intLevel) {
      for (Protocol protocol : Protocol.values()) {
        if (protocol.intLevel() == intLevel) {
          return protocol;
        }
      }
      throw new IllegalArgumentException("No matching Protocol for intLevel '" + intLevel + "'.");
    }
  }

  enum Range {
    LOWER_BOUND(20001), // 20000/udp is securid
    UPPER_BOUND(29999);// 30000/tcp is spoolfax

    private final int intLevel;

    Range(int intLevel) {
      this.intLevel = intLevel;
    }

    public int intLevel() {
      return intLevel;
    }
  }

  static AvailablePort create() {
    return new AvailablePortImpl();
  }

  /**
   * see if there is a gemfire system property that establishes a default address for the given
   * protocol, and return it
   */
  InetAddress getAddress(Protocol protocol);

  /**
   * see if there is a gemfire system property that establishes a default address for the given
   * protocol, and return it
   *
   * @deprecated Please use {@link #getAddress(Protocol)} instead.
   */
  @Deprecated
  default InetAddress getAddress(int protocol) {
    return getAddress(Protocol.valueOf(protocol));
  }

  /**
   * Returns whether or not the given port on the local host is available (that is, unused).
   *
   * @param port The port to check
   * @param protocol The protocol to check (either {@link Protocol#SOCKET} or
   *        {@link Protocol#MULTICAST}).
   *
   * @throws IllegalArgumentException {@code protocol} is unknown
   */
  boolean isPortAvailable(int port, Protocol protocol);

  /**
   * Returns whether or not the given port on the local host is available (that is, unused).
   *
   * @param port The port to check
   * @param protocol The protocol to check (either {@link Protocol#SOCKET} or
   *        {@link Protocol#MULTICAST}).
   *
   * @throws IllegalArgumentException {@code protocol} is unknown
   *
   * @deprecated Please use {@link #isPortAvailable(int, Protocol)} instead.
   */
  @Deprecated
  default boolean isPortAvailable(int port, int protocol) {
    return isPortAvailable(port, Protocol.valueOf(protocol));
  }

  /**
   * Returns whether or not the given port on the local host is available (that is, unused).
   *
   * @param port The port to check
   * @param protocol The protocol to check (either {@link Protocol#SOCKET} or
   *        {@link Protocol#MULTICAST}).
   * @param address The bind address (or mcast address) to use
   *
   * @throws IllegalArgumentException If the given {@code protocol} is unknown
   */
  boolean isPortAvailable(int port, Protocol protocol, InetAddress address);

  /**
   * Returns whether or not the given port on the local host is available (that is, unused).
   *
   * @param port The port to check
   * @param protocol The protocol to check (either {@link Protocol#SOCKET} or
   *        {@link Protocol#MULTICAST}).
   * @param address The bind address (or mcast address) to use
   *
   * @throws IllegalArgumentException If the given {@code protocol} is unknown
   *
   * @deprecated Please use {@link #isPortAvailable(int, Protocol, InetAddress)} instead.
   */
  @Deprecated
  default boolean isPortAvailable(int port, int protocol, InetAddress address) {
    return isPortAvailable(port, Protocol.valueOf(protocol), address);
  }

  Keeper isPortKeepable(int port, Protocol protocol, InetAddress address);

  /**
   * @deprecated Please use {@link #isPortKeepable(int, Protocol, InetAddress)} instead.
   */
  @Deprecated
  default Keeper isPortKeepable(int port, int protocol, InetAddress address) {
    return isPortKeepable(port, Protocol.valueOf(protocol), address);
  }

  /**
   * Returns a randomly selected available port in the range 5001 to 32767.
   *
   * @param protocol The protocol to check (either {@link Protocol#SOCKET} or
   *        {@link Protocol#MULTICAST}).
   *
   * @throws IllegalArgumentException If the given {@code protocol} is unknown
   */
  int getRandomAvailablePort(Protocol protocol);

  /**
   * Returns a randomly selected available port in the range 5001 to 32767.
   *
   * @param protocol The protocol to check (either {@link Protocol#SOCKET} or
   *        {@link Protocol#MULTICAST}).
   *
   * @throws IllegalArgumentException If the given {@code protocol} is unknown
   *
   * @deprecated Please use {@link #getRandomAvailablePort(Protocol)} instead.
   */
  @Deprecated
  default int getRandomAvailablePort(int protocol) {
    return getRandomAvailablePort(Protocol.valueOf(protocol));
  }

  Keeper getRandomAvailablePortKeeper(Protocol protocol);

  /**
   * @deprecated Please use {@link #getRandomAvailablePortKeeper(Protocol)} instead.
   */
  @Deprecated
  default Keeper getRandomAvailablePortKeeper(int protocol) {
    return getRandomAvailablePortKeeper(Protocol.valueOf(protocol));
  }

  /**
   * Returns a randomly selected available port in the provided range.
   *
   * @param protocol The protocol to check (either {@link Protocol#SOCKET} or
   *        {@link Protocol#MULTICAST}).
   *
   * @throws IllegalArgumentException If the given {@code protocol} is unknown
   */
  int getAvailablePortInRange(int rangeBase, int rangeTop, Protocol protocol);

  /**
   * Returns a randomly selected available port in the provided range.
   *
   * @param protocol The protocol to check (either {@link Protocol#SOCKET} or
   *        {@link Protocol#MULTICAST}).
   *
   * @throws IllegalArgumentException If the given {@code protocol} is unknown
   *
   * @deprecated Please use {@link #getAvailablePortInRange(int, int, Protocol)} instead.
   */
  @Deprecated
  default int getAvailablePortInRange(int rangeBase, int rangeTop, int protocol) {
    return getAvailablePortInRange(rangeBase, rangeTop, Protocol.valueOf(protocol));
  }

  /**
   * Returns a randomly selected available port in the range 5001 to 32767 that satisfies a modulus
   *
   * @param protocol The protocol to check (either {@link Protocol#SOCKET} or
   *        {@link Protocol#MULTICAST}).
   *
   * @throws IllegalArgumentException If the given {@code protocol} is unknown
   */
  int getRandomAvailablePortWithMod(Protocol protocol, int mod);

  /**
   * Returns a randomly selected available port in the range 5001 to 32767 that satisfies a modulus
   *
   * @param protocol The protocol to check (either {@link Protocol#SOCKET} or
   *        {@link Protocol#MULTICAST}).
   *
   * @throws IllegalArgumentException If the given {@code protocol} is unknown
   *
   * @deprecated Please use {@link #getRandomAvailablePortWithMod(Protocol, int)} instead.
   */
  @Deprecated
  default int getRandomAvailablePortWithMod(int protocol, int mod) {
    return getRandomAvailablePortWithMod(Protocol.valueOf(protocol), mod);
  }

  /**
   * Returns a randomly selected available port in the range 5001 to 32767.
   *
   * @param protocol The protocol to check (either {@link Protocol#SOCKET} or
   *        {@link Protocol#MULTICAST}).
   * @param address The bind address or mcast address to use
   *
   * @throws IllegalArgumentException If the given {@code protocol} is unknown
   */
  int getRandomAvailablePort(Protocol protocol, InetAddress address);

  /**
   * Returns a randomly selected available port in the range 5001 to 32767.
   *
   * @param protocol The protocol to check (either {@link Protocol#SOCKET} or
   *        {@link Protocol#MULTICAST}).
   * @param address The bind address or mcast address to use
   *
   * @throws IllegalArgumentException If the given {@code protocol} is unknown
   *
   * @deprecated Please use {@link #getRandomAvailablePort(Protocol, InetAddress)} instead.
   */
  @Deprecated
  default int getRandomAvailablePort(int protocol, InetAddress address) {
    return getRandomAvailablePort(Protocol.valueOf(protocol), address);
  }

  /**
   * Returns a randomly selected available port in the range 5001 to 32767.
   *
   * @param protocol The protocol to check (either {@link Protocol#SOCKET} or
   *        {@link Protocol#MULTICAST}).
   * @param address The bind address or mcast address to use
   * @param useMembershipPortRange True if the port will be used for membership
   *
   * @throws IllegalArgumentException If the given {@code protocol} is unknown
   */
  int getRandomAvailablePort(Protocol protocol, InetAddress address,
      boolean useMembershipPortRange);

  /**
   * Returns a randomly selected available port in the range 5001 to 32767.
   *
   * @param protocol The protocol to check (either {@link Protocol#SOCKET} or
   *        {@link Protocol#MULTICAST}).
   * @param address The bind address or mcast address to use
   * @param useMembershipPortRange True if the port will be used for membership
   *
   * @throws IllegalArgumentException If the given {@code protocol} is unknown
   *
   * @deprecated Please use {@link #getRandomAvailablePort(Protocol, InetAddress, boolean)} instead.
   */
  @Deprecated
  default int getRandomAvailablePort(int protocol, InetAddress address,
      boolean useMembershipPortRange) {
    return getRandomAvailablePort(Protocol.valueOf(protocol), address, useMembershipPortRange);
  }

  Keeper getRandomAvailablePortKeeper(Protocol protocol, InetAddress address);

  /**
   * @deprecated Please use {@link #getRandomAvailablePortKeeper(Protocol, InetAddress)} instead.
   */
  @Deprecated
  default Keeper getRandomAvailablePortKeeper(int protocol, InetAddress address) {
    return getRandomAvailablePortKeeper(Protocol.valueOf(protocol), address);
  }

  Keeper getRandomAvailablePortKeeper(Protocol protocol, InetAddress address,
      boolean useMembershipPortRange);

  /**
   * @deprecated Please use {@link #getRandomAvailablePortKeeper(Protocol, InetAddress, boolean)}
   *             instead.
   */
  @Deprecated
  default Keeper getRandomAvailablePortKeeper(int protocol, InetAddress address,
      boolean useMembershipPortRange) {
    return getRandomAvailablePortKeeper(Protocol.valueOf(protocol), address,
        useMembershipPortRange);
  }

  /**
   * Returns a randomly selected available port in the provided range.
   *
   * @param protocol The protocol to check (either {@link Protocol#SOCKET} or
   *        {@link Protocol#MULTICAST}).
   * @param address The bind address or mcast address to use
   *
   * @throws IllegalArgumentException If the given {@code protocol} is unknown
   */
  int getAvailablePortInRange(Protocol protocol, InetAddress address, int rangeBase, int rangeTop);

  /**
   * Returns a randomly selected available port in the provided range.
   *
   * @param protocol The protocol to check (either {@link Protocol#SOCKET} or
   *        {@link Protocol#MULTICAST}).
   * @param address The bind address or mcast address to use
   *
   * @throws IllegalArgumentException If the given {@code protocol} is unknown
   *
   * @deprecated Please use {@link #getAvailablePortInRange(Protocol, InetAddress, int, int)}
   *             instead.
   */
  @Deprecated
  default int getAvailablePortInRange(int protocol, InetAddress address, int rangeBase,
      int rangeTop) {
    return getAvailablePortInRange(Protocol.valueOf(protocol), address, rangeBase, rangeTop);
  }

  /**
   * Returns a randomly selected available port in the range 5001 to 32767 that satisfies a modulus
   * and the provided protocol
   *
   * @param protocol The protocol to check (either {@link Protocol#SOCKET} or
   *        {@link Protocol#MULTICAST}).
   * @param address The bind address or mcast address to use
   *
   * @throws IllegalArgumentException If the given {@code protocol} is unknown
   */
  int getRandomAvailablePortWithMod(Protocol protocol, InetAddress address, int mod);

  /**
   * Returns a randomly selected available port in the range 5001 to 32767 that satisfies a modulus
   * and the provided protocol
   *
   * @param protocol The protocol to check (either {@link Protocol#SOCKET} or
   *        {@link Protocol#MULTICAST}).
   * @param address The bind address or mcast address to use
   *
   * @throws IllegalArgumentException If the given {@code protocol} is unknown
   *
   * @deprecated Please use {@link #getRandomAvailablePortWithMod(Protocol, InetAddress, int)}
   *             instead.
   */
  @Deprecated
  default int getRandomAvailablePortWithMod(int protocol, InetAddress address, int mod) {
    return getRandomAvailablePortWithMod(Protocol.valueOf(protocol), address, mod);
  }

  int getRandomAvailablePortInRange(int rangeBase, int rangeTop, Protocol protocol);

  /**
   * @deprecated Please use {@link #getRandomAvailablePortInRange(int, int, Protocol)} instead.
   */
  @Deprecated
  default int getRandomAvailablePortInRange(int rangeBase, int rangeTop, int protocol) {
    return getRandomAvailablePortInRange(rangeBase, rangeTop, Protocol.valueOf(protocol));
  }

  /**
   * This class will keep an allocated port allocated until it is used.
   */
  class Keeper implements Serializable {

    private final transient ServerSocket serverSocket;
    private final int port;

    public Keeper(ServerSocket serverSocket) {
      this(serverSocket, 0);
    }

    public Keeper(ServerSocket serverSocket, int port) {
      this.serverSocket = serverSocket;
      this.port = port;
    }

    public int getPort() {
      return port;
    }

    /**
     * Once you call this the socket will be freed and can then be reallocated by someone else.
     */
    public void release() {
      try {
        if (serverSocket != null) {
          serverSocket.close();
        }
      } catch (IOException ignore) {
        // ignored
      }
    }
  }
}
