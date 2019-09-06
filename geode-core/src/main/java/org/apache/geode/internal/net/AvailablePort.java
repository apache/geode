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
 *
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

    private final int value;

    Protocol(int value) {
      this.value = value;
    }

    public int value() {
      return value;
    }
  }

  enum Range {
    LOWER_BOUND(20001), // 20000/udp is securid
    UPPER_BOUND(29999);// 30000/tcp is spoolfax

    private final int value;

    Range(int value) {
      this.value = value;
    }

    public int value() {
      return value;
    }
  }

  static AvailablePort create() {
    return new AvailablePortImpl();
  }

  /**
   * see if there is a gemfire system property that establishes a default address for the given
   * protocol, and return it
   */
  InetAddress getAddress(int protocol);

  /**
   * Returns whether or not the given port on the local host is available (that is, unused).
   *
   * @param port The port to check
   * @param protocol The protocol to check (either {@link Protocol#SOCKET} or
   *        {@link Protocol#MULTICAST}).
   *
   * @throws IllegalArgumentException {@code protocol} is unknown
   */
  boolean isPortAvailable(final int port, int protocol);

  /**
   * Returns whether or not the given port on the local host is available (that is, unused).
   *
   * @param port The port to check
   * @param protocol The protocol to check (either {@link Protocol#SOCKET} or
   *        {@link Protocol#MULTICAST}).
   * @param addr The bind address (or mcast address) to use
   *
   * @throws IllegalArgumentException If the given {@code protocol} is unknown
   */
  boolean isPortAvailable(final int port, int protocol, InetAddress addr);

  Keeper isPortKeepable(final int port, int protocol, InetAddress addr);

  /**
   * Returns a randomly selected available port in the range 5001 to 32767.
   *
   * @param protocol The protocol to check (either {@link Protocol#SOCKET} or
   *        {@link Protocol#MULTICAST}).
   *
   * @throws IllegalArgumentException If the given {@code protocol} is unknown
   */
  int getRandomAvailablePort(int protocol);

  Keeper getRandomAvailablePortKeeper(int protocol);

  /**
   * Returns a randomly selected available port in the provided range.
   *
   * @param protocol The protocol to check (either {@link Protocol#SOCKET} or
   *        {@link Protocol#MULTICAST}).
   *
   * @throws IllegalArgumentException If the given {@code protocol} is unknown
   */
  int getAvailablePortInRange(int rangeBase, int rangeTop, int protocol);

  /**
   * Returns a randomly selected available port in the range 5001 to 32767 that satisfies a modulus
   *
   * @param protocol The protocol to check (either {@link Protocol#SOCKET} or
   *        {@link Protocol#MULTICAST}).
   *
   * @throws IllegalArgumentException If the given {@code protocol} is unknown
   */
  int getRandomAvailablePortWithMod(int protocol, int mod);

  /**
   * Returns a randomly selected available port in the range 5001 to 32767.
   *
   * @param protocol The protocol to check (either {@link Protocol#SOCKET} or
   *        {@link Protocol#MULTICAST}).
   * @param addr The bind address or mcast address to use
   *
   * @throws IllegalArgumentException If the given {@code protocol} is unknown
   */
  int getRandomAvailablePort(int protocol, InetAddress addr);

  /**
   * Returns a randomly selected available port in the range 5001 to 32767.
   *
   * @param protocol The protocol to check (either {@link Protocol#SOCKET} or
   *        {@link Protocol#MULTICAST}).
   * @param addr The bind address or mcast address to use
   * @param useMembershipPortRange True if the port will be used for membership
   *
   * @throws IllegalArgumentException If the given {@code protocol} is unknown
   */
  int getRandomAvailablePort(int protocol, InetAddress addr, boolean useMembershipPortRange);

  Keeper getRandomAvailablePortKeeper(int protocol, InetAddress addr);

  Keeper getRandomAvailablePortKeeper(int protocol, InetAddress addr,
      boolean useMembershipPortRange);

  /**
   * Returns a randomly selected available port in the provided range.
   *
   * @param protocol The protocol to check (either {@link Protocol#SOCKET} or
   *        {@link Protocol#MULTICAST}).
   * @param addr The bind address or mcast address to use
   *
   * @throws IllegalArgumentException If the given {@code protocol} is unknown
   */
  int getAvailablePortInRange(int protocol, InetAddress addr, int rangeBase, int rangeTop);

  /**
   * Returns a randomly selected available port in the range 5001 to 32767 that satisfies a modulus
   * and the provided protocol
   *
   * @param protocol The protocol to check (either {@link Protocol#SOCKET} or
   *        {@link Protocol#MULTICAST}).
   * @param addr The bind address or mcast address to use
   *
   * @throws IllegalArgumentException If the given {@code protocol} is unknown
   */
  int getRandomAvailablePortWithMod(int protocol, InetAddress addr, int mod);

  int getRandomAvailablePortInRange(int rangeBase, int rangeTop, int protocol);

  /**
   * This class will keep an allocated port allocated until it is used. This makes the window
   * smaller that can cause bug 46690
   */
  class Keeper implements Serializable {

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
}
