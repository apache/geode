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

import static java.util.stream.Collectors.toList;
import static org.apache.geode.distributed.internal.membership.api.MembershipConfig.DEFAULT_MEMBERSHIP_PORT_RANGE;
import static org.apache.geode.internal.membership.utils.AvailablePort.AVAILABLE_PORTS_LOWER_BOUND;
import static org.apache.geode.internal.membership.utils.AvailablePort.AVAILABLE_PORTS_UPPER_BOUND;
import static org.apache.geode.internal.membership.utils.AvailablePort.MULTICAST;
import static org.apache.geode.internal.membership.utils.AvailablePort.SOCKET;
import static org.apache.geode.internal.membership.utils.AvailablePort.getAddress;
import static org.apache.geode.internal.membership.utils.AvailablePort.isPortAvailable;
import static org.apache.geode.internal.membership.utils.AvailablePort.isPortKeepable;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.geode.internal.membership.utils.AvailablePort.Keeper;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Methods for acquiring one or more available ports. Despite "random" in the names, these methods
 * allocate ports in a round-robin fashion.
 */
public class AvailablePortHelper {
  private static final Pattern MEMBERSHIP_PORT_RANGE_PATTERN =
      Pattern.compile("^\\s*(\\d+)\\s*-\\s*(\\d+)\\s*$");
  private static final int MEMBERSHIP_PORTS_LOWER_BOUND;
  private static final int MEMBERSHIP_PORTS_UPPER_BOUND;

  static {
    String membershipRange = System.getProperty(
        GeodeGlossary.GEMFIRE_PREFIX + "membership-port-range", "");
    Matcher matcher = MEMBERSHIP_PORT_RANGE_PATTERN.matcher(membershipRange);
    if (matcher.matches()) {
      MEMBERSHIP_PORTS_LOWER_BOUND = Integer.parseInt(matcher.group(1));
      MEMBERSHIP_PORTS_UPPER_BOUND = Integer.parseInt(matcher.group(2));
    } else {
      MEMBERSHIP_PORTS_LOWER_BOUND = DEFAULT_MEMBERSHIP_PORT_RANGE[0];
      MEMBERSHIP_PORTS_UPPER_BOUND = DEFAULT_MEMBERSHIP_PORT_RANGE[1];
    }
  }

  private final AtomicInteger nextMembershipPort;
  private final AtomicInteger nextAvailablePort;

  // Singleton object is only used to track the current ports
  private static final AvailablePortHelper singleton = new AvailablePortHelper();

  AvailablePortHelper() {
    Random rand = rand();
    nextAvailablePort =
        randomInRange(rand, AVAILABLE_PORTS_LOWER_BOUND, AVAILABLE_PORTS_UPPER_BOUND);
    nextMembershipPort =
        randomInRange(rand, MEMBERSHIP_PORTS_LOWER_BOUND, MEMBERSHIP_PORTS_UPPER_BOUND);
  }

  /**
   * Returns an available tcp port.
   */
  public static int getRandomAvailableTCPPort() {
    return availablePort(SOCKET);
  }

  /**
   * Returns an available udp port.
   */
  public static int getRandomAvailableUDPPort() {
    return availablePort(MULTICAST);
  }

  /**
   * Returns the requested number of available tcp ports.
   */
  public static int[] getRandomAvailableTCPPorts(int count) {
    return IntStream.generate(AvailablePortHelper::getRandomAvailableTCPPort)
        .limit(count)
        .toArray();
  }

  /**
   * Returns the requested number of consecutive available tcp ports from the specified range.
   */
  public static int[] getRandomAvailableTCPPortRange(final int count,
      final boolean useMembershipPortRange) {
    AtomicInteger targetRange =
        useMembershipPortRange ? singleton.nextMembershipPort : singleton.nextAvailablePort;
    int targetLowerBound =
        useMembershipPortRange ? MEMBERSHIP_PORTS_LOWER_BOUND : AVAILABLE_PORTS_LOWER_BOUND;
    int targetUpperBound =
        useMembershipPortRange ? MEMBERSHIP_PORTS_UPPER_BOUND : AVAILABLE_PORTS_UPPER_BOUND;

    int[] ports = new int[count];
    boolean needMorePorts = true;

    while (needMorePorts) {
      int base = targetRange.getAndAdd(count);
      if (base + count > targetUpperBound) {
        targetRange.set(targetLowerBound);
        continue;
      }

      needMorePorts = false; // Assume we'll find enough in this batch
      for (int i = 0; i < count; i++) {
        int port = base + i;
        if (isPortAvailable(port, SOCKET, getAddress(SOCKET))) {
          ports[i] = port;
          continue;
        }
        needMorePorts = true;
        break;
      }
    }
    return ports;
  }

  /**
   * Returns the specified number of port keepers.
   */
  public static List<Keeper> getRandomAvailableTCPPortKeepers(int count) {
    return Stream.generate(AvailablePortHelper::availableKeeper)
        .limit(count)
        .collect(toList());
  }

  /**
   * Assign this JVM's next available membership and non-membership ports based on the given
   * index. If each JVM on the machine calls this function with a small, distinct number, the
   * algorithm:
   * <ul>
   * <li>Separates the JVMs' next available ports reasonably well</li>
   * <li>Allows adding JVMs without needing to know the total number of JVMs ahead of time</li>
   * </ul>
   *
   * @param jvmIndex a small number different from that of any other JVM running on this machine
   */
  public static void initializeUniquePortRange(int jvmIndex) {
    if (jvmIndex < 0) {
      throw new RuntimeException("Range number cannot be negative.");
    }

    // Generate starting points such that JVM 0 starts at the lower bound of the total port
    // range, JVM 1 starts halfway through, JVM 2 starts 1/4 of the way through, then further
    // ranges are 3/4, 1/8, 3/8, 5/8, 7/8, 1/16, etc.

    singleton.nextMembershipPort.set(MEMBERSHIP_PORTS_LOWER_BOUND);
    singleton.nextAvailablePort.set(AVAILABLE_PORTS_LOWER_BOUND);
    if (jvmIndex == 0) {
      return;
    }

    int membershipRange = MEMBERSHIP_PORTS_UPPER_BOUND - MEMBERSHIP_PORTS_LOWER_BOUND;
    int availableRange = AVAILABLE_PORTS_UPPER_BOUND - AVAILABLE_PORTS_LOWER_BOUND;
    int numChunks = Integer.highestOneBit(jvmIndex) << 1;
    int chunkNumber = 2 * (jvmIndex - Integer.highestOneBit(jvmIndex)) + 1;

    singleton.nextMembershipPort.addAndGet(chunkNumber * membershipRange / numChunks);
    singleton.nextAvailablePort.addAndGet(chunkNumber * availableRange / numChunks);
  }

  private static int availablePort(int protocol) {
    AtomicInteger targetRange = singleton.nextAvailablePort;

    while (true) {
      int port = targetRange.getAndIncrement();
      if (port > AVAILABLE_PORTS_UPPER_BOUND) {
        targetRange.set(AVAILABLE_PORTS_LOWER_BOUND);
        continue;
      }

      if (isPortAvailable(port, protocol, getAddress(protocol))) {
        return port;
      }
    }
  }

  private static Keeper availableKeeper() {
    int count = 1;
    AtomicInteger targetRange = singleton.nextAvailablePort;

    while (true) {
      int uniquePort = targetRange.getAndIncrement();
      if (uniquePort + count > AVAILABLE_PORTS_UPPER_BOUND) {
        targetRange.set(AVAILABLE_PORTS_LOWER_BOUND);
        continue;
      }
      Keeper keeper = isPortKeepable(uniquePort, SOCKET, getAddress(SOCKET));
      if (keeper != null) {
        return keeper;
      }
    }
  }

  private static AtomicInteger randomInRange(Random rand, int lowerBound, int upperBound) {
    return new AtomicInteger(rand.nextInt(upperBound - lowerBound) + lowerBound);
  }

  private static Random rand() {
    boolean fast = Boolean.getBoolean("AvailablePort.fastRandom");
    return fast ? new Random() : new java.security.SecureRandom();
  }
}
