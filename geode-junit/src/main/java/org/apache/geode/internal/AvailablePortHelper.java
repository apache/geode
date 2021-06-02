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

import static org.apache.geode.distributed.internal.DistributionConfig.DEFAULT_MEMBERSHIP_PORT_RANGE;
import static org.apache.geode.internal.AvailablePort.AVAILABLE_PORTS_LOWER_BOUND;
import static org.apache.geode.internal.AvailablePort.AVAILABLE_PORTS_UPPER_BOUND;
import static org.apache.geode.internal.AvailablePort.getAddress;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.geode.internal.AvailablePort.Keeper;

/**
 * Provides helper methods for acquiring a set of unique available ports. It is not safe to simply
 * call AvailablePort.getRandomAvailablePort several times in a row without doing something to
 * ensure that they are unique. Although they are random, it is possible for subsequent calls to
 * getRandomAvailablePort to return the same integer, unless that port is put into use before
 * further calls to getRandomAvailablePort.
 */
public class AvailablePortHelper {
  private final AtomicInteger currentMembershipPort;
  private final AtomicInteger currentAvailablePort;

  // Singleton object is only used to track the current ports
  private static final AvailablePortHelper singleton = new AvailablePortHelper();

  AvailablePortHelper() {
    Random rand;
    boolean fast = Boolean.getBoolean("AvailablePort.fastRandom");
    if (fast) {
      rand = new Random();
    } else {
      rand = new java.security.SecureRandom();
    }
    currentMembershipPort = new AtomicInteger(
        rand.nextInt(DEFAULT_MEMBERSHIP_PORT_RANGE[1] - DEFAULT_MEMBERSHIP_PORT_RANGE[0])
            + DEFAULT_MEMBERSHIP_PORT_RANGE[0]);
    currentAvailablePort =
        new AtomicInteger(rand.nextInt(AVAILABLE_PORTS_UPPER_BOUND - AVAILABLE_PORTS_LOWER_BOUND)
            + AVAILABLE_PORTS_LOWER_BOUND);
  }

  /**
   * Returns array of unique randomly available tcp ports of specified count.
   */
  public static int[] getRandomAvailableTCPPorts(int count) {
    return getRandomAvailableTCPPorts(count, false);
  }

  /**
   * Returns an array of unique randomly available tcp ports
   *
   * @param count number of desired ports
   * @param useMembershipPortRange whether to use the configured membership-port-range
   * @return the ports
   */
  public static int[] getRandomAvailableTCPPorts(int count, boolean useMembershipPortRange) {
    List<Keeper> list = getRandomAvailableTCPPortKeepers(count, useMembershipPortRange);
    int[] ports = new int[list.size()];
    int i = 0;
    for (Keeper k : list) {
      ports[i] = k.getPort();
      k.release();
      i++;
    }
    return ports;
  }

  public static List<Keeper> getRandomAvailableTCPPortKeepers(int count) {
    return getRandomAvailableTCPPortKeepers(count, false);
  }

  private static List<Keeper> getRandomAvailableTCPPortKeepers(int count,
      boolean useMembershipPortRange) {
    List<Keeper> result = new ArrayList<>();
    while (result.size() < count) {
      result.add(getUniquePortKeeper(useMembershipPortRange, AvailablePort.SOCKET));
    }
    return result;
  }

  public static int[] getRandomAvailableTCPPortRange(final int count) {
    return getRandomAvailableTCPPortRange(count, false);
  }

  public static int[] getRandomAvailableTCPPortRange(final int count,
      final boolean useMembershipPortRange) {
    List<Keeper> list =
        getUniquePortRangeKeepers(useMembershipPortRange, AvailablePort.SOCKET,
            count);
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
    return getUniquePortRangeKeepers(useMembershipPortRange, AvailablePort.SOCKET,
        count);
  }

  private static void releaseKeepers(List<Keeper> keepers) {
    for (Keeper keeper : keepers) {
      keeper.release();
    }
  }

  /**
   * Returns array of unique randomly available tcp ports of specified count.
   */
  public static int[] getRandomAvailableTCPPortsForDUnitSite(int count) {
    int site = 1;
    String hostName = System.getProperty("hostName");
    if (hostName != null && hostName.startsWith("host") && hostName.length() > 4) {
      site = Integer.parseInt(hostName.substring(4));
    }

    int[] ports = new int[count];
    int i = 0;
    while (i < count) {
      int port = getUniquePort(false, AvailablePort.SOCKET);
      // This logic is from AvailablePort.getRandomAvailablePortWithMod which this method used to
      // call. It seems like the check should be (port % FOO == site) for some FOO, but given how
      // widely this is used, it's not at all clear that no one's depending on the current behavior.
      while (port % site != 0) {
        port = getUniquePort(false, AvailablePort.SOCKET);
      }
      ports[i] = port;
      ++i;
    }
    return ports;
  }


  /**
   * Returns array of unique randomly available tcp ports of specified count.
   */
  public static int getRandomAvailablePortForDUnitSite() {
    return getRandomAvailableTCPPortsForDUnitSite(1)[0];
  }


  /**
   * Returns randomly available tcp port.
   */
  public static int getRandomAvailableTCPPort() {
    return getRandomAvailableTCPPorts(1)[0];
  }

  /**
   * Returns array of unique randomly available udp ports of specified count.
   */
  private static int[] getRandomAvailableUDPPorts(int count) {
    int[] ports = new int[count];
    int i = 0;
    while (i < count) {
      ports[i] = getUniquePort(false, AvailablePort.MULTICAST);
      ++i;
    }
    return ports;
  }

  /**
   * Returns randomly available udp port.
   */
  public static int getRandomAvailableUDPPort() {
    return getRandomAvailableUDPPorts(1)[0];
  }

  public static void initializeUniquePortRange(int rangeNumber) {
    if (rangeNumber < 0) {
      throw new RuntimeException("Range number cannot be negative.");
    }
    singleton.currentMembershipPort.set(DEFAULT_MEMBERSHIP_PORT_RANGE[0]);
    singleton.currentAvailablePort.set(AVAILABLE_PORTS_LOWER_BOUND);
    if (rangeNumber != 0) {
      // This code will generate starting points such that range 0 starts at the lowest possible
      // value, range 1 starts halfway through the total available ports, 2 starts 1/4 of the way
      // through, then further ranges are 3/4, 1/8, 3/8, 5/8, 7/8, 1/16, etc.

      // This spaces the ranges out as much as possible for low numbers of ranges, while also making
      // it possible to grow the number of ranges without bound (within some reasonable fraction of
      // the number of available ports)
      int membershipRange = DEFAULT_MEMBERSHIP_PORT_RANGE[1] - DEFAULT_MEMBERSHIP_PORT_RANGE[0];
      int availableRange = AVAILABLE_PORTS_UPPER_BOUND - AVAILABLE_PORTS_LOWER_BOUND;
      int numChunks = Integer.highestOneBit(rangeNumber) << 1;
      int chunkNumber = 2 * (rangeNumber - Integer.highestOneBit(rangeNumber)) + 1;

      singleton.currentMembershipPort.addAndGet(chunkNumber * membershipRange / numChunks);
      singleton.currentAvailablePort.addAndGet(chunkNumber * availableRange / numChunks);
    }
  }

  /**
   * Get keeper objects for the next unused, consecutive 'rangeSize' ports on this machine.
   *
   * @param useMembershipPortRange - if true, select ports from the
   *        DistributionConfig.DEFAULT_MEMBERSHIP_PORT_RANGE
   * @param protocol - either AvailablePort.SOCKET (TCP) or AvailablePort.MULTICAST (UDP)
   * @param rangeSize - number of contiguous ports needed
   * @return Keeper objects associated with a range of ports satisfying the request
   */
  private static List<Keeper> getUniquePortRangeKeepers(boolean useMembershipPortRange,
      int protocol, int rangeSize) {
    AtomicInteger targetRange =
        useMembershipPortRange ? singleton.currentMembershipPort : singleton.currentAvailablePort;
    int targetBound =
        useMembershipPortRange ? DEFAULT_MEMBERSHIP_PORT_RANGE[1] : AVAILABLE_PORTS_UPPER_BOUND;

    while (true) {
      int uniquePort = targetRange.getAndAdd(rangeSize);
      if (uniquePort + rangeSize > targetBound) {
        targetRange.set(useMembershipPortRange ? DEFAULT_MEMBERSHIP_PORT_RANGE[0]
            : AVAILABLE_PORTS_LOWER_BOUND);
        continue;
      }
      List<Keeper> keepers = new ArrayList<>();
      int validPortsFound = 0;

      while (validPortsFound < rangeSize) {
        Keeper testKeeper =
            AvailablePort.isPortKeepable(uniquePort++, protocol,
                getAddress(protocol));
        if (testKeeper == null) {
          break;
        }

        keepers.add(testKeeper);
        ++validPortsFound;
      }

      if (validPortsFound == rangeSize) {
        return keepers;
      }

      releaseKeepers(keepers);
    }
  }

  private static Keeper getUniquePortKeeper(boolean useMembershipPortRange, int protocol) {
    return getUniquePortRangeKeepers(useMembershipPortRange, protocol, 1).get(0);
  }

  /**
   * Get the next available port on this machine.
   */
  private static int getUniquePort(boolean useMembershipPortRange, int protocol) {
    AtomicInteger targetRange =
        useMembershipPortRange ? singleton.currentMembershipPort : singleton.currentAvailablePort;
    int targetBound =
        useMembershipPortRange ? DEFAULT_MEMBERSHIP_PORT_RANGE[1] : AVAILABLE_PORTS_UPPER_BOUND;

    while (true) {
      int uniquePort = targetRange.getAndIncrement();
      if (uniquePort > targetBound) {
        targetRange.set(useMembershipPortRange ? DEFAULT_MEMBERSHIP_PORT_RANGE[0]
            : AVAILABLE_PORTS_LOWER_BOUND);
        continue;
      }

      if (AvailablePort.isPortAvailable(uniquePort, protocol, getAddress(protocol))) {
        return uniquePort;
      }
    }
  }
}
