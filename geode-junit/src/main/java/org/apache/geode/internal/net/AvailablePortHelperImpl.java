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

import static java.lang.Boolean.getBoolean;
import static org.apache.geode.distributed.internal.DistributionConfig.DEFAULT_MEMBERSHIP_PORT_RANGE;
import static org.apache.geode.internal.net.AvailablePort.Range.LOWER_BOUND;
import static org.apache.geode.internal.net.AvailablePort.Range.UPPER_BOUND;

import java.net.InetAddress;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provides helper methods for acquiring a set of unique available ports. It is not safe to simply
 * call getRandomAvailablePort several times in a row without doing something to ensure that they
 * are unique. Although they are random, it is possible for subsequent calls to
 * getRandomAvailablePort to return the same integer, unless that port is put into use before
 * further calls to getRandomAvailablePort.
 */
class AvailablePortHelperImpl implements AvailablePortHelper {

  private final AvailablePort availablePort;
  private final AtomicInteger currentMembershipPort;
  private final AtomicInteger currentAvailablePort;

  AvailablePortHelperImpl() {
    availablePort = AvailablePort.create();

    Random random = getBoolean("AvailablePort.fastRandom") ? new Random() : new SecureRandom();

    currentMembershipPort = new AtomicInteger(
        random.nextInt(DEFAULT_MEMBERSHIP_PORT_RANGE[1] - DEFAULT_MEMBERSHIP_PORT_RANGE[0])
            + DEFAULT_MEMBERSHIP_PORT_RANGE[0]);
    currentAvailablePort =
        new AtomicInteger(random.nextInt(UPPER_BOUND.intLevel() - LOWER_BOUND.intLevel())
            + LOWER_BOUND.intLevel());
  }

  /**
   * Returns array of unique randomly available tcp ports of specified count.
   */
  public int[] getRandomAvailableTCPPorts(int count) {
    return getRandomAvailableTCPPorts(count, false);
  }

  /**
   * Returns an array of unique randomly available tcp ports
   *
   * @param count number of desired ports
   * @param useMembershipPortRange whether to use the configured membership-port-range
   * @return the ports
   */
  public int[] getRandomAvailableTCPPorts(int count, boolean useMembershipPortRange) {
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

  public List<Keeper> getRandomAvailableTCPPortKeepers(int count) {
    return getRandomAvailableTCPPortKeepers(count, false);
  }

  private List<Keeper> getRandomAvailableTCPPortKeepers(int count,
      boolean useMembershipPortRange) {
    List<Keeper> result = new ArrayList<>();
    while (result.size() < count) {
      result.add(getUniquePortKeeper(useMembershipPortRange, Protocol.SOCKET.intLevel()));
    }
    return result;
  }

  public int[] getRandomAvailableTCPPortRange(final int count) {
    return getRandomAvailableTCPPortRange(count, false);
  }

  public int[] getRandomAvailableTCPPortRange(final int count,
      final boolean useMembershipPortRange) {
    List<Keeper> list =
        getUniquePortRangeKeepers(useMembershipPortRange, Protocol.SOCKET.intLevel(), count);
    int[] ports = new int[list.size()];
    int i = 0;
    for (Keeper k : list) {
      ports[i] = k.getPort();
      k.release();
      i++;
    }
    return ports;
  }

  public List<Keeper> getRandomAvailableTCPPortRangeKeepers(final int count) {
    return getRandomAvailableTCPPortRangeKeepers(count, false);
  }

  public List<Keeper> getRandomAvailableTCPPortRangeKeepers(final int count,
      final boolean useMembershipPortRange) {
    return getUniquePortRangeKeepers(useMembershipPortRange, Protocol.SOCKET.intLevel(), count);
  }

  private void releaseKeepers(List<Keeper> keepers) {
    for (Keeper keeper : keepers) {
      keeper.release();
    }
  }

  /**
   * Returns array of unique randomly available tcp ports of specified count.
   */
  public int[] getRandomAvailableTCPPortsForDUnitSite(int count) {
    int site = 1;
    String hostName = System.getProperty("hostName");
    if (hostName != null && hostName.startsWith("host") && hostName.length() > 4) {
      site = Integer.parseInt(hostName.substring(4));
    }

    int[] ports = new int[count];
    int i = 0;
    while (i < count) {
      int port = getUniquePort(false, Protocol.SOCKET.intLevel());
      // This logic is from getRandomAvailablePortWithMod which this method used to
      // call. It seems like the check should be (port % FOO == site) for some FOO, but given how
      // widely this is used, it's not at all clear that no one's depending on the current behavior.
      while (port % site != 0) {
        port = getUniquePort(false, Protocol.SOCKET.intLevel());
      }
      ports[i] = port;
      ++i;
    }
    return ports;
  }


  /**
   * Returns array of unique randomly available tcp ports of specified count.
   */
  public int getRandomAvailablePortForDUnitSite() {
    return getRandomAvailableTCPPortsForDUnitSite(1)[0];
  }


  /**
   * Returns randomly available tcp port.
   */
  public int getRandomAvailableTCPPort() {
    return getRandomAvailableTCPPorts(1)[0];
  }

  /**
   * Returns array of unique randomly available udp ports of specified count.
   */
  private int[] getRandomAvailableUDPPorts(int count) {
    int[] ports = new int[count];
    int i = 0;
    while (i < count) {
      ports[i] = getUniquePort(false, Protocol.MULTICAST.intLevel());
      ++i;
    }
    return ports;
  }

  /**
   * Returns randomly available udp port.
   */
  public int getRandomAvailableUDPPort() {
    return getRandomAvailableUDPPorts(1)[0];
  }

  public void initializeUniquePortRange(int rangeNumber) {
    if (rangeNumber < 0) {
      throw new RuntimeException("Range number cannot be negative.");
    }
    currentMembershipPort.set(DEFAULT_MEMBERSHIP_PORT_RANGE[0]);
    currentAvailablePort.set(LOWER_BOUND.intLevel());
    if (rangeNumber != 0) {
      // This code will generate starting points such that range 0 starts at the lowest possible
      // value, range 1 starts halfway through the total available ports, 2 starts 1/4 of the way
      // through, then further ranges are 3/4, 1/8, 3/8, 5/8, 7/8, 1/16, etc.

      // This spaces the ranges out as much as possible for low numbers of ranges, while also making
      // it possible to grow the number of ranges without bound (within some reasonable fraction of
      // the number of available ports)
      int membershipRange = DEFAULT_MEMBERSHIP_PORT_RANGE[1] - DEFAULT_MEMBERSHIP_PORT_RANGE[0];
      int availableRange = UPPER_BOUND.intLevel() - LOWER_BOUND.intLevel();
      int numChunks = Integer.highestOneBit(rangeNumber) << 1;
      int chunkNumber = 2 * (rangeNumber - Integer.highestOneBit(rangeNumber)) + 1;

      currentMembershipPort.addAndGet(chunkNumber * membershipRange / numChunks);
      currentAvailablePort.addAndGet(chunkNumber * availableRange / numChunks);
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
  private List<Keeper> getUniquePortRangeKeepers(boolean useMembershipPortRange,
      int protocol, int rangeSize) {
    AtomicInteger targetRange =
        useMembershipPortRange ? currentMembershipPort : currentAvailablePort;
    int targetBound =
        useMembershipPortRange ? DEFAULT_MEMBERSHIP_PORT_RANGE[1] : UPPER_BOUND.intLevel();

    while (true) {
      int uniquePort = targetRange.getAndAdd(rangeSize);
      if (uniquePort + rangeSize > targetBound) {
        targetRange.set(useMembershipPortRange ? DEFAULT_MEMBERSHIP_PORT_RANGE[0]
            : LOWER_BOUND.intLevel());
        continue;
      }
      List<Keeper> keepers = new ArrayList<>();
      int validPortsFound = 0;

      while (validPortsFound < rangeSize) {
        Keeper testKeeper =
            availablePort.isPortKeepable(uniquePort++, protocol,
                availablePort.getAddress(protocol));
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

  private Keeper getUniquePortKeeper(boolean useMembershipPortRange, int protocol) {
    return getUniquePortRangeKeepers(useMembershipPortRange, protocol, 1).get(0);
  }

  /**
   * Get the next available port on this machine.
   */
  private int getUniquePort(boolean useMembershipPortRange, int protocol) {
    AtomicInteger targetRange =
        useMembershipPortRange ? currentMembershipPort : currentAvailablePort;
    int targetBound =
        useMembershipPortRange ? DEFAULT_MEMBERSHIP_PORT_RANGE[1] : UPPER_BOUND.intLevel();

    while (true) {
      int uniquePort = targetRange.getAndIncrement();
      if (uniquePort > targetBound) {
        targetRange.set(useMembershipPortRange ? DEFAULT_MEMBERSHIP_PORT_RANGE[0]
            : LOWER_BOUND.intLevel());
        continue;
      }

      if (availablePort.isPortAvailable(uniquePort, protocol,
          availablePort.getAddress(protocol))) {
        return uniquePort;
      }
    }
  }

  @Override
  public InetAddress getAddress(Protocol protocol) {
    return availablePort.getAddress(protocol);
  }

  @Override
  public boolean isPortAvailable(int port, Protocol protocol) {
    return availablePort.isPortAvailable(port, protocol);
  }

  @Override
  public boolean isPortAvailable(int port, Protocol protocol, InetAddress address) {
    return availablePort.isPortAvailable(port, protocol, address);
  }

  @Override
  public Keeper isPortKeepable(int port, Protocol protocol, InetAddress address) {
    return availablePort.isPortKeepable(port, protocol, address);
  }

  @Override
  public int getRandomAvailablePort(Protocol protocol) {
    return availablePort.getRandomAvailablePort(protocol);
  }

  @Override
  public Keeper getRandomAvailablePortKeeper(Protocol protocol) {
    return availablePort.getRandomAvailablePortKeeper(protocol);
  }

  @Override
  public int getAvailablePortInRange(int rangeBase, int rangeTop, Protocol protocol) {
    return availablePort.getAvailablePortInRange(rangeBase, rangeTop, protocol);
  }

  @Override
  public int getRandomAvailablePortWithMod(Protocol protocol, int mod) {
    return availablePort.getRandomAvailablePortWithMod(protocol, mod);
  }

  @Override
  public int getRandomAvailablePort(Protocol protocol, InetAddress address) {
    return availablePort.getRandomAvailablePort(protocol, address);
  }

  @Override
  public int getRandomAvailablePort(Protocol protocol, InetAddress address,
      boolean useMembershipPortRange) {
    return availablePort.getRandomAvailablePort(protocol, address, useMembershipPortRange);
  }

  @Override
  public Keeper getRandomAvailablePortKeeper(Protocol protocol, InetAddress address) {
    return availablePort.getRandomAvailablePortKeeper(protocol, address);
  }

  @Override
  public Keeper getRandomAvailablePortKeeper(Protocol protocol, InetAddress address,
      boolean useMembershipPortRange) {
    return availablePort.getRandomAvailablePortKeeper(protocol, address, useMembershipPortRange);
  }

  @Override
  public int getAvailablePortInRange(Protocol protocol, InetAddress address, int rangeBase,
      int rangeTop) {
    return availablePort.getAvailablePortInRange(protocol, address, rangeBase, rangeTop);
  }

  @Override
  public int getRandomAvailablePortWithMod(Protocol protocol, InetAddress address, int mod) {
    return availablePort.getRandomAvailablePortWithMod(protocol, address, mod);
  }

  @Override
  public int getRandomAvailablePortInRange(int rangeBase, int rangeTop, Protocol protocol) {
    return availablePort.getRandomAvailablePortInRange(rangeBase, rangeTop, protocol);
  }
}
