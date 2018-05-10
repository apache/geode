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
import static org.apache.geode.internal.AvailablePort.SOCKET;
import static org.apache.geode.internal.AvailablePort.getAddress;
import static org.apache.geode.internal.AvailablePort.isPortKeepable;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.geode.internal.AvailablePort.Keeper;

/**
 * Provides helper methods for acquiring a set of unique available ports. It is not safe to simply
 * call AvailablePort.getRandomAvailablePort several times in a row without doing something to
 * ensure that they are unique. Although they are random, it is possible for subsequent calls to
 * getRandomAvailablePort to return the same integer, unless that port is put into use before
 * further calls to getRandomAvailablePort.
 */
public class AvailablePortHelper {

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

  public static List<Keeper> getRandomAvailableTCPPortKeepers(int count,
      boolean useMembershipPortRange) {
    List<Keeper> result = new ArrayList<Keeper>();
    while (result.size() < count) {
      result.add(AvailablePort.getRandomAvailablePortKeeper(AvailablePort.SOCKET,
          getAddress(AvailablePort.SOCKET), useMembershipPortRange));
    }
    return result;
  }

  public static int[] getRandomAvailableTCPPortRange(final int count) {
    return getRandomAvailableTCPPortRange(count, false);
  }

  public static int[] getRandomAvailableTCPPortRange(final int count,
      final boolean useMembershipPortRange) {
    List<Keeper> list = getRandomAvailableTCPPortRangeKeepers(count, useMembershipPortRange);
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
    List<Keeper> result = new ArrayList<>();

    InetAddress addr = getAddress(AvailablePort.SOCKET);

    int lowerBound =
        useMembershipPortRange ? DEFAULT_MEMBERSHIP_PORT_RANGE[0] : AVAILABLE_PORTS_LOWER_BOUND;

    int upperBound =
        useMembershipPortRange ? DEFAULT_MEMBERSHIP_PORT_RANGE[1] : AVAILABLE_PORTS_UPPER_BOUND;

    for (int i = lowerBound; i <= upperBound; i++) {
      for (int j = 0; j < count && ((i + j) <= upperBound); j++) {
        int port = i + j;
        Keeper keeper = isPortKeepable(port, SOCKET, addr);
        if (keeper == null) {
          releaseKeepers(result);
          result.clear();
          break;
        }
        result.add(keeper);
        if (result.size() == count) {
          return result;
        }
      }
    }

    return result;
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

    Set set = new HashSet();
    while (set.size() < count) {
      int port = AvailablePort.getRandomAvailablePortWithMod(AvailablePort.SOCKET, site);
      set.add(new Integer(port));
    }
    int[] ports = new int[set.size()];
    int i = 0;
    for (Iterator iter = set.iterator(); iter.hasNext();) {
      ports[i] = ((Integer) iter.next()).intValue();
      i++;
    }
    return ports;
  }


  /**
   * Returns array of unique randomly available tcp ports of specified count.
   */
  public static int getRandomAvailablePortForDUnitSite() {
    int site = 1;
    String hostName = System.getProperty("hostName");
    if (hostName != null && hostName.startsWith("host")) {
      if (hostName.length() > 4) {
        site = Integer.parseInt(hostName.substring(4));
      }
    }
    int port = AvailablePort.getRandomAvailablePortWithMod(AvailablePort.SOCKET, site);
    return port;
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
  public static int[] getRandomAvailableUDPPorts(int count) {
    Set set = new HashSet();
    while (set.size() < count) {
      int port = AvailablePort.getRandomAvailablePort(AvailablePort.MULTICAST);
      set.add(new Integer(port));
    }
    int[] ports = new int[set.size()];
    int i = 0;
    for (Iterator iter = set.iterator(); iter.hasNext();) {
      ports[i] = ((Integer) iter.next()).intValue();
      i++;
    }
    return ports;
  }

  /**
   * Returns randomly available udp port.
   */
  public static int getRandomAvailableUDPPort() {
    return getRandomAvailableUDPPorts(1)[0];
  }

}
