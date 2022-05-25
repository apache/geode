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
package org.apache.geode.distributed;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.ClientServerTest;

/**
 * Extracted from LocatorLauncherLocalIntegrationTest.
 *
 * @since GemFire 8.0
 */
@Category({ClientServerTest.class})
@SuppressWarnings("serial")
public class HostedLocatorsDUnitTest implements Serializable {

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @Test
  public void testGetAllHostedLocators() {
    // Get the localhost IP address
    String ipAddress = getIPAddress().getHostAddress();

    // Collection of expected locator addresses.
    final Set<String> locators = new HashSet<>();
    // Starting 4 locators
    MemberVM locator1 = clusterStartupRule.startLocatorVM(0);
    int locator1Port = locator1.getPort();
    locators.add(ipAddress + "[" + locator1Port + "]");

    MemberVM locator2 =
        clusterStartupRule.startLocatorVM(1, l -> l.withConnectionToLocator(locator1Port));
    int locator2Port = locator2.getPort();
    locators.add(ipAddress + "[" + locator2Port + "]");

    MemberVM locator3 =
        clusterStartupRule.startLocatorVM(2,
            l -> l.withConnectionToLocator(locator1Port, locator2Port));
    int locator3Port = locator3.getPort();
    locators.add(ipAddress + "[" + locator3Port + "]");

    MemberVM locator4 =
        clusterStartupRule.startLocatorVM(3,
            l -> l.withConnectionToLocator(locator1Port, locator2Port, locator3Port));
    int locator4Port = locator4.getPort();
    locators.add(ipAddress + "[" + locator4Port + "]");

    MemberVM server1 =
        clusterStartupRule.startServerVM(4,
            s -> s.withConnectionToLocator(locator1Port, locator2Port, locator3Port, locator4Port));
    // validation within non-locator
    server1.invoke(() -> validateLocators(locators));

    // validations within the locators
    locator1.invoke(() -> validateHostedLocators(locators));
    locator2.invoke(() -> validateHostedLocators(locators));
    locator3.invoke(() -> validateHostedLocators(locators));
    locator4.invoke(() -> validateHostedLocators(locators));
  }

  @Test
  public void testGetAllHostedLocatorsWhenLocatorIsStartedWithPortZero() {
    // Get the localhost IP address
    String ipAddress = getIPAddress().getHostAddress();

    // Collection of expected locator addresses.
    final Set<String> locators = new HashSet<>();
    // Starting 4 locators
    MemberVM locator1 = clusterStartupRule.startLocatorVM(0, l -> l.withPort(0));
    int locator1Port = locator1.getPort();
    locators.add(ipAddress + "[" + locator1Port + "]");

    MemberVM locator2 =
        clusterStartupRule.startLocatorVM(1,
            l -> l.withConnectionToLocator(locator1Port).withPort(0));
    int locator2Port = locator2.getPort();
    locators.add(ipAddress + "[" + locator2Port + "]");

    MemberVM locator3 =
        clusterStartupRule.startLocatorVM(2,
            l -> l.withConnectionToLocator(locator1Port, locator2Port).withPort(0));
    int locator3Port = locator3.getPort();
    locators.add(ipAddress + "[" + locator3Port + "]");

    MemberVM locator4 =
        clusterStartupRule.startLocatorVM(3,
            l -> l.withConnectionToLocator(locator1Port, locator2Port, locator3Port).withPort(0));
    int locator4Port = locator4.getPort();
    locators.add(ipAddress + "[" + locator4Port + "]");

    MemberVM server1 =
        clusterStartupRule.startServerVM(4,
            s -> s.withConnectionToLocator(locator1Port, locator2Port, locator3Port, locator4Port));
    // validation within non-locator
    server1.invoke(() -> validateLocators(locators));

    // validations within the locators
    locator1.invoke(() -> validateHostedLocators(locators));
    locator2.invoke(() -> validateHostedLocators(locators));
    locator3.invoke(() -> validateHostedLocators(locators));
    locator4.invoke(() -> validateHostedLocators(locators));
  }

  private void validateLocators(Set<String> locators) {
    ClusterDistributionManager dm =
        (ClusterDistributionManager) ClusterStartupRule.getCache().getDistributionManager();
    final Set<InternalDistributedMember> locatorIds = dm.getLocatorDistributionManagerIds();

    assertThat(locatorIds.size()).isEqualTo(4);
    final Map<InternalDistributedMember, Collection<String>> hostedLocators =
        dm.getAllHostedLocators();
    assertThat(hostedLocators).isNotEmpty();
    assertThat(hostedLocators.size()).isEqualTo(4);
    for (InternalDistributedMember member : hostedLocators.keySet()) {
      assertThat(hostedLocators.get(member).size()).isEqualTo(1);
      final String hostedLocator = hostedLocators.get(member).iterator().next();
      assertThat(locators).contains(hostedLocator);
    }
  }

  private void validateHostedLocators(Set<String> locators) {
    ClusterDistributionManager dm =
        (ClusterDistributionManager) ClusterStartupRule.getCache().getDistributionManager();
    final InternalDistributedMember self = dm.getDistributionManagerId();
    final Set<InternalDistributedMember> locatorIDs = dm.getLocatorDistributionManagerIds();
    assertThat(locatorIDs.size()).isEqualTo(4);
    assertThat(locatorIDs).contains(self);
    final Map<InternalDistributedMember, Collection<String>> hostedLocators =
        dm.getAllHostedLocators();
    assertThat(hostedLocators).isNotEmpty();
    assertThat(hostedLocators.size()).isEqualTo(4);
    assertThat(hostedLocators).containsKey(self);
    for (InternalDistributedMember member : hostedLocators.keySet()) {
      assertThat(hostedLocators.get(member).size()).isEqualTo(1);
      final String hostedLocator = hostedLocators.get(member).iterator().next();
      assertThat(locators).contains(hostedLocator);
    }
  }

  private static InetAddress getIPAddress() {
    return Boolean.getBoolean("java.net.preferIPv6Addresses") ? getIPv6Address() : getIPv4Address();
  }

  private static InetAddress getIPv4Address() {
    InetAddress host;
    try {
      host = InetAddress.getLocalHost();
      if (host instanceof Inet4Address && !host.isLinkLocalAddress() && !host.isLoopbackAddress()) {
        return host;
      }
    } catch (UnknownHostException e) {
      String s = "Local host not found";
      throw new RuntimeException(s, e);
    }
    try {
      Enumeration<NetworkInterface> i = NetworkInterface.getNetworkInterfaces();
      while (i.hasMoreElements()) {
        NetworkInterface ni = i.nextElement();
        Enumeration<InetAddress> j = ni.getInetAddresses();
        while (j.hasMoreElements()) {
          InetAddress inetAddress = j.nextElement();
          // gemfire won't form connections using link-local addresses
          if (!inetAddress.isLinkLocalAddress() && !inetAddress.isLoopbackAddress()
              && (inetAddress instanceof Inet4Address)) {
            return inetAddress;
          }
        }
      }
      String s = "IPv4 address not found";
      throw new RuntimeException(s);
    } catch (SocketException e) {
      String s = "Problem reading IPv4 address";
      throw new RuntimeException(s, e);
    }
  }

  private static InetAddress getIPv6Address() {
    try {
      Enumeration<NetworkInterface> i = NetworkInterface.getNetworkInterfaces();
      while (i.hasMoreElements()) {
        NetworkInterface ni = i.nextElement();
        Enumeration<InetAddress> j = ni.getInetAddresses();
        while (j.hasMoreElements()) {
          InetAddress inetAddress = j.nextElement();
          // gemfire won't form connections using link-local addresses
          if (!inetAddress.isLinkLocalAddress() && !inetAddress.isLoopbackAddress()
              && (inetAddress instanceof Inet6Address)
              && !isIPv6LinkLocalAddress((Inet6Address) inetAddress)) {
            return inetAddress;
          }
        }
      }
      String s = "IPv6 address not found";
      throw new RuntimeException(s);
    } catch (SocketException e) {
      String s = "Problem reading IPv6 address";
      throw new RuntimeException(s, e);
    }
  }

  /**
   * Detect LinkLocal IPv6 address where the interface is missing, ie %[0-9].
   *
   * @see InetAddress#isLinkLocalAddress()
   */
  private static boolean isIPv6LinkLocalAddress(Inet6Address inet6Address) {
    byte[] addressBytes = inet6Address.getAddress();
    return ((addressBytes[0] == (byte) 0xfe) && (addressBytes[1] == (byte) 0x80));
  }
}
