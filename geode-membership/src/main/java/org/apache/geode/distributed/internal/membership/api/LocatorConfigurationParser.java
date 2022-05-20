/*
 *
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
 *
 */

package org.apache.geode.distributed.internal.membership.api;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.geode.distributed.internal.tcpserver.HostAndPort;
import org.apache.geode.internal.inet.LocalHostUtil;

public class LocatorConfigurationParser {
  /**
   * parse locators & check that the resulting address is compatible with the given address
   *
   * @param locatorsString a DistributionConfig "locators" string
   * @param bindAddress optional address to check for loopback compatibility
   * @return addresses of locators
   */
  public static List<HostAndPort> parseLocators(String locatorsString, String bindAddress)
      throws MembershipConfigurationException {
    InetAddress addr = null;

    try {
      if (bindAddress == null || bindAddress.trim().length() == 0) {
        addr = LocalHostUtil.getLocalHost();
      } else {
        addr = InetAddress.getByName(bindAddress);
      }
    } catch (UnknownHostException e) {
      // ignore
    }
    return parseLocators(locatorsString, addr);
  }

  /**
   * parse locators & check that the resulting address is compatible with the given address
   *
   * @param locatorsString a DistributionConfig "locators" string
   * @param bindAddress optional address to check for loopback compatibility
   * @return addresses of locators
   */
  public static List<HostAndPort> parseLocators(String locatorsString, InetAddress bindAddress)
      throws MembershipConfigurationException {
    List<HostAndPort> result = new ArrayList<>(2);
    Set<InetSocketAddress> inetAddresses = new HashSet<>();
    final boolean isLoopback = ((bindAddress != null) && bindAddress.isLoopbackAddress());

    StringTokenizer parts = new StringTokenizer(locatorsString, ",");
    while (parts.hasMoreTokens()) {
      String str = parts.nextToken();

      HostAndPort hostAndPort = parseLocator(str);
      String host = hostAndPort.getHostName();
      final InetSocketAddress isa = new InetSocketAddress(host, hostAndPort.getPort());

      if (isLoopback) {
        final InetAddress locatorAddress = isa.getAddress();
        if (locatorAddress == null) {
          throw new MembershipConfigurationException("This process is attempting to use a locator" +
              " at an unknown address or FQDN: " + host);
        }

        if (!locatorAddress.isLoopbackAddress()) {
          throw new MembershipConfigurationException(
              "This process is attempting to join with a loopback address (" + bindAddress
                  + ") using a locator that does not have a local address (" + isa
                  + ").  On Unix this usually means that /etc/hosts is misconfigured.");
        }
      }
      if (!inetAddresses.contains(isa)) {
        inetAddresses.add(isa);
        result.add(hostAndPort);
      }
    }

    return result;
  }

  public static HostAndPort parseLocator(String locatorHostAndPort)
      throws MembershipConfigurationException {
    final int portSpecificationStart = locatorHostAndPort.indexOf('[');

    if (portSpecificationStart == -1) {
      throw createBadPortException(locatorHostAndPort);
    }

    String host = locatorHostAndPort.substring(0, portSpecificationStart);

    int idx = host.lastIndexOf('@');
    if (idx < 0) {
      idx = host.lastIndexOf(':');
    }
    String start = host.substring(0, idx > -1 ? idx : host.length());
    if (start.indexOf(':') >= 0) { // a single numeric ipv6 address
      idx = host.lastIndexOf('@');
    }
    if (idx >= 0) {
      host = host.substring(idx + 1);
    }

    int startIdx = portSpecificationStart + 1;
    int endIdx = locatorHostAndPort.indexOf(']');

    if (endIdx == -1) {
      throw createBadPortException(locatorHostAndPort);
    }

    final int port;
    try {
      port = Integer.parseInt(locatorHostAndPort.substring(startIdx, endIdx));
    } catch (NumberFormatException e) {
      throw createBadPortException(locatorHostAndPort);
    }
    return new HostAndPort(host, port);
  }

  private static MembershipConfigurationException createBadPortException(final String str) {
    return new MembershipConfigurationException("This process is attempting to use a locator" +
        " with a malformed port specification: " + str);
  }
}
