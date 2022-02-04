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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class SocketCreatorIntegrationTest {

  /**
   * Google's Public DNS well known IPv4 address.
   * It is reasonable to assume that Google's Public DNS service will persist for the foreseeable
   * future.
   */
  private static final String IPV4_ADDRESS = "8.8.8.8";

  /**
   * Google's Public DNS well known IPv6 address.
   * It is reasonable to assume that Google's Public DNS service will persist for the foreseeable
   * future.
   */
  private static final String IPV6_ADDRESS = "2001:4860:4860::8888";

  /**
   * This address is likely unresolvable to any name since it is in the reserved software block.
   */
  public static final String UNRESOLVABLE_IPV4_ADDRESS = "0.0.0.0";

  /**
   * This address is likely unresolvable to any name since it is in the reserved software block.
   */
  public static final String UNRESOLVABLE_IPV6_ADDRESS = "0:0:0:0:0:0:0:0";

  /**
   * This is not an IP address.
   */
  public static final String NOT_AN_IP_ADDRESS = "uytrdfghjiolmjuytrtYUikJhgrEwsdfgHJkiUyg";

  @Test
  public void convertToHostNameIfIpAddressReturnsHostNameForKnownIpv4Address() {
    final String hostName = SocketCreator.convertToHostNameIfIpAddress(IPV4_ADDRESS);
    assertThat(hostName).isNotEmpty().isNotEqualTo(IPV4_ADDRESS);
  }

  @Test
  public void convertToHostNameIfIpAddressReturnsHostNameForKnownIpv6Address() {
    final String hostName = SocketCreator.convertToHostNameIfIpAddress(IPV6_ADDRESS);
    assertThat(hostName).isNotEmpty().isNotEqualTo(IPV6_ADDRESS);
  }

  @Test
  public void convertToHostNameIfIpAddressReturnsSameHostNameForNonIpAddress() {
    final String hostName = SocketCreator.convertToHostNameIfIpAddress(NOT_AN_IP_ADDRESS);
    assertThat(hostName).isEqualTo(NOT_AN_IP_ADDRESS);
  }

  @Test
  public void convertToHostNameIfIpAddressReturnsSameHostNameForUnresolvableIpv4Address() {
    final String hostName = SocketCreator.convertToHostNameIfIpAddress(UNRESOLVABLE_IPV4_ADDRESS);
    assertThat(hostName).isEqualTo(UNRESOLVABLE_IPV4_ADDRESS);
  }

  @Test
  public void convertToHostNameIfIpAddressReturnsSameHostNameFOrUnresolvableIpv6Address() {
    final String hostName = SocketCreator.convertToHostNameIfIpAddress(UNRESOLVABLE_IPV6_ADDRESS);
    assertThat(hostName).isEqualTo(UNRESOLVABLE_IPV6_ADDRESS);
  }

}
