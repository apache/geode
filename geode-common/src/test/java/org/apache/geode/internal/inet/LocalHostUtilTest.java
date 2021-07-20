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

package org.apache.geode.internal.inet;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;

import junit.framework.TestCase;
import org.junit.Test;

public class LocalHostUtilTest extends TestCase {

  /**
   * We cannot verify that {@link LocalHostUtil#convertFromIPv4MappedAddress}
   * converts an IPv4-mapped address to an IPv4 address because it's not possible to create
   * an IPv4-mapped Inet6Address instance. Methods that allow to create an InetAddress instance
   * (i.e. {@link InetAddress#getByName(String)} or {@link InetAddress#getByAddress(byte[])})
   * internally convert IPv4-mapped IPv6 address to an IPv4 address.
   *
   * This test verifies that if {@link NetworkInterface#getInetAddresses} returns an IPv4-mapped
   * address, it will not be returned by {@link LocalHostUtil#getMyAddresses}.
   */
  @Test
  public void testGetMyAddressesDoesNotReturnIPv4MappedAddresses() {
    assertThat(LocalHostUtil.getMyAddresses())
        .noneMatch(address -> LocalHostUtil.isIPv4MappedAddress(address.getAddress()));
  }

  @Test
  public void testConvertFromIPv4MappedAddressReturnsOriginalAddressIfNotIPv4Mapped()
      throws UnknownHostException {
    InetAddress ipv4 = InetAddress.getByName("127.0.0.1");
    InetAddress ipv6 = InetAddress.getByName("0:0:0:0:0:0:0:1");
    InetAddress ipv4Compatible = InetAddress.getByName("::127.0.0.1");
    assertThat(LocalHostUtil.convertFromIPv4MappedAddress(ipv4))
        .isEqualTo(ipv4);
    assertThat(LocalHostUtil.convertFromIPv4MappedAddress(ipv6))
        .isEqualTo(ipv6);
    assertThat(LocalHostUtil.convertFromIPv4MappedAddress(ipv4Compatible))
        .isEqualTo(ipv4Compatible);
  }
}
