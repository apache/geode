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
package org.apache.geode.distributed.internal.tcpserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetSocketAddress;

import org.junit.Test;

public class LocatorAddressTest {

  /**
   * Test that getSocketInentAddress returns resolved InetSocketAddress
   */
  @Test
  public void Test_getSocketInentAddress_returnsTheSocketAddress() {
    InetSocketAddress host1address = new InetSocketAddress(8080);
    LocatorAddress locator1 = new LocatorAddress(host1address, "localhost");

    InetSocketAddress actual = locator1.getSocketInetAddress();

    assertThat(actual.isUnresolved()).isFalse();
  }

  /**
   * Test whether LocatorAddress are equal, when created from resolved and unresolved
   * InetSocketAddress
   */
  @Test
  public void Test_equals_LocatorAddress() {
    InetSocketAddress host1address = InetSocketAddress.createUnresolved("localhost", 8090);
    LocatorAddress locator1 = new LocatorAddress(host1address, "localhost");

    InetSocketAddress host2address = locator1.getSocketInetAddress();
    LocatorAddress locator2 = new LocatorAddress(host2address, "localhost");

    assertThat(host1address.isUnresolved()).isTrue();
    assertThat(host2address.isUnresolved()).isFalse();
    assertThat(locator1.equals(locator2)).isTrue();
  }

  @Test
  public void Test_getPort_LocatorAddress() {
    InetSocketAddress host1address = InetSocketAddress.createUnresolved("localhost", 8090);
    LocatorAddress locator1 = new LocatorAddress(host1address, "localhost");
    assertThat(locator1.getPort()).isEqualTo(8090);
  }

  @Test
  public void Test_getHostName_LocatorAddress() {
    InetSocketAddress host1address = InetSocketAddress.createUnresolved("fakelocalhost", 8091);
    LocatorAddress locator1 = new LocatorAddress(host1address, "fakelocalhost");
    assertThat(locator1.getHostName()).isEqualTo("fakelocalhost");
  }

  @Test
  public void Test_hashCode_LocatorAddress() {
    InetSocketAddress host1address = InetSocketAddress.createUnresolved("fakelocalhost", 8091);
    LocatorAddress locator1 = new LocatorAddress(host1address, "fakelocalhost");
    assertThat(locator1.hashCode()).isEqualTo(host1address.hashCode());
  }

  @Test
  public void Test_toString_LocatorAddress() {
    InetSocketAddress host1address = InetSocketAddress.createUnresolved("fakelocalhost", 8091);
    LocatorAddress locator1 = new LocatorAddress(host1address, "fakelocalhost");
    assertThat(locator1.toString()).contains("socketInetAddress");
  }

  @Test
  public void Test_isIpString_LocatorAddress() {
    InetSocketAddress host1address = new InetSocketAddress(8080);
    LocatorAddress locator1 = new LocatorAddress(host1address, "127.0.0.1");
    assertThat(locator1.isIpString()).isTrue();
  }

  @Test
  public void Test_isIpString_LocatorAddress2() {
    InetSocketAddress host1address = new InetSocketAddress(8080);
    LocatorAddress locator1 = new LocatorAddress(host1address, "localhost");
    assertThat(locator1.isIpString()).isFalse();
  }
}
