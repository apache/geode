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

public class HostAndPortTest {

  /**
   * Test that getSocketInentAddress returns resolved InetSocketAddress
   */
  @Test
  public void Test_getSocketInentAddress_returns_resolved_SocketAddress() {
    HostAndPort locator1 = new HostAndPort("localhost", 8080);

    InetSocketAddress actual = locator1.getSocketInetAddress();

    assertThat(actual.isUnresolved()).isFalse();
  }

  /**
   * Test that getSocketInentAddress returns unresolved InetSocketAddress
   */
  @Test
  public void Test_getSocketInentAddress_returns_unresolved_SocketAddress() {
    HostAndPort locator1 = new HostAndPort("fakelocalhost", 8090);

    InetSocketAddress actual = locator1.getSocketInetAddress();

    assertThat(actual.isUnresolved()).isTrue();
  }

  /**
   * Test whether HostAndPort are equal, when created from resolved and unresolved
   * InetSocketAddress
   */
  @Test
  public void Test_equals_LocatorAddress_from_resolved_and_unresolved_SocketAddress() {
    HostAndPort locator1 = new HostAndPort("localhost", 8080);

    InetSocketAddress host2address = locator1.getSocketInetAddress();
    HostAndPort locator2 = new HostAndPort("localhost", host2address.getPort());

    assertThat(host2address.isUnresolved()).isFalse();
    assertThat(locator1.equals(locator2)).isTrue();
  }

  @Test
  public void Test_getPort_returns_port() {
    HostAndPort locator1 = new HostAndPort("localhost", 8090);
    assertThat(locator1.getPort()).isEqualTo(8090);
  }

  @Test
  public void Test_getHostName_returns_hostname() {
    HostAndPort locator1 = new HostAndPort("fakelocalhost", 8091);
    assertThat(locator1.getHostName()).isEqualTo("fakelocalhost");
  }

  @Test
  public void Test_hashCode_of_SocketAddress() {
    InetSocketAddress host1address = InetSocketAddress.createUnresolved("fakelocalhost", 8091);
    HostAndPort locator1 = new HostAndPort("fakelocalhost", 8091);
    assertThat(locator1.hashCode()).isEqualTo(host1address.hashCode());
  }

  @Test
  public void Test_toString_LocatorAddress() {
    HostAndPort locator1 = new HostAndPort("fakelocalhost", 8091);
    assertThat(locator1.toString()).contains("socketInetAddress");
  }

}
