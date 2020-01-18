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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.InetSocketAddress;

import org.junit.Test;

public class LocatorAddressTest {

  @Test
  public void getSocketInentAddress_whenResolvableAddress_returnsTheSocketAddress() {
    InetSocketAddress host1address = new InetSocketAddress(8080);
    LocatorAddress locator1 = new LocatorAddress(host1address, "localhost");

    InetSocketAddress actual = locator1.getSocketInetAddress();

    assertTrue(actual.equals(host1address));
    assertTrue(!actual.isUnresolved());
  }

  @Test
  public void check_equal_LocatorAddress() {
    InetSocketAddress host1address = InetSocketAddress.createUnresolved("localhost", 8090);
    LocatorAddress locator1 = new LocatorAddress(host1address, "localhost");

    InetSocketAddress host2address = locator1.getSocketInetAddress();
    LocatorAddress locator2 = new LocatorAddress(host2address, "localhost");

    assertTrue(host1address.isUnresolved());
    assertFalse(host2address.isUnresolved());
    assertTrue(locator1.equals(locator2));
  }
}
