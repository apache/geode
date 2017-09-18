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
package org.apache.geode.management.internal.cli.commands;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

import com.google.common.collect.Iterators;
import org.apache.logging.log4j.Logger;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.rules.GfshShellConnectionRule;
import org.apache.geode.test.junit.rules.LocatorStarterRule;

/**
 * The GfshCommandJUnitTest class is a test suite of test cases testing the contract and
 * functionality of the GfshCommand class for implementing GemFire shell (Gfsh) commands.
 *
 * @see org.apache.geode.management.internal.cli.commands.GfshCommand
 * @see org.jmock.Expectations
 * @see org.jmock.Mockery
 * @see org.jmock.lib.legacy.ClassImposteriser
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since GemFire 7.0
 */

@Category(IntegrationTest.class)
public class DescribeConnectionCommandJUnitTest {
  public static Logger logger = LogService.getLogger();

  @ClassRule
  public static LocatorStarterRule locator = new LocatorStarterRule().withAutoStart();

  @Rule
  public GfshShellConnectionRule gfsh = new GfshShellConnectionRule();

  @Test
  public void executeWhileConnected() throws Exception {
    gfsh.connectAndVerify(locator);
    // We must be sure to catch either IPv4 or IPv6 descriptions.
    String[] acceptableAddresses = getNetworkAddressArray();
    logger.info(
        "Expecting one of the following addresses: " + String.join(", ", acceptableAddresses));
    gfsh.executeAndAssertThat("describe connection")
        .tableHasColumnWithValueMatchingOneOf("Connection Endpoints", acceptableAddresses);
  }

  private String[] getNetworkAddressArray() throws SocketException {
    Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
    NetworkInterface myInterface = networkInterfaces.nextElement();
    Enumeration<InetAddress> myAddresses = myInterface.getInetAddresses();
    return Collections.list(myAddresses).stream().map(InetAddress::getHostAddress)
        .map(address -> formatAddressAndPort(address, locator.getJmxPort())).toArray(String[]::new);
  }

  @Test
  public void executeWhileNotConnected() throws Exception {
    gfsh.executeAndAssertThat("describe connection")
        .tableHasColumnWithValuesContaining("Connection Endpoints", "Not connected");
  }

  private String formatAddressAndPort(String address, int port) {
    address = address.startsWith("/") ? address.substring(1) : address;
    return address + "[" + port + "]";
  }
}
