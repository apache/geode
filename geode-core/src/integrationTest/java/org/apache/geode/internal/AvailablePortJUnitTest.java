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

import static org.apache.geode.internal.AvailablePort.SOCKET;
import static org.apache.geode.internal.AvailablePort.getRandomAvailablePort;
import static org.apache.geode.internal.AvailablePort.isPortAvailable;
import static org.apache.geode.util.internal.GeodeGlossary.GEMFIRE_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;

import org.junit.Test;

import org.apache.geode.internal.inet.LocalHostUtil;

/**
 * multicast availability is tested in JGroupsMessengerJUnitTest
 */
public class AvailablePortJUnitTest {
  private static final String LOOPBACK_ADDRESS =
      LocalHostUtil.preferIPv6Addresses() ? "::1" : "127.0.0.1";
  private static final String GEMFIRE_BIND_ADDRESS =
      System.getProperty(GEMFIRE_PREFIX + "bind-address");

  @Test
  public void aPortBoundOnAnAddress_isNotAvailable_onThatAddress() throws IOException {
    int port = getRandomAvailablePort(SOCKET);
    InetAddress addressWherePortIsBound = loopbackAddress();

    try (ServerSocket socket = new ServerSocket()) {
      socket.bind(new InetSocketAddress(addressWherePortIsBound, port));

      assertThat(isPortAvailable(port, SOCKET, addressWherePortIsBound))
          .as("port " + port + " bound on " + addressWherePortIsBound
              + " is available on that address")
          .isFalse();
    }
  }

  @Test
  public void aPortBoundOnOneAddress_isAvailable_onOtherAddresses() throws IOException {
    InetAddress addressWherePortIsNotBound = InetAddress.getLocalHost();

    // The test is valid only if the local host address is not a loopback address.
    assumeThat(addressWherePortIsNotBound.isLoopbackAddress())
        .isFalse();

    int port = getRandomAvailablePort(SOCKET);
    InetAddress addressWherePortIsBound = loopbackAddress();

    try (ServerSocket socket = new ServerSocket()) {
      socket.bind(new InetSocketAddress(addressWherePortIsBound, port));

      assertThat(isPortAvailable(port, SOCKET, addressWherePortIsNotBound))
          .as("port " + port + " bound on " + addressWherePortIsBound + " is available on "
              + addressWherePortIsNotBound)
          .isTrue();
    }
  }

  @Test
  public void aPortBoundOnOneAddress_isNotAvailable_onAllAddresses() throws IOException {
    int port = getRandomAvailablePort(SOCKET);
    InetAddress addressWherePortIsBound = loopbackAddress();

    try (ServerSocket socket = new ServerSocket()) {
      socket.bind(new InetSocketAddress(addressWherePortIsBound, port));

      // addr == null checks that the port is available on all addresses
      assertThat(isPortAvailable(port, SOCKET, null))
          .as("port " + port + " bound on " + addressWherePortIsBound
              + " is available on all addresses")
          .isFalse();
    }
  }

  @Test
  public void aPortBoundOnWildcardAddress_isNotAvailable_onGemFireBindAddress() throws IOException {
    // assumeThat(SystemUtils.isWindows()).isFalse(); // See bug #39368
    int port = getRandomAvailablePort(SOCKET);
    try (ServerSocket socket = new ServerSocket()) {
      socket.bind(new InetSocketAddress((InetAddress) null, port));
      assertThat(isPortAvailable(port, SOCKET))
          .as("port " + port + " bound on wildcard address is available on gemfire.bind-address "
              + GEMFIRE_BIND_ADDRESS)
          .isFalse();
    }
  }

  private static InetAddress loopbackAddress() throws UnknownHostException {
    return InetAddress.getByName(LOOPBACK_ADDRESS);
  }
}
