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

import static org.apache.geode.distributed.internal.DistributionConfig.GEMFIRE_PREFIX;
import static org.apache.geode.internal.net.InetAddressUtils.getLoopback;
import static org.apache.geode.internal.net.InetAddressUtils.getLoopbackAddress;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

import org.junit.After;
import org.junit.Test;

/**
 * multicast availability is tested in JGroupsMessengerJUnitTest
 */
public class AvailablePortIntegrationTest {

  private ServerSocket socket;

  @After
  public void tearDown() throws IOException {
    if (socket != null) {
      socket.close();
    }
  }

  @Test
  public void testIsPortAvailable() throws IOException {
    socket = new ServerSocket();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    socket.bind(new InetSocketAddress(getLoopback(), port));

    assertThat(AvailablePort.isPortAvailable(port, AvailablePort.SOCKET,
        InetAddress.getByName(getLoopbackAddress()))).isFalse();
    // Get local host will return the hostname for the server, so this should succeed, since we're
    // bound to the loopback address only.
    assertThat(
        AvailablePort.isPortAvailable(port, AvailablePort.SOCKET, InetAddress.getLocalHost()))
            .isTrue();
    // This should test all interfaces.
    assertThat(AvailablePort.isPortAvailable(port, AvailablePort.SOCKET)).isFalse();
  }

  @Test
  public void testWildcardAddressBound() throws IOException {
    socket = new ServerSocket();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    socket.bind(new InetSocketAddress((InetAddress) null, port));
    System.out.println("bind-address=" + System.getProperty(GEMFIRE_PREFIX + "bind-address"));
    assertThat(AvailablePort.isPortAvailable(port, AvailablePort.SOCKET)).isFalse();
  }
}
