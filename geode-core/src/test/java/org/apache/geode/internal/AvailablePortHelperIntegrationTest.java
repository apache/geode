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

import static org.apache.geode.distributed.internal.DistributionConfig.*;
import static org.apache.geode.internal.AvailablePort.*;
import static org.apache.geode.internal.AvailablePortHelper.*;
import static org.assertj.core.api.Assertions.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.internal.AvailablePort.Keeper;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
@RunWith(JUnitParamsRunner.class)
public class AvailablePortHelperIntegrationTest {

  private Set<ServerSocket> serverSockets;

  @Before
  public void before() throws Exception {
    this.serverSockets = new HashSet<>();
  }

  @After
  public void after() throws Exception {
    for (ServerSocket serverSocket : this.serverSockets) {
      try {
        if (serverSocket != null && !serverSocket.isClosed()) {
          serverSocket.close();
        }
      } catch (IOException ignore) {
        System.err.println("Unable to close " + serverSocket);
      }
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void getRandomAvailableTCPPortRange_zero(final boolean useMembershipPortRange)
      throws Exception {
    int[] results = getRandomAvailableTCPPortRange(0, useMembershipPortRange);
    assertThat(results).isEmpty();
  }

  @Test
  @Parameters({"true", "false"})
  public void getRandomAvailableTCPPortRange_one_returnsOne(final boolean useMembershipPortRange)
      throws Exception {
    int[] results = getRandomAvailableTCPPortRange(1, useMembershipPortRange);
    assertThat(results).hasSize(1);
  }

  @Test
  @Parameters({"true", "false"})
  public void getRandomAvailableTCPPortRange_two_returnsTwo(final boolean useMembershipPortRange)
      throws Exception {
    int[] results = getRandomAvailableTCPPortRange(2, useMembershipPortRange);
    assertThat(results).hasSize(2);
  }

  @Test
  @Parameters({"true", "false"})
  public void getRandomAvailableTCPPortRange_many_returnsMany(final boolean useMembershipPortRange)
      throws Exception {
    int[] results = getRandomAvailableTCPPortRange(10, useMembershipPortRange);
    assertThat(results).hasSize(10);
    assertPortsAreUsable(results);
  }

  @Test
  @Parameters({"true", "false"})
  public void getRandomAvailableTCPPortRange_returnsUniquePorts(
      final boolean useMembershipPortRange) throws Exception {
    int[] results = getRandomAvailableTCPPortRange(10, useMembershipPortRange);

    Set<Integer> ports = new HashSet<>();
    for (int port : results) {
      ports.add(port);
    }

    assertThat(ports).hasSize(results.length);
  }

  @Test
  @Parameters({"true", "false"})
  public void getRandomAvailableTCPPortRange_returnsConsecutivePorts(
      final boolean useMembershipPortRange) throws Exception {
    int[] results = getRandomAvailableTCPPortRange(10, useMembershipPortRange);

    List<Integer> ports = new ArrayList<>();
    for (int port : results) {
      if (useMembershipPortRange) {
        assertThat(port).isGreaterThanOrEqualTo(DEFAULT_MEMBERSHIP_PORT_RANGE[0])
            .isLessThanOrEqualTo(DEFAULT_MEMBERSHIP_PORT_RANGE[1]);
      } else {
        assertThat(port).isGreaterThanOrEqualTo(AVAILABLE_PORTS_LOWER_BOUND)
            .isLessThanOrEqualTo(AVAILABLE_PORTS_UPPER_BOUND);
      }
      ports.add(port);
    }

    assertThat(ports).hasSize(10);

    int previousPort = 0;
    for (int port : ports) {
      if (previousPort != 0) {
        assertThat(port).isEqualTo(previousPort + 1);
      }
      previousPort = port;
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void getRandomAvailableTCPPortRangeKeepers_zero(final boolean useMembershipPortRange)
      throws Exception {
    List<Keeper> results = getRandomAvailableTCPPortRangeKeepers(0, useMembershipPortRange);
    assertThat(results).isEmpty();
  }

  @Test
  @Parameters({"true", "false"})
  public void getRandomAvailableTCPPortRangeKeepers_one_returnsOne(
      final boolean useMembershipPortRange) throws Exception {
    List<Keeper> results = getRandomAvailableTCPPortRangeKeepers(1, useMembershipPortRange);
    assertThat(results).hasSize(1);
  }

  @Test
  @Parameters({"true", "false"})
  public void getRandomAvailableTCPPortRangeKeepers_two_returnsTwo(
      final boolean useMembershipPortRange) throws Exception {
    List<Keeper> results = getRandomAvailableTCPPortRangeKeepers(2, useMembershipPortRange);
    assertThat(results).hasSize(2);
  }

  @Test
  @Parameters({"true", "false"})
  public void getRandomAvailableTCPPortRangeKeepers_many_returnsMany(
      final boolean useMembershipPortRange) throws Exception {
    List<Keeper> results = getRandomAvailableTCPPortRangeKeepers(10, useMembershipPortRange);
    assertThat(results).hasSize(10);
    assertKeepersAreUsable(results);
  }

  @Test
  @Parameters({"true", "false"})
  public void getRandomAvailableTCPPortRangeKeepers_returnsUsableKeeper(
      final boolean useMembershipPortRange) throws Exception {
    List<Keeper> results = getRandomAvailableTCPPortRangeKeepers(1, useMembershipPortRange);
    assertKeepersAreUsable(results);
  }

  @Test
  @Parameters({"true", "false"})
  public void getRandomAvailableTCPPortRangeKeepers_returnsUniqueKeepers(
      final boolean useMembershipPortRange) throws Exception {
    List<Keeper> results = getRandomAvailableTCPPortRangeKeepers(10, useMembershipPortRange);

    Set<Integer> ports = new HashSet<>();
    for (Keeper keeper : results) {
      ports.add(keeper.getPort());
    }

    assertThat(ports).hasSize(results.size());
  }

  @Test
  @Parameters({"true", "false"})
  public void getRandomAvailableTCPPortRangeKeepers_returnsConsecutivePorts(
      final boolean useMembershipPortRange) throws Exception {
    List<Keeper> results = getRandomAvailableTCPPortRangeKeepers(10, useMembershipPortRange);

    List<Integer> ports = new ArrayList<>();
    for (Keeper keeper : results) {
      assertThat(keeper).isNotNull();
      int port = keeper.getPort();
      if (useMembershipPortRange) {
        assertThat(port).isGreaterThanOrEqualTo(DEFAULT_MEMBERSHIP_PORT_RANGE[0])
            .isLessThanOrEqualTo(DEFAULT_MEMBERSHIP_PORT_RANGE[1]);
      } else {
        assertThat(port).isGreaterThanOrEqualTo(AVAILABLE_PORTS_LOWER_BOUND)
            .isLessThanOrEqualTo(AVAILABLE_PORTS_UPPER_BOUND);
      }
      ports.add(port);
    }

    assertThat(ports).hasSize(10);

    int previousPort = 0;
    for (int port : ports) {
      if (previousPort != 0) {
        assertThat(port).isEqualTo(previousPort + 1);
      }
      previousPort = port;
    }
  }

  private void assertPortsAreUsable(int[] ports) throws IOException {
    for (int port : ports) {
      assertPortIsUsable(port);
    }
  }

  private void assertPortIsUsable(int port) throws IOException {
    ServerSocket serverSocket = createServerSocket();
    serverSocket.setReuseAddress(true);

    serverSocket.bind(new InetSocketAddress(port));

    assertThat(serverSocket.isBound()).isTrue();
    assertThat(serverSocket.isClosed()).isFalse();
    serverSocket.close();
    assertThat(serverSocket.isClosed()).isTrue();
  }

  private void assertKeepersAreUsable(Collection<Keeper> keepers) throws IOException {
    for (Keeper keeper : keepers) {
      assertKeeperIsUsable(keeper);
    }
  }

  private void assertKeeperIsUsable(Keeper keeper) throws IOException {
    int port = keeper.getPort();
    keeper.release();
    assertPortIsUsable(port);
  }

  private ServerSocket createServerSocket() throws IOException {
    ServerSocket serverSocket = new ServerSocket();
    this.serverSockets.add(serverSocket);
    return serverSocket;
  }

}
