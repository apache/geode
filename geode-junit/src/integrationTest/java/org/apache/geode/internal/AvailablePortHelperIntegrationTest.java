/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal;

import static org.apache.geode.distributed.internal.DistributionConfig.DEFAULT_MEMBERSHIP_PORT_RANGE;
import static org.apache.geode.internal.AvailablePort.AVAILABLE_PORTS_LOWER_BOUND;
import static org.apache.geode.internal.AvailablePort.AVAILABLE_PORTS_UPPER_BOUND;
import static org.apache.geode.internal.AvailablePort.MULTICAST;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPortRange;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPortRangeKeepers;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableUDPPort;
import static org.apache.geode.internal.AvailablePortHelper.initializeUniquePortRange;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assume.assumeFalse;

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
import org.junit.runner.RunWith;

import org.apache.geode.internal.AvailablePort.Keeper;
import org.apache.geode.internal.lang.SystemUtils;

@RunWith(JUnitParamsRunner.class)
public class AvailablePortHelperIntegrationTest {

  private Set<ServerSocket> serverSockets;

  @Before
  public void setUp() {
    serverSockets = new HashSet<>();
  }

  @After
  public void tearDown() {
    for (ServerSocket serverSocket : serverSockets) {
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
  public void getRandomAvailableTCPPortRange_zero(final boolean useMembershipPortRange) {
    int[] results = getRandomAvailableTCPPortRange(0, useMembershipPortRange);
    assertThat(results).isEmpty();
  }

  @Test
  @Parameters({"true", "false"})
  public void getRandomAvailableTCPPortRange_one_returnsOne(final boolean useMembershipPortRange) {
    int[] results = getRandomAvailableTCPPortRange(1, useMembershipPortRange);
    assertThat(results).hasSize(1);
  }

  @Test
  @Parameters({"true", "false"})
  public void getRandomAvailableTCPPortRange_two_returnsTwo(final boolean useMembershipPortRange) {
    int[] results = getRandomAvailableTCPPortRange(2, useMembershipPortRange);
    assertThat(results).hasSize(2);
  }

  @Test
  @Parameters({"true", "false"})
  public void getRandomAvailableTCPPortRange_many_returnsMany(final boolean useMembershipPortRange)
      throws IOException {
    int[] results = getRandomAvailableTCPPortRange(10, useMembershipPortRange);
    assertThat(results).hasSize(10);
    assertPortsAreUsable(results);
  }

  @Test
  @Parameters({"true", "false"})
  public void getRandomAvailableTCPPortRange_returnsUniquePorts(
      final boolean useMembershipPortRange) {
    int[] results = getRandomAvailableTCPPortRange(10, useMembershipPortRange);

    Collection<Integer> ports = new HashSet<>();
    for (int port : results) {
      ports.add(port);
    }

    assertThat(ports).hasSize(results.length);
  }

  @Test
  @Parameters({"true", "false"})
  public void getRandomAvailableTCPPortRange_returnsConsecutivePorts(
      final boolean useMembershipPortRange) {
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
  public void getRandomAvailableTCPPortRangeKeepers_zero(final boolean useMembershipPortRange) {
    List<Keeper> results = getRandomAvailableTCPPortRangeKeepers(0, useMembershipPortRange);
    assertThat(results).isEmpty();
  }

  @Test
  @Parameters({"true", "false"})
  public void getRandomAvailableTCPPortRangeKeepers_one_returnsOne(
      final boolean useMembershipPortRange) {
    List<Keeper> results = getRandomAvailableTCPPortRangeKeepers(1, useMembershipPortRange);
    assertThat(results).hasSize(1);
  }

  @Test
  @Parameters({"true", "false"})
  public void getRandomAvailableTCPPortRangeKeepers_two_returnsTwo(
      final boolean useMembershipPortRange) {
    List<Keeper> results = getRandomAvailableTCPPortRangeKeepers(2, useMembershipPortRange);
    assertThat(results).hasSize(2);
  }

  @Test
  @Parameters({"true", "false"})
  public void getRandomAvailableTCPPortRangeKeepers_many_returnsMany(
      final boolean useMembershipPortRange) throws IOException {
    List<Keeper> results = getRandomAvailableTCPPortRangeKeepers(10, useMembershipPortRange);
    assertThat(results).hasSize(10);
    assertKeepersAreUsable(results);
  }

  @Test
  @Parameters({"true", "false"})
  public void getRandomAvailableTCPPortRangeKeepers_returnsUsableKeeper(
      final boolean useMembershipPortRange) throws IOException {
    List<Keeper> results = getRandomAvailableTCPPortRangeKeepers(1, useMembershipPortRange);
    assertKeepersAreUsable(results);
  }

  @Test
  @Parameters({"true", "false"})
  public void getRandomAvailableTCPPortRangeKeepers_returnsUniqueKeepers(
      final boolean useMembershipPortRange) {
    List<Keeper> results = getRandomAvailableTCPPortRangeKeepers(10, useMembershipPortRange);

    Collection<Integer> ports = new HashSet<>();
    for (Keeper keeper : results) {
      ports.add(keeper.getPort());
    }

    assertThat(ports).hasSize(results.size());
  }

  @Test
  @Parameters({"true", "false"})
  public void getRandomAvailableTCPPortRangeKeepers_returnsConsecutivePorts(
      final boolean useMembershipPortRange) {
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

  @Test
  public void getRandomAvailableUDPPort_succeeds() {
    int udpPort = getRandomAvailableUDPPort();
    assertThat(udpPort).isNotZero();
    assertThat(AvailablePort.isPortAvailable(udpPort, MULTICAST)).isTrue();
  }

  @Test
  @Parameters({"true", "false"})
  public void getRandomAvailableTCPPortRange_returnsUniqueRanges(
      final boolean useMembershipPortRange) {
    Collection<Integer> ports = new HashSet<>();

    for (int i = 0; i < 1000; ++i) {
      int[] testPorts = getRandomAvailableTCPPortRange(5, useMembershipPortRange);
      for (int testPort : testPorts) {
        assertThat(ports).doesNotContain(testPort);
        ports.add(testPort);
      }
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void getRandomAvailableTCPPort_returnsUniqueValues(final boolean useMembershipPortRange) {
    Collection<Integer> ports = new HashSet<>();

    for (int i = 0; i < 1000; ++i) {
      int testPort = getRandomAvailableTCPPorts(1, useMembershipPortRange)[0];
      assertThat(ports).doesNotContain(testPort);
      ports.add(testPort);
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void initializeUniquePortRange_willReturnSamePortsForSameRange(
      final boolean useMembershipPortRange) {
    assumeFalse(
        "Windows has ports scattered throughout the range that makes this test difficult to pass consistently",
        SystemUtils.isWindows());

    for (int i = 0; i < 100; ++i) {
      initializeUniquePortRange(i);
      int[] testPorts = getRandomAvailableTCPPorts(3, useMembershipPortRange);
      initializeUniquePortRange(i);
      assertThat(getRandomAvailableTCPPorts(3, useMembershipPortRange)).isEqualTo(testPorts);
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void initializeUniquePortRange_willReturnUniquePortsForUniqueRanges(
      final boolean useMembershipPortRange) {
    assumeFalse(
        "Windows has ports scattered throughout the range that makes this test difficult to pass consistently",
        SystemUtils.isWindows());

    Collection<Integer> ports = new HashSet<>();

    for (int i = 0; i < 100; ++i) {
      initializeUniquePortRange(i);
      int[] testPorts = getRandomAvailableTCPPorts(5, useMembershipPortRange);
      for (int testPort : testPorts) {
        assertThat(ports).doesNotContain(testPort);
        ports.add(testPort);
      }
    }
  }

  private void assertPortsAreUsable(int[] ports) throws IOException {
    for (int port : ports) {
      assertPortIsUsable(port);
    }
  }

  private void assertPortIsUsable(int port) throws IOException, java.net.SocketException {
    ServerSocket serverSocket = createServerSocket();
    serverSocket.setReuseAddress(true);

    serverSocket.bind(new InetSocketAddress(port));

    assertThat(serverSocket.isBound()).isTrue();
    assertThat(serverSocket.isClosed()).isFalse();
    serverSocket.close();
    assertThat(serverSocket.isClosed()).isTrue();
  }

  private void assertKeepersAreUsable(Iterable<Keeper> keepers) throws IOException {
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
    serverSockets.add(serverSocket);
    return serverSocket;
  }

}
