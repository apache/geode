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

import static java.util.Arrays.stream;
import static java.util.Comparator.naturalOrder;
import static org.apache.geode.internal.AvailablePort.MULTICAST;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPortRange;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPortRangeKeepers;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableUDPPort;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;

import org.apache.geode.internal.AvailablePort.Keeper;

@RunWith(JUnitParamsRunner.class)
public class AvailablePortHelperIntegrationTest {

  private Set<ServerSocket> serverSockets;

  @Rule
  public ErrorCollector errorCollector = new ErrorCollector();

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
      } catch (IOException e) {
        errorCollector.addError(e);
      }
    }
  }

  @Test
  public void getRandomAvailableTCPPortRange_returnsNoPorts_ifCountIsZero() {
    int[] results = getRandomAvailableTCPPortRange(0);

    assertThat(results)
        .isEmpty();
  }

  @Test
  public void getRandomAvailableTCPPortRange_returnsOnePort_ifCountIsOne() {
    int[] results = getRandomAvailableTCPPortRange(1);

    assertThat(results)
        .hasSize(1);
  }

  @Test
  public void getRandomAvailableTCPPortRange_returnsManyPorts_ifCountIsMany() {
    int[] results = getRandomAvailableTCPPortRange(10);

    assertThat(results)
        .hasSize(10);
  }

  @Test
  public void getRandomAvailableTCPPortRange_returnsUsablePorts() {
    int[] results = getRandomAvailableTCPPortRange(10);

    stream(results).forEach(port -> assertThatPort(port)
        .isUsable());
  }

  @Test
  public void getRandomAvailableTCPPortRange_returnsUniquePorts() {
    int[] results = getRandomAvailableTCPPortRange(10);

    assertThat(results)
        .doesNotHaveDuplicates();
  }

  @Test
  public void getRandomAvailableTCPPortRange_returnsNaturallyOrderedPorts() {
    int[] results = getRandomAvailableTCPPortRange(10);

    assertThat(results)
        .isSortedAccordingTo(naturalOrder());
  }

  @Test
  public void getRandomAvailableTCPPortRange_returnsConsecutivePorts() {
    int[] results = getRandomAvailableTCPPortRange(10);

    assertThatSequence(results)
        .isConsecutive();
  }

  @Test
  public void getRandomAvailableTCPPortRangeKeepers_returnsNoKeepers_ifCountIsZero() {
    List<Keeper> results = getRandomAvailableTCPPortRangeKeepers(0);

    assertThat(results)
        .isEmpty();
  }

  @Test
  public void getRandomAvailableTCPPortRangeKeepers_returnsOneKeeper_ifCountIsOne() {
    List<Keeper> results = getRandomAvailableTCPPortRangeKeepers(1);

    assertThat(results)
        .hasSize(1);
  }

  @Test
  public void getRandomAvailableTCPPortRangeKeepers_returnsManyKeepers_ifCountIsMany() {
    List<Keeper> results = getRandomAvailableTCPPortRangeKeepers(10);

    assertThat(results)
        .hasSize(10);
  }

  @Test
  @Parameters({"true", "false"})
  @TestCaseName("{method}(useMembershipPortRange={0})")
  public void getRandomAvailableTCPPortRangeKeepers_returnsUsableKeepers(
      boolean useMembershipPortRange) {
    List<Keeper> results = getRandomAvailableTCPPortRangeKeepers(10, useMembershipPortRange);

    results.stream().forEach(keeper ->

    assertThatKeeper(keeper)
        .isUsable());
  }

  @Test
  @Parameters({"true", "false"})
  @TestCaseName("{method}(useMembershipPortRange={0})")
  public void getRandomAvailableTCPPortRangeKeepers_returnsUniqueKeepers(
      boolean useMembershipPortRange) {
    List<Keeper> results = getRandomAvailableTCPPortRangeKeepers(10, useMembershipPortRange);

    assertThat(keeperPorts(results))
        .doesNotHaveDuplicates();
  }

  @Test
  @Parameters({"true", "false"})
  @TestCaseName("{method}(useMembershipPortRange={0})")
  public void getRandomAvailableTCPPortRangeKeepers_returnsNaturallyOrderedPorts(
      boolean useMembershipPortRange) {
    List<Keeper> results = getRandomAvailableTCPPortRangeKeepers(10, useMembershipPortRange);

    assertThat(keeperPorts(results))
        .isSortedAccordingTo(naturalOrder());
  }

  @Test
  public void getRandomAvailableTCPPortRangeKeepers_returnsConsecutivePorts() {
    List<Keeper> results = getRandomAvailableTCPPortRangeKeepers(10);

    assertThatSequence(keeperPorts(results))
        .isConsecutive();
  }

  @Test
  public void getRandomAvailableUDPPort_returnsNonZeroUdpPort() {
    int udpPort = getRandomAvailableUDPPort();

    assertThat(udpPort)
        .isNotZero();
  }

  @Test
  public void getRandomAvailableUDPPort_returnsAvailableUdpPort() {
    int udpPort = getRandomAvailableUDPPort();

    assertThat(AvailablePort.isPortAvailable(udpPort, MULTICAST))
        .isTrue();
  }

  @Test
  public void getRandomAvailableTCPPortRange_returnsUniqueRanges() {
    Collection<Integer> previousPorts = new HashSet<>();
    for (int i = 0; i < 3; ++i) {

      int[] results = getRandomAvailableTCPPortRange(5);

      Collection<Integer> ports = toSet(results);

      assertThat(previousPorts)
          .doesNotContainAnyElementsOf(ports);

      previousPorts.addAll(ports);
    }
  }

  @Test
  public void getRandomAvailableTCPPort_returnsUniqueValues() {
    Collection<Integer> previousPorts = new HashSet<>();
    for (int i = 0; i < 3; ++i) {

      int port = getRandomAvailableTCPPorts(1)[0];

      assertThat(previousPorts)
          .doesNotContain(port);

      previousPorts.add(port);
    }
  }

  private ServerSocket createServerSocket() {
    try {
      ServerSocket serverSocket = new ServerSocket();
      serverSockets.add(serverSocket);
      return serverSocket;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private List<Integer> toList(int[] integers) {
    return stream(integers)
        .boxed()
        .collect(Collectors.toList());
  }

  private Set<Integer> toSet(int[] integers) {
    return stream(integers)
        .boxed()
        .collect(Collectors.toSet());
  }

  private List<Integer> keeperPorts(List<Keeper> keepers) {
    return keepers.stream()
        .map(Keeper::getPort)
        .collect(Collectors.toList());
  }

  private PortAssertion assertThatPort(int port) {
    return new PortAssertion(port);
  }

  private KeeperAssertion assertThatKeeper(Keeper keeper) {
    return new KeeperAssertion(keeper);
  }

  private SequenceAssertion assertThatSequence(int[] integers) {
    return new SequenceAssertion(toList(integers));
  }

  private SequenceAssertion assertThatSequence(List<Integer> integers) {
    return new SequenceAssertion(integers);
  }

  private class PortAssertion {

    private final int port;

    PortAssertion(int port) {
      this.port = port;
    }

    PortAssertion isUsable() {
      try {
        ServerSocket serverSocket = createServerSocket();
        serverSocket.setReuseAddress(true);

        serverSocket.bind(new InetSocketAddress(port));

        assertThat(serverSocket.isBound()).isTrue();
        assertThat(serverSocket.isClosed()).isFalse();
        serverSocket.close();
        assertThat(serverSocket.isClosed()).isTrue();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      return this;
    }
  }

  private class KeeperAssertion {

    private final Keeper keeper;

    KeeperAssertion(Keeper keeper) {
      this.keeper = keeper;
    }

    KeeperAssertion isUsable() {
      int port = keeper.getPort();
      keeper.release();
      assertThatPort(port).isUsable();
      return this;
    }
  }

  private class SequenceAssertion {

    private final List<Integer> actual;

    SequenceAssertion(List<Integer> actual) {
      this.actual = actual;
      assertThat(actual)
          .isNotEmpty();
    }

    SequenceAssertion isConsecutive() {
      int start = actual.get(0);
      int end = actual.get(actual.size() - 1);

      List<Integer> expected = IntStream.rangeClosed(start, end)
          .boxed()
          .collect(Collectors.toList());

      assertThat(actual)
          .isEqualTo(expected);

      return this;
    }
  }
}
