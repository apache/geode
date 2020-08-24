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
import static org.apache.geode.internal.AvailablePortHelper.MULTICAST;
import static org.apache.geode.internal.AvailablePortHelper.isPortAvailable;
import static org.apache.geode.internal.AvailablePortHelper.SOCKET;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPortKeepers;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableUDPPort;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import junitparams.JUnitParamsRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;

import org.apache.geode.internal.AvailablePortHelper.TcpPortKeeper;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.util.internal.GeodeGlossary;

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
  public void getRandomAvailableTCPPorts_returnsNoPorts_ifCountIsZero() {
    int[] results = getRandomAvailableTCPPorts(0);

    assertThat(results)
        .isEmpty();
  }

  @Test
  public void getRandomAvailableTCPPorts_returnsOnePort_ifCountIsOne() {
    int[] results = getRandomAvailableTCPPorts(1);

    assertThat(results)
        .hasSize(1);
  }

  @Test
  public void getRandomAvailableTCPPorts_returnsManyPorts_ifCountIsMany() {
    int[] results = getRandomAvailableTCPPorts(10);

    assertThat(results)
        .hasSize(10);
  }

  @Test
  public void getRandomAvailableTCPPorts_returnsUsablePorts() {
    int[] results = getRandomAvailableTCPPorts(10);

    stream(results).forEach(port -> assertThatPort(port)
        .isUsable());
  }

  @Test
  public void getRandomAvailableTCPPorts_returnsUniquePorts() {
    int[] results = getRandomAvailableTCPPorts(10);

    assertThat(results)
        .doesNotHaveDuplicates();
  }

  @Test
  public void getRandomAvailableTCPPortKeepers_returnsNoKeepers_ifCountIsZero() {
    List<TcpPortKeeper> results = getRandomAvailableTCPPortKeepers(0);

    assertThat(results)
        .isEmpty();
  }

  @Test
  public void getRandomAvailableTCPPortKeepers_returnsOneKeeper_ifCountIsOne() {
    List<TcpPortKeeper> results = getRandomAvailableTCPPortKeepers(1);

    assertThat(results)
        .hasSize(1);
  }

  @Test
  public void getRandomAvailableTCPPortKeepers_returnsManyKeepers_ifCountIsMany() {
    List<TcpPortKeeper> results = getRandomAvailableTCPPortKeepers(10);

    assertThat(results)
        .hasSize(10);
  }

  @Test
  public void getRandomAvailableTCPPortKeepers_returnsUsableKeepers() {
    List<TcpPortKeeper> results = getRandomAvailableTCPPortKeepers(10);

    results.stream().forEach(keeper ->

    assertThatKeeper(keeper)
        .isUsable());
  }

  @Test
  public void getRandomAvailableTCPPortKeepers_returnsUniqueKeepers() {
    List<TcpPortKeeper> results = getRandomAvailableTCPPortKeepers(10);

    assertThat(keeperPorts(results))
        .doesNotHaveDuplicates();
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

    assertThat(isPortAvailable(udpPort, MULTICAST)).isTrue();
  }

  @Test
  public void getRandomAvailableTCPPorts_returnsUniqueRanges() {
    Collection<Integer> previousPorts = new HashSet<>();
    for (int i = 0; i < 3; ++i) {

      int[] results = getRandomAvailableTCPPorts(5);

      Collection<Integer> ports = toSet(results);

      assertThat(previousPorts).doesNotContainAnyElementsOf(ports);

      previousPorts.addAll(ports);
    }
  }

  @Test
  public void getRandomAvailableTCPPort_returnsUniqueValues() {
    Collection<Integer> previousPorts = new HashSet<>();
    for (int i = 0; i < 3; ++i) {
      int port = getRandomAvailableTCPPorts(1)[0];
      assertThat(previousPorts).doesNotContain(port);
      previousPorts.add(port);
    }
  }

  private static final String LOOPBACK_ADDRESS =
      LocalHostUtil.preferIPv6Addresses() ? "::1" : "127.0.0.1";

  private InetAddress getLoopback() throws UnknownHostException {
    return InetAddress.getByName(LOOPBACK_ADDRESS);
  }

  @Test
  public void testIsPortAvailable() throws IOException {
    try (ServerSocket socket = new ServerSocket()) {
      int port = AvailablePortHelper.getRandomAvailablePort(SOCKET);
      socket.bind(new InetSocketAddress(getLoopback(), port));

      assertFalse(AvailablePort.isPortAvailable(port, SOCKET,
          InetAddress.getByName(LOOPBACK_ADDRESS)));

      InetAddress localHostAddress = InetAddress.getLocalHost();
      // The next assertion assumes that the local host address is not a loopback address. Skip the
      // assertion on host machines that don't satisfy the assumption.
      if (!localHostAddress.isLoopbackAddress()) {
        assertTrue(AvailablePort.isPortAvailable(port, SOCKET, localHostAddress));
      }

      // This should test all interfaces.
      assertFalse(isPortAvailable(port, SOCKET));
    }
  }

  @Test
  public void testWildcardAddressBound() throws IOException {
    try (ServerSocket socket = new ServerSocket()) {
      int port = AvailablePortHelper.getRandomAvailablePort(AvailablePort.SOCKET);
      socket.bind(new InetSocketAddress((InetAddress) null, port));
      System.out.println(
          "bind addr=" + System.getProperty(GeodeGlossary.GEMFIRE_PREFIX + "bind-address"));
      assertFalse(isPortAvailable(port, AvailablePort.SOCKET));
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

  private List<Integer> keeperPorts(List<TcpPortKeeper> keepers) {
    return keepers.stream()
        .map(TcpPortKeeper::getPort)
        .collect(Collectors.toList());
  }

  private PortAssertion assertThatPort(int port) {
    return new PortAssertion(port);
  }

  private KeeperAssertion assertThatKeeper(TcpPortKeeper keeper) {
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

    private final TcpPortKeeper keeper;

    KeeperAssertion(TcpPortKeeper keeper) {
      this.keeper = keeper;
    }

    KeeperAssertion isUsable() {
      int port = keeper.getPort();
      keeper.release();
      assertThatPort(port).isUsable();
      return this;
    }
  }

  private static class SequenceAssertion {

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
