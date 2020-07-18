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
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;

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
  public void getRandomAvailableTCPPorts_returnsNaturallyOrderedPorts() {
    int[] results = getRandomAvailableTCPPorts(10);

    assertThat(results)
        .isSortedAccordingTo(naturalOrder());
  }

  @Test
  public void getRandomAvailableTCPPorts_returnsConsecutivePorts() {
    int[] results = getRandomAvailableTCPPorts(10);

    assertThatSequence(results)
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
  public void getRandomAvailableTCPPorts_returnsUniqueRanges() {
    Collection<Integer> previousPorts = new HashSet<>();
    for (int i = 0; i < 3; ++i) {

      int[] results = getRandomAvailableTCPPorts(5);

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

  private PortAssertion assertThatPort(int port) {
    return new PortAssertion(port);
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
