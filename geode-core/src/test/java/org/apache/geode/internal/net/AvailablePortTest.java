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
package org.apache.geode.internal.net;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;

import org.junit.Test;

import org.apache.geode.internal.net.AvailablePort.Keeper;
import org.apache.geode.internal.net.AvailablePort.Protocol;
import org.apache.geode.internal.net.AvailablePort.Range;

public class AvailablePortTest {

  private AvailablePort availablePort =
      mock(AvailablePort.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
  private InetAddress address = mock(InetAddress.class);

  @Test
  public void create_returnsNewInstance() {
    assertThat(AvailablePort.create()).isNotSameAs(AvailablePort.create());
  }

  @Test
  public void create_returnsAvailablePortImpl() {
    assertThat(AvailablePort.create()).isInstanceOf(AvailablePortImpl.class);
  }

  @Test
  public void protocol_SOCKET_intLevelIsZero() {
    assertThat(Protocol.SOCKET.intLevel()).isZero();
  }

  @Test
  public void protocol_MULTICAST_intLevelIsOne() {
    assertThat(Protocol.MULTICAST.intLevel()).isOne();
  }

  @Test
  public void range_LOWER_BOUND_intLevelIs20001() {
    assertThat(Range.LOWER_BOUND.intLevel()).isEqualTo(20001);
  }

  @Test
  public void range_UPPER_BOUND_intLevelIs29999() {
    assertThat(Range.UPPER_BOUND.intLevel()).isEqualTo(29999);
  }

  @Test
  public void deprecated_getAddress_int_delegates() {
    availablePort.getAddress(1);

    verify(availablePort).getAddress(same(Protocol.MULTICAST));
  }

  @Test
  public void deprecated_isPortAvailable_int_int_delegates() {
    availablePort.isPortAvailable(1, 1);

    verify(availablePort).isPortAvailable(eq(1), same(Protocol.MULTICAST));
  }

  @Test
  public void deprecated_isPortAvailable_int_int_InetAddress_delegates() {
    availablePort.isPortAvailable(1, 1, address);

    verify(availablePort).isPortAvailable(eq(1), same(Protocol.MULTICAST), same(address));
  }

  @Test
  public void deprecated_isPortKeepable_int_int_InetAddress_delegates() {
    availablePort.isPortKeepable(1, 1, address);

    verify(availablePort).isPortKeepable(eq(1), same(Protocol.MULTICAST), same(address));
  }

  @Test
  public void deprecated_getRandomAvailablePort_int_delegates() {
    availablePort.getRandomAvailablePort(1);

    verify(availablePort).getRandomAvailablePort(same(Protocol.MULTICAST));
  }

  @Test
  public void deprecated_getRandomAvailablePortKeeper_int_delegates() {
    availablePort.getRandomAvailablePortKeeper(1);

    verify(availablePort).getRandomAvailablePortKeeper(same(Protocol.MULTICAST));
  }

  @Test
  public void deprecated_getAvailablePortInRange_int_int_int_delegates() {
    availablePort.getAvailablePortInRange(1, 10, 1);

    verify(availablePort).getAvailablePortInRange(eq(1), eq(10), same(Protocol.MULTICAST));
  }

  @Test
  public void deprecated_getRandomAvailablePortWithMod_int_int_delegates() {
    availablePort.getRandomAvailablePortWithMod(1, 10);

    verify(availablePort).getRandomAvailablePortWithMod(same(Protocol.MULTICAST), eq(10));
  }

  @Test
  public void deprecated_getRandomAvailablePort_int_InetAddress_delegates() {
    availablePort.getRandomAvailablePort(1, address);

    verify(availablePort).getRandomAvailablePort(same(Protocol.MULTICAST), same(address));
  }

  @Test
  public void deprecated_getRandomAvailablePort_int_InetAddress_boolean_delegates() {
    availablePort.getRandomAvailablePort(1, address, true);

    verify(availablePort).getRandomAvailablePort(same(Protocol.MULTICAST), same(address), eq(true));
  }

  @Test
  public void deprecated_getRandomAvailablePortKeeper_int_InetAddress_delegates() {
    availablePort.getRandomAvailablePortKeeper(1, address);

    verify(availablePort).getRandomAvailablePortKeeper(same(Protocol.MULTICAST), same(address));
  }

  @Test
  public void deprecated_getRandomAvailablePortKeeper_int_InetAddress_boolean_delegates() {
    availablePort.getRandomAvailablePortKeeper(1, address, true);

    verify(availablePort).getRandomAvailablePortKeeper(same(Protocol.MULTICAST), same(address),
        eq(true));
  }

  @Test
  public void deprecated_getAvailablePortInRange_int_InetAddress_int_int_delegates() {
    availablePort.getAvailablePortInRange(1, address, 10, 20);

    verify(availablePort).getAvailablePortInRange(same(Protocol.MULTICAST), same(address), eq(10),
        eq(20));
  }

  @Test
  public void deprecated_getRandomAvailablePortWithMod_int_InetAddress_int_delegates() {
    availablePort.getRandomAvailablePortWithMod(1, address, 5);

    verify(availablePort).getRandomAvailablePortWithMod(same(Protocol.MULTICAST), same(address),
        eq(5));
  }

  @Test
  public void deprecated_getRandomAvailablePortInRange_int_int_int_delegates() {
    availablePort.getRandomAvailablePortInRange(20, 40, 1);

    verify(availablePort).getRandomAvailablePortInRange(eq(20), eq(40), same(Protocol.MULTICAST));
  }

  @Test
  public void keeper_getPort_returnsZero_withoutPort() {
    Keeper keeper = new Keeper(mock(ServerSocket.class));

    assertThat(keeper.getPort()).isZero();
  }

  @Test
  public void keeper_getPort_returnsPort_withPort() {
    Keeper keeper = new Keeper(mock(ServerSocket.class), 10);

    assertThat(keeper.getPort()).isEqualTo(10);
  }

  @Test
  public void keeper_release_closesServerSocket() throws IOException {
    ServerSocket serverSocket = mock(ServerSocket.class);
    Keeper keeper = new Keeper(serverSocket);

    keeper.release();

    verify(serverSocket).close();
  }
}
