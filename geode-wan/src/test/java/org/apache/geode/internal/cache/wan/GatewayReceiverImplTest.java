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
package org.apache.geode.internal.cache.wan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.InOrder;

import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheServer;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.test.junit.categories.WanTest;

@Category(WanTest.class)
public class GatewayReceiverImplTest {

  private InternalCache cache;
  private InternalCacheServer receiverServer;

  @Before
  public void setUp() {
    cache = mock(InternalCache.class);
    receiverServer = mock(InternalCacheServer.class);

    InternalDistributedSystem system = mock(InternalDistributedSystem.class);

    when(cache.getInternalDistributedSystem()).thenReturn(system);
    when(cache.addGatewayReceiverServer(any())).thenReturn(receiverServer);
    when(receiverServer.getExternalAddress()).thenReturn("hello");
  }

  @Test
  public void getHostOnUnstartedGatewayShouldReturnLocalHostName() throws UnknownHostException {
    GatewayReceiver gateway = new GatewayReceiverImpl(cache, 2000, 2001, 5, 100, null, null, null,
        true, true, 2000);

    assertThat(gateway.getHost()).isEqualTo(LocalHostUtil.getLocalHostName());
  }

  @Test
  public void getHostOnRunningGatewayShouldReturnCacheServerExternalAddress() {
    String theExternalAddress = "theExternalAddress";
    when(receiverServer.getExternalAddress()).thenReturn(theExternalAddress);
    GatewayReceiverImpl gateway = new GatewayReceiverImpl(cache, 2000, 2001, 5, 100, null, null,
        null, true, true, 2000);

    gateway.start();

    assertThat(gateway.getHost()).isEqualTo(theExternalAddress);
  }

  @Test
  public void destroyCalledOnRunningGatewayReceiverShouldThrowGatewayReceiverException() {
    GatewayReceiverImpl gateway = new GatewayReceiverImpl(cache, 2000, 2001, 5, 100, null, null,
        null, true, true, 2000);
    when(receiverServer.isRunning()).thenReturn(true);
    gateway.start();

    Throwable thrown = catchThrowable(() -> gateway.destroy());

    assertThat(thrown).isInstanceOf(GatewayReceiverException.class);
  }

  @Test
  public void destroyCalledOnStoppedGatewayReceiverShouldRemoveReceiverFromCacheServers() {
    GatewayReceiverImpl gateway = new GatewayReceiverImpl(cache, 2000, 2001, 5, 100, null, null,
        null, true, true, 2000);
    gateway.start();
    when(receiverServer.isRunning()).thenReturn(false);

    gateway.destroy();

    InOrder inOrder = inOrder(cache, cache);
    inOrder.verify(cache).removeGatewayReceiver(same(gateway));
    inOrder.verify(cache).removeGatewayReceiverServer(same(receiverServer));
  }

  @Test
  public void destroyCalledOnStoppedGatewayReceiverShouldRemoveReceiverFromReceivers() {
    GatewayReceiverImpl gateway = new GatewayReceiverImpl(cache, 2000, 2001, 5, 100, null, null,
        null, true, true, 2000);
    gateway.start();
    when(receiverServer.isRunning()).thenReturn(false);

    gateway.destroy();

    verify(cache).removeGatewayReceiver(same(gateway));
  }

  @Test
  public void startShouldThrowGatewayReceiverExceptionIfIOExceptionIsThrown() throws IOException {
    doThrow(new SocketException("Address already in use")).when(receiverServer).start();
    GatewayReceiverImpl gateway = new GatewayReceiverImpl(cache, 2000, 2000, 5, 100, null, null,
        null, true, true, 2000);

    Throwable thrown = catchThrowable(() -> gateway.start());

    assertThat(thrown)
        .isInstanceOf(GatewayReceiverException.class)
        .hasMessageContaining("No available free port found in the given range");

    verify(receiverServer).start();
  }

  @Test
  public void startShouldTryTwoPortsInPortRangeIfIOExceptionIsThrown() throws IOException {
    doThrow(new SocketException("Address already in use")).when(receiverServer).start();
    GatewayReceiverImpl gateway = new GatewayReceiverImpl(cache, 2000, 2001, 5, 100, null, null,
        null, true, true, 2000);

    Throwable thrown = catchThrowable(() -> gateway.start());

    assertThat(thrown)
        .isInstanceOf(GatewayReceiverException.class)
        .hasMessageContaining("No available free port found in the given range");

    verify(receiverServer, times(2)).start();
  }

  @Test
  public void startShouldTryAllPortsInPortRangeIfIOExceptionIsThrown() throws IOException {
    doThrow(new SocketException("Address already in use")).when(receiverServer).start();
    int startPort = 2000;
    int endPort = 2100;
    GatewayReceiverImpl gateway = new GatewayReceiverImpl(cache, startPort, endPort, 5, 100, null,
        null, null, true, true, 2000);

    Throwable thrown = catchThrowable(() -> gateway.start());

    assertThat(thrown)
        .isInstanceOf(GatewayReceiverException.class)
        .hasMessageContaining("No available free port found in the given range");

    assertThat(gateway.getPort()).isEqualTo(0);

    int numberOfInvocations = endPort - startPort + 1;
    verify(receiverServer, times(numberOfInvocations)).start();
  }

  @Test
  public void startsSucceedsIfRandomPortInPortRangeSucceeds() throws IOException {
    IOException ioException = new SocketException("Address already in use");
    doThrow(ioException).doThrow(ioException).doNothing().when(receiverServer).start();
    GatewayReceiverImpl gateway = new GatewayReceiverImpl(cache, 2000, 2010, 5, 100, null, null,
        null, true, true, 2000);

    gateway.start();

    assertThat(gateway.getPort()).isGreaterThanOrEqualTo(2000);

    verify(receiverServer, times(3)).start();
  }
}
