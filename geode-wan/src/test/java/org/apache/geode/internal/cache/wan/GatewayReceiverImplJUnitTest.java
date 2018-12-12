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

import static org.apache.geode.internal.Assert.assertTrue;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.net.SocketCreator;

public class GatewayReceiverImplJUnitTest {

  @Test
  public void getHostOnUnstartedGatewayShouldReturnLocalhost() throws UnknownHostException {
    InternalCache cache = mock(InternalCache.class);
    GatewayReceiverImpl gateway =
        new GatewayReceiverImpl(cache, 2000, 2001, 5, 100, null, null, null, true);
    assertEquals(SocketCreator.getLocalHost().getHostName(), gateway.getHost());
  }

  @Test
  public void getHostOnRunningGatewayShouldReturnCacheServerAddress() throws IOException {
    InternalCache cache = mock(InternalCache.class);
    CacheServerImpl server = mock(CacheServerImpl.class);
    InternalDistributedSystem system = mock(InternalDistributedSystem.class);
    when(cache.getInternalDistributedSystem()).thenReturn(system);
    when(server.getExternalAddress()).thenReturn("hello");
    when(cache.addCacheServer(eq(true))).thenReturn(server);
    GatewayReceiverImpl gateway =
        new GatewayReceiverImpl(cache, 2000, 2001, 5, 100, null, null, null, true);
    gateway.start();
    assertEquals("hello", gateway.getHost());
  }

  @Test
  public void destroyCalledOnRunningGatewayReceiverShouldThrowException() throws IOException {
    InternalCache cache = mock(InternalCache.class);
    CacheServerImpl server = mock(CacheServerImpl.class);
    InternalDistributedSystem system = mock(InternalDistributedSystem.class);
    when(cache.getInternalDistributedSystem()).thenReturn(system);
    when(server.getExternalAddress()).thenReturn("hello");
    when(server.isRunning()).thenReturn(true);
    when(cache.addCacheServer(eq(true))).thenReturn(server);
    GatewayReceiverImpl gateway =
        new GatewayReceiverImpl(cache, 2000, 2001, 5, 100, null, null, null, true);
    gateway.start();
    try {
      gateway.destroy();
      fail();
    } catch (GatewayReceiverException e) {
      assertEquals("Gateway Receiver is running and needs to be stopped first", e.getMessage());
    }
  }

  @Test
  public void destroyCalledOnStoppedGatewayReceiverShouldRemoveReceiverFromCacheServers()
      throws IOException {
    InternalCache cache = mock(InternalCache.class);
    CacheServerImpl server = mock(CacheServerImpl.class);
    InternalDistributedSystem system = mock(InternalDistributedSystem.class);
    when(cache.getInternalDistributedSystem()).thenReturn(system);
    when(server.getExternalAddress()).thenReturn("hello");
    when(cache.addCacheServer(eq(true))).thenReturn(server);
    GatewayReceiverImpl gateway =
        new GatewayReceiverImpl(cache, 2000, 2001, 5, 100, null, null, null, true);
    gateway.start();
    // sender is mocked already to say running is false
    gateway.destroy();
    verify(cache, times(1)).removeCacheServer(server);
  }

  @Test
  public void destroyCalledOnStoppedGatewayReceiverShouldRemoveReceiverFromReceivers()
      throws IOException {
    InternalCache cache = mock(InternalCache.class);
    CacheServerImpl server = mock(CacheServerImpl.class);
    InternalDistributedSystem system = mock(InternalDistributedSystem.class);
    when(cache.getInternalDistributedSystem()).thenReturn(system);
    when(server.getExternalAddress()).thenReturn("hello");
    when(cache.addCacheServer(eq(true))).thenReturn(server);
    GatewayReceiverImpl gateway =
        new GatewayReceiverImpl(cache, 2000, 2001, 5, 100, null, null, null, true);
    gateway.start();
    // sender is mocked already to say running is false
    gateway.destroy();
    verify(cache, times(1)).removeGatewayReceiver(gateway);
  }

  @Test
  public void testFailToStartWith2NextPorts() throws IOException {
    InternalCache cache = mock(InternalCache.class);
    CacheServerImpl server = mock(CacheServerImpl.class);
    when(cache.addCacheServer(eq(true))).thenReturn(server);
    doThrow(new SocketException("Address already in use")).when(server).start();
    GatewayReceiverImpl gateway =
        new GatewayReceiverImpl(cache, 2000, 2001, 5, 100, null, null, null, true);
    assertThatThrownBy(() -> gateway.start()).isInstanceOf(GatewayReceiverException.class)
        .hasMessageContaining("No available free port found in the given range");
    verify(server, times(2)).start();
  }

  @Test
  public void testFailToStartWithSamePort() throws IOException {
    InternalCache cache = mock(InternalCache.class);
    CacheServerImpl server = mock(CacheServerImpl.class);
    when(cache.addCacheServer(eq(true))).thenReturn(server);
    doThrow(new SocketException("Address already in use")).when(server).start();
    GatewayReceiverImpl gateway =
        new GatewayReceiverImpl(cache, 2000, 2000, 5, 100, null, null, null, true);
    assertThatThrownBy(() -> gateway.start()).isInstanceOf(GatewayReceiverException.class)
        .hasMessageContaining("No available free port found in the given range");
    verify(server, times(1)).start();
  }

  @Test
  public void testFailToStartWithARangeOfPorts() throws IOException {
    InternalCache cache = mock(InternalCache.class);
    CacheServerImpl server = mock(CacheServerImpl.class);
    when(cache.addCacheServer(eq(true))).thenReturn(server);
    doThrow(new SocketException("Address already in use")).when(server).start();
    GatewayReceiverImpl gateway =
        new GatewayReceiverImpl(cache, 2000, 2100, 5, 100, null, null, null, true);
    assertThatThrownBy(() -> gateway.start()).isInstanceOf(GatewayReceiverException.class)
        .hasMessageContaining("No available free port found in the given range");
    assertTrue(gateway.getPort() == 0);
    verify(server, times(101)).start(); // 2000-2100: contains 101 ports
  }

  @Test
  public void testSuccessToStartAtSpecifiedPort() throws IOException {
    InternalCache cache = mock(InternalCache.class);
    CacheServerImpl server = mock(CacheServerImpl.class);
    InternalDistributedSystem system = mock(InternalDistributedSystem.class);
    when(cache.getInternalDistributedSystem()).thenReturn(system);
    when(cache.addCacheServer(eq(true))).thenReturn(server);
    AtomicInteger callCount = new AtomicInteger();
    doAnswer(invocation -> {
      // only throw IOException for 2 times
      if (callCount.get() < 2) {
        callCount.incrementAndGet();
        throw new SocketException("Address already in use");
      }
      return 0;
    }).when(server).start();
    GatewayReceiverImpl gateway =
        new GatewayReceiverImpl(cache, 2000, 2010, 5, 100, null, null, null, true);
    gateway.start();
    assertTrue(gateway.getPort() >= 2000);
    assertEquals(2, callCount.get());
    verify(server, times(3)).start(); // 2 failed tries, 1 succeeded
  }
}
