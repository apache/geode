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

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.UnknownHostException;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
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

}
