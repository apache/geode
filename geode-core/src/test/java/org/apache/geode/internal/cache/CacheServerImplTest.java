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
package org.apache.geode.internal.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.OverflowAttributes;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.ClientHealthMonitor;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category(ClientServerTest.class)
public class CacheServerImplTest {

  private InternalCache cache;
  private SecurityService securityService;
  private SocketCreator socketCreator;
  private CacheClientNotifier cacheClientNotifier;
  private ClientHealthMonitor clientHealthMonitor;

  @Before
  public void setUp() throws IOException {
    cache = mock(InternalCache.class);
    securityService = mock(SecurityService.class);
    socketCreator = mock(SocketCreator.class);
    cacheClientNotifier = mock(CacheClientNotifier.class);
    clientHealthMonitor = mock(ClientHealthMonitor.class);

    InternalDistributedSystem system = mock(InternalDistributedSystem.class);
    ServerSocket serverSocket = mock(ServerSocket.class);

    when(cache.getDistributedSystem()).thenReturn(system);
    when(cache.getInternalDistributedSystem()).thenReturn(system);
    when(serverSocket.getLocalSocketAddress()).thenReturn(mock(SocketAddress.class));
    when(socketCreator.createServerSocket(anyInt(), anyInt(), any(), any(), anyInt()))
        .thenReturn(serverSocket);
    when(system.getConfig()).thenReturn(mock(DistributionConfig.class));
    when(system.getProperties()).thenReturn(new Properties());
  }

  @Test
  public void isServerEndpoint() throws IOException {
    OverflowAttributes overflowAttributes = mock(OverflowAttributes.class);
    InternalCacheServer server = new CacheServerImpl(cache, securityService, () -> socketCreator,
        (a, b, c, d, e, f, g) -> cacheClientNotifier, (a, b, c) -> clientHealthMonitor);

    Acceptor acceptor = server.createAcceptor(overflowAttributes);

    assertThat(acceptor.isGatewayReceiver()).isFalse();
  }
}
