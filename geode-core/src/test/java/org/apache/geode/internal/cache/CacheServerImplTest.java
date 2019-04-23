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

import org.apache.geode.cache.server.CacheServer;
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
  private CacheClientNotifier cacheClientNotifier;
  private ClientHealthMonitor clientHealthMonitor;
  private DistributionConfig config;
  private SecurityService securityService;
  private SocketCreator socketCreator;

  @Before
  public void setUp() throws IOException {
    cache = mock(InternalCache.class);
    cacheClientNotifier = mock(CacheClientNotifier.class);
    clientHealthMonitor = mock(ClientHealthMonitor.class);
    config = mock(DistributionConfig.class);
    securityService = mock(SecurityService.class);
    socketCreator = mock(SocketCreator.class);

    InternalDistributedSystem system = mock(InternalDistributedSystem.class);
    ServerSocket serverSocket = mock(ServerSocket.class);

    when(cache.getDistributedSystem()).thenReturn(system);
    when(cache.getInternalDistributedSystem()).thenReturn(system);
    when(serverSocket.getLocalSocketAddress()).thenReturn(mock(SocketAddress.class));
    when(socketCreator.createServerSocket(anyInt(), anyInt(), any(), any(), anyInt()))
        .thenReturn(serverSocket);
    when(system.getConfig()).thenReturn(config);
    when(system.getProperties()).thenReturn(new Properties());
  }

  @Test
  public void createdAcceptorIsGatewayEndpoint() throws IOException {
    OverflowAttributes overflowAttributes = mock(OverflowAttributes.class);
    InternalCacheServer server = new CacheServerImpl(cache, securityService, () -> socketCreator,
        (a, b, c, d, e, f, g) -> cacheClientNotifier, (a, b, c) -> clientHealthMonitor);

    Acceptor acceptor = server.createAcceptor(overflowAttributes);

    assertThat(acceptor.isGatewayReceiver()).isFalse();
  }

  @Test
  public void getGroups_returnsSpecifiedGroup() {
    CacheServer server = new CacheServerImpl(cache, securityService, () -> socketCreator,
        (a, b, c, d, e, f, g) -> cacheClientNotifier, (a, b, c) -> clientHealthMonitor);
    String specifiedGroup = "group0";

    server.setGroups(new String[] {specifiedGroup});

    assertThat(server.getGroups())
        .containsExactly(specifiedGroup);
  }

  @Test
  public void getGroups_returnsMultipleSpecifiedGroups() {
    CacheServer server = new CacheServerImpl(cache, securityService, () -> socketCreator,
        (a, b, c, d, e, f, g) -> cacheClientNotifier, (a, b, c) -> clientHealthMonitor);
    String specifiedGroup1 = "group1";
    String specifiedGroup2 = "group2";
    String specifiedGroup3 = "group3";

    server.setGroups(new String[] {specifiedGroup1, specifiedGroup2, specifiedGroup3});

    assertThat(server.getGroups())
        .containsExactlyInAnyOrder(specifiedGroup1, specifiedGroup2, specifiedGroup3);
  }

  @Test
  public void getCombinedGroups_includesMembershipGroup() {
    String membershipGroup = "group-m0";
    when(config.getGroups()).thenReturn(membershipGroup);
    CacheServerImpl server = new CacheServerImpl(cache, securityService, () -> socketCreator,
        (a, b, c, d, e, f, g) -> cacheClientNotifier, (a, b, c) -> clientHealthMonitor);

    assertThat(server.getCombinedGroups())
        .contains(membershipGroup);
  }

  @Test
  public void getCombinedGroups_includesMultipleMembershipGroups() {
    String membershipGroup1 = "group-m1";
    String membershipGroup2 = "group-m2";
    String membershipGroup3 = "group-m3";
    when(config.getGroups())
        .thenReturn(membershipGroup1 + "," + membershipGroup2 + "," + membershipGroup3);
    CacheServerImpl server = new CacheServerImpl(cache, securityService, () -> socketCreator,
        (a, b, c, d, e, f, g) -> cacheClientNotifier, (a, b, c) -> clientHealthMonitor);

    assertThat(server.getCombinedGroups())
        .contains(membershipGroup1, membershipGroup2, membershipGroup3);
  }

  @Test
  public void getCombinedGroups_includesSpecifiedGroupsAndMembershipGroups() {
    String membershipGroup1 = "group-m1";
    String membershipGroup2 = "group-m2";
    String membershipGroup3 = "group-m3";
    when(config.getGroups())
        .thenReturn(membershipGroup1 + "," + membershipGroup2 + "," + membershipGroup3);
    CacheServerImpl server = new CacheServerImpl(cache, securityService, () -> socketCreator,
        (a, b, c, d, e, f, g) -> cacheClientNotifier, (a, b, c) -> clientHealthMonitor);
    String specifiedGroup1 = "group1";
    String specifiedGroup2 = "group2";
    String specifiedGroup3 = "group3";

    server.setGroups(new String[] {specifiedGroup1, specifiedGroup2, specifiedGroup3});

    assertThat(server.getCombinedGroups())
        .containsExactlyInAnyOrder(membershipGroup1, membershipGroup2, membershipGroup3,
            specifiedGroup1, specifiedGroup2, specifiedGroup3);
  }
}
