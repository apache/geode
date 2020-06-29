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

import static java.util.Collections.singletonList;
import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.STRICT_STUBS;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.statistics.StatisticsClockFactory;
import org.apache.geode.services.module.ModuleService;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category(ClientServerTest.class)
public class ServerBuilderIntegrationTest {

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(STRICT_STUBS);

  private GatewayReceiver gatewayReceiver;
  private InternalCache cache;
  private InternalCacheServer server;

  @Before
  public void setUp() {
    gatewayReceiver = mock(GatewayReceiver.class);

    cache = (InternalCache) new CacheFactory().create();
  }

  @After
  public void tearDown() {
    if (server != null) {
      server.stop();
    }
    cache.close();
  }

  @Test
  public void byDefaultCreatesServerWithCacheServerAcceptor() throws IOException {
    server = new ServerBuilder(cache, cache.getSecurityService(),
        StatisticsClockFactory.disabledClock(), ModuleService.DEFAULT)
            .createServer();
    server.setPort(0);

    server.start();

    Acceptor acceptor = server.getAcceptor();
    assertThat(acceptor.isGatewayReceiver()).isFalse();
  }

  @Test
  public void forGatewayReceiverCreatesServerWithGatewayReceiverAcceptor() throws IOException {
    when(gatewayReceiver.getGatewayTransportFilters())
        .thenReturn(singletonList(mock(GatewayTransportFilter.class)));
    server = new ServerBuilder(cache, cache.getSecurityService(),
        StatisticsClockFactory.disabledClock(), ModuleService.DEFAULT)
            .forGatewayReceiver(gatewayReceiver)
            .createServer();
    server.setPort(0);

    server.start();

    Acceptor acceptor = server.getAcceptor();
    assertThat(acceptor.isGatewayReceiver()).isTrue();
  }

  @Test
  public void byDefaultCreatesServerWithMembershipGroup() {
    cache.close();
    String membershipGroup = "group-m0";
    cache = (InternalCache) new CacheFactory().set(GROUPS, membershipGroup).create();
    server = new ServerBuilder(cache, cache.getSecurityService(),
        StatisticsClockFactory.disabledClock(), ModuleService.DEFAULT)
            .createServer();

    assertThat(server.getCombinedGroups()).containsExactly(membershipGroup);
  }

  @Test
  public void forGatewayReceiverCreatesServerWithoutMembershipGroup() {
    when(gatewayReceiver.getGatewayTransportFilters())
        .thenReturn(singletonList(mock(GatewayTransportFilter.class)));
    cache.close();
    String membershipGroup = "group-m0";
    cache = (InternalCache) new CacheFactory().set(GROUPS, membershipGroup).create();
    server = new ServerBuilder(cache, cache.getSecurityService(),
        StatisticsClockFactory.disabledClock(), ModuleService.DEFAULT)
            .forGatewayReceiver(gatewayReceiver)
            .createServer();

    assertThat(server.getCombinedGroups()).doesNotContain(membershipGroup);
  }
}
