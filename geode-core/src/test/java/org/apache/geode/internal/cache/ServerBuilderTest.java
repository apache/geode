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
import static org.apache.geode.internal.cache.ServerBuilder.SocketCreatorType.GATEWAY;
import static org.apache.geode.internal.cache.ServerBuilder.SocketCreatorType.SERVER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.STRICT_STUBS;

import java.util.function.Function;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.distributed.internal.DistributionAdvisee;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier.CacheClientNotifierProvider;
import org.apache.geode.internal.cache.tier.sockets.ClientHealthMonitor.ClientHealthMonitorProvider;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.services.module.internal.impl.ServiceLoaderModuleService;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category(ClientServerTest.class)
public class ServerBuilderTest {

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(STRICT_STUBS);

  private InternalCache cache;
  private SecurityService securityService;
  private StatisticsClock statisticsClock;

  @Before
  public void setUp() {
    cache = mock(InternalCache.class);
    securityService = mock(SecurityService.class);
    statisticsClock = mock(StatisticsClock.class);
  }

  @Test
  public void sendResourceEventsIsTrueByDefault() {
    ServerBuilder builder =
        new ServerBuilder(cache, securityService, statisticsClock,
            new ServiceLoaderModuleService(LogService.getLogger()));

    assertThat(builder.isSendResourceEvents()).isTrue();
  }

  @Test
  public void includeMemberGroupsIsTrueByDefault() {
    ServerBuilder builder =
        new ServerBuilder(cache, securityService, statisticsClock,
            new ServiceLoaderModuleService(LogService.getLogger()));

    assertThat(builder.isIncludeMemberGroups()).isTrue();
  }

  @Test
  public void socketCreatorIsForServerByDefault() {
    ServerBuilder builder =
        new ServerBuilder(cache, securityService, statisticsClock,
            new ServiceLoaderModuleService(LogService.getLogger()));

    assertThat(builder.getSocketCreatorSupplier()).isSameAs(SERVER.getSupplier());
  }

  @Test
  public void forGatewayReceiverUnsetsSendResourceEvents() {
    GatewayReceiver gatewayReceiver = mock(GatewayReceiver.class);
    when(gatewayReceiver.getGatewayTransportFilters())
        .thenReturn(singletonList(mock(GatewayTransportFilter.class)));
    ServerBuilder builder =
        new ServerBuilder(cache, securityService, statisticsClock,
            new ServiceLoaderModuleService(LogService.getLogger()));

    builder.forGatewayReceiver(gatewayReceiver);

    assertThat(builder.isSendResourceEvents()).isFalse();
  }

  @Test
  public void forGatewayReceiverUnsetsIncludeMemberGroups() {
    GatewayReceiver gatewayReceiver = mock(GatewayReceiver.class);
    when(gatewayReceiver.getGatewayTransportFilters())
        .thenReturn(singletonList(mock(GatewayTransportFilter.class)));
    ServerBuilder builder =
        new ServerBuilder(cache, securityService, statisticsClock,
            new ServiceLoaderModuleService(LogService.getLogger()));

    builder.forGatewayReceiver(gatewayReceiver);

    assertThat(builder.isIncludeMemberGroups()).isFalse();
  }

  @Test
  public void forGatewayReceiverSetsSocketCreatorForGateway() {
    GatewayReceiver gatewayReceiver = mock(GatewayReceiver.class);
    when(gatewayReceiver.getGatewayTransportFilters())
        .thenReturn(singletonList(mock(GatewayTransportFilter.class)));
    ServerBuilder builder =
        new ServerBuilder(cache, securityService, statisticsClock,
            new ServiceLoaderModuleService(LogService.getLogger()));

    builder.forGatewayReceiver(gatewayReceiver);

    assertThat(builder.getSocketCreatorSupplier()).isSameAs(GATEWAY.getSupplier());
  }

  @Test
  public void setCacheClientNotifierProviderReplacesCacheClientNotifierProvider() {
    CacheClientNotifierProvider cacheClientNotifierProvider =
        mock(CacheClientNotifierProvider.class);
    ServerBuilder builder =
        new ServerBuilder(cache, securityService, statisticsClock,
            new ServiceLoaderModuleService(LogService.getLogger()));

    builder.setCacheClientNotifierProvider(cacheClientNotifierProvider);

    assertThat(builder.getCacheClientNotifierProvider()).isSameAs(cacheClientNotifierProvider);
  }

  @Test
  public void setClientHealthMonitorProviderReplacesClientHealthMonitorProvider() {
    ClientHealthMonitorProvider clientHealthMonitorProvider =
        mock(ClientHealthMonitorProvider.class);
    ServerBuilder builder =
        new ServerBuilder(cache, securityService, statisticsClock,
            new ServiceLoaderModuleService(LogService.getLogger()));

    builder.setClientHealthMonitorProvider(clientHealthMonitorProvider);

    assertThat(builder.getClientHealthMonitorProvider()).isSameAs(clientHealthMonitorProvider);
  }

  @Test
  public void setCacheServerAdvisorProviderReplacesCacheServerAdvisorProvider() {
    Function<DistributionAdvisee, CacheServerAdvisor> cacheServerAdvisorProvider =
        a -> mock(CacheServerAdvisor.class);
    ServerBuilder builder =
        new ServerBuilder(cache, securityService, statisticsClock,
            new ServiceLoaderModuleService(LogService.getLogger()));

    builder.setCacheServerAdvisorProvider(cacheServerAdvisorProvider);

    assertThat(builder.getCacheServerAdvisorProvider()).isSameAs(cacheServerAdvisorProvider);
  }
}
