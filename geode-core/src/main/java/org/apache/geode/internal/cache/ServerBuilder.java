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

import static org.apache.geode.internal.cache.ServerBuilder.SocketCreatorType.GATEWAY;
import static org.apache.geode.internal.cache.ServerBuilder.SocketCreatorType.SERVER;
import static org.apache.geode.internal.net.SocketCreatorFactory.getSocketCreatorForComponent;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.distributed.internal.DistributionAdvisee;
import org.apache.geode.internal.cache.tier.sockets.AcceptorBuilder;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier.CacheClientNotifierProvider;
import org.apache.geode.internal.cache.tier.sockets.ClientHealthMonitor;
import org.apache.geode.internal.cache.tier.sockets.ClientHealthMonitor.ClientHealthMonitorProvider;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.statistics.StatisticsClock;

/**
 * Builds instances of {@link InternalCacheServer}.
 */
class ServerBuilder implements ServerFactory {

  private final InternalCache cache;
  private final SecurityService securityService;
  private final StatisticsClock statisticsClock;

  private boolean sendResourceEvents = true;
  private boolean includeMemberGroups = true;
  private SocketCreatorType socketCreatorType = SERVER;
  private Supplier<SocketCreator> socketCreatorSupplier = socketCreatorType.getSupplier();

  private CacheClientNotifierProvider cacheClientNotifierProvider =
      CacheClientNotifier.singletonProvider();
  private ClientHealthMonitorProvider clientHealthMonitorProvider =
      ClientHealthMonitor.singletonProvider();
  private Function<DistributionAdvisee, CacheServerAdvisor> cacheServerAdvisorProvider =
      CacheServerAdvisor::createCacheServerAdvisor;

  private List<GatewayTransportFilter> gatewayTransportFilters = Collections.emptyList();

  ServerBuilder(InternalCache cache, SecurityService securityService,
      StatisticsClock statisticsClock) {
    this.cache = cache;
    this.securityService = securityService;
    this.statisticsClock = statisticsClock;
  }

  /**
   * Populates many builder fields for creating the {@link InternalCacheServer} for the specified
   * {@link GatewayReceiver}.
   */
  ServerBuilder forGatewayReceiver(GatewayReceiver gatewayReceiver) {
    sendResourceEvents = false;
    includeMemberGroups = false;
    socketCreatorType = GATEWAY;
    socketCreatorSupplier = GATEWAY.getSupplier();
    gatewayTransportFilters = gatewayReceiver.getGatewayTransportFilters();
    return this;
  }

  @VisibleForTesting
  ServerBuilder setSocketCreatorSupplier(Supplier<SocketCreator> socketCreatorSupplier) {
    this.socketCreatorSupplier = socketCreatorSupplier;
    return this;
  }

  @VisibleForTesting
  ServerBuilder setCacheClientNotifierProvider(
      CacheClientNotifierProvider cacheClientNotifierProvider) {
    this.cacheClientNotifierProvider = cacheClientNotifierProvider;
    return this;
  }

  @VisibleForTesting
  ServerBuilder setClientHealthMonitorProvider(
      ClientHealthMonitorProvider clientHealthMonitorProvider) {
    this.clientHealthMonitorProvider = clientHealthMonitorProvider;
    return this;
  }

  @VisibleForTesting
  ServerBuilder setCacheServerAdvisorProvider(
      Function<DistributionAdvisee, CacheServerAdvisor> cacheServerAdvisorProvider) {
    this.cacheServerAdvisorProvider = cacheServerAdvisorProvider;
    return this;
  }

  @Override
  public InternalCacheServer createServer() {
    AcceptorBuilder acceptorBuilder = new AcceptorBuilder();
    acceptorBuilder.setIsGatewayReceiver(socketCreatorType.isGateway());
    acceptorBuilder.setGatewayTransportFilters(gatewayTransportFilters);

    return new CacheServerImpl(cache, securityService, statisticsClock, acceptorBuilder,
        sendResourceEvents, includeMemberGroups, socketCreatorSupplier, cacheClientNotifierProvider,
        clientHealthMonitorProvider, cacheServerAdvisorProvider);
  }

  @VisibleForTesting
  boolean isSendResourceEvents() {
    return sendResourceEvents;
  }

  @VisibleForTesting
  boolean isIncludeMemberGroups() {
    return includeMemberGroups;
  }

  @VisibleForTesting
  Supplier<SocketCreator> getSocketCreatorSupplier() {
    return socketCreatorSupplier;
  }

  @VisibleForTesting
  CacheClientNotifierProvider getCacheClientNotifierProvider() {
    return cacheClientNotifierProvider;
  }

  @VisibleForTesting
  ClientHealthMonitorProvider getClientHealthMonitorProvider() {
    return clientHealthMonitorProvider;
  }

  @VisibleForTesting
  Function<DistributionAdvisee, CacheServerAdvisor> getCacheServerAdvisorProvider() {
    return cacheServerAdvisorProvider;
  }

  @VisibleForTesting
  List<GatewayTransportFilter> getGatewayTransportFilters() {
    return gatewayTransportFilters;
  }

  enum SocketCreatorType {
    SERVER(() -> getSocketCreatorForComponent(SecurableCommunicationChannel.SERVER)),
    GATEWAY(() -> getSocketCreatorForComponent(SecurableCommunicationChannel.GATEWAY));

    private final Supplier<SocketCreator> supplier;

    SocketCreatorType(Supplier<SocketCreator> supplier) {
      this.supplier = supplier;
    }

    Supplier<SocketCreator> getSupplier() {
      return supplier;
    }

    boolean isGateway() {
      return this == GATEWAY;
    }
  }
}
