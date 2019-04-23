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

import static org.apache.geode.internal.net.SocketCreatorFactory.getSocketCreatorForComponent;
import static org.apache.geode.internal.security.SecurableCommunicationChannel.GATEWAY;

import java.io.IOException;
import java.util.function.Supplier;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.CacheServerResourceEventNotifier;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.OverflowAttributes;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier.CacheClientNotifierProvider;
import org.apache.geode.internal.cache.tier.sockets.ClientHealthMonitor;
import org.apache.geode.internal.cache.tier.sockets.ClientHealthMonitor.ClientHealthMonitorProvider;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.security.SecurityService;

public class GatewayReceiverEndpoint extends CacheServerImpl implements GatewayReceiverServer {

  private final GatewayReceiver gatewayReceiver;
  private final GatewayReceiverMetrics gatewayReceiverMetrics;

  private final Supplier<SocketCreator> socketCreatorSupplier;
  private final CacheClientNotifierProvider cacheClientNotifierProvider;
  private final ClientHealthMonitorProvider clientHealthMonitorProvider;

  public GatewayReceiverEndpoint(final InternalCache cache, final SecurityService securityService,
      final GatewayReceiver gatewayReceiver, final GatewayReceiverMetrics gatewayReceiverMetrics) {
    this(cache, securityService, gatewayReceiver, gatewayReceiverMetrics,
        () -> getSocketCreatorForComponent(GATEWAY), CacheClientNotifier.singletonProvider(),
        ClientHealthMonitor.singletonProvider());
  }

  @VisibleForTesting
  public GatewayReceiverEndpoint(final InternalCache cache, final SecurityService securityService,
      final GatewayReceiver gatewayReceiver, final GatewayReceiverMetrics gatewayReceiverMetrics,
      final Supplier<SocketCreator> socketCreatorSupplier,
      final CacheClientNotifierProvider cacheClientNotifierProvider,
      final ClientHealthMonitorProvider clientHealthMonitorProvider) {
    this(cache, securityService, new CacheServerResourceEventNotifier() {}, gatewayReceiver,
        gatewayReceiverMetrics, socketCreatorSupplier, cacheClientNotifierProvider,
        clientHealthMonitorProvider);
  }

  private GatewayReceiverEndpoint(final InternalCache cache, final SecurityService securityService,
      final CacheServerResourceEventNotifier resourceEventNotifier,
      final GatewayReceiver gatewayReceiver, final GatewayReceiverMetrics gatewayReceiverMetrics,
      final Supplier<SocketCreator> socketCreatorSupplier,
      final CacheClientNotifierProvider cacheClientNotifierProvider,
      final ClientHealthMonitorProvider clientHealthMonitorProvider) {
    super(cache, securityService, resourceEventNotifier, false);
    this.gatewayReceiver = gatewayReceiver;
    this.gatewayReceiverMetrics = gatewayReceiverMetrics;
    this.socketCreatorSupplier = socketCreatorSupplier;
    this.cacheClientNotifierProvider = cacheClientNotifierProvider;
    this.clientHealthMonitorProvider = clientHealthMonitorProvider;
  }

  @Override
  public Acceptor createAcceptor(OverflowAttributes overflowAttributes) throws IOException {
    return new AcceptorImpl(getPort(), getBindAddress(), getNotifyBySubscription(),
        getSocketBufferSize(), getMaximumTimeBetweenPings(), getCache(), getMaxConnections(),
        getMaxThreads(), getMaximumMessageCount(), getMessageTimeToLive(), connectionListener(),
        overflowAttributes, getTcpNoDelay(), serverConnectionFactory(), timeLimitMillis(),
        securityService(), gatewayReceiver, gatewayReceiverMetrics,
        gatewayReceiver.getGatewayTransportFilters(), socketCreatorSupplier,
        cacheClientNotifierProvider, clientHealthMonitorProvider);
  }
}
