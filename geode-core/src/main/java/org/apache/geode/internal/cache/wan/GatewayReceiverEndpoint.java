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
import org.apache.geode.internal.cache.InternalCacheServer;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.OverflowAttributes;
import org.apache.geode.internal.cache.tier.sockets.AcceptorFactory;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier.CacheClientNotifierProvider;
import org.apache.geode.internal.cache.tier.sockets.ClientHealthMonitor;
import org.apache.geode.internal.cache.tier.sockets.ClientHealthMonitor.ClientHealthMonitorProvider;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.security.SecurityService;

public class GatewayReceiverEndpoint extends CacheServerImpl implements GatewayReceiverServer {

  private final GatewayReceiverAcceptorFactory acceptorFactory;
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
    this(cache, securityService, new GatewayReceiverAcceptorFactory(),
        new CacheServerResourceEventNotifier() {}, gatewayReceiver,
        gatewayReceiverMetrics, socketCreatorSupplier, cacheClientNotifierProvider,
        clientHealthMonitorProvider);
  }

  private GatewayReceiverEndpoint(final InternalCache cache, final SecurityService securityService,
      final GatewayReceiverAcceptorFactory acceptorFactory,
      final CacheServerResourceEventNotifier resourceEventNotifier,
      final GatewayReceiver gatewayReceiver, final GatewayReceiverMetrics gatewayReceiverMetrics,
      final Supplier<SocketCreator> socketCreatorSupplier,
      final CacheClientNotifierProvider cacheClientNotifierProvider,
      final ClientHealthMonitorProvider clientHealthMonitorProvider) {
    super(cache, securityService, acceptorFactory, resourceEventNotifier, false);
    this.acceptorFactory = acceptorFactory;
    this.gatewayReceiver = gatewayReceiver;
    this.gatewayReceiverMetrics = gatewayReceiverMetrics;
    this.socketCreatorSupplier = socketCreatorSupplier;
    this.cacheClientNotifierProvider = cacheClientNotifierProvider;
    this.clientHealthMonitorProvider = clientHealthMonitorProvider;

    this.acceptorFactory.setGatewayReceiverEndpoint(this);
  }

  private static class GatewayReceiverAcceptorFactory implements AcceptorFactory {

    private InternalCacheServer internalCacheServer;
    private GatewayReceiverEndpoint gatewayReceiverEndpoint;

    @Override
    public void accept(InternalCacheServer internalCacheServer) {
      this.internalCacheServer = internalCacheServer;
    }

    public void setGatewayReceiverEndpoint(GatewayReceiverEndpoint gatewayReceiverEndpoint) {
      this.gatewayReceiverEndpoint = gatewayReceiverEndpoint;
    }

    @Override
    public Acceptor create(OverflowAttributes overflowAttributes) throws IOException {
      return new AcceptorImpl(internalCacheServer.getPort(), internalCacheServer.getBindAddress(),
          internalCacheServer.getNotifyBySubscription(), internalCacheServer.getSocketBufferSize(),
          internalCacheServer.getMaximumTimeBetweenPings(), internalCacheServer.getCache(),
          internalCacheServer.getMaxConnections(), internalCacheServer.getMaxThreads(),
          internalCacheServer.getMaximumMessageCount(), internalCacheServer.getMessageTimeToLive(),
          internalCacheServer.connectionListener(), overflowAttributes,
          internalCacheServer.getTcpNoDelay(), internalCacheServer.serverConnectionFactory(),
          internalCacheServer.timeLimitMillis(), internalCacheServer.securityService(),
          gatewayReceiverEndpoint.gatewayReceiver, gatewayReceiverEndpoint.gatewayReceiverMetrics,
          gatewayReceiverEndpoint.gatewayReceiver.getGatewayTransportFilters(),
          gatewayReceiverEndpoint.socketCreatorSupplier,
          gatewayReceiverEndpoint.cacheClientNotifierProvider,
          gatewayReceiverEndpoint.clientHealthMonitorProvider);
    }
  }
}
