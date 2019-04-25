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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.OverflowAttributes;
import org.apache.geode.internal.cache.tier.sockets.AcceptorFactory;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier.CacheClientNotifierProvider;
import org.apache.geode.internal.cache.tier.sockets.ClientHealthMonitor.ClientHealthMonitorProvider;
import org.apache.geode.internal.cache.tier.sockets.ConnectionListener;
import org.apache.geode.internal.cache.tier.sockets.ServerConnectionFactory;
import org.apache.geode.internal.cache.wan.GatewayReceiverMetrics;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.security.SecurityService;

/**
 * Builds instances of {@link Acceptor}.
 */
public class AcceptorBuilder implements AcceptorFactory {

  private int port;
  private String bindAddress;
  private boolean notifyBySubscription;
  private int socketBufferSize;

  private InternalCache cache;
  private int maximumTimeBetweenPings;
  private int maxConnections;
  private int maxThreads;
  private int maximumMessageCount;
  private int messageTimeToLive;
  private ConnectionListener connectionListener;
  private boolean tcpNoDelay;
  private ServerConnectionFactory serverConnectionFactory;
  private long timeLimitMillis;
  private SecurityService securityService;

  private boolean isGatewayReceiver;
  private GatewayReceiverMetrics gatewayReceiverMetrics;
  private List<GatewayTransportFilter> gatewayTransportFilters = Collections.emptyList();

  private Supplier<SocketCreator> socketCreatorSupplier;
  private CacheClientNotifierProvider cacheClientNotifierProvider;
  private ClientHealthMonitorProvider clientHealthMonitorProvider;

  /**
   * Populates many builder fields for creating the {@link Acceptor} from the specified
   * {@link InternalCacheServer}.
   */
  public AcceptorBuilder forServer(InternalCacheServer server) {
    port = server.getPort();
    bindAddress = server.getBindAddress();
    notifyBySubscription = server.getNotifyBySubscription();
    socketBufferSize = server.getSocketBufferSize();
    maximumTimeBetweenPings = server.getMaximumTimeBetweenPings();
    cache = server.getCache();
    maxConnections = server.getMaxConnections();
    maxThreads = server.getMaxThreads();
    maximumMessageCount = server.getMaximumMessageCount();
    messageTimeToLive = server.getMessageTimeToLive();
    connectionListener = server.getConnectionListener();
    tcpNoDelay = server.getTcpNoDelay();
    serverConnectionFactory = server.getServerConnectionFactory();
    timeLimitMillis = server.getTimeLimitMillis();
    securityService = server.getSecurityService();

    socketCreatorSupplier = server.getSocketCreatorSupplier();
    cacheClientNotifierProvider = server.getCacheClientNotifierProvider();
    clientHealthMonitorProvider = server.getClientHealthMonitorProvider();
    return this;
  }

  /**
   * Sets {@code isGatewayReceiver}. Default is false.
   */
  public AcceptorBuilder setIsGatewayReceiver(boolean isGatewayReceiver) {
    this.isGatewayReceiver = isGatewayReceiver;
    return this;
  }

  /**
   * Sets {@code gatewayReceiverMetrics}. Default is null.
   */
  public AcceptorBuilder setGatewayReceiverMetrics(GatewayReceiverMetrics gatewayReceiverMetrics) {
    this.gatewayReceiverMetrics = gatewayReceiverMetrics;
    return this;
  }

  /**
   * Sets {@code gatewayTransportFilters}. Default is {@code empty}.
   */
  public AcceptorBuilder setGatewayTransportFilters(
      List<GatewayTransportFilter> gatewayTransportFilters) {
    this.gatewayTransportFilters = gatewayTransportFilters;
    return this;
  }

  /**
   * Sets {@code cache}. Must be invoked after or instead of
   * {@link #forServer(InternalCacheServer)}.
   */
  @VisibleForTesting
  public AcceptorBuilder setCache(InternalCache cache) {
    this.cache = cache;
    return this;
  }

  /**
   * Sets {@code connectionListener}. Must be invoked after or instead of
   * {@link #forServer(InternalCacheServer)}.
   */
  @VisibleForTesting
  public AcceptorBuilder setConnectionListener(ConnectionListener connectionListener) {
    this.connectionListener = connectionListener;
    return this;
  }

  /**
   * Sets {@code securityService}. Must be invoked after or instead of
   * {@link #forServer(InternalCacheServer)}.
   */
  @VisibleForTesting
  public AcceptorBuilder setSecurityService(SecurityService securityService) {
    this.securityService = securityService;
    return this;
  }

  /**
   * Sets {@code socketCreatorSupplier}. Must be invoked after or instead of
   * {@link #forServer(InternalCacheServer)}.
   */
  @VisibleForTesting
  public AcceptorBuilder setSocketCreatorSupplier(Supplier<SocketCreator> socketCreatorSupplier) {
    this.socketCreatorSupplier = socketCreatorSupplier;
    return this;
  }

  /**
   * Sets {@code cacheClientNotifierProvider}. Must be invoked after or instead of
   * {@link #forServer(InternalCacheServer)}.
   */
  @VisibleForTesting
  public AcceptorBuilder setCacheClientNotifierProvider(
      CacheClientNotifierProvider cacheClientNotifierProvider) {
    this.cacheClientNotifierProvider = cacheClientNotifierProvider;
    return this;
  }

  /**
   * Sets {@code clientHealthMonitorProvider}. Must be invoked after or instead of
   * {@link #forServer(InternalCacheServer)}.
   */
  @VisibleForTesting
  public AcceptorBuilder setClientHealthMonitorProvider(
      ClientHealthMonitorProvider clientHealthMonitorProvider) {
    this.clientHealthMonitorProvider = clientHealthMonitorProvider;
    return this;
  }

  /**
   * Creates a new instance of {@link Acceptor}.
   */
  @Override
  public Acceptor create(OverflowAttributes overflowAttributes) throws IOException {
    return new AcceptorImpl(port, bindAddress, notifyBySubscription, socketBufferSize,
        maximumTimeBetweenPings, cache, maxConnections, maxThreads, maximumMessageCount,
        messageTimeToLive, connectionListener, overflowAttributes, tcpNoDelay,
        serverConnectionFactory, timeLimitMillis, securityService, socketCreatorSupplier,
        cacheClientNotifierProvider, clientHealthMonitorProvider, isGatewayReceiver,
        gatewayReceiverMetrics, gatewayTransportFilters);
  }

  @VisibleForTesting
  int getPort() {
    return port;
  }

  @VisibleForTesting
  String getBindAddress() {
    return bindAddress;
  }

  @VisibleForTesting
  boolean isNotifyBySubscription() {
    return notifyBySubscription;
  }

  @VisibleForTesting
  int getSocketBufferSize() {
    return socketBufferSize;
  }

  @VisibleForTesting
  InternalCache getCache() {
    return cache;
  }

  @VisibleForTesting
  int getMaximumTimeBetweenPings() {
    return maximumTimeBetweenPings;
  }

  @VisibleForTesting
  int getMaxConnections() {
    return maxConnections;
  }

  @VisibleForTesting
  int getMaxThreads() {
    return maxThreads;
  }

  @VisibleForTesting
  int getMaximumMessageCount() {
    return maximumMessageCount;
  }

  @VisibleForTesting
  int getMessageTimeToLive() {
    return messageTimeToLive;
  }

  @VisibleForTesting
  ConnectionListener getConnectionListener() {
    return connectionListener;
  }

  @VisibleForTesting
  boolean isTcpNoDelay() {
    return tcpNoDelay;
  }

  @VisibleForTesting
  ServerConnectionFactory getServerConnectionFactory() {
    return serverConnectionFactory;
  }

  @VisibleForTesting
  long getTimeLimitMillis() {
    return timeLimitMillis;
  }

  @VisibleForTesting
  SecurityService getSecurityService() {
    return securityService;
  }

  @VisibleForTesting
  boolean isGatewayReceiver() {
    return isGatewayReceiver;
  }

  @VisibleForTesting
  GatewayReceiverMetrics getGatewayReceiverMetrics() {
    return gatewayReceiverMetrics;
  }

  @VisibleForTesting
  List<GatewayTransportFilter> getGatewayTransportFilters() {
    return gatewayTransportFilters;
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
}
