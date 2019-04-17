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

import java.io.IOException;
import java.util.List;

import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.internal.security.SecurityService;

public class GatewayReceiverEndpoint extends CacheServerImpl implements GatewayReceiverServer {

  private final GatewayReceiver gatewayReceiver;
  private final GatewayReceiverMetrics gatewayReceiverMetrics;


  public GatewayReceiverEndpoint(final InternalCache cache, final SecurityService securityService,
      final GatewayReceiver gatewayReceiver, final GatewayReceiverMetrics gatewayReceiverMetrics) {
    super(cache, securityService);
    this.gatewayReceiver = gatewayReceiver;
    this.gatewayReceiverMetrics = gatewayReceiverMetrics;
  }

  @Override
  public EndpointType getEndpointType() {
    return EndpointType.GATEWAY;
  }

  @Override
  public Acceptor createAcceptor(List overflowAttributesList) throws IOException {
    return new AcceptorImpl(getPort(), getBindAddress(), getNotifyBySubscription(),
        getSocketBufferSize(), getMaximumTimeBetweenPings(), getCache(), getMaxConnections(),
        getMaxThreads(), getMaximumMessageCount(), getMessageTimeToLive(), connectionListener(),
        overflowAttributesList, getTcpNoDelay(), serverConnectionFactory(), timeLimitMillis(),
        securityService(),
        gatewayReceiver, gatewayReceiverMetrics, gatewayReceiver.getGatewayTransportFilters());
  }
}
