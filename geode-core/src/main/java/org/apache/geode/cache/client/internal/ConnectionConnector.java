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
package org.apache.geode.cache.client.internal;

import java.io.IOException;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.client.SocketFactory;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.tier.ClientSideHandshake;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.cache.tier.sockets.CacheClientUpdater;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class ConnectionConnector {
  private static final Logger logger = LogService.getLogger();

  private final ClientSideHandshakeImpl handshake;
  private final int socketBufferSize;
  private final int handshakeTimeout;
  private final boolean usedByGateway;
  private final SocketCreator socketCreator;
  private final int readTimeout;
  private final InternalDistributedSystem distributedSystem;
  private final EndpointManager endpointManager;
  private final GatewaySender gatewaySender;
  private final SocketFactory socketFactory;

  public ConnectionConnector(EndpointManager endpointManager,
      InternalDistributedSystem distributedSystem,
      int socketBufferSize, int handshakeTimeout, int readTimeout, boolean usedByGateway,
      GatewaySender gatewaySender, SocketCreator socketCreator, ClientSideHandshakeImpl handshake,
      SocketFactory socketFactory) {
    this.handshake = handshake;
    this.handshake.setClientReadTimeout(readTimeout);
    this.endpointManager = endpointManager;
    this.distributedSystem = distributedSystem;
    this.socketBufferSize = socketBufferSize;
    this.handshakeTimeout = handshakeTimeout;
    this.readTimeout = readTimeout;
    this.usedByGateway = usedByGateway;
    this.gatewaySender = gatewaySender;
    this.socketCreator = socketCreator;
    this.socketFactory = socketFactory;
    if (this.socketCreator != null && (this.usedByGateway || (gatewaySender != null))) {
      if (gatewaySender != null && !gatewaySender.getGatewayTransportFilters().isEmpty()) {
        this.socketCreator.initializeTransportFilterClientSocketFactory(gatewaySender);
      }
    }
  }

  public ConnectionImpl connectClientToServer(ServerLocation location, boolean forQueue)
      throws IOException {
    ConnectionImpl connection = null;
    boolean initialized = false;
    try {
      connection = getConnection(distributedSystem);
      ClientSideHandshake connHandShake = getClientSideHandshake(handshake);
      connection.connect(endpointManager, location, connHandShake, socketBufferSize,
          handshakeTimeout, readTimeout, getCommMode(forQueue), gatewaySender, socketCreator,
          socketFactory);
      connection.setHandshake(connHandShake);
      initialized = true;
      return connection;
    } finally {
      if (!initialized && connection != null) {
        if (logger.isDebugEnabled()) {
          logger.debug("Destroying failed connection to {}", location);
        }
        destroyConnection(connection);
      }
    }
  }

  void destroyConnection(ConnectionImpl connection) {
    connection.destroy();
  }

  ConnectionImpl getConnection(InternalDistributedSystem ds) {
    return new ConnectionImpl(ds);
  }

  ClientSideHandshake getClientSideHandshake(ClientSideHandshakeImpl handshake) {
    return new ClientSideHandshakeImpl(handshake);
  }

  CacheClientUpdater connectServerToClient(Endpoint endpoint, QueueManager qManager,
      boolean isPrimary, ClientUpdater failedUpdater, String clientUpdateName) {
    CacheClientUpdater updater = new CacheClientUpdater(clientUpdateName, endpoint.getLocation(),
        isPrimary, distributedSystem, new ClientSideHandshakeImpl(handshake), qManager,
        endpointManager,
        endpoint, handshakeTimeout, socketCreator, socketFactory);

    if (!updater.isConnected()) {
      return null;
    }

    updater.setFailedUpdater(failedUpdater);
    updater.start();
    return updater;
  }

  private CommunicationMode getCommMode(boolean forQueue) {
    if (usedByGateway || (gatewaySender != null)) {
      return CommunicationMode.GatewayToGateway;
    } else if (forQueue) {
      return CommunicationMode.ClientToServerForQueue;
    } else {
      return CommunicationMode.ClientToServer;
    }
  }
}
