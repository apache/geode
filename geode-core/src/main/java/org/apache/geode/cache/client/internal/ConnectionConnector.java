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

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.tier.ClientSideHandshake;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.cache.tier.sockets.CacheClientUpdater;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.net.SocketCreator;

public class ConnectionConnector {
  private static final Logger logger = LogService.getLogger();

  private final ClientSideHandshakeImpl handshake;
  private final int socketBufferSize;
  private final int handshakeTimeout;
  private final boolean usedByGateway;
  private final CancelCriterion cancelCriterion;
  private final SocketCreator socketCreator;
  private int readTimeout;
  private InternalDistributedSystem ds;
  private EndpointManager endpointManager;
  private GatewaySender gatewaySender;

  public ConnectionConnector(EndpointManager endpointManager, InternalDistributedSystem sys,
      int socketBufferSize, int handshakeTimeout, int readTimeout, CancelCriterion cancelCriterion,
      boolean usedByGateway, GatewaySender sender, SocketCreator socketCreator,
      ClientSideHandshakeImpl handshake) {

    this.handshake = handshake;
    this.handshake.setClientReadTimeout(readTimeout);
    this.endpointManager = endpointManager;
    this.ds = sys;
    this.socketBufferSize = socketBufferSize;
    this.handshakeTimeout = handshakeTimeout;
    this.readTimeout = readTimeout;
    this.usedByGateway = usedByGateway;
    this.gatewaySender = sender;
    this.cancelCriterion = cancelCriterion;
    this.socketCreator = socketCreator;
    if (this.socketCreator != null && (this.usedByGateway || (this.gatewaySender != null))) {
      if (sender != null && !sender.getGatewayTransportFilters().isEmpty()) {
        this.socketCreator.initializeTransportFilterClientSocketFactory(sender);
      }
    }
  }

  public ConnectionImpl connectClientToServer(ServerLocation location, boolean forQueue)
      throws IOException {
    ConnectionImpl connection = null;
    boolean initialized = false;
    try {
      connection = getConnection(this.ds, this.cancelCriterion);
      ClientSideHandshake connHandShake = getClientSideHandshake(handshake);
      connection.connect(endpointManager, location, connHandShake, socketBufferSize,
          handshakeTimeout, readTimeout, getCommMode(forQueue), this.gatewaySender,
          this.socketCreator);
      connection.setHandshake(connHandShake);
      initialized = true;
      return connection;
    } finally {
      if (!initialized && connection != null) {
        if (logger.isDebugEnabled()) {
          logger.debug("Destroy failed connection to {}", location);
        }
        destroyConnection(connection);
      }
    }
  }

  void destroyConnection(ConnectionImpl connection) {
    connection.destroy();
  }

  ConnectionImpl getConnection(InternalDistributedSystem ds, CancelCriterion cancelCriterion) {
    return new ConnectionImpl(ds, cancelCriterion);
  }

  ClientSideHandshake getClientSideHandshake(ClientSideHandshakeImpl handshake) {
    return new ClientSideHandshakeImpl(handshake);
  }

  public CacheClientUpdater connectServerToClient(Endpoint endpoint, QueueManager qManager,
      boolean isPrimary, ClientUpdater failedUpdater, String clientUpdateName) {
    CacheClientUpdater updater = new CacheClientUpdater(clientUpdateName, endpoint.getLocation(),
        isPrimary, ds, new ClientSideHandshakeImpl(this.handshake), qManager, endpointManager,
        endpoint, handshakeTimeout, this.socketCreator);

    if (!updater.isConnected()) {
      return null;
    }

    updater.setFailedUpdater(failedUpdater);
    updater.start();
    return updater;
  }

  private CommunicationMode getCommMode(boolean forQueue) {
    if (this.usedByGateway || (this.gatewaySender != null)) {
      return CommunicationMode.GatewayToGateway;
    } else if (forQueue) {
      return CommunicationMode.ClientToServerForQueue;
    } else {
      return CommunicationMode.ClientToServer;
    }
  }
}
