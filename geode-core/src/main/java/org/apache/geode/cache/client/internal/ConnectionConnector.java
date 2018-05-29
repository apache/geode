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

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.tier.ClientSideHandshake;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.cache.tier.sockets.CacheClientUpdater;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;

public class ConnectionConnector {
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
      int socketBufferSize, int handshakeTimeout, int readTimeout, ClientProxyMembershipID proxyId,
      CancelCriterion cancelCriterion, boolean usedByGateway, GatewaySender sender,
      boolean multiuserSecureMode) {

    this.handshake =
        new ClientSideHandshakeImpl(proxyId, sys, sys.getSecurityService(), multiuserSecureMode);
    this.handshake.setClientReadTimeout(readTimeout);
    this.endpointManager = endpointManager;
    this.ds = sys;
    this.socketBufferSize = socketBufferSize;
    this.handshakeTimeout = handshakeTimeout;
    this.readTimeout = readTimeout;
    this.usedByGateway = usedByGateway;
    this.gatewaySender = sender;
    this.cancelCriterion = cancelCriterion;
    if (this.usedByGateway || (this.gatewaySender != null)) {
      this.socketCreator =
          SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.GATEWAY);
      if (sender != null && !sender.getGatewayTransportFilters().isEmpty()) {
        this.socketCreator.initializeTransportFilterClientSocketFactory(sender);
      }
    } else {
      // If configured use SSL properties for cache-server
      this.socketCreator =
          SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.SERVER);
    }
  }

  public ConnectionImpl connectClientToServer(ServerLocation location, boolean forQueue)
      throws IOException {
    ConnectionImpl connection = new ConnectionImpl(this.ds, this.cancelCriterion);
    ClientSideHandshake connHandShake = new ClientSideHandshakeImpl(handshake);
    connection.connect(endpointManager, location, connHandShake, socketBufferSize, handshakeTimeout,
        readTimeout, getCommMode(forQueue), this.gatewaySender, this.socketCreator);
    connection.setHandshake(connHandShake);
    return connection;
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
