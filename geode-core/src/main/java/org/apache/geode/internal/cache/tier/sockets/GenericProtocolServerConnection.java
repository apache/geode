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

package org.apache.geode.internal.cache.tier.sockets;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.apache.geode.cache.IncompatibleVersionException;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.client.protocol.ClientProtocolProcessor;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.security.SecurityService;

/**
 * Holds the socket and protocol handler for the new client protocol.
 */
public class GenericProtocolServerConnection extends ServerConnection {
  // The new protocol lives in a separate module and gets loaded when this class is instantiated.
  private final ClientProtocolProcessor protocolProcessor;
  private boolean cleanedUp;
  private ClientProxyMembershipID clientProxyMembershipID;

  /**
   * Creates a new <code>GenericProtocolServerConnection</code> that processes messages received
   * from an edge client over a given <code>Socket</code>.
   */
  public GenericProtocolServerConnection(Socket socket, InternalCache c, CachedRegionHelper helper,
      CacheServerStats stats, int hsTimeout, int socketBufferSize, String communicationModeStr,
      byte communicationMode, Acceptor acceptor, ClientProtocolProcessor clientProtocolProcessor,
      SecurityService securityService) {
    super(socket, c, helper, stats, hsTimeout, socketBufferSize, communicationModeStr,
        communicationMode, acceptor, securityService);
    this.protocolProcessor = clientProtocolProcessor;

    setClientProxyMembershipId();

    doHandShake(CommunicationMode.ProtobufClientServerProtocol.getModeNumber(), 0);
  }

  @Override
  protected void doOneMessage() {
    try {
      Socket socket = this.getSocket();
      InputStream inputStream = socket.getInputStream();
      OutputStream outputStream = socket.getOutputStream();

      protocolProcessor.processMessage(inputStream, outputStream);
    } catch (EOFException e) {
      this.setFlagProcessMessagesAsFalse();
      setClientDisconnectedException(e);
      logger.debug("Encountered EOF while processing message: {}", e);
    } catch (IOException | IncompatibleVersionException e) {
      logger.warn(e);
      this.setFlagProcessMessagesAsFalse();
      setClientDisconnectedException(e);
    } finally {
      acceptor.getClientHealthMonitor().receivedPing(this.clientProxyMembershipID);
    }
  }

  private void setClientProxyMembershipId() {
    ServerLocation serverLocation = new ServerLocation(
        ((InetSocketAddress) this.getSocket().getRemoteSocketAddress()).getHostName(),
        this.getSocketPort());
    DistributedMember distributedMember = new InternalDistributedMember(serverLocation);
    // no handshake for new client protocol.
    clientProxyMembershipID = new ClientProxyMembershipID(distributedMember);
  }

  @Override
  public boolean cleanup() {
    synchronized (this) {
      if (!cleanedUp) {
        cleanedUp = true;
        protocolProcessor.close();
      }
    }
    return super.cleanup();
  }

  @Override
  protected boolean doHandShake(byte epType, int qSize) {
    ClientHealthMonitor clientHealthMonitor = getAcceptor().getClientHealthMonitor();
    clientHealthMonitor.registerClient(clientProxyMembershipID);
    clientHealthMonitor.addConnection(clientProxyMembershipID, this);

    return true;
  }

  @Override
  protected int getClientReadTimeout() {
    return PoolFactory.DEFAULT_READ_TIMEOUT;
  }

  @Override
  public boolean isClientServerConnection() {
    return true;
  }
}
