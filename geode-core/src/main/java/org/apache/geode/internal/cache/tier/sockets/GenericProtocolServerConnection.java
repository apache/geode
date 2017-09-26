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
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.SecurityManager;
import org.apache.geode.security.server.Authenticator;

/**
 * Holds the socket and protocol handler for the new client protocol.
 */
public class GenericProtocolServerConnection extends ServerConnection {
  // The new protocol lives in a separate module and gets loaded when this class is instantiated.
  private final ClientProtocolMessageHandler messageHandler;
  private final SecurityManager securityManager;
  private final ClientProtocolHandshaker handshaker;
  private Authenticator authenticator;
  private boolean cleanedUp;
  private ClientProxyMembershipID clientProxyMembershipID;

  /**
   * Creates a new <code>GenericProtocolServerConnection</code> that processes messages received
   * from an edge client over a given <code>Socket</code>.
   */
  public GenericProtocolServerConnection(Socket socket, InternalCache c, CachedRegionHelper helper,
      CacheServerStats stats, int hsTimeout, int socketBufferSize, String communicationModeStr,
      byte communicationMode, Acceptor acceptor, ClientProtocolMessageHandler newClientProtocol,
      SecurityService securityService, ClientProtocolHandshaker handshaker) {
    super(socket, c, helper, stats, hsTimeout, socketBufferSize, communicationModeStr,
        communicationMode, acceptor, securityService);
    securityManager = securityService.getSecurityManager();
    this.messageHandler = newClientProtocol;
    this.messageHandler.getStatistics().clientConnected();
    this.handshaker = handshaker;

    setClientProxyMembershipId();

    doHandShake(CommunicationMode.ProtobufClientServerProtocol.getModeNumber(), 0);
  }

  @Override
  protected void doOneMessage() {
    try {
      Socket socket = this.getSocket();
      InputStream inputStream = socket.getInputStream();
      OutputStream outputStream = socket.getOutputStream();

      if (!handshaker.shaken()) { // not stirred
        authenticator = handshaker.handshake(inputStream, outputStream);
      } else if (!authenticator.isAuthenticated()) {
        authenticator.authenticate(inputStream, outputStream, securityManager);
      } else {
        messageHandler.receiveMessage(inputStream, outputStream, new MessageExecutionContext(
            this.getCache(), authenticator.getAuthorizer(), messageHandler.getStatistics()));
      }
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
        messageHandler.getStatistics().clientDisconnected();
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
