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

import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.apache.geode.cache.IncompatibleVersionException;
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
class ProtobufServerConnection extends ServerConnection {

  // The new protocol lives in a separate module and gets loaded when this class is instantiated.
  private final ClientProtocolProcessor protocolProcessor;
  private boolean cleanedUp;
  private ClientProxyMembershipID clientProxyMembershipID;
  private final BufferedOutputStream output;

  /**
   * Creates a new {@code ProtobufServerConnection} that processes messages received from an
   * edge client over a given {@code Socket}.
   */
  ProtobufServerConnection(final Socket socket, final InternalCache internalCache,
      final CachedRegionHelper cachedRegionHelper, final CacheServerStats stats,
      final int hsTimeout, final int socketBufferSize, final String communicationModeStr,
      final byte communicationMode, final Acceptor acceptor,
      final ClientProtocolProcessor clientProtocolProcessor, final SecurityService securityService)
      throws IOException {
    super(socket, internalCache, cachedRegionHelper, stats, hsTimeout, socketBufferSize,
        communicationModeStr, communicationMode, acceptor, securityService);
    protocolProcessor = clientProtocolProcessor;

    output = new BufferedOutputStream(socket.getOutputStream(), socketBufferSize);
    setClientProxyMembershipId();

    doHandShake(CommunicationMode.ProtobufClientServerProtocol.getModeNumber(), 0);
  }

  @Override
  protected void doOneMessage() {
    Socket socket = getSocket();
    try {
      InputStream inputStream = socket.getInputStream();

      InternalCache cache = getCache();
      cache.setReadSerializedForCurrentThread(true);
      try {
        try {
          protocolProcessor.processMessage(inputStream, output);
        } finally {
          output.flush();
        }
      } finally {
        cache.setReadSerializedForCurrentThread(false);
      }

      if (protocolProcessor.socketProcessingIsFinished()) {
        setFlagProcessMessagesAsFalse();
      }
    } catch (EOFException e) {
      setFlagProcessMessagesAsFalse();
      setClientDisconnectedException(e);
      logger.debug("Encountered EOF while processing message: {}", e);
    } catch (IOException | IncompatibleVersionException e) {
      if (!socket.isClosed()) {
        logger.warn(e);
      }
      setFlagProcessMessagesAsFalse();
      setClientDisconnectedException(e);
    } finally {
      acceptor.getClientHealthMonitor().receivedPing(clientProxyMembershipID);
    }
  }

  private void setClientProxyMembershipId() {
    ServerLocation serverLocation = new ServerLocation(
        ((InetSocketAddress) getSocket().getRemoteSocketAddress()).getHostString(),
        getSocketPort());
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
  protected boolean doHandShake(byte endpointType, int queueSize) {
    ClientHealthMonitor clientHealthMonitor = getAcceptor().getClientHealthMonitor();
    clientHealthMonitor.registerClient(clientProxyMembershipID);
    clientHealthMonitor.addConnection(clientProxyMembershipID, this);

    return true;
  }

  @Override
  protected int getClientReadTimeout() {
    return 0;
  }

  @Override
  public boolean isClientServerConnection() {
    return true;
  }
}
