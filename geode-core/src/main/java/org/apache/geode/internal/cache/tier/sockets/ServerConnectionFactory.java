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

import static org.apache.geode.internal.cache.tier.CommunicationMode.ProtobufClientServerProtocol;

import java.io.IOException;
import java.net.Socket;
import java.util.Collections;
import java.util.Map;
import java.util.ServiceLoader;

import org.apache.geode.StatisticsFactory;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.server.Authenticator;

/**
 * Creates instances of ServerConnection based on the connection mode provided.
 */
public class ServerConnectionFactory {
  private ClientProtocolMessageHandler protocolHandler;
  private Map<String, Class<? extends Authenticator>> authenticators = null;
  private ClientProtocolService clientProtocolService;

  public ServerConnectionFactory() {}

  private synchronized ClientProtocolService initializeClientProtocolService(
      StatisticsFactory statisticsFactory, String statisticsName) {
    if (clientProtocolService != null) {
      return clientProtocolService;
    }

    ClientProtocolService service = new ClientProtocolServiceLoader().loadService();
    ClientProtocolMessageHandler messageHandler = service.getMessageHandler();
    messageHandler.initializeStatistics(statisticsName, statisticsFactory);

    ensureAuthenticatorsAreInitialized();

    this.protocolHandler = messageHandler;
    this.clientProtocolService = service;
    return this.clientProtocolService;
  }

  private void ensureAuthenticatorsAreInitialized() {
    if (authenticators == null) {
      initializeAuthenticatorsMap();
    }
  }

  private synchronized void initializeAuthenticatorsMap() {
    if (authenticators != null) {
      return;
    }

    String authenticationMode = System.getProperty("geode.protocol-authentication-mode", "NOOP");

    ServiceLoader<Authenticator> loader = ServiceLoader.load(Authenticator.class);

    Authenticator found = null;

    for (Authenticator streamAuthenticator : loader) {
      if (authenticationMode.equals(streamAuthenticator.implementationID())) {
        if (found != null) {
          throw new ServiceLoadingFailureException(
              "Multiple Authenticators present for mode " + authenticationMode);
        }
        found = streamAuthenticator;
      }
    }

    if (found == null) {
      throw new ServiceLoadingFailureException(
          "Could not find implementation for Authenticator with implementation ID "
              + authenticationMode);
    }

    authenticators = Collections.singletonMap(authenticationMode, found.getClass());
  }

  private ClientProtocolService getOrCreateClientProtocolService(
      StatisticsFactory statisticsFactory, String serverName) {
    if (clientProtocolService == null) {
      return initializeClientProtocolService(statisticsFactory, serverName);
    }
    return clientProtocolService;
  }

  public ServerConnection makeServerConnection(Socket socket, InternalCache cache,
      CachedRegionHelper helper, CacheServerStats stats, int hsTimeout, int socketBufferSize,
      String communicationModeStr, byte communicationMode, Acceptor acceptor,
      SecurityService securityService) throws IOException {
    if (communicationMode == ProtobufClientServerProtocol.getModeNumber()) {
      if (!Boolean.getBoolean("geode.feature-protobuf-protocol")) {
        throw new IOException("Server received unknown communication mode: " + communicationMode);
      } else {


        getOrCreateClientProtocolService(cache.getDistributedSystem(), acceptor.getServerName());

        return new GenericProtocolServerConnection(socket, cache, helper, stats, hsTimeout,
            socketBufferSize, communicationModeStr, communicationMode, acceptor, protocolHandler,
            securityService, clientProtocolService.getHandshaker(authenticators));
      }
    } else {
      return new LegacyServerConnection(socket, cache, helper, stats, hsTimeout, socketBufferSize,
          communicationModeStr, communicationMode, acceptor, securityService);
    }
  }
}
