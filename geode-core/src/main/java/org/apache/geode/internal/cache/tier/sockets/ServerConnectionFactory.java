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
import java.util.HashMap;
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
  private volatile ClientProtocolService clientProtocolService;
  private Map<String, Class<? extends Authenticator>> authenticators = null;

  public ServerConnectionFactory() {}

  private synchronized void initializeAuthenticatorsMap() {
    if (authenticators != null) {
      return;
    }
    HashMap<String, Class<? extends Authenticator>> tmp = new HashMap<>();

    ServiceLoader<Authenticator> loader = ServiceLoader.load(Authenticator.class);
    for (Authenticator streamAuthenticator : loader) {
      tmp.put(streamAuthenticator.implementationID(), streamAuthenticator.getClass());
    }

    authenticators = tmp;
  }

  private synchronized ClientProtocolService initializeClientProtocolService(
      StatisticsFactory statisticsFactory, String statisticsName) {
    if (clientProtocolService != null) {
      return clientProtocolService;
    }

    // use temp to make sure we publish properly.
    ClientProtocolService tmp = new ClientProtocolServiceLoader().loadService();
    tmp.initializeStatistics(statisticsName, statisticsFactory);

    clientProtocolService = tmp;
    return clientProtocolService;
  }

  private Authenticator findStreamAuthenticator(String implementationID) {
    if (authenticators == null) {
      initializeAuthenticatorsMap();
    }
    Class<? extends Authenticator> streamAuthenticatorClass = authenticators.get(implementationID);
    if (streamAuthenticatorClass == null) {
      throw new ServiceLoadingFailureException(
          "Could not find implementation for Authenticator with implementation ID "
              + implementationID);
    } else {
      try {
        return streamAuthenticatorClass.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new ServiceLoadingFailureException(
            "Unable to instantiate authenticator for ID " + implementationID, e);
      }
    }
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
        String authenticationMode =
            System.getProperty("geode.protocol-authentication-mode", "NOOP");

        ClientProtocolService service = getOrCreateClientProtocolService(
            cache.getDistributedSystem(), acceptor.getServerName());

        ClientProtocolProcessor cachePipeline = service.createProcessorForCache(cache,
            findStreamAuthenticator(authenticationMode), securityService);

        return new GenericProtocolServerConnection(socket, cache, helper, stats, hsTimeout,
            socketBufferSize, communicationModeStr, communicationMode, acceptor, cachePipeline,
            securityService);
      }
    } else {
      return new LegacyServerConnection(socket, cache, helper, stats, hsTimeout, socketBufferSize,
          communicationModeStr, communicationMode, acceptor, securityService);
    }
  }
}
