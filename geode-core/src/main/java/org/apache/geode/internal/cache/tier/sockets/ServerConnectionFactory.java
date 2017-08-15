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

import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.security.SecurityService;

import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Creates instances of ServerConnection based on the connection mode provided.
 */
public class ServerConnectionFactory {
  private ClientProtocolMessageHandler protobufProtocolHandler;
  private Map<String, Class<? extends StreamAuthenticator>> authenticators = null;

  public ServerConnectionFactory() {}

  private synchronized void initializeAuthenticatorsMap() {
    if (authenticators != null) {
      return;
    }
    authenticators = new HashMap<>();
    ServiceLoader<StreamAuthenticator> loader = ServiceLoader.load(StreamAuthenticator.class);
    for (StreamAuthenticator streamAuthenticator : loader) {
      authenticators.put(streamAuthenticator.implementationID(), streamAuthenticator.getClass());
    }
  }

  private synchronized ClientProtocolMessageHandler initializeMessageHandler() {
    if (protobufProtocolHandler != null) {
      return protobufProtocolHandler;
    }
    ServiceLoader<ClientProtocolMessageHandler> loader =
        ServiceLoader.load(ClientProtocolMessageHandler.class);
    Iterator<ClientProtocolMessageHandler> iterator = loader.iterator();

    if (!iterator.hasNext()) {
      throw new ServiceLoadingFailureException(
          "There is no ClientProtocolMessageHandler implementation found in JVM");
    }

    protobufProtocolHandler = iterator.next();
    return protobufProtocolHandler;
  }

  private StreamAuthenticator findStreamAuthenticator(String implementationID) {
    if (authenticators == null) {
      initializeAuthenticatorsMap();
    }
    Class<? extends StreamAuthenticator> streamAuthenticatorClass =
        authenticators.get(implementationID);
    if (streamAuthenticatorClass == null) {
      throw new ServiceLoadingFailureException(
          "Could not find implementation for StreamAuthenticator with implementation ID "
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

  private ClientProtocolMessageHandler getClientProtocolMessageHandler() {
    if (protobufProtocolHandler == null) {
      initializeMessageHandler();
    }
    return protobufProtocolHandler;
  }

  public ServerConnection makeServerConnection(Socket s, InternalCache c, CachedRegionHelper helper,
      CacheServerStats stats, int hsTimeout, int socketBufferSize, String communicationModeStr,
      byte communicationMode, Acceptor acceptor, SecurityService securityService)
      throws IOException {
    if (communicationMode == Acceptor.PROTOBUF_CLIENT_SERVER_PROTOCOL) {
      if (!Boolean.getBoolean("geode.feature-protobuf-protocol")) {
        throw new IOException("Acceptor received unknown communication mode: " + communicationMode);
      } else {
        String authenticationMode =
            System.getProperty("geode.protocol-authentication-mode", "NOOP");

        return new GenericProtocolServerConnection(s, c, helper, stats, hsTimeout, socketBufferSize,
            communicationModeStr, communicationMode, acceptor, getClientProtocolMessageHandler(),
            securityService, findStreamAuthenticator(authenticationMode));
      }
    } else {
      return new LegacyServerConnection(s, c, helper, stats, hsTimeout, socketBufferSize,
          communicationModeStr, communicationMode, acceptor, securityService);
    }
  }
}
