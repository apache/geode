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
import java.util.Iterator;
import java.util.ServiceLoader;
import javax.management.ServiceNotFoundException;

/**
 * Creates instances of ServerConnection based on the connection mode provided.
 */
public class ServerConnectionFactory {
  private static ClientProtocolMessageHandler protobufProtocolHandler;
  private static final Object protocolLoadLock = new Object();

  private static ClientProtocolMessageHandler findClientProtocolMessageHandler() {
    if (protobufProtocolHandler != null) {
      return protobufProtocolHandler;
    }

    synchronized (protocolLoadLock) {
      if (protobufProtocolHandler != null) {
        return protobufProtocolHandler;
      }

      ServiceLoader<ClientProtocolMessageHandler> loader =
          ServiceLoader.load(ClientProtocolMessageHandler.class);
      Iterator<ClientProtocolMessageHandler> iterator = loader.iterator();

      if (!iterator.hasNext()) {
        throw new ServiceLoadingFailureException(
            "ClientProtocolMessageHandler implementation not found in JVM");
      }

      ClientProtocolMessageHandler returnValue = iterator.next();

      if (iterator.hasNext()) {
        throw new ServiceLoadingFailureException(
            "Multiple service implementations found for ClientProtocolMessageHandler");
      }

      return returnValue;
    }
  }

  public static ServerConnection makeServerConnection(Socket s, InternalCache c,
      CachedRegionHelper helper, CacheServerStats stats, int hsTimeout, int socketBufferSize,
      String communicationModeStr, byte communicationMode, Acceptor acceptor,
      SecurityService securityService) throws IOException {
    if (communicationMode == Acceptor.PROTOBUF_CLIENT_SERVER_PROTOCOL) {
      if (!Boolean.getBoolean("geode.feature-protobuf-protocol")) {
        throw new IOException("Acceptor received unknown communication mode: " + communicationMode);
      } else {
        protobufProtocolHandler = findClientProtocolMessageHandler();
        return new GenericProtocolServerConnection(s, c, helper, stats, hsTimeout, socketBufferSize,
            communicationModeStr, communicationMode, acceptor, protobufProtocolHandler,
            securityService);
      }
    } else {
      return new LegacyServerConnection(s, c, helper, stats, hsTimeout, socketBufferSize,
          communicationModeStr, communicationMode, acceptor, securityService);
    }
  }
}
