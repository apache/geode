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
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ServerConnectionFactoryTest {
  /**
   * Safeguard that we won't create the new client protocol object unless the feature flag is enabled.
   */
  @Test(expected = IOException.class)
  public void newClientProtocolThrows() throws Exception {
    serverConnectionMockedExceptForCommunicationMode(Acceptor.CLIENT_TO_SERVER_NEW_PROTOCOL);
  }

  @Test
  public void newClientProtocolSucceedsWithSystemPropertySet() throws Exception {
    System.setProperty("geode.feature-protobuf-protocol", "true");
    ServerConnection serverConnection = serverConnectionMockedExceptForCommunicationMode(Acceptor.CLIENT_TO_SERVER_NEW_PROTOCOL);
    assertTrue(serverConnection instanceof NewProtocolServerConnection);
    System.clearProperty("geode.feature-protobuf-protocol");
  }

  @Test
  public void makeServerConnection() throws Exception {
    byte[] communicationModes = new byte[]{
      Acceptor.CLIENT_TO_SERVER,
      Acceptor.PRIMARY_SERVER_TO_CLIENT,
      Acceptor.SECONDARY_SERVER_TO_CLIENT,
      Acceptor.GATEWAY_TO_GATEWAY,
      Acceptor.MONITOR_TO_SERVER,
      Acceptor.SUCCESSFUL_SERVER_TO_CLIENT,
      Acceptor.UNSUCCESSFUL_SERVER_TO_CLIENT,
      Acceptor.CLIENT_TO_SERVER_FOR_QUEUE,
    };

    for (byte communicationMode : communicationModes) {
      ServerConnection serverConnection = serverConnectionMockedExceptForCommunicationMode(communicationMode);
      assertTrue(serverConnection instanceof LegacyServerConnection);
    }
  }

  private static ServerConnection serverConnectionMockedExceptForCommunicationMode(byte communicationMode) throws IOException {
    Socket socketMock = mock(Socket.class);
    when(socketMock.getInetAddress()).thenReturn(InetAddress.getByName("localhost"));

    return ServerConnectionFactory.makeServerConnection(
      socketMock, mock(InternalCache.class), mock(CachedRegionHelper.class),
      mock(CacheServerStats.class), 0, 0, "",
      communicationMode, mock(AcceptorImpl.class), mock(SecurityService.class));
  }

}