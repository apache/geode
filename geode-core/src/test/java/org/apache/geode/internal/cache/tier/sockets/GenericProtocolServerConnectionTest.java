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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.server.NoOpAuthenticator;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

@Category(UnitTest.class)
public class GenericProtocolServerConnectionTest {
  @Test
  public void testProcessFlag() throws IOException {
    try {
      System.setProperty("geode.feature-protobuf-protocol", "true");
      ServerConnection serverConnection = IOExceptionThrowingServerConnection();
      Assert.assertTrue(serverConnection.processMessages);
      serverConnection.doOneMessage();
      Assert.assertTrue(!serverConnection.processMessages);
    } finally {
      System.clearProperty("geode.feature-protobuf-protocol");
    }
  }

  private static ServerConnection IOExceptionThrowingServerConnection() throws IOException {
    Socket socketMock = mock(Socket.class);
    when(socketMock.getInetAddress()).thenReturn(InetAddress.getByName("localhost"));

    ClientProtocolMessageHandler clientProtocolMock = mock(ClientProtocolMessageHandler.class);
    doThrow(new IOException()).when(clientProtocolMock).receiveMessage(any(), any(), any());

    return new GenericProtocolServerConnection(socketMock, mock(InternalCache.class),
        mock(CachedRegionHelper.class), mock(CacheServerStats.class), 0, 0, "",
        CommunicationMode.ProtobufClientServerProtocol.getModeNumber(), mock(AcceptorImpl.class),
        clientProtocolMock, mock(SecurityService.class), new NoOpAuthenticator());
  }
}
