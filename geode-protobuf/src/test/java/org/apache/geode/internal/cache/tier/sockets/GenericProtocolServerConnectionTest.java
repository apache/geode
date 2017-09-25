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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.protocol.protobuf.statistics.NoOpProtobufStatistics;
import org.apache.geode.security.server.NoOpAuthenticator;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class GenericProtocolServerConnectionTest {

  private ClientHealthMonitor clientHealthMonitorMock;

  @Test
  public void testProcessFlag() throws IOException {
    ServerConnection serverConnection = IOExceptionThrowingServerConnection();
    Assert.assertTrue(serverConnection.processMessages);
    serverConnection.doOneMessage();
    Assert.assertTrue(!serverConnection.processMessages);
  }

  @Test
  public void emergencyCloseClosesSocket() throws IOException {
    Socket socketMock = mock(Socket.class);
    when(socketMock.getInetAddress()).thenReturn(InetAddress.getByName("localhost"));

    AcceptorImpl acceptorStub = mock(AcceptorImpl.class);
    ClientProtocolMessageHandler mockHandler = mock(ClientProtocolMessageHandler.class);
    GenericProtocolServerConnection genericProtocolServerConnection =
        getServerConnection(socketMock, mockHandler, acceptorStub);

    genericProtocolServerConnection.emergencyClose();

    Mockito.verify(socketMock).close();
  }

  @Test
  public void testClientHealthMonitorRegistration() throws UnknownHostException {
    AcceptorImpl acceptorStub = mock(AcceptorImpl.class);

    ClientProtocolMessageHandler clientProtocolMock = mock(ClientProtocolMessageHandler.class);

    ServerConnection serverConnection = getServerConnection(clientProtocolMock, acceptorStub);

    ArgumentCaptor<ClientProxyMembershipID> registerCpmidArgumentCaptor =
        ArgumentCaptor.forClass(ClientProxyMembershipID.class);

    ArgumentCaptor<ClientProxyMembershipID> addConnectionCpmidArgumentCaptor =
        ArgumentCaptor.forClass(ClientProxyMembershipID.class);

    verify(clientHealthMonitorMock).addConnection(addConnectionCpmidArgumentCaptor.capture(),
        eq(serverConnection));
    verify(clientHealthMonitorMock).registerClient(registerCpmidArgumentCaptor.capture());
    assertEquals("identity(localhost<ec>:0,connection=1",
        registerCpmidArgumentCaptor.getValue().toString());
    assertEquals("identity(localhost<ec>:0,connection=1",
        addConnectionCpmidArgumentCaptor.getValue().toString());
  }

  @Test
  public void testDoOneMessageNotifiesClientHealthMonitor() throws UnknownHostException {
    AcceptorImpl acceptorStub = mock(AcceptorImpl.class);
    ClientProtocolMessageHandler clientProtocolMock = mock(ClientProtocolMessageHandler.class);

    ServerConnection serverConnection = getServerConnection(clientProtocolMock, acceptorStub);
    serverConnection.doOneMessage();

    ArgumentCaptor<ClientProxyMembershipID> clientProxyMembershipIDArgumentCaptor =
        ArgumentCaptor.forClass(ClientProxyMembershipID.class);
    verify(clientHealthMonitorMock).receivedPing(clientProxyMembershipIDArgumentCaptor.capture());
    assertEquals("identity(localhost<ec>:0,connection=1",
        clientProxyMembershipIDArgumentCaptor.getValue().toString());
  }

  private GenericProtocolServerConnection IOExceptionThrowingServerConnection() throws IOException {
    ClientProtocolMessageHandler clientProtocolMock = mock(ClientProtocolMessageHandler.class);
    ClientProtocolStatistics statisticsMock = mock(ClientProtocolStatistics.class);
    when(clientProtocolMock.getStatistics()).thenReturn(statisticsMock);
    doThrow(new IOException()).when(clientProtocolMock).receiveMessage(any(), any(), any());

    return getServerConnection(clientProtocolMock, mock(AcceptorImpl.class));
  }

  private GenericProtocolServerConnection getServerConnection(Socket socketMock,
      ClientProtocolMessageHandler clientProtocolMock, AcceptorImpl acceptorStub)
      throws UnknownHostException {
    clientHealthMonitorMock = mock(ClientHealthMonitor.class);
    when(acceptorStub.getClientHealthMonitor()).thenReturn(clientHealthMonitorMock);
    InetSocketAddress inetSocketAddressStub = InetSocketAddress.createUnresolved("localhost", 9071);
    InetAddress inetAddressStub = mock(InetAddress.class);
    when(socketMock.getInetAddress()).thenReturn(InetAddress.getByName("localhost"));
    when(socketMock.getRemoteSocketAddress()).thenReturn(inetSocketAddressStub);
    when(socketMock.getInetAddress()).thenReturn(inetAddressStub);

    when(clientProtocolMock.getStatistics()).thenReturn(new NoOpProtobufStatistics());

    return new GenericProtocolServerConnection(socketMock, mock(InternalCache.class),
        mock(CachedRegionHelper.class), mock(CacheServerStats.class), 0, 0, "",
        CommunicationMode.ProtobufClientServerProtocol.getModeNumber(), acceptorStub,
        clientProtocolMock, mock(SecurityService.class), new NoOpAuthenticator());
  }

  private GenericProtocolServerConnection getServerConnection(
      ClientProtocolMessageHandler clientProtocolMock, AcceptorImpl acceptorStub)
      throws UnknownHostException {
    Socket socketMock = mock(Socket.class);
    return getServerConnection(socketMock, clientProtocolMock, acceptorStub);
  }
}
