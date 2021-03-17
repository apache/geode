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

import static java.net.InetSocketAddress.createUnresolved;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.STRICT_STUBS;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
import org.apache.geode.internal.cache.client.protocol.ClientProtocolProcessor;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category(ClientServerTest.class)
public class ProtobufServerConnectionTest {

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(STRICT_STUBS);

  private Acceptor acceptor;
  private InternalCacheForClientAccess cache;
  private CachedRegionHelper cachedRegionHelper;
  private ClientHealthMonitor clientHealthMonitor;
  private Socket socket;

  @Before
  public void setUp() throws IOException {
    acceptor = mock(Acceptor.class);
    cache = mock(InternalCacheForClientAccess.class);
    cachedRegionHelper = mock(CachedRegionHelper.class);
    clientHealthMonitor = mock(ClientHealthMonitor.class);
    socket = mock(Socket.class);
    InternalDistributedSystem internalDistributedSystem = mock(InternalDistributedSystem.class);
    DistributionManager distributionManager = mock(DistributionManager.class);
    ThreadsMonitoring threadsMonitoring = mock(ThreadsMonitoring.class);

    when(acceptor.getClientHealthMonitor()).thenReturn(clientHealthMonitor);
    when(socket.getInetAddress()).thenReturn(mock(InetAddress.class));
    when(socket.getOutputStream()).thenReturn(new ByteArrayOutputStream());
    when(socket.getRemoteSocketAddress()).thenReturn(createUnresolved("localhost", 9071));
    when(cachedRegionHelper.getCache()).thenReturn(cache);
    when(cache.getInternalDistributedSystem()).thenReturn(internalDistributedSystem);
    when(internalDistributedSystem.getDM()).thenReturn(distributionManager);
    when(distributionManager.getThreadMonitoring()).thenReturn(threadsMonitoring);
  }

  @Test
  public void doOneMessageUnsetsProcessMessagesFlag() throws Exception {
    ClientProtocolProcessor clientProtocolProcessor = mock(ClientProtocolProcessor.class);
    doThrow(new IOException("throw me")).when(clientProtocolProcessor).processMessage(any(), any());
    ServerConnection serverConnection = new ProtobufServerConnection(socket, cache,
        cachedRegionHelper, mock(CacheServerStats.class), 0, 1024, "",
        CommunicationMode.ProtobufClientServerProtocol.getModeNumber(), acceptor,
        clientProtocolProcessor, mock(SecurityService.class));

    assertThat(serverConnection.getProcessMessages()).isTrue();

    serverConnection.doOneMessage();

    assertThat(serverConnection.getProcessMessages()).isFalse();
  }

  @Test
  public void emergencyCloseClosesSocket() throws IOException {
    when(socket.getInetAddress()).thenReturn(InetAddress.getByName("localhost"));
    ProtobufServerConnection protobufServerConnection = new ProtobufServerConnection(socket, cache,
        cachedRegionHelper, mock(CacheServerStats.class), 0, 1024, "",
        CommunicationMode.ProtobufClientServerProtocol.getModeNumber(), acceptor,
        mock(ClientProtocolProcessor.class), mock(SecurityService.class));

    protobufServerConnection.emergencyClose();

    verify(socket).close();
  }

  @Test
  public void addsConnectionToClientHealthMonitorForClient() throws IOException {
    ServerConnection serverConnection = new ProtobufServerConnection(socket, cache,
        cachedRegionHelper, mock(CacheServerStats.class), 0, 1024, "",
        CommunicationMode.ProtobufClientServerProtocol.getModeNumber(), acceptor,
        mock(ClientProtocolProcessor.class), mock(SecurityService.class));

    ArgumentCaptor<ClientProxyMembershipID> clientProxyMembershipIdFromAddConnection =
        forClass(ClientProxyMembershipID.class);
    verify(clientHealthMonitor)
        .addConnection(clientProxyMembershipIdFromAddConnection.capture(), eq(serverConnection));
    assertThat(clientProxyMembershipIdFromAddConnection.getValue().toString())
        .isEqualTo("identity(localhost<ec>:0,connection=1");
  }

  @Test
  public void registersClientWithClientHealthMonitor() throws IOException {
    new ProtobufServerConnection(socket, cache, cachedRegionHelper, mock(CacheServerStats.class), 0,
        1024, "", CommunicationMode.ProtobufClientServerProtocol.getModeNumber(), acceptor,
        mock(ClientProtocolProcessor.class), mock(SecurityService.class));

    ArgumentCaptor<ClientProxyMembershipID> clientProxyMembershipIdFromRegisterClient =
        forClass(ClientProxyMembershipID.class);
    verify(clientHealthMonitor)
        .registerClient(clientProxyMembershipIdFromRegisterClient.capture());
    assertThat(clientProxyMembershipIdFromRegisterClient.getValue().toString())
        .isEqualTo("identity(localhost<ec>:0,connection=1");
  }

  @Test
  public void doOneMessageNotifiesClientHealthMonitorOfPing() throws IOException {
    ServerConnection serverConnection = new ProtobufServerConnection(socket, cache,
        cachedRegionHelper, mock(CacheServerStats.class), 0, 1024, "",
        CommunicationMode.ProtobufClientServerProtocol.getModeNumber(), acceptor,
        mock(ClientProtocolProcessor.class), mock(SecurityService.class));

    serverConnection.doOneMessage();

    ArgumentCaptor<ClientProxyMembershipID> clientProxyMembershipIdFromReceivedPing =
        forClass(ClientProxyMembershipID.class);
    verify(clientHealthMonitor).receivedPing(clientProxyMembershipIdFromReceivedPing.capture());
    assertThat(clientProxyMembershipIdFromReceivedPing.getValue().toString())
        .isEqualTo("identity(localhost<ec>:0,connection=1");
  }
}
