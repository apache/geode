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
 *
 */
package org.apache.geode.internal.cache.tier.sockets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.STRICT_STUBS;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.cache.tier.Encryptor;
import org.apache.geode.internal.cache.tier.ServerSideHandshake;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.AuthenticationRequiredException;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category(ClientServerTest.class)
public class ServerConnectionTest {

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(STRICT_STUBS);

  private AcceptorImpl acceptor;
  private Message requestMessage;
  private ServerSideHandshake handshake;

  private ServerConnection serverConnection;

  @Before
  public void setUp() throws IOException {
    InetAddress inetAddress = mock(InetAddress.class);
    Socket socket = mock(Socket.class);

    acceptor = mock(AcceptorImpl.class);
    handshake = mock(ServerSideHandshake.class);
    requestMessage = mock(Message.class);

    when(inetAddress.getHostAddress()).thenReturn("localhost");
    when(socket.getInetAddress()).thenReturn(inetAddress);
    InternalCacheForClientAccess cache = mock(InternalCacheForClientAccess.class);
    CachedRegionHelper cachedRegionHelper = mock(CachedRegionHelper.class);
    InternalDistributedSystem internalDistributedSystem = mock(InternalDistributedSystem.class);
    DistributionManager distributionManager = mock(DistributionManager.class);
    ThreadsMonitoring threadsMonitoring = mock(ThreadsMonitoring.class);

    when(cachedRegionHelper.getCache()).thenReturn(cache);
    when(cache.getInternalDistributedSystem()).thenReturn(internalDistributedSystem);
    when(internalDistributedSystem.getDM()).thenReturn(distributionManager);
    when(distributionManager.getThreadMonitoring()).thenReturn(threadsMonitoring);

    serverConnection =
        new ServerConnectionFactory().makeServerConnection(socket, cache,
            cachedRegionHelper, mock(CacheServerStats.class), 0, 0, null,
            CommunicationMode.PrimaryServerToClient.getModeNumber(), acceptor,
            mock(SecurityService.class));

    serverConnection.setHandshake(handshake);
  }

  @Test
  public void whenAuthenticationRequiredExceptionIsThrownItShouldBeCaught() {
    when(acceptor.getClientHealthMonitor()).thenReturn(mock(ClientHealthMonitor.class));
    when(acceptor.isSelector()).thenReturn(true);
    doThrow(new AuthenticationRequiredException("Test"))
        .when(serverConnection.stats).decThreadQueueSize();
    serverConnection.setProcessMessages(true);

    assertThatCode(() -> serverConnection.run()).doesNotThrowAnyException();
  }

  @Test
  public void shouldUseUniqueIdFromMessage() {
    long uniqueIdFromMessage = 23456L;
    MessageIdExtractor messageIdExtractor = mock(MessageIdExtractor.class);
    when(handshake.getEncryptor()).thenReturn(mock(Encryptor.class));
    when(messageIdExtractor.getUniqueIdFromMessage(any(Message.class), any(Encryptor.class),
        anyLong())).thenReturn(uniqueIdFromMessage);
    when(requestMessage.isSecureMode()).thenReturn(true);
    serverConnection.setMessageIdExtractor(messageIdExtractor);
    serverConnection.setRequestMessage(requestMessage);

    long value = serverConnection.getUniqueId();

    assertThat(value).isEqualTo(uniqueIdFromMessage);
  }

  @Test
  public void nonSecureShouldThrow() {
    assertThatThrownBy(() -> serverConnection.getUniqueId())
        .isExactlyInstanceOf(AuthenticationRequiredException.class)
        .hasMessage("No security credentials are provided");
  }
}
