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
import static org.assertj.core.api.Assertions.catchThrowable;
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

import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.cache.tier.Encryptor;
import org.apache.geode.internal.cache.tier.ServerSideHandshake;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.security.AuthenticationRequiredException;
import org.apache.geode.services.module.internal.impl.ServiceLoaderModuleService;
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

    serverConnection =
        new ServerConnectionFactory(new ServiceLoaderModuleService(LogService.getLogger()))
            .makeServerConnection(socket,
                mock(InternalCache.class),
                mock(CachedRegionHelper.class), mock(CacheServerStats.class), 0, 0, null,
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
  public void pre65SecureShouldReturnUserAuthId() {
    long userAuthId = 12345L;
    when(handshake.getVersion()).thenReturn(Version.GFE_61);
    serverConnection.setUserAuthId(userAuthId);

    long value = serverConnection.getUniqueId();

    assertThat(value).isEqualTo(userAuthId);
  }

  @Test
  public void pre65NonSecureShouldReturnUserAuthId() {
    when(handshake.getVersion()).thenReturn(Version.GFE_61);
    long userAuthId = 12345L;
    serverConnection.setUserAuthId(userAuthId);

    long value = serverConnection.getUniqueId();

    assertThat(value).isEqualTo(userAuthId);
  }

  @Test
  public void post65SecureShouldUseUniqueIdFromMessage() {
    long uniqueIdFromMessage = 23456L;
    MessageIdExtractor messageIdExtractor = mock(MessageIdExtractor.class);
    when(handshake.getEncryptor()).thenReturn(mock(Encryptor.class));
    when(handshake.getVersion()).thenReturn(Version.GFE_82);
    when(messageIdExtractor.getUniqueIdFromMessage(any(Message.class), any(Encryptor.class),
        anyLong())).thenReturn(uniqueIdFromMessage);
    when(requestMessage.isSecureMode()).thenReturn(true);
    serverConnection.setMessageIdExtractor(messageIdExtractor);
    serverConnection.setRequestMessage(requestMessage);

    long value = serverConnection.getUniqueId();

    assertThat(value).isEqualTo(uniqueIdFromMessage);
  }

  @Test
  public void post65NonSecureShouldThrow() {
    when(handshake.getVersion()).thenReturn(Version.GFE_82);

    Throwable thrown = catchThrowable(() -> serverConnection.getUniqueId());

    assertThat(thrown)
        .isExactlyInstanceOf(AuthenticationRequiredException.class)
        .hasMessage("No security credentials are provided");
  }
}
