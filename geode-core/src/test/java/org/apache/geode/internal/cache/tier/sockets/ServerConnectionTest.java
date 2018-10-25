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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.cache.tier.Encryptor;
import org.apache.geode.internal.cache.tier.ServerSideHandshake;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.AuthenticationRequiredException;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class ServerConnectionTest {

  @Mock
  private Message requestMsg;

  @Mock
  private MessageIdExtractor messageIdExtractor;

  @InjectMocks
  private ServerConnection serverConnection;

  private ServerSideHandshake handshake;

  @Before
  public void setUp() throws IOException {
    AcceptorImpl acceptor = mock(AcceptorImpl.class);

    InetAddress inetAddress = mock(InetAddress.class);
    when(inetAddress.getHostAddress()).thenReturn("localhost");

    Socket socket = mock(Socket.class);
    when(socket.getInetAddress()).thenReturn(inetAddress);

    InternalCache cache = mock(InternalCache.class);
    SecurityService securityService = mock(SecurityService.class);

    CacheServerStats stats = mock(CacheServerStats.class);

    handshake = mock(ServerSideHandshake.class);
    when(handshake.getEncryptor()).thenReturn(mock(Encryptor.class));

    serverConnection =
        new ServerConnectionFactory().makeServerConnection(socket, cache, null, stats, 0, 0, null,
            CommunicationMode.PrimaryServerToClient.getModeNumber(), acceptor, securityService);
    MockitoAnnotations.initMocks(this);
  }


  @Test
  public void pre65SecureShouldReturnUserAuthId() {
    long userAuthId = 12345L;
    serverConnection.setUserAuthId(userAuthId);

    when(handshake.getVersion()).thenReturn(Version.GFE_61);
    when(requestMsg.isSecureMode()).thenReturn(true);

    assertThat(serverConnection.getUniqueId()).isEqualTo(userAuthId);
  }

  @Test
  public void pre65NonSecureShouldReturnUserAuthId() {
    long userAuthId = 12345L;
    serverConnection.setUserAuthId(userAuthId);

    when(handshake.getVersion()).thenReturn(Version.GFE_61);
    when(requestMsg.isSecureMode()).thenReturn(false);

    assertThat(serverConnection.getUniqueId()).isEqualTo(userAuthId);
  }


  @Test
  public void post65SecureShouldUseUniqueIdFromMessage() {
    long uniqueIdFromMessage = 23456L;
    when(handshake.getVersion()).thenReturn(Version.GFE_82);
    serverConnection.setRequestMessage(requestMsg);

    assertThat(serverConnection.getRequestMessage()).isSameAs(requestMsg);
    when(requestMsg.isSecureMode()).thenReturn(true);

    when(messageIdExtractor.getUniqueIdFromMessage(any(Message.class), any(Encryptor.class),
        anyLong())).thenReturn(uniqueIdFromMessage);
    serverConnection.setMessageIdExtractor(messageIdExtractor);

    assertThat(serverConnection.getUniqueId()).isEqualTo(uniqueIdFromMessage);
  }

  @Test
  public void post65NonSecureShouldThrow() {
    when(handshake.getVersion()).thenReturn(Version.GFE_82);
    when(requestMsg.isSecureMode()).thenReturn(false);

    assertThatThrownBy(serverConnection::getUniqueId)
        .isExactlyInstanceOf(AuthenticationRequiredException.class)
        .hasMessage("No security credentials are provided");
  }
}
