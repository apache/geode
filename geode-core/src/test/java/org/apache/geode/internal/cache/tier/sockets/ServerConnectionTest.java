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


import static org.apache.geode.internal.i18n.LocalizedStrings.HandShake_NO_SECURITY_CREDENTIALS_ARE_PROVIDED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.geode.cache.Cache;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.AuthenticationRequiredException;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.net.InetAddress;
import java.net.Socket;

@Category(UnitTest.class)
public class ServerConnectionTest {
  @Mock
  private Message requestMsg;

  @Mock
  private HandShake handshake;

  @Mock
  private MessageIdExtractor messageIdExtractor;

  @InjectMocks
  private ServerConnection serverConnection;

  @Before
  public void setUp() {
    AcceptorImpl acceptor = mock(AcceptorImpl.class);

    InetAddress inetAddress = mock(InetAddress.class);
    when(inetAddress.getHostAddress()).thenReturn("localhost");

    Socket socket = mock(Socket.class);
    when(socket.getInetAddress()).thenReturn(inetAddress);

    InternalCache cache = mock(InternalCache.class);
    SecurityService securityService = mock(SecurityService.class);

    serverConnection = new ServerConnection(socket, cache, null, null, 0, 0, null,
        Acceptor.PRIMARY_SERVER_TO_CLIENT, acceptor, securityService);
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
    serverConnection.setRequestMsg(requestMsg);

    assertThat(serverConnection.getRequestMessage()).isSameAs(requestMsg);
    when(requestMsg.isSecureMode()).thenReturn(true);

    when(messageIdExtractor.getUniqueIdFromMessage(any(Message.class), any(HandShake.class),
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
        .hasMessage(HandShake_NO_SECURITY_CREDENTIALS_ARE_PROVIDED.getRawText());
  }

}
