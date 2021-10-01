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
package org.apache.geode.cache.client.internal;

import static org.apache.geode.cache.client.internal.AuthenticateUserOp.NOT_A_USER_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.security.AuthenticationExpiredException;
import org.apache.geode.security.AuthenticationFailedException;

public class AuthenticateUserOpTest {

  private AuthenticateUserOp.AuthenticateUserOpImpl impl;
  private ConnectionImpl connection;
  private InternalDistributedSystem system;
  private byte[] credentialBytes;
  private Message message;
  private ServerLocation server;

  @Before
  public void before() throws Exception {
    connection = mock(ConnectionImpl.class);
    system = mock(InternalDistributedSystem.class);
    credentialBytes = "test".getBytes(StandardCharsets.UTF_8);
    message = mock(Message.class);
    server = mock(ServerLocation.class);
    when(connection.getServer()).thenReturn(server);
    impl = spy(new AuthenticateUserOp.AuthenticateUserOpImpl());
  }

  @Test
  public void constructorWithNoArg() throws Exception {
    doReturn(system).when(impl).getConnectedSystem();
    Properties properties = new Properties();
    when(system.getSecurityProperties()).thenReturn(properties);
    doReturn(credentialBytes).when(impl).getCredentialBytes(any(), any());
    doReturn(NOT_A_USER_ID).when(impl).getUserId(connection);
    doReturn(message).when(impl).getMessage();
    impl.sendMessage(connection);

    // verify we are using system.getSecurityProperties to get the credential bytes
    ArgumentCaptor<Properties> captor = ArgumentCaptor.forClass(Properties.class);
    verify(impl).getCredentialBytes(eq(connection), captor.capture());
    assertThat(captor.getValue()).isEqualTo(properties);
  }

  @Test
  public void constructorWithProperties() throws Exception {
    Properties properties = new Properties();
    impl = spy(new AuthenticateUserOp.AuthenticateUserOpImpl(properties));
    doReturn(system).when(impl).getConnectedSystem();
    doReturn(credentialBytes).when(impl).getCredentialBytes(any(), any());
    doReturn(NOT_A_USER_ID).when(impl).getUserId(connection);
    doReturn(message).when(impl).getMessage();
    impl.sendMessage(connection);

    // verify we are using the properties in constructor to get the credential bytes
    ArgumentCaptor<Properties> captor = ArgumentCaptor.forClass(Properties.class);
    verify(impl).getCredentialBytes(eq(connection), captor.capture());
    assertThat(captor.getValue()).isEqualTo(properties);
  }

  @Test
  public void noAttempt_if_NotRequireCredentials() throws Exception {
    when(server.getRequiresCredentials()).thenReturn(false);
    impl.attempt(connection);
    verify(impl, never()).parentAttempt(connection);
  }

  @Test
  public void callPrentAttempt_IfRequireCredentials() throws Exception {
    when(server.getRequiresCredentials()).thenReturn(true);
    doReturn(null).when(impl).parentAttempt(connection);
    impl.attempt(connection);
    verify(impl).parentAttempt(connection);
  }

  @Test
  public void whenParentAttemptThrowAuthenticationExpiredException() throws Exception {
    when(server.getRequiresCredentials()).thenReturn(true);
    doThrow(new AuthenticationExpiredException("expired")).when(impl).parentAttempt(connection);
    assertThatThrownBy(() -> impl.attempt(connection))
        .isInstanceOf(AuthenticationFailedException.class)
        .hasCauseInstanceOf(AuthenticationExpiredException.class)
        .hasMessageContaining("expired");
    verify(impl, times(2)).parentAttempt(connection);
  }

  @Test
  public void whenParentAttemptThrowAuthenticationFailedException() throws Exception {
    when(server.getRequiresCredentials()).thenReturn(true);
    doThrow(new AuthenticationFailedException("failed")).when(impl).parentAttempt(connection);
    assertThatThrownBy(() -> impl.attempt(connection))
        .isInstanceOf(AuthenticationFailedException.class)
        .hasNoCause()
        .hasMessageContaining("failed");
    verify(impl).parentAttempt(connection);
  }

  @Test
  public void whenParentAttemptThrowAuthenticationExpiredException_ThenAuthenticationFailedException()
      throws Exception {
    when(server.getRequiresCredentials()).thenReturn(true);
    doThrow(new AuthenticationExpiredException("expired"),
        new AuthenticationFailedException("failed")).when(impl).parentAttempt(connection);
    assertThatThrownBy(() -> impl.attempt(connection))
        .isInstanceOf(AuthenticationFailedException.class)
        .hasNoCause()
        .hasMessageContaining("failed");
    verify(impl, times(2)).parentAttempt(connection);
  }
}
