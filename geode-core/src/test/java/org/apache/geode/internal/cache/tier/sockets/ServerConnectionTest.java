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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.STRICT_STUBS;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

import org.apache.shiro.subject.Subject;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.cache.tier.Encryptor;
import org.apache.geode.internal.cache.tier.ServerSideHandshake;
import org.apache.geode.internal.cache.tier.sockets.command.PutUserCredentials;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.AuthenticationRequiredException;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category(ClientServerTest.class)
public class ServerConnectionTest {

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(STRICT_STUBS);

  private AcceptorImpl acceptor;
  private Message requestMessage;
  private ServerSideHandshake handshake;
  private SecurityService securityService;

  private ServerConnection serverConnection;
  private Command command;
  private InetAddress inetAddress;
  private Socket socket;
  private InternalCacheForClientAccess cache;
  private CachedRegionHelper cachedRegionHelper;
  private InternalDistributedSystem internalDistributedSystem;
  private DistributionManager distributionManager;
  private ThreadsMonitoring threadsMonitoring;
  private CacheClientNotifier notifier;

  @Before
  public void setUp() throws IOException {
    inetAddress = mock(InetAddress.class);
    socket = mock(Socket.class);
    acceptor = mock(AcceptorImpl.class);
    handshake = mock(ServerSideHandshake.class);
    requestMessage = mock(Message.class);
    securityService = mock(SecurityService.class);
    command = mock(Command.class);
    notifier = mock(CacheClientNotifier.class);

    when(inetAddress.getHostAddress()).thenReturn("localhost");
    when(socket.getInetAddress()).thenReturn(inetAddress);
    cache = mock(InternalCacheForClientAccess.class);
    cachedRegionHelper = mock(CachedRegionHelper.class);
    internalDistributedSystem = mock(InternalDistributedSystem.class);
    distributionManager = mock(DistributionManager.class);
    threadsMonitoring = mock(ThreadsMonitoring.class);

    when(cachedRegionHelper.getCache()).thenReturn(cache);
    when(cache.getInternalDistributedSystem()).thenReturn(internalDistributedSystem);
    when(internalDistributedSystem.getDM()).thenReturn(distributionManager);
    when(distributionManager.getThreadMonitoring()).thenReturn(threadsMonitoring);

    serverConnection =
        new ServerConnection(socket, cache,
            cachedRegionHelper, mock(CacheServerStats.class), 0, 0, null,
            CommunicationMode.PrimaryServerToClient.getModeNumber(), acceptor,
            securityService);

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

  @Test
  public void bindSubjectDoesNothingIfNotIntegratedService() {
    ServerConnection connection = spy(serverConnection);
    assertThat(securityService.isIntegratedSecurity()).isFalse();
    connection.bindSubject(command);
    verify(connection, never()).getUniqueId();
  }

  @Test
  public void bindSubjectDoesNothingIfWAN() {
    serverConnection =
        new ServerConnection(socket, cache,
            cachedRegionHelper, mock(CacheServerStats.class), 0, 0, null,
            CommunicationMode.GatewayToGateway.getModeNumber(), acceptor,
            securityService);
    ServerConnection connection = spy(serverConnection);
    when(securityService.isIntegratedSecurity()).thenReturn(true);
    connection.bindSubject(command);
    verify(connection, never()).getUniqueId();
  }

  @Test
  public void bindSubjectDoesNothingIfPutUserCredential() {
    ServerConnection connection = spy(serverConnection);
    when(securityService.isIntegratedSecurity()).thenReturn(true);
    command = mock(PutUserCredentials.class);
    connection.bindSubject(command);
    verify(connection, never()).getUniqueId();
  }

  @Test
  public void bindSubjectDoesNothingIfInternalMessage() {
    ServerConnection connection = spy(serverConnection);
    when(securityService.isIntegratedSecurity()).thenReturn(true);
    doReturn(true).when(connection).isInternalMessage(any(Message.class), anyBoolean());
    connection.bindSubject(command);
    verify(connection, never()).getUniqueId();
  }

  @Test
  public void bindSubject() {
    ServerConnection connection = spy(serverConnection);
    initializeClientUserAuth(connection);

    when(acceptor.getCacheClientNotifier()).thenReturn(notifier);
    when(securityService.isIntegratedSecurity()).thenReturn(true);
    doReturn(false).when(connection).isInternalMessage(any(Message.class), anyBoolean());
    doReturn(123L).when(connection).getUniqueId();

    // right now the clientUserAuth contains no such subject
    assertThatThrownBy(() -> connection.bindSubject(command))
        .isInstanceOf(AuthenticationRequiredException.class)
        .hasMessageContaining("Failed to find the authenticated user.");

    // verify bindSubject will be called if we have an existing subject
    Subject subject = mock(Subject.class);
    connection.putSubject(subject, 123L);
    connection.bindSubject(command);
    verify(securityService).bindSubject(subject);
  }

  // this initializes the clientUserAuths
  private void initializeClientUserAuth(ServerConnection connection) {
    doNothing().when(connection).initializeCommands();
    ClientProxyMembershipID proxyId = mock(ClientProxyMembershipID.class);
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    when(proxyId.getDistributedMember()).thenReturn(member);
    connection.setProxyId(proxyId);
    connection.doHandshake();
  }

  @Test
  public void putSubject() {
    ServerConnection connection = spy(serverConnection);
    initializeClientUserAuth(connection);

    when(acceptor.getCacheClientNotifier()).thenReturn(notifier);
    Subject subject = mock(Subject.class);
    CacheClientProxy proxy = mock(CacheClientProxy.class);
    when(notifier.getClientProxy(any(ClientProxyMembershipID.class))).thenReturn(proxy);

    // when proxy begins with null subject
    long userId = connection.putSubject(subject, 123L);
    // if proxy has no subject set, then setSubject won't be called
    assertThat(userId).isEqualTo(123L);
    verify(proxy, never()).setSubject(any());

    // if proxy has existing subject, but the proxy is not waitingForReAuth, subject won't be
    // updated
    when(proxy.getSubject()).thenReturn(subject);
    connection.putSubject(subject, 123L);
    verify(proxy, never()).setSubject(any());

    // if proxy has existing subject, and the proxy is waitingForReAuth, then subject is updated
    when(proxy.isWaitingForReAuthentication()).thenReturn(true);
    connection.putSubject(subject, 123L);
    verify(proxy).setSubject(any());
  }

  @Test
  public void messageNonSecureModeShouldThrowAuthenticationFailedException() {
    when(requestMessage.isSecureMode()).thenReturn(false);
    assertThatThrownBy(() -> serverConnection.setCredentials(requestMessage, -1))
        .isInstanceOf(AuthenticationFailedException.class);
  }

  @Test
  public void getUniqueIdBytesShouldThrowCacheClosedException() throws Exception {
    ServerConnection spy = spy(serverConnection);
    doThrow(new CacheClosedException()).when(spy).getUniqueId(requestMessage, -1);
    assertThatThrownBy(() -> spy.getUniqueIdBytes(requestMessage, -1))
        .isInstanceOf(CacheClosedException.class);
  }
}
