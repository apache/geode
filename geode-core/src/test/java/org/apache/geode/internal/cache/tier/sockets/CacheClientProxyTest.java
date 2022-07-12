/*
 *
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.shiro.subject.Subject;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import org.apache.geode.StatisticsFactory;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.OperationExecutors;
import org.apache.geode.internal.cache.Conflatable;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy.CacheClientProxyStatsFactory;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy.MessageDispatcherFactory;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class CacheClientProxyTest {
  private CacheClientProxy proxyWithSingleUser;
  private CacheClientProxy proxyWithMultiUser;
  private CacheClientNotifier notifier;
  private Socket socket;
  private ClientProxyMembershipID id;
  private KnownVersion version;
  private SecurityService securityService;
  private Subject subject;
  private StatisticsClock clock;
  private InternalCache cache;
  private StatisticsFactory statsFactory;
  private CacheClientProxyStatsFactory proxyStatsFactory;
  private MessageDispatcherFactory dispatcherFactory;
  private InetAddress inetAddress;
  private CacheServerStats stats;
  private ClientUserAuths clientUserAuths;

  @Before
  public void before() throws Exception {
    notifier = mock(CacheClientNotifier.class);
    stats = mock(CacheServerStats.class);
    socket = mock(Socket.class);
    inetAddress = mock(InetAddress.class);
    when(socket.getInetAddress()).thenReturn(inetAddress);
    when(notifier.getAcceptorStats()).thenReturn(stats);
    id = mock(ClientProxyMembershipID.class);
    when(id.getDurableId()).thenReturn("proxy_id");
    version = KnownVersion.TEST_VERSION;
    securityService = mock(SecurityService.class);
    subject = mock(Subject.class);
    clock = mock(StatisticsClock.class);
    cache = mock(InternalCache.class);
    statsFactory = mock(StatisticsFactory.class);
    proxyStatsFactory = mock(CacheClientProxyStatsFactory.class);
    dispatcherFactory = mock(MessageDispatcherFactory.class);
    clientUserAuths = mock(ClientUserAuths.class);
    when(proxyStatsFactory.create(any(), any(), any()))
        .thenReturn(mock(CacheClientProxyStats.class));

    proxyWithSingleUser =
        new CacheClientProxy(cache, notifier, socket, id, true, (byte) 1, version, 1L, true,
            securityService, subject, clock, statsFactory, proxyStatsFactory, dispatcherFactory,
            clientUserAuths);

    proxyWithMultiUser =
        new CacheClientProxy(cache, notifier, socket, id, true, (byte) 1, version, 1L, true,
            securityService, null, clock, statsFactory, proxyStatsFactory, dispatcherFactory,
            clientUserAuths);
  }

  @Test
  public void noExceptionWhenGettingSubjectForCQWhenSubjectIsNotNull() {
    proxyWithSingleUser.getSubject("cq");
  }

  @Test
  public void noExceptionWhenGettingSubjectForCQWhenSubjectIsNull() {
    proxyWithMultiUser.getSubject("cq");
  }

  @Test
  public void deliverMessageWhenSubjectIsNotNull() {
    proxyWithSingleUser =
        new CacheClientProxy(cache, notifier, socket, id, true, (byte) 1, version, 1L, true,
            securityService, subject, clock, statsFactory, proxyStatsFactory, dispatcherFactory,
            clientUserAuths);
    assertThat(proxyWithSingleUser.getSubject()).isNotNull();
    Conflatable message = mock(ClientUpdateMessage.class);
    when(securityService.needPostProcess()).thenReturn(true);
    proxyWithSingleUser.deliverMessage(message);
    verify(securityService).bindSubject(subject);
    verify(securityService).postProcess(any(), any(), any(), anyBoolean());
  }

  @Test
  public void deliverMessageWhenSubjectIsNull() {
    proxyWithMultiUser =
        new CacheClientProxy(cache, notifier, socket, id, true, (byte) 1, version, 1L, true,
            securityService, null, clock, statsFactory, proxyStatsFactory, dispatcherFactory,
            clientUserAuths);
    assertThat(proxyWithMultiUser.getSubject()).isNull();
    Conflatable message = mock(ClientUpdateMessage.class);
    when(securityService.needPostProcess()).thenReturn(true);
    when(proxyStatsFactory.create(any(), any(), any()))
        .thenReturn(mock(CacheClientProxyStats.class));
    proxyWithMultiUser.deliverMessage(message);
    verify(securityService, never()).bindSubject(subject);
    verify(securityService, never()).postProcess(any(), any(), any(), anyBoolean());
  }

  @Test
  public void replacingSubjectShouldNotLogout() {
    proxyWithSingleUser.setSubject(mock(Subject.class));
    verify(subject, never()).logout();
  }

  @Test
  public void close_keepProxy_ShouldNotLogoutUser() {
    when(id.isDurable()).thenReturn(true);
    boolean keepProxy = proxyWithSingleUser.close(true, false);
    assertThat(keepProxy).isTrue();
    verify(subject, never()).logout();
    verify(clientUserAuths, never()).cleanup(anyBoolean());

    keepProxy = proxyWithMultiUser.close(true, false);
    assertThat(keepProxy).isTrue();
    verify(subject, never()).logout();
    verify(clientUserAuths, never()).cleanup(anyBoolean());
  }

  @Test
  public void close_singleUser_logout_subject() {
    when(id.isDurable()).thenReturn(false);
    CacheClientProxy spy = spy(proxyWithSingleUser);
    doNothing().when(spy).closeTransientFields();
    boolean keepProxy = spy.close(true, false);
    assertThat(keepProxy).isFalse();
    verify(subject).logout();
    verify(clientUserAuths, never()).cleanup(anyBoolean());
  }

  @Test
  public void close_multiUser_calls_ClientUserAuthsCleanUp() {
    when(id.isDurable()).thenReturn(false);
    CacheClientProxy spy = spy(proxyWithMultiUser);
    doNothing().when(spy).closeTransientFields();
    boolean keepProxy = spy.close(true, false);
    assertThat(keepProxy).isFalse();
    verify(subject, never()).logout();
    verify(clientUserAuths, times(1)).cleanup(anyBoolean());
  }

  @Rule
  public ExecutorServiceRule executorService = new ExecutorServiceRule();

  @Test
  public void notifyReAuthenticationIsNotBlocked() {
    CacheClientProxy spy = spy(proxyWithSingleUser);
    MessageDispatcher dispatcher = mock(MessageDispatcher.class);
    doReturn(dispatcher).when(spy).createMessageDispatcher(any());
    spy.initializeMessageDispatcher();
    DistributionManager manager = mock(DistributionManager.class);
    OperationExecutors executors = mock(OperationExecutors.class);
    ExecutorService executor = executorService.getExecutorService();
    when(cache.getDistributionManager()).thenReturn(manager);
    when(manager.getExecutors()).thenReturn(executors);
    when(executors.getWaitingThreadPool()).thenReturn(executor);

    AtomicBoolean updated = new AtomicBoolean(false);

    // simulating a blocked message dispatcher when notify reauth
    doAnswer((Answer<Void>) invocation -> {
      while (!updated.get()) {
        Thread.sleep(200);
      }
      return null;
    }).when(dispatcher).notifyReAuthentication();

    // proxy.notifyReauthentication won't be blocked
    spy.notifyReAuthentication();
    assertThat(updated.get()).isFalse();
  }


}
