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

import static org.apache.geode.internal.lang.SystemPropertyHelper.RE_AUTHENTICATE_WAIT_TIME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.shiro.subject.Subject;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.ha.HARegionQueue;
import org.apache.geode.internal.cache.ha.HARegionQueueStats;
import org.apache.geode.internal.cache.tier.sockets.ClientUpdateMessageImpl.CqNameToOp;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.security.AuthenticationExpiredException;
import org.apache.geode.security.ResourcePermission;

public class MessageDispatcherTest {
  private MessageDispatcher dispatcher;
  private CacheClientProxy proxy;
  private ClientUpdateMessage message;
  private InternalCache cache;
  private SecurityService securityService;
  private Subject subject;
  private CacheClientNotifier notifier;
  private ClientProxyMembershipID proxyId;
  private HARegionQueue messageQueue;
  private HARegionQueueStats queueStats;
  private CancelCriterion cancelCriterion;
  private CacheClientProxyStats proxyStats;
  private EventID eventID;

  @Before
  public void before() throws Exception {
    proxy = mock(CacheClientProxy.class);
    message = mock(ClientUpdateMessageImpl.class);
    cache = mock(InternalCache.class);
    subject = mock(Subject.class);
    securityService = mock(SecurityService.class);
    notifier = mock(CacheClientNotifier.class);
    proxyId = mock(ClientProxyMembershipID.class);
    messageQueue = mock(HARegionQueue.class);
    queueStats = mock(HARegionQueueStats.class);
    cancelCriterion = mock(CancelCriterion.class);
    proxyStats = mock(CacheClientProxyStats.class);
    eventID = mock(EventID.class);
    when(messageQueue.getStatistics()).thenReturn(queueStats);
    when(cache.getSecurityService()).thenReturn(securityService);
    when(proxy.getCache()).thenReturn(cache);
    when(cache.getCancelCriterion()).thenReturn(cancelCriterion);
    when(proxy.getCacheClientNotifier()).thenReturn(notifier);
    when(proxy.getProxyID()).thenReturn(proxyId);
    when(proxy.getHARegionName()).thenReturn("haRegion");
    when(proxy.getStatistics()).thenReturn(proxyStats);
    dispatcher = spy(new MessageDispatcher(proxy, "dispatcher", messageQueue));
  }

  @Test
  public void onlyAuthorizeClientUpdateMessage() throws Exception {
    ClientMessage message = mock(ClientMessage.class);
    when(securityService.isIntegratedSecurity()).thenReturn(true);
    dispatcher.dispatchMessage(message);
    verify(securityService, never()).authorize(any(ResourcePermission.class), any(Subject.class));
  }

  @Test
  public void noAuthorizeIfNoIntegratedSecurity() throws Exception {
    when(securityService.isIntegratedSecurity()).thenReturn(false);
    dispatcher.dispatchMessage(message);
    verify(securityService, never()).authorize(any(ResourcePermission.class), any(Subject.class));
  }


  @Test
  public void useProxySingleUserToAuthorizeIfExists() throws Exception {
    when(securityService.isIntegratedSecurity()).thenReturn(true);
    when(proxy.getSubject()).thenReturn(subject);
    dispatcher.dispatchMessage(message);
    verify(securityService).authorize(any(ResourcePermission.class), eq(subject));
  }

  @Test
  public void useCqNameToFindSubjectToAuthorize() throws Exception {
    when(securityService.isIntegratedSecurity()).thenReturn(true);
    when(proxy.getSubject()).thenReturn(null);
    CqNameToOp clientCqs = mock(CqNameToOp.class);
    when(message.getClientCq(any())).thenReturn(clientCqs);
    String[] names = {"cq1"};
    when(clientCqs.getNames()).thenReturn(names);
    when(proxy.getSubject("cq1")).thenReturn(subject);
    dispatcher.dispatchMessage(message);
    verify(securityService).authorize(any(ResourcePermission.class), eq(subject));
  }

  @Test
  public void normalRunWillDispatchMessage() throws Exception {
    doReturn(false, false, true).when(dispatcher).isStopped();
    when(messageQueue.peek()).thenReturn(message);
    dispatcher.runDispatcher();
    verify(dispatcher).dispatchMessage(message);
  }

  @Test
  public void newClientWillGetClientReAuthenticateMessage() throws Exception {
    doReturn(false, false, false, false, false, true).when(dispatcher).isStopped();
    doThrow(AuthenticationExpiredException.class).when(dispatcher).dispatchMessage(any());
    when(messageQueue.peek()).thenReturn(message);
    when(proxy.getVersion()).thenReturn(KnownVersion.GEODE_1_15_0);
    doReturn(eventID).when(dispatcher).createEventId();
    doNothing().when(dispatcher).sendMessageDirectly(any());

    // make sure wait time is short
    doReturn(-1L).when(dispatcher).getSystemProperty(eq(RE_AUTHENTICATE_WAIT_TIME), anyLong());
    dispatcher.runDispatcher();

    // verify a ReAuthenticate message will be send to the user
    verify(dispatcher).sendMessageDirectly(any(ClientReAuthenticateMessage.class));

    // since we keep throwing the AuthenticationExpiredException, we will eventually call this
    verify(dispatcher).pauseOrUnregisterProxy(any(AuthenticationExpiredException.class));
    verify(dispatcher, never()).dispatchResidualMessages();
  }

  @Test
  public void oldClientWillNotGetClientReAuthenticateMessage() throws Exception {
    doReturn(false, false, false, false, false, true).when(dispatcher).isStopped();
    // make sure wait time is short
    doReturn(-1L).when(dispatcher).getSystemProperty(eq(RE_AUTHENTICATE_WAIT_TIME), anyLong());

    doThrow(AuthenticationExpiredException.class).when(dispatcher).dispatchMessage(any());
    when(messageQueue.peek()).thenReturn(message);
    when(proxy.getVersion()).thenReturn(KnownVersion.GEODE_1_14_0);
    dispatcher.runDispatcher();
    verify(dispatcher, never()).sendMessageDirectly(any());
    // we will eventually pauseOrUnregisterProxy
    verify(dispatcher).pauseOrUnregisterProxy(any(AuthenticationExpiredException.class));
    verify(dispatcher, never()).dispatchResidualMessages();
  }
}
