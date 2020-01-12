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
package org.apache.geode.internal.cache.tier.sockets.command;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.operations.GetOperationContext;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class Get70Test {

  private static final String REGION_NAME = "region1";
  private static final String KEY = "key1";
  private static final Object CALLBACK_ARG = "arg";

  @Mock
  private SecurityService securityService;
  @Mock
  private Message message;
  @Mock
  private ServerConnection serverConnection;
  @Mock
  private AuthorizeRequest authzRequest;
  @Mock
  private LocalRegion region;
  @Mock
  private InternalCache cache;
  @Mock
  private CacheServerStats cacheServerStats;
  @Mock
  private Message responseMessage;
  @Mock
  private Message errorResponseMessage;
  @Mock
  private Part regionNamePart;
  @Mock
  private Part keyPart;
  @Mock
  private Part valuePart;
  @Mock
  private GetOperationContext getOperationContext;

  private Get70 get70;

  @Before
  public void setUp() throws Exception {
    get70 = new Get70();
    MockitoAnnotations.initMocks(this);

    when(authzRequest.getAuthorize(any(), any(), any())).thenReturn(getOperationContext);

    when(cache.getRegion(isA(String.class))).thenReturn(region);
    when(cache.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));

    when(getOperationContext.getCallbackArg()).thenReturn(CALLBACK_ARG);

    when(keyPart.getStringOrObject()).thenReturn(KEY);

    when(message.getNumberOfParts()).thenReturn(3);
    when(message.getPart(eq(0))).thenReturn(regionNamePart);
    when(message.getPart(eq(1))).thenReturn(keyPart);
    when(message.getPart(eq(2))).thenReturn(valuePart);

    when(regionNamePart.getCachedString()).thenReturn(REGION_NAME);

    when(serverConnection.getCache()).thenReturn(cache);
    when(serverConnection.getCacheServerStats()).thenReturn(cacheServerStats);
    when(serverConnection.getAuthzRequest()).thenReturn(authzRequest);
    when(serverConnection.getResponseMessage()).thenReturn(responseMessage);
    when(serverConnection.getCachedRegionHelper()).thenReturn(mock(CachedRegionHelper.class));
    when(serverConnection.getErrorResponseMessage()).thenReturn(errorResponseMessage);
    when(serverConnection.getClientVersion()).thenReturn(Version.CURRENT);

    when(valuePart.getObject()).thenReturn(CALLBACK_ARG);

    when(securityService.isClientSecurityRequired()).thenReturn(false);
  }

  @Test
  public void noSecurityShouldSucceed() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(false);

    get70.cmdExecute(message, serverConnection, securityService, 0);
    verify(responseMessage).send(serverConnection);
  }

  @Test
  public void integratedSecurityShouldSucceedIfAuthorized() throws Exception {
    givenIntegratedSecurity();

    get70.cmdExecute(message, serverConnection, securityService, 0);

    verify(securityService).authorize(Resource.DATA, Operation.READ, REGION_NAME, KEY);
    verify(responseMessage).send(serverConnection);
  }

  @Test
  public void integratedSecurityShouldFailIfNotAuthorized() throws Exception {
    givenIntegratedSecurity();
    givenIntegratedSecurityNotAuthorized();

    get70.cmdExecute(message, serverConnection, securityService, 0);

    verify(securityService).authorize(Resource.DATA, Operation.READ, REGION_NAME, KEY);
    verify(errorResponseMessage).send(eq(serverConnection));
  }

  @Test
  public void oldSecurityShouldSucceedIfAuthorized() throws Exception {
    givenOldSecurity();

    get70.cmdExecute(message, serverConnection, securityService, 0);

    verify(authzRequest).getAuthorize(eq(REGION_NAME), eq(KEY), eq(CALLBACK_ARG));
    verify(responseMessage).send(serverConnection);
  }

  @Test
  public void oldSecurityShouldFailIfNotAuthorized() throws Exception {
    givenOldSecurity();
    givenOldSecurityNotAuthorized();

    get70.cmdExecute(message, serverConnection, securityService, 0);

    verify(authzRequest).getAuthorize(eq(REGION_NAME), eq(KEY), eq(CALLBACK_ARG));
    verify(errorResponseMessage).send(eq(serverConnection));
  }

  @Test
  public void callsEndGetForClient_ifGetSucceedsWithHit() throws IOException {
    CachePerfStats regionPerfStats = mock(CachePerfStats.class);
    when(region.getRegionPerfStats()).thenReturn(regionPerfStats);
    when(region.getRetained(any(), any(), anyBoolean(), anyBoolean(), any(), any(), anyBoolean()))
        .thenReturn("data");

    get70.cmdExecute(message, serverConnection, securityService, 0);

    verify(regionPerfStats).endGetForClient(0, false);
  }

  @Test
  public void callsEndGetForClient_ifGetSucceedsWithMiss() throws IOException {
    CachePerfStats regionPerfStats = mock(CachePerfStats.class);
    when(region.getRegionPerfStats()).thenReturn(regionPerfStats);
    when(region.getRetained(any(), any(), anyBoolean(), anyBoolean(), any(), any(), anyBoolean()))
        .thenReturn(null);

    get70.cmdExecute(message, serverConnection, securityService, 0);

    verify(regionPerfStats).endGetForClient(0, true);
  }

  @Test
  public void doesNotCallEndGetForClient_ifGetFails() throws IOException {
    givenIntegratedSecurity();
    givenIntegratedSecurityNotAuthorized();

    CachePerfStats regionPerfStats = mock(CachePerfStats.class);
    when(region.getRegionPerfStats()).thenReturn(regionPerfStats);

    get70.cmdExecute(message, serverConnection, securityService, 0);

    verify(regionPerfStats, never()).endGetForClient(anyLong(), anyBoolean());
  }

  @Test
  public void doesNotThrow_ifRegionPerfStatsIsNull() {
    when(region.getRegionPerfStats()).thenReturn(null);

    assertThatCode(() -> get70.cmdExecute(message, serverConnection, securityService, 0))
        .doesNotThrowAnyException();
  }

  private void givenIntegratedSecurity() {
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(true);
  }

  private void givenOldSecurity() {
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(false);
  }

  private void givenIntegratedSecurityNotAuthorized() {
    doThrow(new NotAuthorizedException("")).when(securityService).authorize(Resource.DATA,
        Operation.READ, REGION_NAME, KEY);
  }

  private void givenOldSecurityNotAuthorized() {
    doThrow(new NotAuthorizedException("")).when(authzRequest).getAuthorize(eq(REGION_NAME),
        eq(KEY), eq(CALLBACK_ARG));
  }
}
