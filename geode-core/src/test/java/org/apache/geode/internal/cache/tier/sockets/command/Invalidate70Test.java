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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.operations.InvalidateOperationContext;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class Invalidate70Test {

  private static final String REGION_NAME = "region1";
  private static final String KEY_STRING = "key1";
  private static final byte[] EVENT = new byte[8];
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
  private InternalCache cache;
  @Mock
  private Message errorResponseMessage;
  @Mock
  private Part regionNamePart;
  @Mock
  private Part keyPart;
  @Mock
  private Part eventPart;
  @Mock
  private Part callbackArgPart;
  @Mock
  private Message responseMessage;

  @InjectMocks
  private Invalidate70 invalidate;

  @Before
  public void setUp() throws Exception {
    invalidate = (Invalidate70) Invalidate70.getCommand();

    MockitoAnnotations.initMocks(this);

    when(authzRequest.invalidateAuthorize(any(), any(), any()))
        .thenReturn(mock(InvalidateOperationContext.class));

    when(cache.getRegion(isA(String.class))).thenReturn(mock(LocalRegion.class));
    when(cache.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));

    when(callbackArgPart.getObject()).thenReturn(CALLBACK_ARG);

    when(eventPart.getSerializedForm()).thenReturn(EVENT);

    when(keyPart.getStringOrObject()).thenReturn(KEY_STRING);

    when(message.getNumberOfParts()).thenReturn(4);
    when(message.getPart(eq(0))).thenReturn(regionNamePart);
    when(message.getPart(eq(1))).thenReturn(keyPart);
    when(message.getPart(eq(2))).thenReturn(eventPart);
    when(message.getPart(eq(3))).thenReturn(callbackArgPart);

    when(regionNamePart.getCachedString()).thenReturn(REGION_NAME);

    when(serverConnection.getCache()).thenReturn(cache);
    when(serverConnection.getCacheServerStats()).thenReturn(mock(CacheServerStats.class));
    when(serverConnection.getAuthzRequest()).thenReturn(authzRequest);
    when(serverConnection.getCachedRegionHelper()).thenReturn(mock(CachedRegionHelper.class));
    when(serverConnection.getReplyMessage()).thenReturn(responseMessage);
    when(serverConnection.getErrorResponseMessage()).thenReturn(errorResponseMessage);
    when(serverConnection.getClientVersion()).thenReturn(KnownVersion.CURRENT);
  }

  @Test
  public void noSecurityShouldSucceed() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(false);

    invalidate.cmdExecute(message, serverConnection, securityService, 0);

    verify(responseMessage).send(serverConnection);
  }

  @Test
  public void integratedSecurityShouldSucceedIfAuthorized() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(true);

    invalidate.cmdExecute(message, serverConnection, securityService, 0);

    verify(securityService).authorize(Resource.DATA, Operation.WRITE, REGION_NAME, KEY_STRING);
    verify(responseMessage).send(serverConnection);
  }

  @Test
  public void integratedSecurityShouldFailIfNotAuthorized() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(true);
    doThrow(new NotAuthorizedException("")).when(securityService).authorize(Resource.DATA,
        Operation.WRITE, REGION_NAME, KEY_STRING);

    invalidate.cmdExecute(message, serverConnection, securityService, 0);

    verify(securityService).authorize(Resource.DATA, Operation.WRITE, REGION_NAME, KEY_STRING);
    verify(errorResponseMessage).send(serverConnection);
  }

  @Test
  public void oldSecurityShouldSucceedIfAuthorized() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(false);

    invalidate.cmdExecute(message, serverConnection, securityService, 0);

    verify(authzRequest).invalidateAuthorize(eq(REGION_NAME), eq(KEY_STRING),
        eq(CALLBACK_ARG));
    verify(responseMessage).send(serverConnection);
  }

  @Test
  public void oldSecurityShouldFailIfNotAuthorized() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(false);
    doThrow(new NotAuthorizedException("")).when(authzRequest)
        .invalidateAuthorize(eq(REGION_NAME), eq(KEY_STRING), eq(CALLBACK_ARG));

    invalidate.cmdExecute(message, serverConnection, securityService, 0);

    verify(authzRequest).invalidateAuthorize(eq(REGION_NAME), eq(KEY_STRING),
        eq(CALLBACK_ARG));

    ArgumentCaptor<NotAuthorizedException> argument =
        ArgumentCaptor.forClass(NotAuthorizedException.class);
    verify(errorResponseMessage).addObjPart(argument.capture());
    assertThat(argument.getValue()).isExactlyInstanceOf(NotAuthorizedException.class);
    verify(errorResponseMessage).send(serverConnection);
  }

}
