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

import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.operations.UnregisterInterestOperationContext;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.InterestType;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
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
public class UnregisterInterestTest {

  private static final String REGION_NAME = "region1";
  private static final String KEY = "key1";
  private static final Object CALLBACK_ARG = "arg";
  private static final byte[] BYTE_ARRAY = new byte[8];

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
  private Message replyMessage;
  @Mock
  private Message errorResponseMessage;
  @Mock
  private Part regionNamePart;
  @Mock
  private Part keyPart;
  @Mock
  private Part keepAlivePart;
  @Mock
  private Part isClosingPart;
  @Mock
  private Part interestTypePart;
  @Mock
  private Part valuePart;
  @Mock
  private AcceptorImpl acceptor;
  @Mock
  private UnregisterInterestOperationContext unregisterInterestOperationContext;
  @InjectMocks
  private UnregisterInterest unregisterInterest;

  @Before
  public void setUp() throws Exception {
    unregisterInterest = new UnregisterInterest();
    MockitoAnnotations.openMocks(this);

    when(authzRequest.unregisterInterestAuthorize(eq(REGION_NAME), eq(KEY),
        eq(InterestType.KEY)))
            .thenReturn(unregisterInterestOperationContext);

    when(cache.getRegion(any(String.class))).thenReturn(uncheckedCast(region));
    when(cache.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));

    when(interestTypePart.getInt()).thenReturn(0);

    when(isClosingPart.getObject()).thenReturn(BYTE_ARRAY);

    when(keyPart.getStringOrObject()).thenReturn(KEY);

    when(keepAlivePart.getObject()).thenReturn(BYTE_ARRAY);

    when(message.getNumberOfParts()).thenReturn(5);
    when(message.getPart(eq(0))).thenReturn(regionNamePart);
    when(message.getPart(eq(1))).thenReturn(interestTypePart);
    when(message.getPart(eq(2))).thenReturn(keyPart);
    when(message.getPart(eq(3))).thenReturn(isClosingPart);
    when(message.getPart(eq(4))).thenReturn(keepAlivePart);

    when(regionNamePart.getCachedString()).thenReturn(REGION_NAME);

    when(serverConnection.getCache()).thenReturn(cache);
    when(serverConnection.getCacheServerStats()).thenReturn(cacheServerStats);
    when(serverConnection.getAuthzRequest()).thenReturn(authzRequest);
    when(serverConnection.getReplyMessage()).thenReturn(replyMessage);
    when(serverConnection.getCachedRegionHelper()).thenReturn(mock(CachedRegionHelper.class));
    when(serverConnection.getErrorResponseMessage()).thenReturn(errorResponseMessage);
    when(serverConnection.getAcceptor()).thenReturn(acceptor);
    when(serverConnection.getClientVersion()).thenReturn(KnownVersion.CURRENT);

    when(valuePart.getObject()).thenReturn(CALLBACK_ARG);

    when(unregisterInterestOperationContext.getKey()).thenReturn(KEY);

    CacheClientNotifier ccn = mock(CacheClientNotifier.class);

    when(acceptor.getCacheClientNotifier()).thenReturn(ccn);
  }

  @Test
  public void noSecurityShouldSucceed() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(false);

    unregisterInterest.cmdExecute(message, serverConnection, securityService,
        0);
    verify(replyMessage).send(serverConnection);
  }

  @Test
  public void integratedSecurityShouldSucceedIfAuthorized() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(true);

    unregisterInterest.cmdExecute(message, serverConnection, securityService,
        0);

    verify(securityService).authorize(Resource.DATA, Operation.READ, REGION_NAME, KEY);
    verify(replyMessage).send(serverConnection);
  }

  @Test
  public void integratedSecurityShouldFailIfNotAuthorized() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(true);
    doThrow(new NotAuthorizedException("")).when(securityService).authorize(Resource.DATA,
        Operation.READ, REGION_NAME, KEY);

    unregisterInterest.cmdExecute(message, serverConnection, securityService,
        0);

    verify(securityService).authorize(Resource.DATA, Operation.READ, REGION_NAME, KEY);
    verify(errorResponseMessage).send(eq(serverConnection));
  }

  @Test
  public void oldSecurityShouldSucceedIfAuthorized() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(false);

    unregisterInterest.cmdExecute(message, serverConnection, securityService,
        0);

    verify(authzRequest).unregisterInterestAuthorize(eq(REGION_NAME), eq(KEY),
        any(InterestType.class));
    verify(replyMessage).send(serverConnection);
  }

  @Test
  public void oldSecurityShouldFailIfNotAuthorized() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(false);
    doThrow(new NotAuthorizedException("")).when(authzRequest).getAuthorize(eq(REGION_NAME),
        eq(KEY), any());

    unregisterInterest.cmdExecute(message, serverConnection, securityService,
        0);

    verify(authzRequest).unregisterInterestAuthorize(eq(REGION_NAME), eq(KEY),
        any(InterestType.class));
    verify(replyMessage).send(eq(serverConnection));
  }

}
