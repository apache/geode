/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.cache.tier.sockets.command;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.operations.UnregisterInterestOperationContext;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.offheap.ReferenceCountHelper;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.test.junit.categories.UnitTest;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({ "*.UnitTest" })
@PrepareForTest({ CacheClientNotifier.class })
@Category(UnitTest.class)
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
  private Cache cache;
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
    this.unregisterInterest = new UnregisterInterest();
    MockitoAnnotations.initMocks(this);

    when(this.authzRequest.unregisterInterestAuthorize(eq(REGION_NAME), eq(KEY), eq(0))).thenReturn(this.unregisterInterestOperationContext);

    when(this.cache.getRegion(isA(String.class))).thenReturn(this.region);
    when(this.cache.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));

    when(this.interestTypePart.getInt()).thenReturn(0);

    when(this.isClosingPart.getObject()).thenReturn(BYTE_ARRAY);

    when(this.keyPart.getStringOrObject()).thenReturn(KEY);

    when(this.keepAlivePart.getObject()).thenReturn(BYTE_ARRAY);

    when(this.message.getNumberOfParts()).thenReturn(5);
    when(this.message.getPart(eq(0))).thenReturn(this.regionNamePart);
    when(this.message.getPart(eq(1))).thenReturn(this.interestTypePart);
    when(this.message.getPart(eq(2))).thenReturn(this.keyPart);
    when(this.message.getPart(eq(3))).thenReturn(this.isClosingPart);
    when(this.message.getPart(eq(4))).thenReturn(this.keepAlivePart);

    when(this.regionNamePart.getString()).thenReturn(REGION_NAME);

    when(this.serverConnection.getCache()).thenReturn(this.cache);
    when(this.serverConnection.getCacheServerStats()).thenReturn(this.cacheServerStats);
    when(this.serverConnection.getAuthzRequest()).thenReturn(this.authzRequest);
    when(this.serverConnection.getReplyMessage()).thenReturn(this.replyMessage);
    when(this.serverConnection.getCachedRegionHelper()).thenReturn(mock(CachedRegionHelper.class));
    when(this.serverConnection.getErrorResponseMessage()).thenReturn(this.errorResponseMessage);
    when(this.serverConnection.getAcceptor()).thenReturn(this.acceptor);
    when(this.serverConnection.getClientVersion()).thenReturn(Version.CURRENT);


    when(this.valuePart.getObject()).thenReturn(CALLBACK_ARG);

    when(this.unregisterInterestOperationContext.getKey()).thenReturn(KEY);

    CacheClientNotifier ccn = mock(CacheClientNotifier.class);
    PowerMockito.mockStatic(CacheClientNotifier.class, Mockito.CALLS_REAL_METHODS);
    PowerMockito.when(CacheClientNotifier.getInstance()).thenReturn(ccn);

    when(this.acceptor.getCacheClientNotifier()).thenReturn(ccn);
  }

  @Test
  public void noSecurityShouldSucceed() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(false);

    this.unregisterInterest.cmdExecute(this.message, this.serverConnection, 0);
    verify(this.replyMessage).send(this.serverConnection);
  }

  @Test
  public void integratedSecurityShouldSucceedIfAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(true);

    this.unregisterInterest.cmdExecute(this.message, this.serverConnection, 0);

    verify(this.securityService).authorizeRegionRead(eq(REGION_NAME), eq(KEY));
    verify(this.replyMessage).send(this.serverConnection);
  }

  @Test
  public void integratedSecurityShouldFailIfNotAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(true);
    doThrow(new NotAuthorizedException("")).when(this.securityService).authorizeRegionRead(eq(REGION_NAME), eq(KEY));

    this.unregisterInterest.cmdExecute(this.message, this.serverConnection, 0);

    verify(this.securityService).authorizeRegionRead(eq(REGION_NAME), eq(KEY));
    verify(this.errorResponseMessage).send(eq(this.serverConnection));
  }

  @Test
  public void oldSecurityShouldSucceedIfAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(false);

    this.unregisterInterest.cmdExecute(this.message, this.serverConnection, 0);

    verify(this.authzRequest).unregisterInterestAuthorize(eq(REGION_NAME), eq(KEY), anyInt());
    verify(this.replyMessage).send(this.serverConnection);
  }

  @Test
  public void oldSecurityShouldFailIfNotAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(false);
    doThrow(new NotAuthorizedException("")).when(this.authzRequest).getAuthorize(eq(REGION_NAME), eq(KEY), any());

    this.unregisterInterest.cmdExecute(this.message, this.serverConnection, 0);

    verify(this.authzRequest).unregisterInterestAuthorize(eq(REGION_NAME), eq(KEY), anyInt());
    verify(this.replyMessage).send(eq(this.serverConnection));
  }

}
