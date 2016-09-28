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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.nio.ByteBuffer;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.operations.DestroyOperationContext;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class DestroyTest {

  private static final String REGION_NAME = "region1";
  private static final String KEY = "key1";
  private static final Object CALLBACK_ARG = "arg";
  private static final byte[] EVENT = new byte[8];

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
  private Message errorResponseMessage;
  @Mock
  private Message replyMessage;
  @Mock
  private Part regionNamePart;
  @Mock
  private Part keyPart;
  @Mock
  private Part eventPart;
  @Mock
  private Part callbackArgPart;
  @Mock
  private DestroyOperationContext destroyOperationContext;
  @InjectMocks
  private Destroy destroy;

  @Before
  public void setUp() throws Exception {
    this.destroy = new Destroy();
    MockitoAnnotations.initMocks(this);

    when(this.authzRequest.destroyAuthorize(eq(REGION_NAME), eq(KEY), eq(CALLBACK_ARG))).thenReturn(this.destroyOperationContext);

    when(this.cache.getRegion(isA(String.class))).thenReturn(this.region);
    when(this.cache.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));

    when(this.callbackArgPart.getObject()).thenReturn(CALLBACK_ARG);

    when(this.destroyOperationContext.getCallbackArg()).thenReturn(CALLBACK_ARG);

    when(this.eventPart.getSerializedForm()).thenReturn(EVENT);

    when(this.keyPart.getStringOrObject()).thenReturn(KEY);

    when(this.message.getNumberOfParts()).thenReturn(4);
    when(this.message.getPart(eq(0))).thenReturn(this.regionNamePart);
    when(this.message.getPart(eq(1))).thenReturn(this.keyPart);
    when(this.message.getPart(eq(2))).thenReturn(this.eventPart);
    when(this.message.getPart(eq(3))).thenReturn(this.callbackArgPart);

    when(this.region.containsKey(eq(REGION_NAME))).thenReturn(true);

    when(this.regionNamePart.getString()).thenReturn(REGION_NAME);

    when(this.serverConnection.getCache()).thenReturn(this.cache);
    when(this.serverConnection.getCacheServerStats()).thenReturn(this.cacheServerStats);
    when(this.serverConnection.getAuthzRequest()).thenReturn(this.authzRequest);
    when(this.serverConnection.getCachedRegionHelper()).thenReturn(mock(CachedRegionHelper.class));
    when(this.serverConnection.getErrorResponseMessage()).thenReturn(this.errorResponseMessage);
    when(this.serverConnection.getReplyMessage()).thenReturn(this.replyMessage);
    when(this.serverConnection.getClientVersion()).thenReturn(Version.CURRENT);
  }

  @Test
  public void noSecurityShouldSucceed() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(false);

    this.destroy.cmdExecute(this.message, this.serverConnection, 0);

    verify(this.replyMessage).send(this.serverConnection);
  }

  @Test
  public void integratedSecurityShouldSucceedIfAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(true);

    this.destroy.cmdExecute(this.message, this.serverConnection, 0);

    verify(this.securityService).authorizeRegionWrite(eq(REGION_NAME), eq(KEY));
    verify(this.replyMessage).send(this.serverConnection);
  }

  @Test
  public void integratedSecurityShouldFailIfNotAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(true);
    doThrow(new NotAuthorizedException("")).when(this.securityService).authorizeRegionWrite(eq(REGION_NAME), eq(KEY));

    this.destroy.cmdExecute(this.message, this.serverConnection, 0);


    verify(this.errorResponseMessage).send(eq(this.serverConnection));
  }

  @Test
  public void oldSecurityShouldSucceedIfAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(false);

    this.destroy.cmdExecute(this.message, this.serverConnection, 0);

    verify(this.authzRequest).destroyAuthorize(eq(REGION_NAME), eq(KEY), eq(CALLBACK_ARG));
    verify(this.replyMessage).send(this.serverConnection);
  }

  @Test
  public void oldSecurityShouldFailIfNotAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(false);
    doThrow(new NotAuthorizedException("")).when(this.authzRequest).destroyAuthorize(eq(REGION_NAME), eq(KEY), eq(CALLBACK_ARG));

    this.destroy.cmdExecute(this.message, this.serverConnection, 0);

    verify(this.authzRequest).destroyAuthorize(eq(REGION_NAME), eq(KEY), eq(CALLBACK_ARG));
    verify(this.errorResponseMessage).send(eq(this.serverConnection));
  }

}
