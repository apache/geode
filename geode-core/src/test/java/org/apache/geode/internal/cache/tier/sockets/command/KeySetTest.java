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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.operations.DestroyOperationContext;
import org.apache.geode.cache.operations.KeySetOperationContext;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.ObjectPartList;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class KeySetTest {

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
  private ChunkedMessage chunkedResponseMessage;
  @Mock
  private Part regionNamePart;
  @Mock
  private KeySetOperationContext keySetOperationContext;
  @InjectMocks
  private KeySet keySet;

  @Before
  public void setUp() throws Exception {
    this.keySet = new KeySet();
    MockitoAnnotations.initMocks(this);

    when(this.authzRequest.keySetAuthorize(eq(REGION_NAME)))
        .thenReturn(this.keySetOperationContext);

    when(this.cache.getRegion(isA(String.class))).thenReturn(this.region);
    when(this.cache.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));

    when(this.message.getPart(eq(0))).thenReturn(this.regionNamePart);

    when(this.regionNamePart.getString()).thenReturn(REGION_NAME);

    when(this.serverConnection.getCache()).thenReturn(this.cache);
    when(this.serverConnection.getCacheServerStats()).thenReturn(mock(CacheServerStats.class));
    when(this.serverConnection.getAuthzRequest()).thenReturn(this.authzRequest);
    when(this.serverConnection.getCachedRegionHelper()).thenReturn(mock(CachedRegionHelper.class));
    when(this.serverConnection.getChunkedResponseMessage()).thenReturn(this.chunkedResponseMessage);
  }

  @Test
  public void noSecurityShouldSucceed() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(false);

    this.keySet.cmdExecute(this.message, this.serverConnection, 0);

    verify(this.chunkedResponseMessage).sendChunk(this.serverConnection);
  }

  @Test
  public void integratedSecurityShouldSucceedIfAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(true);

    this.keySet.cmdExecute(this.message, this.serverConnection, 0);

    verify(this.securityService).authorizeRegionRead(eq(REGION_NAME));
    verify(this.chunkedResponseMessage).sendChunk(this.serverConnection);
  }

  @Test
  public void integratedSecurityShouldFailIfNotAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(true);
    doThrow(new NotAuthorizedException("")).when(this.securityService)
        .authorizeRegionRead(eq(REGION_NAME));

    this.keySet.cmdExecute(this.message, this.serverConnection, 0);

    verify(this.securityService).authorizeRegionRead(eq(REGION_NAME));
    verify(this.chunkedResponseMessage).sendChunk(this.serverConnection);
  }

  @Test
  public void oldSecurityShouldSucceedIfAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(false);

    this.keySet.cmdExecute(this.message, this.serverConnection, 0);

    verify(this.authzRequest).keySetAuthorize(eq(REGION_NAME));
    verify(this.chunkedResponseMessage).sendChunk(this.serverConnection);
  }

  @Test
  public void oldSecurityShouldFailIfNotAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(false);
    doThrow(new NotAuthorizedException("")).when(this.authzRequest)
        .keySetAuthorize(eq(REGION_NAME));

    this.keySet.cmdExecute(this.message, this.serverConnection, 0);

    verify(this.authzRequest).keySetAuthorize(eq(REGION_NAME));

    ArgumentCaptor<NotAuthorizedException> argument =
        ArgumentCaptor.forClass(NotAuthorizedException.class);
    verify(this.chunkedResponseMessage).addObjPart(argument.capture());
    assertThat(argument.getValue()).isExactlyInstanceOf(NotAuthorizedException.class);
    verify(this.chunkedResponseMessage).sendChunk(eq(this.serverConnection));
  }


}
