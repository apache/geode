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
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerStats;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.security.AuthorizeRequest;
import com.gemstone.gemfire.internal.security.SecurityService;
import com.gemstone.gemfire.security.NotAuthorizedException;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class ContainsKeyTest {

  private static final String REGION_NAME = "region1";
  private static final String KEY = "key1";

  @Mock
  private SecurityService securityService;
  @Mock
  private Message message;
  @Mock
  private Message replyMessage;
  @Mock
  private Message errorResponseMessage;
  @Mock
  private ServerConnection serverConnection;
  @Mock
  private AuthorizeRequest authzRequest;

  @InjectMocks
  private ContainsKey containsKey;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    Region region = mock(LocalRegion.class);
    when(region.containsKey(isA(String.class))).thenReturn(true);

    Cache cache = mock(Cache.class);
    when(cache.getRegion(isA(String.class))).thenReturn(region);

    CacheServerStats cacheServerStats = mock(CacheServerStats.class);

    when(this.serverConnection.getCache()).thenReturn(cache);
    when(this.serverConnection.getCacheServerStats()).thenReturn(cacheServerStats);
    when(this.serverConnection.getResponseMessage()).thenReturn(this.replyMessage);
    when(this.serverConnection.getAuthzRequest()).thenReturn(this.authzRequest);
    when(this.serverConnection.getErrorResponseMessage()).thenReturn(this.errorResponseMessage);

    Part regionNamePart = mock(Part.class);
    when(regionNamePart.getString()).thenReturn(REGION_NAME);

    Part keyPart = mock(Part.class);
    when(keyPart.getStringOrObject()).thenReturn(KEY);

    when(this.message.getPart(eq(0))).thenReturn(regionNamePart);
    when(this.message.getPart(eq(1))).thenReturn(keyPart);
  }

  @Test
  public void noSecurityShouldSucceed() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(false);

    containsKey.cmdExecute(this.message, this.serverConnection, 0);

    verify(this.replyMessage).send(this.serverConnection);
  }

  @Test
  public void integratedSecurityShouldSucceedIfAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(true);

    containsKey.cmdExecute(this.message, this.serverConnection, 0);

    verify(this.securityService).authorizeRegionRead(eq(REGION_NAME), eq(KEY));
    verify(this.replyMessage).send(this.serverConnection);
  }

  @Test
  public void integratedSecurityShouldFailIfNotAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(true);
    doThrow(new NotAuthorizedException("")).when(this.securityService).authorizeRegionRead(eq(REGION_NAME), eq(KEY));

    containsKey.cmdExecute(this.message, this.serverConnection, 0);

    verify(this.securityService).authorizeRegionRead(eq(REGION_NAME), eq(KEY));
    verify(this.errorResponseMessage).send(eq(this.serverConnection));
  }

  @Test
  public void oldSecurityShouldSucceedIfAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(false);


    containsKey.cmdExecute(this.message, this.serverConnection, 0);

    verify(this.authzRequest).containsKeyAuthorize(eq(REGION_NAME), eq(KEY));
    verify(this.replyMessage).send(this.serverConnection);
  }

  @Test
  public void oldSecurityShouldFailIfNotAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(false);
    doThrow(new NotAuthorizedException("")).when(this.authzRequest).containsKeyAuthorize(eq(REGION_NAME), eq(KEY));

    containsKey.cmdExecute(this.message, this.serverConnection, 0);

    verify(this.authzRequest).containsKeyAuthorize(eq(REGION_NAME), eq(KEY));
    verify(this.errorResponseMessage).send(eq(this.serverConnection));
  }

}
