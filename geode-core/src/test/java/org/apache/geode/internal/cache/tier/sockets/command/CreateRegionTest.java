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

import static org.apache.geode.cache.Region.SEPARATOR;
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
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
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
public class CreateRegionTest {

  private static final String REGION_NAME = "region1";
  private static final String PARENT_REGION_NAME = "parent-region";

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
  private LocalRegion parentRegion;
  @Mock
  private InternalCache cache;
  @Mock
  private Message errorResponseMessage;
  @Mock
  private Message responseMessage;
  @Mock
  private Part regionNamePart;
  @Mock
  private Part parentRegionNamePart;
  @InjectMocks
  private CreateRegion createRegion;

  @Before
  public void setUp() throws Exception {
    createRegion = new CreateRegion();
    MockitoAnnotations.initMocks(this);

    when(cache.getRegion(eq(PARENT_REGION_NAME))).thenReturn(parentRegion);
    when(cache.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));

    when(message.getPart(eq(0))).thenReturn(parentRegionNamePart);
    when(message.getPart(eq(1))).thenReturn(regionNamePart);

    when(parentRegion.getAttributes()).thenReturn(new AttributesFactory().create());

    when(parentRegionNamePart.getCachedString()).thenReturn(PARENT_REGION_NAME);

    when(regionNamePart.getCachedString()).thenReturn(REGION_NAME);

    when(serverConnection.getCache()).thenReturn(cache);
    when(serverConnection.getAuthzRequest()).thenReturn(authzRequest);
    when(serverConnection.getCacheServerStats()).thenReturn(mock(CacheServerStats.class));
    when(serverConnection.getReplyMessage()).thenReturn(responseMessage);
    when(serverConnection.getErrorResponseMessage()).thenReturn(errorResponseMessage);
    when(serverConnection.getClientVersion()).thenReturn(KnownVersion.CURRENT);
  }

  @Test
  public void noSecurityShouldSucceed() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(false);

    createRegion.cmdExecute(message, serverConnection, securityService, 0);

    verify(responseMessage).send(serverConnection);
  }

  @Test
  public void integratedSecurityShouldSucceedIfAuthorized() throws Exception {
    // arrange
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(true);

    // act
    createRegion.cmdExecute(message, serverConnection, securityService, 0);

    // assert
    verify(securityService).authorize(Resource.DATA, Operation.MANAGE);
    verify(responseMessage).send(serverConnection);
  }

  @Test
  public void integratedSecurityShouldFailIfNotAuthorized() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(true);
    doThrow(new NotAuthorizedException("")).when(securityService).authorize(Resource.DATA,
        Operation.MANAGE);

    createRegion.cmdExecute(message, serverConnection, securityService, 0);

    verify(securityService).authorize(Resource.DATA, Operation.MANAGE);
    verify(errorResponseMessage).send(eq(serverConnection));
  }

  @Test
  public void oldSecurityShouldSucceedIfAuthorized() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(false);

    createRegion.cmdExecute(message, serverConnection, securityService, 0);

    verify(authzRequest)
        .createRegionAuthorize(eq(PARENT_REGION_NAME + SEPARATOR + REGION_NAME));
    verify(responseMessage).send(serverConnection);
  }

  @Test
  public void oldSecurityShouldFailIfNotAuthorized() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(false);
    doThrow(new NotAuthorizedException("")).when(authzRequest)
        .createRegionAuthorize(eq(PARENT_REGION_NAME + SEPARATOR + REGION_NAME));

    createRegion.cmdExecute(message, serverConnection, securityService, 0);

    verify(authzRequest)
        .createRegionAuthorize(eq(PARENT_REGION_NAME + SEPARATOR + REGION_NAME));
    verify(errorResponseMessage).send(eq(serverConnection));
  }

}
