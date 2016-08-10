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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.util.Collections;
import java.util.Properties;

import org.apache.regexp.RE;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.internal.cache.EntriesMap.Attributes;
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
  private Cache cache;
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
    this.createRegion = new CreateRegion();
    MockitoAnnotations.initMocks(this);

    when(this.cache.getRegion(eq(PARENT_REGION_NAME))).thenReturn(this.parentRegion);
    when(this.cache.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));

    when(this.message.getPart(eq(0))).thenReturn(this.parentRegionNamePart);
    when(this.message.getPart(eq(1))).thenReturn(this.regionNamePart);

    when(this.parentRegion.getAttributes()).thenReturn(new AttributesFactory().create());

    when(this.parentRegionNamePart.getString()).thenReturn(PARENT_REGION_NAME);

    when(this.regionNamePart.getString()).thenReturn(REGION_NAME);

    when(this.serverConnection.getCache()).thenReturn(this.cache);
    when(this.serverConnection.getAuthzRequest()).thenReturn(this.authzRequest);
    when(this.serverConnection.getCacheServerStats()).thenReturn(mock(CacheServerStats.class));
    when(this.serverConnection.getReplyMessage()).thenReturn(this.responseMessage);
    when(this.serverConnection.getErrorResponseMessage()).thenReturn(this.errorResponseMessage);
  }

  @Test
  public void noSecurityShouldSucceed() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(false);

    this.createRegion.cmdExecute(this.message, this.serverConnection, 0);

    verify(this.responseMessage).send(this.serverConnection);
  }

  @Test
  public void integratedSecurityShouldSucceedIfAuthorized() throws Exception {
    // arrange
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(true);

    // act
    this.createRegion.cmdExecute(this.message, this.serverConnection, 0);

    // assert
    verify(this.securityService).authorizeDataManage();
    verify(this.responseMessage).send(this.serverConnection);
  }

  @Test
  public void integratedSecurityShouldFailIfNotAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(true);
    doThrow(new NotAuthorizedException("")).when(this.securityService).authorizeDataManage();

    this.createRegion.cmdExecute(this.message, this.serverConnection, 0);

    verify(this.securityService).authorizeDataManage();
    verify(this.errorResponseMessage).send(eq(this.serverConnection));
  }

  @Test
  public void oldSecurityShouldSucceedIfAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(false);

    this.createRegion.cmdExecute(this.message, this.serverConnection, 0);

    verify(this.authzRequest).createRegionAuthorize(eq(PARENT_REGION_NAME + '/' + REGION_NAME));
    verify(this.responseMessage).send(this.serverConnection);
  }

  @Test
  public void oldSecurityShouldFailIfNotAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(false);
    doThrow(new NotAuthorizedException("")).when(this.authzRequest).createRegionAuthorize(eq(PARENT_REGION_NAME + '/' + REGION_NAME));

    this.createRegion.cmdExecute(this.message, this.serverConnection, 0);

    verify(this.authzRequest).createRegionAuthorize(eq(PARENT_REGION_NAME + '/' + REGION_NAME));
    verify(this.errorResponseMessage).send(eq(this.serverConnection));
  }

}
