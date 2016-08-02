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

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.operations.GetOperationContext;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.tier.CachedRegionHelper;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerStats;
import com.gemstone.gemfire.internal.cache.tier.sockets.ChunkedMessage;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.ObjectPartList;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.security.AuthorizeRequest;
import com.gemstone.gemfire.internal.security.SecurityService;
import com.gemstone.gemfire.security.NotAuthorizedException;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class GetAllTest {

  private static final String REGION_NAME = "region1";
  private static final Object[] KEYS = new Object[] { "key1", "key2", "key3" };

  @Mock
  private SecurityService securityService;
  @Mock
  private Message message;
  @Mock
  private ServerConnection serverConnection;
  @Mock
  private AuthorizeRequest authzRequest;
  @Mock
  private Cache cache;
  @Mock
  private Part regionNamePart;
  @Mock
  private Part keyPart;
  @Mock
  private ChunkedMessage chunkedResponseMessage;

  @InjectMocks
  private GetAll getAll;

  @Before
  public void setUp() throws Exception {
    this.getAll = new GetAll();
    MockitoAnnotations.initMocks(this);

    when(this.authzRequest.getAuthorize(any(), any(), any())).thenReturn(mock(GetOperationContext.class));

    when(this.cache.getRegion(isA(String.class))).thenReturn(mock(LocalRegion.class));
    when(this.cache.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));

    when(this.keyPart.getObject()).thenReturn(KEYS);

    when(this.message.getPart(eq(0))).thenReturn(this.regionNamePart);
    when(this.message.getPart(eq(1))).thenReturn(this.keyPart);

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

    this.getAll.cmdExecute(this.message, this.serverConnection, 0);

    verify(this.chunkedResponseMessage).sendChunk(eq(this.serverConnection));
  }

  @Test
  public void integratedSecurityShouldSucceedIfAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(true);

    this.getAll.cmdExecute(this.message, this.serverConnection, 0);

    ArgumentCaptor<ObjectPartList> argument = ArgumentCaptor.forClass(ObjectPartList.class);
    verify(this.chunkedResponseMessage).addObjPart(argument.capture(), eq(false));

    assertThat(argument.getValue().getObjects()).hasSize(KEYS.length);
    for(Object key : argument.getValue().getKeys()) {
      assertThat(key).isIn(KEYS);
    }
    for (Object key : KEYS) {
      verify(this.securityService).authorizeRegionRead(eq(REGION_NAME), eq(key.toString()));
    }

    verify(this.chunkedResponseMessage).sendChunk(eq(this.serverConnection));
  }

  @Test
  public void integratedSecurityShouldFailIfNotAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(true);

    for (Object key : KEYS) {
      doThrow(new NotAuthorizedException("")).when(this.securityService).authorizeRegionRead(eq(REGION_NAME), eq(key.toString()));
    }

    this.getAll.cmdExecute(this.message, this.serverConnection, 0);

    for (Object key : KEYS) {
      verify(this.securityService).authorizeRegionRead(eq(REGION_NAME), eq(key.toString()));
    }

    ArgumentCaptor<ObjectPartList> argument = ArgumentCaptor.forClass(ObjectPartList.class);
    verify(this.chunkedResponseMessage).addObjPart(argument.capture(), eq(false));

    assertThat(argument.getValue().getObjects()).hasSize(KEYS.length);
    for(Object key : argument.getValue().getObjects()){
      assertThat(key).isExactlyInstanceOf(NotAuthorizedException.class);
    }

    verify(this.chunkedResponseMessage).sendChunk(eq(this.serverConnection));
  }

  @Test
  public void oldSecurityShouldSucceedIfAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(false);

    this.getAll.cmdExecute(this.message, this.serverConnection, 0);

    ArgumentCaptor<ObjectPartList> argument = ArgumentCaptor.forClass(ObjectPartList.class);
    verify(this.chunkedResponseMessage).addObjPart(argument.capture(), eq(false));

    assertThat(argument.getValue().getObjects()).hasSize(KEYS.length);
    for(Object key : argument.getValue().getKeys()) {
      assertThat(key).isIn(KEYS);
    }

    for (Object key: KEYS) {
      verify(this.authzRequest).getAuthorize(eq(REGION_NAME), eq(key.toString()), eq(null));
    }

    verify(this.chunkedResponseMessage).sendChunk(eq(this.serverConnection));
  }

  @Test
  public void oldSecurityShouldFailIfNotAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(false);

    for (Object key : KEYS) {
      doThrow(new NotAuthorizedException("")).when(this.authzRequest).getAuthorize(eq(REGION_NAME), eq(key.toString()), eq(null));
    }
    this.getAll.cmdExecute(this.message, this.serverConnection, 0);

    ArgumentCaptor<ObjectPartList> argument = ArgumentCaptor.forClass(ObjectPartList.class);
    verify(this.chunkedResponseMessage).addObjPart(argument.capture(), eq(false));

    assertThat(argument.getValue().getObjects()).hasSize(KEYS.length);
    for(Object o : argument.getValue().getObjects()){
      assertThat(o).isExactlyInstanceOf(NotAuthorizedException.class);
    }

    for (Object key: KEYS) {
      verify(this.authzRequest).getAuthorize(eq(REGION_NAME), eq(key.toString()), eq(null));
    }
    verify(this.chunkedResponseMessage).sendChunk(eq(this.serverConnection));
  }

}
