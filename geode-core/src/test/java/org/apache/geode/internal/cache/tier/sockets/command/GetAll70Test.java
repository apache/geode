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
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.operations.GetOperationContext;
import org.apache.geode.internal.cache.InternalCache;
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
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class GetAll70Test {

  private static final String REGION_NAME = "region1";
  private static final Object[] KEYS = new Object[] {"key1", "key2", "key3"};

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
  private LocalRegion region;
  @Mock
  private Part regionNamePart;
  @Mock
  private Part keyPart;
  @Mock
  private Part requestSerializableValuesPart;
  @Mock
  private RegionAttributes regionAttributes;
  @Mock
  private ChunkedMessage chunkedResponseMessage;

  @InjectMocks
  private GetAll70 getAll70;

  @Before
  public void setUp() throws Exception {
    getAll70 = new GetAll70();
    MockitoAnnotations.initMocks(this);

    when(authzRequest.getAuthorize(any(), any(), any()))
        .thenReturn(mock(GetOperationContext.class));

    when(cache.getRegion(isA(String.class))).thenReturn(region);
    when(cache.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));

    when(keyPart.getObject()).thenReturn(KEYS);

    when(message.getPart(eq(0))).thenReturn(regionNamePart);
    when(message.getPart(eq(1))).thenReturn(keyPart);
    when(message.getPart(eq(2))).thenReturn(requestSerializableValuesPart);

    when(region.getAttributes()).thenReturn(regionAttributes);

    when(regionAttributes.getConcurrencyChecksEnabled()).thenReturn(true);

    when(regionNamePart.getCachedString()).thenReturn(REGION_NAME);

    when(requestSerializableValuesPart.getInt()).thenReturn(0);

    when(serverConnection.getCache()).thenReturn(cache);
    when(serverConnection.getCacheServerStats()).thenReturn(mock(CacheServerStats.class));
    when(serverConnection.getAuthzRequest()).thenReturn(authzRequest);
    when(serverConnection.getCachedRegionHelper()).thenReturn(mock(CachedRegionHelper.class));
    when(serverConnection.getChunkedResponseMessage()).thenReturn(chunkedResponseMessage);
  }

  @Test
  public void noSecurityShouldSucceed() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(false);

    getAll70.cmdExecute(message, serverConnection, securityService, 0);

    verify(chunkedResponseMessage).sendChunk(serverConnection);
  }

  @Test
  public void integratedSecurityShouldSucceedIfAuthorized() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(true);

    getAll70.cmdExecute(message, serverConnection, securityService, 0);

    ArgumentCaptor<ObjectPartList> argument = ArgumentCaptor.forClass(ObjectPartList.class);
    verify(chunkedResponseMessage).addObjPartNoCopying(argument.capture());

    assertThat(argument.getValue().getObjects()).hasSize(KEYS.length);
    for (Object key : argument.getValue().getKeys()) {
      assertThat(key).isIn(KEYS);
    }
    for (Object key : KEYS) {
      verify(securityService).authorize(Resource.DATA, Operation.READ, REGION_NAME,
          key.toString());
    }

    verify(chunkedResponseMessage).sendChunk(serverConnection);
  }

  @Test
  public void integratedSecurityShouldFailIfNotAuthorized() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(true);

    for (Object key : KEYS) {
      doThrow(new NotAuthorizedException("")).when(securityService).authorize(Resource.DATA,
          Operation.READ, REGION_NAME, key.toString());
    }

    getAll70.cmdExecute(message, serverConnection, securityService, 0);

    for (Object key : KEYS) {
      verify(securityService).authorize(Resource.DATA, Operation.READ, REGION_NAME,
          key.toString());
    }

    ArgumentCaptor<ObjectPartList> argument = ArgumentCaptor.forClass(ObjectPartList.class);
    verify(chunkedResponseMessage).addObjPartNoCopying(argument.capture());

    assertThat(argument.getValue().getObjects()).hasSize(KEYS.length);
    for (Object key : argument.getValue().getObjects()) {
      assertThat(key).isExactlyInstanceOf(NotAuthorizedException.class);
    }

    verify(chunkedResponseMessage).sendChunk(eq(serverConnection));
  }

  @Test
  public void oldSecurityShouldSucceedIfAuthorized() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(false);

    getAll70.cmdExecute(message, serverConnection, securityService, 0);

    ArgumentCaptor<ObjectPartList> argument = ArgumentCaptor.forClass(ObjectPartList.class);
    verify(chunkedResponseMessage).addObjPartNoCopying(argument.capture());

    assertThat(argument.getValue().getObjects()).hasSize(KEYS.length);
    for (Object key : argument.getValue().getKeys()) {
      assertThat(key).isIn(KEYS);
    }

    for (Object key : KEYS) {
      verify(authzRequest).getAuthorize(eq(REGION_NAME), eq(key.toString()), eq(null));
    }

    verify(chunkedResponseMessage).sendChunk(eq(serverConnection));
  }

  @Test
  public void oldSecurityShouldFailIfNotAuthorized() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(false);

    for (Object key : KEYS) {
      doThrow(new NotAuthorizedException("")).when(authzRequest).getAuthorize(eq(REGION_NAME),
          eq(key.toString()), eq(null));
    }

    getAll70.cmdExecute(message, serverConnection, securityService, 0);

    ArgumentCaptor<ObjectPartList> argument = ArgumentCaptor.forClass(ObjectPartList.class);
    verify(chunkedResponseMessage).addObjPartNoCopying(argument.capture());

    assertThat(argument.getValue().getObjects()).hasSize(KEYS.length);
    for (Object o : argument.getValue().getObjects()) {
      assertThat(o).isExactlyInstanceOf(NotAuthorizedException.class);
    }
    for (Object key : KEYS) {
      verify(authzRequest).getAuthorize(eq(REGION_NAME), eq(key.toString()), eq(null));
    }
    verify(chunkedResponseMessage).sendChunk(eq(serverConnection));
  }

}
