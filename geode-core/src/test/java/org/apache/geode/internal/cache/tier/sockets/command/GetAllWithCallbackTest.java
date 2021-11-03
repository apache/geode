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

import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.operations.GetOperationContext;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
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
public class GetAllWithCallbackTest {

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
    this.getAll70 = new GetAll70();
    MockitoAnnotations.initMocks(this);

    when(this.authzRequest.getAuthorize(any(), any(), any()))
        .thenReturn(mock(GetOperationContext.class));

    when(this.cache.getRegion(isA(String.class))).thenReturn(this.region);

    when(this.keyPart.getObject()).thenReturn(KEYS);

    when(this.message.getPart(eq(0))).thenReturn(this.regionNamePart);
    when(this.message.getPart(eq(1))).thenReturn(this.keyPart);
    when(this.message.getPart(eq(2))).thenReturn(this.requestSerializableValuesPart);

    when(this.region.getAttributes()).thenReturn(this.regionAttributes);

    when(this.regionAttributes.getConcurrencyChecksEnabled()).thenReturn(true);

    when(this.regionNamePart.getCachedString()).thenReturn(REGION_NAME);

    when(this.requestSerializableValuesPart.getInt()).thenReturn(0);

    when(this.serverConnection.getCache()).thenReturn(this.cache);
    when(this.serverConnection.getAuthzRequest()).thenReturn(this.authzRequest);
    when(this.serverConnection.getChunkedResponseMessage()).thenReturn(this.chunkedResponseMessage);
  }

  @Test
  public void noSecurityShouldSucceed() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(false);

    this.getAll70.cmdExecute(this.message, this.serverConnection, this.securityService, 0);

    verify(this.chunkedResponseMessage).sendChunk(eq(this.serverConnection));
  }

  @Test
  public void integratedSecurityShouldSucceedIfAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(true);

    this.getAll70.cmdExecute(this.message, this.serverConnection, this.securityService, 0);

    ArgumentCaptor<ObjectPartList> argument = ArgumentCaptor.forClass(ObjectPartList.class);
    verify(this.chunkedResponseMessage).addObjPartNoCopying(argument.capture());

    assertThat(argument.getValue().getObjects()).hasSize(KEYS.length);
    for (Object key : argument.getValue().getKeys()) {
      assertThat(key).isIn(KEYS);
    }
    for (Object key : KEYS) {
      verify(this.securityService).authorize(Resource.DATA, Operation.READ, REGION_NAME,
          key.toString());
    }

    verify(this.chunkedResponseMessage).sendChunk(eq(this.serverConnection));
  }

  @Test
  public void integratedSecurityShouldFailIfNotAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(true);

    for (Object key : KEYS) {
      doThrow(new NotAuthorizedException("")).when(this.securityService).authorize(Resource.DATA,
          Operation.READ, REGION_NAME, key.toString());
    }

    this.getAll70.cmdExecute(this.message, this.serverConnection, this.securityService, 0);

    for (Object key : KEYS) {
      verify(this.securityService).authorize(Resource.DATA, Operation.READ, REGION_NAME,
          key.toString());
    }

    ArgumentCaptor<ObjectPartList> argument = ArgumentCaptor.forClass(ObjectPartList.class);
    verify(this.chunkedResponseMessage).addObjPartNoCopying(argument.capture());

    assertThat(argument.getValue().getObjects()).hasSize(KEYS.length);
    for (Object key : argument.getValue().getObjects()) {
      assertThat(key).isExactlyInstanceOf(NotAuthorizedException.class);
    }

    verify(this.chunkedResponseMessage).sendChunk(eq(this.serverConnection));
  }

  @Test
  public void oldSecurityShouldSucceedIfAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(false);

    this.getAll70.cmdExecute(this.message, this.serverConnection, this.securityService, 0);

    ArgumentCaptor<ObjectPartList> argument = ArgumentCaptor.forClass(ObjectPartList.class);
    verify(this.chunkedResponseMessage).addObjPartNoCopying(argument.capture());

    assertThat(argument.getValue().getObjects()).hasSize(KEYS.length);
    for (Object key : argument.getValue().getKeys()) {
      assertThat(key).isIn(KEYS);
    }

    for (Object key : KEYS) {
      verify(this.authzRequest).getAuthorize(eq(REGION_NAME), eq(key.toString()), eq(null));
    }

    verify(this.chunkedResponseMessage).sendChunk(eq(this.serverConnection));
  }

  @Test
  public void oldSecurityShouldFailIfNotAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(false);

    for (Object key : KEYS) {
      doThrow(new NotAuthorizedException("")).when(this.authzRequest).getAuthorize(eq(REGION_NAME),
          eq(key.toString()), eq(null));
    }

    this.getAll70.cmdExecute(this.message, this.serverConnection, this.securityService, 0);

    ArgumentCaptor<ObjectPartList> argument = ArgumentCaptor.forClass(ObjectPartList.class);
    verify(this.chunkedResponseMessage).addObjPartNoCopying(argument.capture());

    assertThat(argument.getValue().getObjects()).hasSize(KEYS.length);
    for (Object o : argument.getValue().getObjects()) {
      assertThat(o).isExactlyInstanceOf(NotAuthorizedException.class);
    }

    for (Object key : KEYS) {
      verify(this.authzRequest).getAuthorize(eq(REGION_NAME), eq(key.toString()), eq(null));
    }

    verify(this.chunkedResponseMessage).sendChunk(eq(this.serverConnection));
  }

}
