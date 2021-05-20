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

import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.operations.RegisterInterestOperationContext;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
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
public class RegisterInterestList66Test {

  private static final String REGION_NAME = "region1";
  private static final String KEY = "key1";
  private static final byte[] DURABLE = new byte[8];
  private static final byte[] DATA_POLICY = new byte[] {0x01, 0x01};

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
  private Part regionNamePart;
  @Mock
  private Part interestTypePart;
  @Mock
  private Part durablePart;
  @Mock
  private Part keyPart;
  @Mock
  private Part numberOfKeysPart;
  @Mock
  private Part notifyPart;
  @Mock
  private Part regionDataPolicyPart;
  @Mock
  private RegisterInterestOperationContext registerInterestOperationContext;
  @Mock
  private ChunkedMessage chunkedResponseMessage;

  @InjectMocks
  private RegisterInterestList66 registerInterestList66;

  @Before
  public void setUp() throws Exception {
    this.registerInterestList66 = new RegisterInterestList66();
    MockitoAnnotations.initMocks(this);

    when(this.authzRequest.registerInterestListAuthorize(eq(REGION_NAME), any(), any()))
        .thenReturn(this.registerInterestOperationContext);

    when(this.cache.getRegion(isA(String.class))).thenReturn(mock(LocalRegion.class));
    when(this.cache.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));

    when(this.durablePart.getObject()).thenReturn(DURABLE);

    when(this.interestTypePart.getInt()).thenReturn(0);

    when(this.keyPart.getObject()).thenReturn(mock(ArrayList.class));

    when(this.message.getNumberOfParts()).thenReturn(6);
    when(this.message.getPart(eq(0))).thenReturn(this.regionNamePart);
    when(this.message.getPart(eq(1))).thenReturn(this.interestTypePart);
    when(this.message.getPart(eq(2))).thenReturn(this.durablePart);
    when(this.message.getPart(eq(3))).thenReturn(this.keyPart);
    when(this.message.getPart(eq(4))).thenReturn(this.notifyPart);
    when(this.message.getPart(eq(5))).thenReturn(this.regionDataPolicyPart);

    when(this.notifyPart.getObject()).thenReturn(DURABLE);

    when(this.numberOfKeysPart.getInt()).thenReturn(1);

    when(this.regionDataPolicyPart.getObject()).thenReturn(DATA_POLICY);

    when(this.regionNamePart.getCachedString()).thenReturn(REGION_NAME);

    when(this.registerInterestOperationContext.getKey()).thenReturn(KEY);

    when(this.serverConnection.getCache()).thenReturn(this.cache);
    when(this.serverConnection.getAuthzRequest()).thenReturn(this.authzRequest);
    when(this.serverConnection.getCachedRegionHelper()).thenReturn(mock(CachedRegionHelper.class));
    when(this.serverConnection.getChunkedResponseMessage()).thenReturn(this.chunkedResponseMessage);
    when(this.serverConnection.getClientVersion()).thenReturn(KnownVersion.GFE_8009);
    when(this.serverConnection.getAcceptor()).thenReturn(mock(AcceptorImpl.class));
  }

  @Test
  public void noSecurityShouldSucceed() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(false);

    this.registerInterestList66.cmdExecute(this.message, this.serverConnection,
        this.securityService, 0);

    verify(this.chunkedResponseMessage).sendChunk(this.serverConnection);
  }

  @Test
  public void integratedSecurityShouldSucceedIfAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(true);

    this.registerInterestList66.cmdExecute(this.message, this.serverConnection,
        this.securityService, 0);

    verify(this.securityService).authorize(Resource.DATA, Operation.READ, REGION_NAME);
    verify(this.chunkedResponseMessage).sendChunk(this.serverConnection);
  }

  @Test
  public void integratedSecurityShouldThrowIfNotAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(true);
    doThrow(new NotAuthorizedException("")).when(this.securityService).authorize(Resource.DATA,
        Operation.READ, REGION_NAME);

    this.registerInterestList66.cmdExecute(this.message, this.serverConnection,
        this.securityService, 0);

    verify(this.securityService).authorize(Resource.DATA, Operation.READ, REGION_NAME);
    verify(this.chunkedResponseMessage).sendChunk(this.serverConnection);
  }

  @Test
  public void oldSecurityShouldSucceedIfAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(false);

    this.registerInterestList66.cmdExecute(this.message, this.serverConnection,
        this.securityService, 0);

    verify(this.authzRequest).registerInterestListAuthorize(eq(REGION_NAME), any(), any());
    verify(this.chunkedResponseMessage).sendChunk(this.serverConnection);
  }

  @Test
  public void oldSecurityShouldFailIfNotAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(false);

    doThrow(new NotAuthorizedException("")).when(this.authzRequest)
        .registerInterestListAuthorize(eq(REGION_NAME), any(), any());

    this.registerInterestList66.cmdExecute(this.message, this.serverConnection,
        this.securityService, 0);

    verify(this.authzRequest).registerInterestListAuthorize(eq(REGION_NAME), any(), any());

    ArgumentCaptor<NotAuthorizedException> argument =
        ArgumentCaptor.forClass(NotAuthorizedException.class);
    verify(this.chunkedResponseMessage).addObjPart(argument.capture());

    assertThat(argument.getValue()).isExactlyInstanceOf(NotAuthorizedException.class);
    verify(this.chunkedResponseMessage).sendChunk(this.serverConnection);
  }
}
