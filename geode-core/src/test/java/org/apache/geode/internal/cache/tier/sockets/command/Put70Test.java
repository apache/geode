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

import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.operations.PutOperationContext;
import org.apache.geode.internal.cache.EventIDHolder;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
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
public class Put70Test {

  private static final String REGION_NAME = "region1";
  private static final String KEY = "key1";
  private static final Object CALLBACK_ARG = "arg";
  private static final byte[] EVENT = new byte[8];
  private static final byte[] VALUE = new byte[8];
  private static final byte[] OK_BYTES = new byte[] {0};

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
  private LocalRegion localRegion;
  @Mock
  private Part regionNamePart;
  @Mock
  private Part keyPart;
  @Mock
  private Part valuePart;
  @Mock
  private Part oldValuePart;
  @Mock
  private Part deltaPart;
  @Mock
  private Part operationPart;
  @Mock
  private Part flagsPart;
  @Mock
  private Part eventPart;
  @Mock
  private Part callbackArgsPart;
  @Mock
  private PutOperationContext putOperationContext;
  @Mock
  private Message errorResponseMessage;
  @Mock
  private Message replyMessage;
  @Mock
  private RegionAttributes<?, ?> attributes;
  @Mock
  private EventIDHolder clientEvent;
  @Mock
  private DataPolicy dataPolicy;

  @InjectMocks
  private Put70 put70;

  private AutoCloseable mockitoMocks;

  @Before
  public void setUp() throws Exception {
    put70 = (Put70) Put70.getCommand();
    mockitoMocks = MockitoAnnotations.openMocks(this);

    when(
        authzRequest.putAuthorize(eq(REGION_NAME), eq(KEY), any(), eq(true), eq(CALLBACK_ARG)))
            .thenReturn(putOperationContext);

    when(cache.getRegion(isA(String.class))).thenReturn(uncheckedCast(localRegion));
    when(cache.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));
    when(cache.getCacheTransactionManager()).thenReturn(mock(TXManagerImpl.class));

    when(callbackArgsPart.getObject()).thenReturn(CALLBACK_ARG);

    when(deltaPart.getObject()).thenReturn(Boolean.FALSE);

    when(eventPart.getSerializedForm()).thenReturn(EVENT);

    when(flagsPart.getInt()).thenReturn(1);

    when(keyPart.getStringOrObject()).thenReturn(KEY);

    when(localRegion.basicBridgePut(eq(KEY), eq(VALUE), eq(null), eq(true), eq(CALLBACK_ARG),
        any(), any(), eq(true))).thenReturn(true);

    when(message.getNumberOfParts()).thenReturn(8);
    when(message.getPart(eq(0))).thenReturn(regionNamePart);
    when(message.getPart(eq(1))).thenReturn(operationPart);
    when(message.getPart(eq(2))).thenReturn(flagsPart);
    when(message.getPart(eq(3))).thenReturn(keyPart);
    when(message.getPart(eq(4))).thenReturn(deltaPart);
    when(message.getPart(eq(5))).thenReturn(valuePart);
    when(message.getPart(eq(6))).thenReturn(eventPart);
    when(message.getPart(eq(7))).thenReturn(callbackArgsPart);

    when(operationPart.getObject()).thenReturn(null);

    when(oldValuePart.getObject()).thenReturn(mock(Object.class));

    when(putOperationContext.getCallbackArg()).thenReturn(CALLBACK_ARG);
    when(putOperationContext.getValue()).thenReturn(VALUE);
    when(putOperationContext.isObject()).thenReturn(true);

    when(regionNamePart.getCachedString()).thenReturn(REGION_NAME);

    when(serverConnection.getCache()).thenReturn(cache);
    when(serverConnection.getCacheServerStats()).thenReturn(mock(CacheServerStats.class));
    when(serverConnection.getAuthzRequest()).thenReturn(authzRequest);
    when(serverConnection.getCachedRegionHelper()).thenReturn(mock(CachedRegionHelper.class));
    when(serverConnection.getReplyMessage()).thenReturn(replyMessage);
    when(serverConnection.getErrorResponseMessage()).thenReturn(errorResponseMessage);
    when(serverConnection.getClientVersion()).thenReturn(KnownVersion.CURRENT);

    when(valuePart.getSerializedForm()).thenReturn(VALUE);
    when(valuePart.isObject()).thenReturn(true);

    when(localRegion.getAttributes()).thenReturn(attributes);
    when(attributes.getDataPolicy()).thenReturn(dataPolicy);
  }

  @After
  public void after() throws Exception {
    mockitoMocks.close();
  }

  @Test
  public void noSecurityShouldSucceed() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(false);

    put70.cmdExecute(message, serverConnection, securityService, 0);

    verify(replyMessage).send(serverConnection);
  }

  @Test
  public void noRegionNameShouldFail() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(false);
    when(regionNamePart.getCachedString()).thenReturn(null);

    put70.cmdExecute(message, serverConnection, securityService, 0);

    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(errorResponseMessage).addStringPart(argument.capture());
    assertThat(argument.getValue()).contains("The input region name for the put request is null");
    verify(errorResponseMessage).send(serverConnection);
  }

  @Test
  public void noKeyShouldFail() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(false);
    when(keyPart.getStringOrObject()).thenReturn(null);

    put70.cmdExecute(message, serverConnection, securityService, 0);

    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(errorResponseMessage).addStringPart(argument.capture());
    assertThat(argument.getValue()).contains("The input key for the put request is null");
    verify(errorResponseMessage).send(serverConnection);
  }

  @Test
  public void integratedSecurityShouldSucceedIfAuthorized() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(true);

    put70.cmdExecute(message, serverConnection, securityService, 0);

    verify(securityService).authorize(Resource.DATA, Operation.WRITE, REGION_NAME, KEY);
    verify(replyMessage).send(serverConnection);
  }

  @Test
  public void integratedSecurityShouldFailIfNotAuthorized() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(true);
    doThrow(new NotAuthorizedException("")).when(securityService).authorize(Resource.DATA,
        Operation.WRITE, REGION_NAME, KEY);

    put70.cmdExecute(message, serverConnection, securityService, 0);

    verify(securityService).authorize(Resource.DATA, Operation.WRITE, REGION_NAME, KEY);
    verify(errorResponseMessage).send(serverConnection);
  }

  @Test
  public void oldSecurityShouldSucceedIfAuthorized() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(false);

    put70.cmdExecute(message, serverConnection, securityService, 0);

    ArgumentCaptor<byte[]> argument = ArgumentCaptor.forClass(byte[].class);
    verify(replyMessage).addBytesPart(argument.capture());

    assertThat(argument.getValue()).isEqualTo(OK_BYTES);

    verify(authzRequest).putAuthorize(eq(REGION_NAME), eq(KEY), eq(VALUE), eq(true),
        eq(CALLBACK_ARG));
    verify(replyMessage).send(serverConnection);
  }

  @Test
  public void oldSecurityShouldFailIfNotAuthorized() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(false);
    doThrow(new NotAuthorizedException("")).when(authzRequest).putAuthorize(eq(REGION_NAME),
        eq(KEY), eq(VALUE), eq(true), eq(CALLBACK_ARG));

    put70.cmdExecute(message, serverConnection, securityService, 0);

    verify(authzRequest).putAuthorize(eq(REGION_NAME), eq(KEY), eq(VALUE), eq(true),
        eq(CALLBACK_ARG));

    ArgumentCaptor<NotAuthorizedException> argument =
        ArgumentCaptor.forClass(NotAuthorizedException.class);
    verify(errorResponseMessage).addObjPart(argument.capture());

    assertThat(argument.getValue()).isExactlyInstanceOf(NotAuthorizedException.class);
    verify(errorResponseMessage).send(serverConnection);
  }

  @Test
  public void shouldSetPossibleDuplicateReturnsTrueIfConcurrencyChecksNotEnabled() {

    when(attributes.getConcurrencyChecksEnabled()).thenReturn(false);

    assertThat(put70.shouldSetPossibleDuplicate(localRegion, clientEvent)).isTrue();
  }

  @Test
  public void shouldSetPossibleDuplicateReturnsTrueIfRecoveredVersionTagForRetriedOperation() {
    Put70 spy = Mockito.spy(put70);
    when(attributes.getConcurrencyChecksEnabled()).thenReturn(true);
    doReturn(true).when(spy).recoverVersionTagForRetriedOperation(clientEvent);

    assertThat(spy.shouldSetPossibleDuplicate(localRegion, clientEvent)).isTrue();
  }

  @Test
  public void shouldSetPossibleDuplicateReturnsFalseIfNotRecoveredVersionTagAndNoPersistence() {
    Put70 spy = Mockito.spy(put70);
    when(attributes.getConcurrencyChecksEnabled()).thenReturn(true);
    when(dataPolicy.withPersistence()).thenReturn(false);
    doReturn(false).when(spy).recoverVersionTagForRetriedOperation(clientEvent);

    assertThat(spy.shouldSetPossibleDuplicate(localRegion, clientEvent)).isFalse();
  }

  @Test
  public void shouldSetPossibleDuplicateReturnsTrueIfNotRecoveredVersionTagAndWithPersistence() {
    Put70 spy = Mockito.spy(put70);
    when(attributes.getConcurrencyChecksEnabled()).thenReturn(true);
    when(dataPolicy.withPersistence()).thenReturn(true);
    doReturn(false).when(spy).recoverVersionTagForRetriedOperation(clientEvent);

    assertThat(spy.shouldSetPossibleDuplicate(localRegion, clientEvent)).isTrue();
  }

}
