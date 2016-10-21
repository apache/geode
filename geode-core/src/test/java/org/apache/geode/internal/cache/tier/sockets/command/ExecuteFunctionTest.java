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

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import org.junit.Rule;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.powermock.api.mockito.PowerMockito;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.operations.ExecuteFunctionOperationContext;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.control.HeapMemoryMonitor;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.HandShake;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.geode.cache.execute.Function;

@Category(UnitTest.class)
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("*.UnitTest")
@PrepareForTest({FunctionService.class})
public class ExecuteFunctionTest {
  private static final String FUNCTION = "function";
  private static final String FUNCTION_ID = "function_id";
  private static final boolean OPTIMIZE_FOR_WRITE = false;
  private static final Object CALLBACK_ARG = "arg";
  private static final byte[] RESULT = new byte[] {Integer.valueOf(1).byteValue()};

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
  private GemFireCacheImpl cache;
  @Mock
  private ChunkedMessage functionResponseMessage;
  @Mock
  private ChunkedMessage chunkedResponseMessage;
  @Mock
  private Part hasResultPart;
  @Mock
  private Part functionPart;
  @Mock
  private Part argsPart;
  @Mock
  private Part partPart;
  @Mock
  private Part callbackArgPart;
  @Mock
  private Function functionObject;
  @Mock
  private AcceptorImpl acceptor;
  @Mock
  private HandShake handShake;
  @Mock
  private InternalResourceManager internalResourceManager;
  @Mock
  private ExecuteFunctionOperationContext executeFunctionOperationContext;

  @InjectMocks
  private ExecuteFunction executeFunction;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setUp() throws Exception {
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "statsDisabled", "true");

    this.executeFunction = new ExecuteFunction();
    MockitoAnnotations.initMocks(this);

    when(this.authzRequest.executeFunctionAuthorize(eq(FUNCTION_ID), eq(null), eq(null), eq(null),
        eq(OPTIMIZE_FOR_WRITE))).thenReturn(this.executeFunctionOperationContext);

    when(this.cache.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));
    when(this.cache.getDistributedSystem()).thenReturn(mock(InternalDistributedSystem.class));
    when(this.cache.getResourceManager()).thenReturn(this.internalResourceManager);

    when(this.callbackArgPart.getObject()).thenReturn(CALLBACK_ARG);

    when(this.functionObject.getId()).thenReturn(FUNCTION_ID);

    when(this.functionPart.getStringOrObject()).thenReturn(FUNCTION);

    when(this.hasResultPart.getSerializedForm()).thenReturn(RESULT);

    when(this.internalResourceManager.getHeapMonitor()).thenReturn(mock(HeapMemoryMonitor.class));

    when(this.message.getNumberOfParts()).thenReturn(4);
    when(this.message.getPart(eq(0))).thenReturn(this.hasResultPart);
    when(this.message.getPart(eq(1))).thenReturn(this.functionPart);
    when(this.message.getPart(eq(2))).thenReturn(this.argsPart);
    when(this.message.getPart(eq(3))).thenReturn(this.partPart);

    when(this.serverConnection.getCache()).thenReturn(this.cache);
    when(this.serverConnection.getAuthzRequest()).thenReturn(this.authzRequest);
    when(this.serverConnection.getCachedRegionHelper()).thenReturn(mock(CachedRegionHelper.class));
    when(this.serverConnection.getFunctionResponseMessage())
        .thenReturn(this.functionResponseMessage);
    when(this.serverConnection.getChunkedResponseMessage()).thenReturn(this.chunkedResponseMessage);
    when(this.serverConnection.getAcceptor()).thenReturn(this.acceptor);
    when(this.serverConnection.getHandshake()).thenReturn(this.handShake);

    PowerMockito.mockStatic(FunctionService.class);
    PowerMockito.when(FunctionService.getFunction(eq(FUNCTION))).thenReturn(functionObject);
  }

  @Test
  public void nonSecureShouldSucceed() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(false);

    this.executeFunction.cmdExecute(this.message, this.serverConnection, 0);

    // verify(this.functionResponseMessage).sendChunk(this.serverConnection); // TODO: why do none
    // of the reply message types get sent?
  }

  @Test
  public void withIntegratedSecurityShouldSucceedIfAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(true);

    this.executeFunction.cmdExecute(this.message, this.serverConnection, 0);

    verify(this.securityService).authorizeDataWrite();
    // verify(this.replyMessage).send(this.serverConnection); TODO: why do none of the reply message
    // types get sent?
  }

  @Test
  public void withIntegratedSecurityShouldThrowIfNotAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(true);
    doThrow(new NotAuthorizedException("")).when(this.securityService).authorizeDataWrite();

    this.executeFunction.cmdExecute(this.message, this.serverConnection, 0);

    verify(this.securityService).authorizeDataWrite();
    verify(this.chunkedResponseMessage).sendChunk(this.serverConnection);
  }

  @Test
  public void withOldSecurityShouldSucceedIfAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(false);

    this.executeFunction.cmdExecute(this.message, this.serverConnection, 0);

    verify(this.authzRequest).executeFunctionAuthorize(eq(FUNCTION_ID), any(), any(), any(),
        eq(false));
    // verify(this.replyMessage).send(this.serverConnection); TODO: why do none of the reply message
    // types get sent?
  }

  @Test
  public void withOldSecurityShouldThrowIfNotAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(false);
    doThrow(new NotAuthorizedException("")).when(this.authzRequest)
        .executeFunctionAuthorize(eq(FUNCTION_ID), any(), any(), any(), eq(false));

    this.executeFunction.cmdExecute(this.message, this.serverConnection, 0);

    verify(this.chunkedResponseMessage).sendChunk(this.serverConnection);
  }

}
