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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.operations.ExecuteFunctionOperationContext;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.control.HeapMemoryMonitor;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.execute.FunctionContextImpl;
import org.apache.geode.internal.cache.execute.InternalFunctionExecutionService;
import org.apache.geode.internal.cache.execute.ServerToClientFunctionResultSender;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.ServerSideHandshake;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.OldClientSupportService;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.cache.tier.sockets.command.ExecuteFunction.FunctionContextImplFactory;
import org.apache.geode.internal.cache.tier.sockets.command.ExecuteFunction.ServerToClientFunctionResultSenderFactory;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.internal.security.ResourcePermissions;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.util.internal.GeodeGlossary;

@Category(ClientServerTest.class)
public class ExecuteFunctionTest {
  private static final String FUNCTION = "function";
  private static final String FUNCTION_ID = "function_id";
  private static final boolean OPTIMIZE_FOR_WRITE = false;
  private static final Object CALLBACK_ARG = "arg";
  private static final byte[] RESULT = new byte[] {Integer.valueOf(1).byteValue()};

  private AuthorizeRequest authorizeRequest;
  private ChunkedMessage chunkedResponseMessage;
  private ChunkedMessage functionResponseMessage;
  private Message message;
  private SecurityService securityService;
  private ServerConnection serverConnection;
  private ServerToClientFunctionResultSenderFactory serverToClientFunctionResultSenderFactory;

  private ExecuteFunction executeFunction;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setUp() throws Exception {
    System.setProperty(GeodeGlossary.GEMFIRE_PREFIX + "statsDisabled", "true");

    authorizeRequest = mock(AuthorizeRequest.class);
    chunkedResponseMessage = mock(ChunkedMessage.class);
    functionResponseMessage = mock(ChunkedMessage.class);
    message = mock(Message.class);
    securityService = mock(SecurityService.class);
    serverConnection = mock(ServerConnection.class);
    serverToClientFunctionResultSenderFactory =
        mock(ServerToClientFunctionResultSenderFactory.class);

    InternalCache cache = mock(InternalCache.class, RETURNS_DEEP_STUBS);
    AcceptorImpl acceptor = mock(AcceptorImpl.class);
    FunctionContextImplFactory functionContextImplFactory = mock(FunctionContextImplFactory.class);
    ExecuteFunctionOperationContext executeFunctionOperationContext =
        mock(ExecuteFunctionOperationContext.class);
    Function functionObject = mock(Function.class);
    FunctionContextImpl functionContextImpl = mock(FunctionContextImpl.class);
    InternalFunctionExecutionService internalFunctionExecutionService =
        mock(InternalFunctionExecutionService.class);
    InternalResourceManager internalResourceManager =
        mock(InternalResourceManager.class, RETURNS_DEEP_STUBS);
    OldClientSupportService oldClientSupportService = mock(OldClientSupportService.class);
    ServerSideHandshake handshake = mock(ServerSideHandshake.class);
    ServerToClientFunctionResultSender serverToClientFunctionResultSender =
        mock(ServerToClientFunctionResultSender.class);

    Part argsPart = mock(Part.class);
    Part callbackArgPart = mock(Part.class);
    Part functionPart = mock(Part.class);
    Part hasResultPart = mock(Part.class);
    Part partPart = mock(Part.class);

    when(authorizeRequest.executeFunctionAuthorize(eq(FUNCTION_ID), eq(null), eq(null), eq(null),
        eq(OPTIMIZE_FOR_WRITE))).thenReturn(executeFunctionOperationContext);

    when(cache.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));
    when(cache.getDistributedSystem()).thenReturn(mock(InternalDistributedSystem.class));
    when(cache.getResourceManager()).thenReturn(internalResourceManager);
    when(cache.getService(eq(OldClientSupportService.class))).thenReturn(oldClientSupportService);

    when(callbackArgPart.getObject()).thenReturn(CALLBACK_ARG);

    when(functionContextImplFactory.create(any(), any(), any(), any())).thenReturn(
        functionContextImpl);

    when(functionObject.getId()).thenReturn(FUNCTION_ID);
    doCallRealMethod().when(functionObject).getRequiredPermissions(any());
    doCallRealMethod().when(functionObject).getRequiredPermissions(any(), any());

    when(functionPart.getStringOrObject()).thenReturn(FUNCTION);
    when(hasResultPart.getSerializedForm()).thenReturn(RESULT);
    when(internalFunctionExecutionService.getFunction(eq(FUNCTION))).thenReturn(functionObject);
    when(internalResourceManager.getHeapMonitor()).thenReturn(mock(HeapMemoryMonitor.class));

    when(message.getNumberOfParts()).thenReturn(4);
    when(message.getPart(eq(0))).thenReturn(hasResultPart);
    when(message.getPart(eq(1))).thenReturn(functionPart);
    when(message.getPart(eq(2))).thenReturn(argsPart);
    when(message.getPart(eq(3))).thenReturn(partPart);

    when(oldClientSupportService.getThrowable(any(), any())).thenReturn(mock(Throwable.class));

    when(serverConnection.getAcceptor()).thenReturn(acceptor);
    when(serverConnection.getAuthzRequest()).thenReturn(authorizeRequest);
    when(serverConnection.getCache()).thenReturn(cache);
    when(serverConnection.getCachedRegionHelper()).thenReturn(mock(CachedRegionHelper.class));
    when(serverConnection.getChunkedResponseMessage()).thenReturn(chunkedResponseMessage);
    when(serverConnection.getFunctionResponseMessage()).thenReturn(functionResponseMessage);
    when(serverConnection.getHandshake()).thenReturn(handshake);

    when(serverToClientFunctionResultSenderFactory.create(any(), anyInt(), any(), any(), any()))
        .thenReturn(
            serverToClientFunctionResultSender);

    executeFunction = new ExecuteFunction(internalFunctionExecutionService,
        serverToClientFunctionResultSenderFactory,
        functionContextImplFactory);
  }

  @Test
  public void nonSecureShouldSucceed() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(false);

    executeFunction.cmdExecute(message, serverConnection, securityService, 0);

    verify(serverToClientFunctionResultSenderFactory).create(eq(functionResponseMessage), anyInt(),
        any(), any(), any());
  }

  @Test
  public void withIntegratedSecurityShouldSucceedIfAuthorized() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(true);

    executeFunction.cmdExecute(message, serverConnection, securityService, 0);

    verify(securityService).authorize(ResourcePermissions.DATA_WRITE);
    verify(serverToClientFunctionResultSenderFactory).create(eq(functionResponseMessage), anyInt(),
        any(), any(), any());
  }

  @Test
  public void withIntegratedSecurityShouldThrowIfNotAuthorized() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(true);
    doThrow(new NotAuthorizedException("")).when(securityService)
        .authorize(ResourcePermissions.DATA_WRITE);

    executeFunction.cmdExecute(message, serverConnection, securityService, 0);

    verify(securityService).authorize(ResourcePermissions.DATA_WRITE);
    verify(chunkedResponseMessage).sendChunk(serverConnection);
    verifyZeroInteractions(serverToClientFunctionResultSenderFactory);
  }

  @Test
  public void withOldSecurityShouldSucceedIfAuthorized() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(false);

    executeFunction.cmdExecute(message, serverConnection, securityService, 0);

    verify(authorizeRequest).executeFunctionAuthorize(eq(FUNCTION_ID), any(), any(), any(),
        eq(false));
    verify(serverToClientFunctionResultSenderFactory).create(eq(functionResponseMessage), anyInt(),
        any(), any(), any());
  }

  @Test
  public void withOldSecurityShouldThrowIfNotAuthorized() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(false);
    doThrow(new NotAuthorizedException("")).when(authorizeRequest)
        .executeFunctionAuthorize(eq(FUNCTION_ID), any(), any(), any(), eq(false));

    executeFunction.cmdExecute(message, serverConnection, securityService, 0);

    verify(chunkedResponseMessage).sendChunk(serverConnection);
    verifyZeroInteractions(serverToClientFunctionResultSenderFactory);
  }
}
