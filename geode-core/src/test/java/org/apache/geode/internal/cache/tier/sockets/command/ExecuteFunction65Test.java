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
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.LowMemoryException;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.operations.ExecuteFunctionOperationContext;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.control.HeapMemoryMonitor;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.execute.InternalFunctionExecutionService;
import org.apache.geode.internal.cache.execute.ServerToClientFunctionResultSender65;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.cache.tier.sockets.ServerSideHandshakeImpl;
import org.apache.geode.internal.cache.tier.sockets.command.ExecuteFunction65.FunctionContextImplFactory;
import org.apache.geode.internal.cache.tier.sockets.command.ExecuteFunction65.ServerToClientFunctionResultSender65Factory;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.internal.security.ResourcePermissions;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.util.internal.GeodeGlossary;

@Category(ClientServerTest.class)
public class ExecuteFunction65Test {

  private static final String FUNCTION = "function";
  private static final String FUNCTION_ID = "function_id";
  private static final boolean OPTIMIZE_FOR_WRITE = false;
  private static final Object CALLBACK_ARG = "arg";
  private static final byte[] RESULT = new byte[] {Integer.valueOf(0).byteValue()};

  private AuthorizeRequest authorizeRequest;
  private ChunkedMessage chunkedResponseMessage;
  private ChunkedMessage functionResponseMessage;
  private Message message;
  private SecurityService securityService;
  private ServerConnection serverConnection;
  private ServerToClientFunctionResultSender65Factory serverToClientFunctionResultSender65Factory;

  private ExecuteFunction65 executeFunction65;

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
    serverToClientFunctionResultSender65Factory =
        mock(ServerToClientFunctionResultSender65Factory.class);

    AcceptorImpl acceptor = mock(AcceptorImpl.class);
    InternalCache cache = mock(InternalCache.class);
    ExecuteFunctionOperationContext executeFunctionOperationContext =
        mock(ExecuteFunctionOperationContext.class);
    Function functionObject = mock(Function.class);
    FunctionContextImplFactory functionContextImplFactory = mock(FunctionContextImplFactory.class);
    HeapMemoryMonitor heapMemoryMonitor = mock(HeapMemoryMonitor.class);
    InternalFunctionExecutionService internalFunctionExecutionService =
        mock(InternalFunctionExecutionService.class);
    InternalResourceManager internalResourceManager = mock(InternalResourceManager.class);
    ServerToClientFunctionResultSender65 serverToClientFunctionResultSender65 =
        mock(ServerToClientFunctionResultSender65.class);

    Part argsPart = mock(Part.class);
    Part callbackArgPart = mock(Part.class);
    Part functionPart = mock(Part.class);
    Part functionStatePart = mock(Part.class);
    Part partPart = mock(Part.class);

    when(authorizeRequest.executeFunctionAuthorize(eq(FUNCTION_ID), eq(null), eq(null), eq(null),
        eq(OPTIMIZE_FOR_WRITE))).thenReturn(executeFunctionOperationContext);

    when(cache.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));
    when(cache.getDistributedSystem()).thenReturn(mock(InternalDistributedSystem.class));
    when(cache.getInternalResourceManager()).thenReturn(internalResourceManager);
    when(cache.getMyId()).thenReturn(mock(InternalDistributedMember.class));

    when(callbackArgPart.getObject()).thenReturn(CALLBACK_ARG);

    when(functionObject.getId()).thenReturn(FUNCTION_ID);
    doCallRealMethod().when(functionObject).getRequiredPermissions(any());
    doCallRealMethod().when(functionObject).getRequiredPermissions(any(), any());

    when(functionPart.getStringOrObject()).thenReturn(FUNCTION);
    when(functionStatePart.getSerializedForm()).thenReturn(RESULT);
    when(heapMemoryMonitor.createLowMemoryIfNeeded(any(), any(DistributedMember.class)))
        .thenReturn(mock(LowMemoryException.class));
    when(internalFunctionExecutionService.getFunction(eq(FUNCTION))).thenReturn(functionObject);
    when(internalResourceManager.getHeapMonitor()).thenReturn(heapMemoryMonitor);

    when(message.getNumberOfParts()).thenReturn(4);
    when(message.getPart(eq(0))).thenReturn(functionStatePart);
    when(message.getPart(eq(1))).thenReturn(functionPart);
    when(message.getPart(eq(2))).thenReturn(argsPart);
    when(message.getPart(eq(3))).thenReturn(partPart);

    when(internalResourceManager.getHeapMonitor()).thenReturn(heapMemoryMonitor);

    when(serverConnection.getAcceptor()).thenReturn(acceptor);
    when(serverConnection.getAuthzRequest()).thenReturn(authorizeRequest);
    when(serverConnection.getCache()).thenReturn(cache);
    when(serverConnection.getCachedRegionHelper()).thenReturn(mock(CachedRegionHelper.class));
    when(serverConnection.getChunkedResponseMessage()).thenReturn(chunkedResponseMessage);
    when(serverConnection.getFunctionResponseMessage()).thenReturn(functionResponseMessage);
    when(serverConnection.getHandshake()).thenReturn(mock(ServerSideHandshakeImpl.class));

    when(serverToClientFunctionResultSender65Factory.create(any(), anyInt(), any(), any(), any()))
        .thenReturn(
            serverToClientFunctionResultSender65);

    executeFunction65 = new ExecuteFunction65(internalFunctionExecutionService,
        serverToClientFunctionResultSender65Factory, functionContextImplFactory);
  }

  @Test
  public void nonSecureShouldSucceed() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(false);

    executeFunction65.cmdExecute(message, serverConnection, securityService, 0);

    verify(serverToClientFunctionResultSender65Factory).create(eq(functionResponseMessage),
        anyInt(), any(), any(), any());
  }

  @Test
  public void withIntegratedSecurityShouldSucceedIfAuthorized() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(true);

    executeFunction65.cmdExecute(message, serverConnection, securityService, 0);

    verify(securityService).authorize(ResourcePermissions.DATA_WRITE);
    verify(serverToClientFunctionResultSender65Factory).create(eq(functionResponseMessage),
        anyInt(), any(), any(), any());
  }

  @Test
  public void withIntegratedSecurityShouldThrowIfNotAuthorized() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(true);
    doThrow(new NotAuthorizedException("")).when(securityService).authorize(Resource.DATA,
        Operation.WRITE);

    executeFunction65.cmdExecute(message, serverConnection, securityService, 0);

    verify(securityService).authorize(ResourcePermissions.DATA_WRITE);
    verify(serverToClientFunctionResultSender65Factory).create(eq(functionResponseMessage),
        anyInt(), any(), any(), any());
  }

  @Test
  public void withOldSecurityShouldSucceedIfAuthorized() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(false);

    executeFunction65.cmdExecute(message, serverConnection, securityService, 0);

    verify(authorizeRequest).executeFunctionAuthorize(eq(FUNCTION_ID), any(), any(), any(),
        eq(false));

    verify(securityService).authorize(ResourcePermissions.DATA_WRITE);
    verify(serverToClientFunctionResultSender65Factory).create(eq(functionResponseMessage),
        anyInt(), any(), any(), any());
  }

  @Test
  public void withOldSecurityShouldThrowIfNotAuthorized() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(false);
    doThrow(new NotAuthorizedException("")).when(authorizeRequest)
        .executeFunctionAuthorize(eq(FUNCTION_ID), any(), any(), any(), eq(false));

    executeFunction65.cmdExecute(message, serverConnection, securityService, 0);

    verify(securityService).authorize(ResourcePermissions.DATA_WRITE);
    verifyNoMoreInteractions(serverToClientFunctionResultSender65Factory);
  }
}
