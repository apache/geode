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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.concurrent.Executor;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.operations.ExecuteFunctionOperationContext;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.OperationExecutors;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.control.HeapMemoryMonitor;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.execute.InternalFunctionExecutionService;
import org.apache.geode.internal.cache.execute.ServerToClientFunctionResultSender65;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.ServerSideHandshake;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.OldClientSupportService;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.cache.tier.sockets.command.ExecuteFunction66.FunctionContextImplFactory;
import org.apache.geode.internal.cache.tier.sockets.command.ExecuteFunction66.ServerToClientFunctionResultSender65Factory;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.internal.security.ResourcePermissions;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category(ClientServerTest.class)
public class ExecuteFunction66Test {

  private static final String FUNCTION = "function";
  private static final String FUNCTION_ID = "function_id";
  private static final boolean OPTIMIZE_FOR_WRITE = false;
  private static final Object CALLBACK_ARG = "arg";
  private static final byte[] RESULT = new byte[] {Integer.valueOf(0).byteValue()};

  private AuthorizeRequest authzRequest;
  private ChunkedMessage functionResponseMessage;
  private Message message;
  private OldClientSupportService oldClientSupportService;
  private SecurityService securityService;
  private ServerConnection serverConnection;

  // the following fields are all accessed in sub-class
  InternalFunctionExecutionService internalFunctionExecutionService;
  ServerToClientFunctionResultSender65Factory serverToClientFunctionResultSender65Factory;
  FunctionContextImplFactory functionContextImplFactory;

  ExecuteFunction66 executeFunction;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setUp() throws Exception {
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "statsDisabled", "true");

    authzRequest = mock(AuthorizeRequest.class);
    functionResponseMessage = mock(ChunkedMessage.class);
    internalFunctionExecutionService = mock(InternalFunctionExecutionService.class);
    message = mock(Message.class);
    oldClientSupportService = mock(OldClientSupportService.class);
    securityService = mock(SecurityService.class);
    serverConnection = mock(ServerConnection.class);

    serverToClientFunctionResultSender65Factory =
        mock(ServerToClientFunctionResultSender65Factory.class);
    functionContextImplFactory = mock(FunctionContextImplFactory.class);

    AcceptorImpl acceptor = mock(AcceptorImpl.class);
    InternalCache cache = mock(InternalCache.class);
    ClusterDistributionManager clusterDistributionManager = mock(ClusterDistributionManager.class);
    Message errorResponseMessage = mock(Message.class);
    ExecuteFunctionOperationContext executeFunctionOperationContext =
        mock(ExecuteFunctionOperationContext.class);
    Function functionObject = mock(Function.class);
    Executor functionExecutor = mock(Executor.class);
    InternalResourceManager internalResourceManager = mock(InternalResourceManager.class);
    Message replyMessage = mock(Message.class);
    ServerToClientFunctionResultSender65 serverToClientFunctionResultSender65 =
        mock(ServerToClientFunctionResultSender65.class);

    Part argsPart = mock(Part.class);
    Part callbackArgPart = mock(Part.class);
    Part functionPart = mock(Part.class);
    Part functionStatePart = mock(Part.class);
    Part partPart = mock(Part.class);

    when(authzRequest.executeFunctionAuthorize(eq(FUNCTION_ID), eq(null), eq(null), eq(null),
        eq(OPTIMIZE_FOR_WRITE))).thenReturn(executeFunctionOperationContext);

    when(cache.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));
    when(cache.getDistributionManager()).thenReturn(clusterDistributionManager);
    when(cache.getDistributedSystem()).thenReturn(mock(InternalDistributedSystem.class));
    when(cache.getInternalResourceManager()).thenReturn(internalResourceManager);
    when(cache.getResourceManager()).thenReturn(internalResourceManager);
    when(cache.getService(eq(OldClientSupportService.class))).thenReturn(oldClientSupportService);

    when(callbackArgPart.getObject()).thenReturn(CALLBACK_ARG);

    OperationExecutors executors = mock(OperationExecutors.class);
    when(clusterDistributionManager.getExecutors()).thenReturn(executors);
    when(executors.getFunctionExecutor()).thenReturn(functionExecutor);

    when(functionObject.getId()).thenReturn(FUNCTION_ID);
    doCallRealMethod().when(functionObject).getRequiredPermissions(any());
    doCallRealMethod().when(functionObject).getRequiredPermissions(any(), any());

    when(functionPart.getStringOrObject()).thenReturn(FUNCTION);
    when(functionStatePart.getSerializedForm()).thenReturn(RESULT);
    when(internalFunctionExecutionService.getFunction(eq(FUNCTION))).thenReturn(functionObject);
    when(internalResourceManager.getHeapMonitor()).thenReturn(mock(HeapMemoryMonitor.class));

    when(message.getNumberOfParts()).thenReturn(4);
    when(message.getPart(eq(0))).thenReturn(functionStatePart);
    when(message.getPart(eq(1))).thenReturn(functionPart);
    when(message.getPart(eq(2))).thenReturn(argsPart);
    when(message.getPart(eq(3))).thenReturn(partPart);

    when(serverConnection.getAcceptor()).thenReturn(acceptor);
    when(serverConnection.getAuthzRequest()).thenReturn(authzRequest);
    when(serverConnection.getCache()).thenReturn(cache);
    when(serverConnection.getCachedRegionHelper()).thenReturn(mock(CachedRegionHelper.class));
    when(serverConnection.getErrorResponseMessage()).thenReturn(errorResponseMessage);
    when(serverConnection.getFunctionResponseMessage()).thenReturn(functionResponseMessage);
    when(serverConnection.getHandshake()).thenReturn(mock(ServerSideHandshake.class));
    when(serverConnection.getReplyMessage()).thenReturn(replyMessage);

    when(serverToClientFunctionResultSender65Factory.create(any(), anyInt(), any(), any(), any()))
        .thenReturn(
            serverToClientFunctionResultSender65);

    executeFunction = new ExecuteFunction66(internalFunctionExecutionService,
        serverToClientFunctionResultSender65Factory, functionContextImplFactory);

    postSetUp();
  }

  void postSetUp() {
    // override in sub-class
  }

  @Test
  public void nonSecureShouldSucceed() throws Exception {
    when(oldClientSupportService.getThrowable(any(), any())).thenReturn(mock(Throwable.class));
    when(securityService.isClientSecurityRequired()).thenReturn(false);

    executeFunction.cmdExecute(message, serverConnection, securityService, 0);

    verify(serverToClientFunctionResultSender65Factory).create(eq(functionResponseMessage),
        anyInt(), any(), any(), any());
  }

  @Test
  public void withIntegratedSecurityShouldSucceedIfAuthorized() throws Exception {
    when(oldClientSupportService.getThrowable(any(), any())).thenReturn(mock(Throwable.class));
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(true);

    executeFunction.cmdExecute(message, serverConnection, securityService, 0);

    verify(securityService).authorize(ResourcePermissions.DATA_WRITE);
    verify(serverToClientFunctionResultSender65Factory).create(eq(functionResponseMessage),
        anyInt(), any(), any(), any());
  }

  @Test
  public void withIntegratedSecurityShouldThrowIfNotAuthorized() {
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(true);
    doThrow(new NotAuthorizedException("")).when(securityService)
        .authorize(ResourcePermissions.DATA_WRITE);

    assertThatThrownBy(() -> executeFunction.cmdExecute(message, serverConnection,
        securityService, 0)).isExactlyInstanceOf(NullPointerException.class);

    verify(securityService).authorize(ResourcePermissions.DATA_WRITE);
    // verify(this.chunkedResponseMessage).sendChunk(this.serverConnection);
    verifyZeroInteractions(serverToClientFunctionResultSender65Factory);
  }

  @Test
  public void withOldSecurityShouldSucceedIfAuthorized() throws Exception {
    when(oldClientSupportService.getThrowable(any(), any())).thenReturn(mock(Throwable.class));
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(false);

    executeFunction.cmdExecute(message, serverConnection, securityService, 0);

    verify(authzRequest).executeFunctionAuthorize(eq(FUNCTION_ID), any(), any(), any(),
        eq(false));
    verify(serverToClientFunctionResultSender65Factory).create(eq(functionResponseMessage),
        anyInt(), any(), any(), any());
  }

  @Test
  public void withOldSecurityShouldThrowIfNotAuthorized() {
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(false);
    doThrow(new NotAuthorizedException("")).when(authzRequest)
        .executeFunctionAuthorize(eq(FUNCTION_ID), any(), any(), any(), eq(false));

    assertThatThrownBy(() -> executeFunction.cmdExecute(message, serverConnection,
        securityService, 0)).isExactlyInstanceOf(NullPointerException.class);

    verify(securityService).authorize(ResourcePermissions.DATA_WRITE);
    verifyZeroInteractions(serverToClientFunctionResultSender65Factory);
  }
}
