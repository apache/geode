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
package org.apache.geode.internal.protocol.protobuf.v1.operations;

import static org.apache.geode.internal.protocol.protobuf.v1.BasicTypes.ErrorCode.AUTHORIZATION_FAILED;
import static org.apache.geode.internal.protocol.protobuf.v1.BasicTypes.ErrorCode.INVALID_REQUEST;
import static org.apache.geode.internal.protocol.protobuf.v1.BasicTypes.ErrorCode.NO_AVAILABLE_SERVER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.protocol.protobuf.statistics.ProtobufClientStatistics;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.Failure;
import org.apache.geode.internal.protocol.protobuf.v1.FunctionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.ServerMessageExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.state.exception.OperationNotAuthorizedException;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.internal.security.ResourcePermissions;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class ExecuteFunctionOnGroupRequestOperationHandlerJUnitTest {
  private static final String TEST_GROUP1 = "group1";
  private static final String TEST_GROUP2 = "group2";
  private static final String TEST_FUNCTION_ID = "testFunction";
  public static final String NOT_A_GROUP = "notAGroup";
  private InternalCache cacheStub;
  private DistributionManager distributionManager;
  private ExecuteFunctionOnGroupRequestOperationHandler operationHandler;
  private ProtobufSerializationService serializationService;
  private TestFunction function;
  private InternalDistributedSystem distributedSystem;

  private static class TestFunction implements Function {
    // non-null iff function has been executed.
    private AtomicReference<FunctionContext> context = new AtomicReference<>();

    @Override
    public String getId() {
      return TEST_FUNCTION_ID;
    }

    @Override
    public void execute(FunctionContext context) {
      this.context.set(context);
      context.getResultSender().lastResult("result");
    }

    FunctionContext getContext() {
      return context.get();
    }
  }

  @Before
  public void setUp() throws Exception {
    cacheStub = mock(InternalCache.class);
    serializationService = new ProtobufSerializationService();
    when(cacheStub.getSecurityService()).thenReturn(mock(SecurityService.class));

    distributionManager = mock(DistributionManager.class);
    when(cacheStub.getDistributionManager()).thenReturn(distributionManager);

    distributedSystem = mock(InternalDistributedSystem.class);
    when(distributionManager.getSystem()).thenReturn(distributedSystem);

    InternalDistributedMember localhost = new InternalDistributedMember("localhost", 0);
    Set<DistributedMember> members = new HashSet<>();
    members.add(localhost);
    when(distributedSystem.getGroupMembers(TEST_GROUP1)).thenReturn(members);

    operationHandler = new ExecuteFunctionOnGroupRequestOperationHandler();

    function = new TestFunction();
    FunctionService.registerFunction(function);
  }

  @After
  public void tearDown() throws Exception {
    FunctionService.unregisterFunction(TEST_FUNCTION_ID);
  }

  @Test
  public void failsOnUnknownGroup() throws Exception {
    final FunctionAPI.ExecuteFunctionOnGroupRequest request =
        FunctionAPI.ExecuteFunctionOnGroupRequest.newBuilder().setFunctionID(TEST_FUNCTION_ID)
            .addGroupName(NOT_A_GROUP).build();

    final Result<FunctionAPI.ExecuteFunctionOnGroupResponse> result =
        operationHandler.process(serializationService, request, mockedMessageExecutionContext());

    assertTrue(result instanceof Failure);
    assertEquals(NO_AVAILABLE_SERVER, result.getErrorMessage().getError().getErrorCode());
  }

  @Test
  public void failsIfNoGroupSpecified() throws Exception {
    final FunctionAPI.ExecuteFunctionOnGroupRequest request =
        FunctionAPI.ExecuteFunctionOnGroupRequest.newBuilder().setFunctionID(TEST_FUNCTION_ID)
            .build();

    final Result<FunctionAPI.ExecuteFunctionOnGroupResponse> result =
        operationHandler.process(serializationService, request, mockedMessageExecutionContext());

    assertTrue(result instanceof Failure);
    assertEquals(NO_AVAILABLE_SERVER, result.getErrorMessage().getError().getErrorCode());
  }

  @Test(expected = DistributedSystemDisconnectedException.class)
  public void succeedsWithValidMembers() throws Exception {
    when(distributionManager.getMemberWithName(any(String.class))).thenReturn(
        new InternalDistributedMember("localhost", 0),
        new InternalDistributedMember("localhost", 1), null);

    final FunctionAPI.ExecuteFunctionOnGroupRequest request =
        FunctionAPI.ExecuteFunctionOnGroupRequest.newBuilder().setFunctionID(TEST_FUNCTION_ID)
            .addGroupName(TEST_GROUP1).addGroupName(TEST_GROUP2).build();

    final Result<FunctionAPI.ExecuteFunctionOnGroupResponse> result =
        operationHandler.process(serializationService, request, mockedMessageExecutionContext());

    // unfortunately FunctionService fishes for a DistributedSystem and throws an exception
    // if it can't find one. It uses a static method on InternalDistributedSystem, so no
    // mocking is possible. If the test throws DistributedSystemDisconnectedException it
    // means that the operation handler got to the point of trying get an execution
    // context
  }

  @Test
  public void requiresPermissions() throws Exception {
    final SecurityService securityService = mock(SecurityService.class);
    doThrow(new NotAuthorizedException("we should catch this")).when(securityService)
        .authorize(ResourcePermissions.DATA_WRITE);
    when(cacheStub.getSecurityService()).thenReturn(securityService);

    final FunctionAPI.ExecuteFunctionOnGroupRequest request =
        FunctionAPI.ExecuteFunctionOnGroupRequest.newBuilder().setFunctionID(TEST_FUNCTION_ID)
            .addGroupName(TEST_GROUP1).build();

    try {
      operationHandler.process(serializationService, request, mockedMessageExecutionContext());
      fail("Should not have been authorized.");
    } catch (OperationNotAuthorizedException ex) {
      // Expected failure
    }
  }

  @Test
  public void functionNotFound() throws Exception {
    final FunctionAPI.ExecuteFunctionOnGroupRequest request =
        FunctionAPI.ExecuteFunctionOnGroupRequest.newBuilder()
            .setFunctionID("I am not a function, I am a human").addGroupName(TEST_GROUP1).build();

    final Result<FunctionAPI.ExecuteFunctionOnGroupResponse> result =
        operationHandler.process(serializationService, request, mockedMessageExecutionContext());

    final ClientProtocol.ErrorResponse errorMessage = result.getErrorMessage();

    assertEquals(INVALID_REQUEST, errorMessage.getError().getErrorCode());
  }

  private ServerMessageExecutionContext mockedMessageExecutionContext() {
    return new ServerMessageExecutionContext(cacheStub, mock(ProtobufClientStatistics.class), null);
  }
}
