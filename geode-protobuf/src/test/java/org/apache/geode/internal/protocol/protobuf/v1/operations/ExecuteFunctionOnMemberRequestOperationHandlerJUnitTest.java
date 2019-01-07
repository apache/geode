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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
import org.apache.geode.internal.protocol.protobuf.statistics.ProtobufClientStatistics;
import org.apache.geode.internal.protocol.protobuf.v1.FunctionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.ServerMessageExecutionContext;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.internal.security.ResourcePermissions;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class ExecuteFunctionOnMemberRequestOperationHandlerJUnitTest {
  private static final String TEST_MEMBER1 = "member1";
  private static final String TEST_MEMBER2 = "member2";
  private static final String TEST_FUNCTION_ID = "testFunction";
  public static final String NOT_A_MEMBER = "notAMember";
  private InternalCacheForClientAccess cacheStub;
  private DistributionManager distributionManager;
  private ExecuteFunctionOnMemberRequestOperationHandler operationHandler;
  private ProtobufSerializationService serializationService;
  private TestFunction function;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

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
    cacheStub = mock(InternalCacheForClientAccess.class);
    when(cacheStub.getCacheForProcessingClientRequests()).thenReturn(cacheStub);
    serializationService = new ProtobufSerializationService();
    when(cacheStub.getSecurityService()).thenReturn(mock(SecurityService.class));

    distributionManager = mock(DistributionManager.class);
    when(cacheStub.getDistributionManager()).thenReturn(distributionManager);


    operationHandler = new ExecuteFunctionOnMemberRequestOperationHandler();

    function = new TestFunction();
    FunctionService.registerFunction(function);
  }

  @After
  public void tearDown() throws Exception {
    FunctionService.unregisterFunction(TEST_FUNCTION_ID);
  }

  @Test
  public void failsOnUnknownMember() throws Exception {
    final FunctionAPI.ExecuteFunctionOnMemberRequest request =
        FunctionAPI.ExecuteFunctionOnMemberRequest.newBuilder().setFunctionID(TEST_FUNCTION_ID)
            .addMemberName(NOT_A_MEMBER).build();

    expectedException.expect(IllegalArgumentException.class);
    final Result<FunctionAPI.ExecuteFunctionOnMemberResponse> result =
        operationHandler.process(serializationService, request, mockedMessageExecutionContext());
  }

  @Test
  public void failsIfNoMemberSpecified() throws Exception {
    final FunctionAPI.ExecuteFunctionOnMemberRequest request =
        FunctionAPI.ExecuteFunctionOnMemberRequest.newBuilder().setFunctionID(TEST_FUNCTION_ID)
            .build();

    expectedException.expect(IllegalArgumentException.class);
    final Result<FunctionAPI.ExecuteFunctionOnMemberResponse> result =
        operationHandler.process(serializationService, request, mockedMessageExecutionContext());
  }

  @Test(expected = DistributedSystemDisconnectedException.class)
  public void succeedsWithValidMembers() throws Exception {
    when(distributionManager.getMemberWithName(any(String.class))).thenReturn(
        new InternalDistributedMember("localhost", 0),
        new InternalDistributedMember("localhost", 1), null);

    final FunctionAPI.ExecuteFunctionOnMemberRequest request =
        FunctionAPI.ExecuteFunctionOnMemberRequest.newBuilder().setFunctionID(TEST_FUNCTION_ID)
            .addMemberName(TEST_MEMBER1).addMemberName(TEST_MEMBER2).build();

    final Result<FunctionAPI.ExecuteFunctionOnMemberResponse> result =
        operationHandler.process(serializationService, request, mockedMessageExecutionContext());

    // unfortunately FunctionService fishes for a DistributedSystem and throws an exception
    // if it can't find one. It uses a static method on InternalDistributedSystem, so no
    // mocking is possible. If the test throws DistributedSystemDisconnectedException it
    // means that the operation handler got to the point of trying get an execution
    // context
  }

  @Test
  public void requiresPermissions() throws Exception {
    final FunctionAPI.ExecuteFunctionOnMemberRequest request =
        FunctionAPI.ExecuteFunctionOnMemberRequest.newBuilder().setFunctionID(TEST_FUNCTION_ID)
            .addMemberName(TEST_MEMBER1).build();
    SecurityService securityService = mock(SecurityService.class);
    when(securityService.isIntegratedSecurity()).thenReturn(true);
    doThrow(new NotAuthorizedException("we should catch this")).when(securityService)
        .authorize(Mockito.eq(ResourcePermissions.DATA_WRITE), any());
    ServerMessageExecutionContext context = new ServerMessageExecutionContext(cacheStub,
        mock(ProtobufClientStatistics.class), securityService);;
    expectedException.expect(NotAuthorizedException.class);
    operationHandler.process(serializationService, request, context);
  }

  @Test
  public void functionNotFound() throws Exception {
    final FunctionAPI.ExecuteFunctionOnMemberRequest request =
        FunctionAPI.ExecuteFunctionOnMemberRequest.newBuilder()
            .setFunctionID("I am not a function, I am a human").addMemberName(TEST_MEMBER1).build();

    expectedException.expect(IllegalArgumentException.class);
    final Result<FunctionAPI.ExecuteFunctionOnMemberResponse> result =
        operationHandler.process(serializationService, request, mockedMessageExecutionContext());
  }

  private ServerMessageExecutionContext mockedMessageExecutionContext() {
    return new ServerMessageExecutionContext(cacheStub, mock(ProtobufClientStatistics.class),
        mock(SecurityService.class));
  }
}
