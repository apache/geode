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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
import org.apache.geode.internal.protocol.protobuf.statistics.ProtobufClientStatistics;
import org.apache.geode.internal.protocol.protobuf.v1.FunctionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.ServerMessageExecutionContext;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.internal.security.ResourcePermissions;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class ExecuteFunctionOnGroupRequestOperationHandlerJUnitTest {
  private static final String TEST_GROUP1 = "group1";
  private static final String TEST_GROUP2 = "group2";
  private static final String TEST_FUNCTION_ID = "testFunction";
  private InternalCacheForClientAccess cacheStub;
  private DistributionManager distributionManager;
  private ExecuteFunctionOnGroupRequestOperationHandler operationHandler;
  private ProtobufSerializationService serializationService;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private static class TestFunction implements Function<Void> {
    // non-null iff function has been executed.
    private AtomicReference<FunctionContext<Void>> context = new AtomicReference<>();

    @Override
    public String getId() {
      return TEST_FUNCTION_ID;
    }

    @Override
    public void execute(FunctionContext<Void> context) {
      this.context.set(context);
      context.getResultSender().lastResult("result");
    }
  }

  @Before
  public void setUp() {
    cacheStub = mock(InternalCacheForClientAccess.class);
    when(cacheStub.getCacheForProcessingClientRequests()).thenReturn(cacheStub);
    serializationService = new ProtobufSerializationService();
    when(cacheStub.getSecurityService()).thenReturn(mock(SecurityService.class));

    distributionManager = mock(DistributionManager.class);
    when(cacheStub.getDistributionManager()).thenReturn(distributionManager);

    InternalDistributedSystem distributedSystem = mock(InternalDistributedSystem.class);
    when(distributionManager.getSystem()).thenReturn(distributedSystem);

    InternalDistributedMember localhost = new InternalDistributedMember("localhost", 0);
    Set<DistributedMember> members = new HashSet<>();
    members.add(localhost);
    when(distributedSystem.getGroupMembers(TEST_GROUP1)).thenReturn(members);

    operationHandler = new ExecuteFunctionOnGroupRequestOperationHandler();

    TestFunction function = new TestFunction();
    FunctionService.registerFunction(function);
  }

  @After
  public void tearDown() {
    FunctionService.unregisterFunction(TEST_FUNCTION_ID);
  }

  @Test
  public void succeedsWithValidMembers() {
    when(distributionManager.getMemberWithName(any(String.class))).thenReturn(
        new InternalDistributedMember("localhost", 0),
        new InternalDistributedMember("localhost", 1), null);

    final FunctionAPI.ExecuteFunctionOnGroupRequest request =
        FunctionAPI.ExecuteFunctionOnGroupRequest.newBuilder().setFunctionID(TEST_FUNCTION_ID)
            .addGroupName(TEST_GROUP1).addGroupName(TEST_GROUP2).build();

    assertThatThrownBy(() -> operationHandler.process(serializationService, request,
        mockedMessageExecutionContext()))
            .isInstanceOf(DistributedSystemDisconnectedException.class);

    // unfortunately FunctionService fishes for a DistributedSystem and throws an exception
    // if it can't find one. It uses a static method on InternalDistributedSystem, so no
    // mocking is possible. If the test throws DistributedSystemDisconnectedException it
    // means that the operation handler got to the point of trying get an execution
    // context
  }

  @Test
  public void requiresPermissions() {
    final SecurityService securityService = mock(SecurityService.class);
    when(securityService.isIntegratedSecurity()).thenReturn(true);
    doThrow(new NotAuthorizedException("we should catch this")).when(securityService)
        .authorize(eq(ResourcePermissions.DATA_WRITE), any());
    when(cacheStub.getSecurityService()).thenReturn(securityService);

    final FunctionAPI.ExecuteFunctionOnGroupRequest request =
        FunctionAPI.ExecuteFunctionOnGroupRequest.newBuilder().setFunctionID(TEST_FUNCTION_ID)
            .addGroupName(TEST_GROUP1).build();

    ServerMessageExecutionContext context = new ServerMessageExecutionContext(cacheStub,
        mock(ProtobufClientStatistics.class), securityService);
    assertThatThrownBy(() -> operationHandler.process(serializationService, request, context))
        .isInstanceOf(NotAuthorizedException.class);
  }

  @Test
  public void functionNotFound() {
    final FunctionAPI.ExecuteFunctionOnGroupRequest request =
        FunctionAPI.ExecuteFunctionOnGroupRequest.newBuilder()
            .setFunctionID("I am not a function, I am a human").addGroupName(TEST_GROUP1).build();

    assertThatThrownBy(() -> operationHandler.process(serializationService, request,
        mockedMessageExecutionContext())).isInstanceOf(IllegalArgumentException.class);

  }

  private ServerMessageExecutionContext mockedMessageExecutionContext() {
    return new ServerMessageExecutionContext(cacheStub, mock(ProtobufClientStatistics.class),
        mock(SecurityService.class));
  }
}
