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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.protocol.protobuf.statistics.ProtobufClientStatistics;
import org.apache.geode.internal.protocol.protobuf.v1.FunctionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.ServerMessageExecutionContext;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.internal.security.ResourcePermissions;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.test.junit.categories.ClientServerTest;

/**
 * Unfortunately, we can't test the happy path with a unit test, because the function service is
 * static, and there's mocking function execution is too complicated.
 */
@Category({ClientServerTest.class})
public class ExecuteFunctionOnRegionRequestOperationHandlerJUnitTest {
  private static final String TEST_REGION = "testRegion";
  private static final String TEST_FUNCTION_ID = "testFunction";
  public static final String NOT_A_REGION = "notARegion";
  private InternalCache cacheStub;
  private ExecuteFunctionOnRegionRequestOperationHandler operationHandler;
  private ProtobufSerializationService serializationService;

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
    @SuppressWarnings("unchecked")
    Region<Object, Object> regionStub = mock(LocalRegion.class);
    cacheStub = mock(InternalCacheForClientAccess.class);
    doReturn(cacheStub).when(cacheStub).getCacheForProcessingClientRequests();
    serializationService = new ProtobufSerializationService();

    when(cacheStub.getRegion(TEST_REGION)).thenReturn(regionStub);
    when(cacheStub.getSecurityService()).thenReturn(mock(SecurityService.class));



    operationHandler = new ExecuteFunctionOnRegionRequestOperationHandler();

    TestFunction function = new TestFunction();
    FunctionService.registerFunction(function);
  }

  @After
  public void tearDown() {
    FunctionService.unregisterFunction(TEST_FUNCTION_ID);
  }

  @Test
  public void failsOnUnknownRegion() {
    final FunctionAPI.ExecuteFunctionOnRegionRequest request =
        FunctionAPI.ExecuteFunctionOnRegionRequest.newBuilder().setFunctionID(TEST_FUNCTION_ID)
            .setRegion(NOT_A_REGION).build();

    assertThatThrownBy(() -> operationHandler.process(serializationService, request,
        mockedMessageExecutionContext())).isInstanceOf(RegionDestroyedException.class);
  }

  @Test
  public void requiresPermissions() {
    final FunctionAPI.ExecuteFunctionOnRegionRequest request =
        FunctionAPI.ExecuteFunctionOnRegionRequest.newBuilder().setFunctionID(TEST_FUNCTION_ID)
            .setRegion(TEST_REGION).build();
    SecurityService securityService = mock(SecurityService.class);
    when(securityService.isIntegratedSecurity()).thenReturn(true);
    doThrow(new NotAuthorizedException("we should catch this")).when(securityService)
        .authorize(Mockito.eq(ResourcePermissions.DATA_WRITE), any());
    ServerMessageExecutionContext context = new ServerMessageExecutionContext(cacheStub,
        mock(ProtobufClientStatistics.class), securityService);
    assertThatThrownBy(() -> operationHandler.process(serializationService, request, context))
        .isInstanceOf(NotAuthorizedException.class);
  }

  @Test
  public void functionNotFound() {
    final FunctionAPI.ExecuteFunctionOnRegionRequest request =
        FunctionAPI.ExecuteFunctionOnRegionRequest.newBuilder().setFunctionID(TEST_FUNCTION_ID)
            .setRegion(TEST_REGION).build();

    FunctionService.unregisterFunction(TEST_FUNCTION_ID);

    assertThatThrownBy(() -> operationHandler.process(serializationService, request,
        mockedMessageExecutionContext())).isInstanceOf(IllegalArgumentException.class);

  }

  private ServerMessageExecutionContext mockedMessageExecutionContext() {
    return new ServerMessageExecutionContext(cacheStub, mock(ProtobufClientStatistics.class),
        mock(SecurityService.class));
  }
}
