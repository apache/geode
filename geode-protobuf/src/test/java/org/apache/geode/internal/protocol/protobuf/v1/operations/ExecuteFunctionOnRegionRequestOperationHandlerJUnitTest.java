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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.protocol.protobuf.statistics.ProtobufClientStatistics;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
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

/**
 * Unfortunately, we can't test the happy path with a unit test, because the function service is
 * static, and there's mocking function execution is too complicated.
 */
@Category(UnitTest.class)
public class ExecuteFunctionOnRegionRequestOperationHandlerJUnitTest {
  private static final String TEST_REGION = "testRegion";
  private static final String TEST_FUNCTION_ID = "testFunction";
  public static final String NOT_A_REGION = "notARegion";
  private Region regionStub;
  private InternalCache cacheStub;
  private ExecuteFunctionOnRegionRequestOperationHandler operationHandler;
  private ProtobufSerializationService serializationService;
  private TestFunction function;

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
  public void setUp() {
    regionStub = mock(LocalRegion.class);
    cacheStub = mock(InternalCache.class);
    serializationService = new ProtobufSerializationService();

    when(cacheStub.getRegion(TEST_REGION)).thenReturn(regionStub);
    when(cacheStub.getSecurityService()).thenReturn(mock(SecurityService.class));



    operationHandler = new ExecuteFunctionOnRegionRequestOperationHandler();

    function = new TestFunction();
    FunctionService.registerFunction(function);
  }

  @After
  public void tearDown() {
    FunctionService.unregisterFunction(TEST_FUNCTION_ID);
  }

  @Test
  public void failsOnUnknownRegion() throws Exception {
    final FunctionAPI.ExecuteFunctionOnRegionRequest request =
        FunctionAPI.ExecuteFunctionOnRegionRequest.newBuilder().setFunctionID(TEST_FUNCTION_ID)
            .setRegion(NOT_A_REGION).build();

    final Result<FunctionAPI.ExecuteFunctionOnRegionResponse> result =
        operationHandler.process(serializationService, request, mockedMessageExecutionContext());

    assertTrue(result instanceof Failure);

    verify(cacheStub).getRegion(NOT_A_REGION);
  }

  @Test
  public void requiresPermissions() throws Exception {
    final SecurityService securityService = mock(SecurityService.class);
    doThrow(new NotAuthorizedException("we should catch this")).when(securityService)
        .authorize(ResourcePermissions.DATA_WRITE);
    when(cacheStub.getSecurityService()).thenReturn(securityService);

    final FunctionAPI.ExecuteFunctionOnRegionRequest request =
        FunctionAPI.ExecuteFunctionOnRegionRequest.newBuilder().setFunctionID(TEST_FUNCTION_ID)
            .setRegion(TEST_REGION).build();
    try {
      operationHandler.process(serializationService, request, mockedMessageExecutionContext());
      fail("Should not have been authorized.");
    } catch (OperationNotAuthorizedException ex) {
      // Expected failure
    }
  }

  @Test
  public void functionNotFound() throws Exception {
    final FunctionAPI.ExecuteFunctionOnRegionRequest request =
        FunctionAPI.ExecuteFunctionOnRegionRequest.newBuilder().setFunctionID(TEST_FUNCTION_ID)
            .setRegion(TEST_REGION).build();

    FunctionService.unregisterFunction(TEST_FUNCTION_ID);

    final Result<FunctionAPI.ExecuteFunctionOnRegionResponse> result =
        operationHandler.process(serializationService, request, mockedMessageExecutionContext());

    final ClientProtocol.ErrorResponse errorMessage = result.getErrorMessage();

    assertEquals(BasicTypes.ErrorCode.INVALID_REQUEST, errorMessage.getError().getErrorCode());
  }

  private ServerMessageExecutionContext mockedMessageExecutionContext() {
    return new ServerMessageExecutionContext(cacheStub, mock(ProtobufClientStatistics.class), null);
  }
}
