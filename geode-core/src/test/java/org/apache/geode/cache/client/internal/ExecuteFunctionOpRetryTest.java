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
package org.apache.geode.cache.client.internal;

import static org.apache.geode.cache.client.internal.ExecuteFunctionTestSupport.FUNCTION_HAS_RESULT;
import static org.apache.geode.cache.client.internal.ExecuteFunctionTestSupport.FUNCTION_NAME;
import static org.apache.geode.cache.client.internal.ExecuteFunctionTestSupport.OPTIMIZE_FOR_WRITE_SETTING;
import static org.apache.geode.internal.cache.execute.AbstractExecution.DEFAULT_CLIENT_FUNCTION_TIMEOUT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;

import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;

import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.internal.ExecuteFunctionTestSupport.FailureMode;
import org.apache.geode.cache.client.internal.ExecuteFunctionTestSupport.FunctionIdentifierType;
import org.apache.geode.cache.client.internal.ExecuteFunctionTestSupport.HAStatus;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.internal.cache.execute.MemberMappedArgument;
import org.apache.geode.internal.cache.execute.ServerFunctionExecutor;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

/**
 * Test retry counts for onServer() function execution (via ExecuteFunctionOp).
 * Convince ourselves they aren't going to ever be infinite!
 */
@Category({ClientServerTest.class})
@RunWith(GeodeParamsRunner.class)
public class ExecuteFunctionOpRetryTest {

  private static final boolean IS_FN_SERIALIZATION_REQUIRED = false;
  private static final boolean ALL_SERVERS_SETTING = false;

  private ExecuteFunctionTestSupport testSupport;
  private Object args;
  private MemberMappedArgument memberMappedArg;
  private UserAttributes userAttributes;
  private String[] groups;
  private ServerFunctionExecutor serverFunctionExecutor;

  @Test
  @Parameters({
      "NOT_HA, OBJECT_REFERENCE, -1, 1",
      "NOT_HA, OBJECT_REFERENCE, 0, 1",
      "NOT_HA, OBJECT_REFERENCE, 3, 1",
      "NOT_HA, STRING, -1, 1",
      "NOT_HA, STRING, 0, 1",
      "NOT_HA, STRING, 3, 1",
      "HA, OBJECT_REFERENCE, -1, 1",
      "HA, OBJECT_REFERENCE, 0, 1",
      "HA, OBJECT_REFERENCE, 3, 1",
      "HA, STRING, -1, 1",
      "HA, STRING, 0, 1",
      "HA, STRING, 3, 1",
  })
  @TestCaseName("[{index}] {method}: {params}")
  public void executeDoesNotRetryOnServerOperationException(
      final HAStatus haStatus,
      final FunctionIdentifierType functionIdentifierType,
      final int retryAttempts,
      final int expectTries) {

    runExecuteTest(haStatus, functionIdentifierType, retryAttempts, expectTries,
        FailureMode.THROW_SERVER_OPERATION_EXCEPTION);
  }

  @Test
  @Parameters({
      "NOT_HA, OBJECT_REFERENCE, -1, 1",
      "NOT_HA, OBJECT_REFERENCE, 0, 1",
      "NOT_HA, OBJECT_REFERENCE, 3, 1",
      "NOT_HA, STRING, -1, 1",
      "NOT_HA, STRING, 0, 1",
      "NOT_HA, STRING, 3, 1",
      "HA, OBJECT_REFERENCE, -1, 2",
      "HA, OBJECT_REFERENCE, 0, 1",
      "HA, OBJECT_REFERENCE, 3, 4",
      "HA, STRING, -1, 2",
      "HA, STRING, 0, 1",
      "HA, STRING, 3, 4",
  })
  @TestCaseName("[{index}] {method}: {params}")
  public void executeReTriesOnServerConnectivityException(
      final HAStatus haStatus,
      final FunctionIdentifierType functionIdentifierType,
      final int retryAttempts,
      final int expectTries) {

    runExecuteTest(haStatus, functionIdentifierType, retryAttempts, expectTries,
        FailureMode.THROW_SERVER_CONNECTIVITY_EXCEPTION);
  }

  @Test
  @Parameters({
      "NOT_HA, OBJECT_REFERENCE, -1, 1",
      "NOT_HA, OBJECT_REFERENCE, 0, 1",
      "NOT_HA, OBJECT_REFERENCE, 3, 1",
      "NOT_HA, STRING, -1, 1",
      "NOT_HA, STRING, 0, 1",
      "NOT_HA, STRING, 3, 1",
      "HA, OBJECT_REFERENCE, -1, 3",
      "HA, OBJECT_REFERENCE, 0, 2",
      "HA, OBJECT_REFERENCE, 3, 5",
      "HA, STRING, -1, 3",
      "HA, STRING, 0, 2",
      "HA, STRING, 3, 5",
  })
  @TestCaseName("[{index}] {method}: {params}")
  public void executeReTriesOnInternalFunctionInvocationTargetException(
      final HAStatus haStatus,
      final FunctionIdentifierType functionIdentifierType,
      final int retryAttempts,
      final int expectTries) {

    runExecuteTest(haStatus, functionIdentifierType, retryAttempts, expectTries,
        FailureMode.THROW_INTERNAL_FUNCTION_INVOCATION_TARGET_EXCEPTION);
  }

  private void runExecuteTest(final HAStatus haStatus,
      final FunctionIdentifierType functionIdentifierType,
      final int retryAttempts, final int expectTries,
      final FailureMode failureMode) {

    createMocks(haStatus, failureMode, retryAttempts);

    executeFunction(haStatus, functionIdentifierType,
        testSupport.getExecutablePool(), testSupport.getFunction(), serverFunctionExecutor,
        args, memberMappedArg, testSupport.getResultCollector(), userAttributes, groups);

    verify(testSupport.getExecutablePool(), times(expectTries)).execute(
        ArgumentMatchers.<AbstractOp>any(),
        ArgumentMatchers.anyInt());
  }

  private void executeFunction(final HAStatus haStatus,
      final FunctionIdentifierType functionIdentifierType,
      final PoolImpl executablePool,
      final Function<Integer> function,
      final ServerFunctionExecutor serverFunctionExecutor,
      final Object args,
      final MemberMappedArgument memberMappedArg,
      final ResultCollector<Integer, Collection<Integer>> resultCollector,
      final UserAttributes userAttributes, final String[] groups) {

    switch (functionIdentifierType) {
      case STRING:
        ignoreServerConnectivityException(
            () -> ExecuteFunctionOp.execute(executablePool,
                ALL_SERVERS_SETTING, resultCollector,
                testSupport.toBoolean(haStatus),
                userAttributes, groups,
                new ExecuteFunctionOp.ExecuteFunctionOpImpl(FUNCTION_NAME, args, memberMappedArg,
                    FUNCTION_HAS_RESULT,
                    resultCollector, IS_FN_SERIALIZATION_REQUIRED, testSupport.toBoolean(haStatus),
                    OPTIMIZE_FOR_WRITE_SETTING, (byte) 0, groups, ALL_SERVERS_SETTING,
                    serverFunctionExecutor.isIgnoreDepartedMembers(),
                    DEFAULT_CLIENT_FUNCTION_TIMEOUT),
                () -> new ExecuteFunctionOp.ExecuteFunctionOpImpl(FUNCTION_NAME, args,
                    memberMappedArg, FUNCTION_HAS_RESULT,
                    resultCollector, IS_FN_SERIALIZATION_REQUIRED, testSupport.toBoolean(haStatus),
                    OPTIMIZE_FOR_WRITE_SETTING, (byte) 0,
                    null/* onGroups does not use single-hop for now */, false, false,
                    DEFAULT_CLIENT_FUNCTION_TIMEOUT),
                () -> new ExecuteFunctionOp.ExecuteFunctionOpImpl(FUNCTION_NAME, args,
                    serverFunctionExecutor.getMemberMappedArgument(),
                    FUNCTION_HAS_RESULT, resultCollector, IS_FN_SERIALIZATION_REQUIRED,
                    testSupport.toBoolean(haStatus), OPTIMIZE_FOR_WRITE_SETTING, (byte) 1,
                    groups, ALL_SERVERS_SETTING, serverFunctionExecutor.isIgnoreDepartedMembers(),
                    DEFAULT_CLIENT_FUNCTION_TIMEOUT)));
        break;

      case OBJECT_REFERENCE:
        ignoreServerConnectivityException(
            () -> ExecuteFunctionOp.execute(executablePool,
                ALL_SERVERS_SETTING, resultCollector,
                function.isHA(), userAttributes, groups,
                new ExecuteFunctionOp.ExecuteFunctionOpImpl(function, args, memberMappedArg,
                    resultCollector,
                    IS_FN_SERIALIZATION_REQUIRED, (byte) 0, groups, ALL_SERVERS_SETTING,
                    serverFunctionExecutor
                        .isIgnoreDepartedMembers(),
                    DEFAULT_CLIENT_FUNCTION_TIMEOUT),
                () -> new ExecuteFunctionOp.ExecuteFunctionOpImpl(function, args, memberMappedArg,
                    resultCollector, IS_FN_SERIALIZATION_REQUIRED, (byte) 0,
                    null/* onGroups does not use single-hop for now */,
                    false, false, DEFAULT_CLIENT_FUNCTION_TIMEOUT),
                () -> new ExecuteFunctionOp.ExecuteFunctionOpImpl(function,
                    serverFunctionExecutor.getArguments(),
                    serverFunctionExecutor.getMemberMappedArgument(),
                    resultCollector,
                    IS_FN_SERIALIZATION_REQUIRED, (byte) 1, groups, ALL_SERVERS_SETTING,
                    serverFunctionExecutor.isIgnoreDepartedMembers(),
                    DEFAULT_CLIENT_FUNCTION_TIMEOUT)));
        break;
      default:
        throw new AssertionError("unknown FunctionIdentifierType type: " + functionIdentifierType);
    }

  }

  private void createMocks(final HAStatus haStatus,
      final FailureMode failureModeArg,
      final int retryAttempts) {

    testSupport = new ExecuteFunctionTestSupport(haStatus, failureModeArg,
        (pool, failureMode) -> ExecuteFunctionTestSupport.thenThrow(when(pool
            .execute(ArgumentMatchers.<AbstractOp>any(), ArgumentMatchers.anyInt())),
            failureMode),
        retryAttempts);

    args = null;
    memberMappedArg = mock(MemberMappedArgument.class);
    userAttributes = mock(UserAttributes.class);
    groups = new String[] {};
    serverFunctionExecutor = mock(ServerFunctionExecutor.class);
  }

  /*
   * We could be pedantic about this exception but that's not the purpose of the retryTest()
   */
  private void ignoreServerConnectivityException(final Runnable runnable) {
    try {
      runnable.run();
    } catch (final ServerConnectivityException ignored) {
    }
  }

}
