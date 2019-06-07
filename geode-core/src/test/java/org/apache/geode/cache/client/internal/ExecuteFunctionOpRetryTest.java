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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.stubbing.OngoingStubbing;

import org.apache.geode.cache.client.NoAvailableServersException;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.client.internal.ExecuteFunctionTestSupport.FailureMode;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.internal.cache.execute.InternalFunctionInvocationTargetException;
import org.apache.geode.internal.cache.execute.MemberMappedArgument;
import org.apache.geode.internal.cache.execute.ServerFunctionExecutor;
import org.apache.geode.test.junit.categories.ClientServerTest;

/**
 * Test retry counts for onServer() function execution (via ExecuteFunctionOp).
 * Convince ourselves they aren't going to ever be infinite!
 */
@Category({ClientServerTest.class})
@RunWith(JUnitParamsRunner.class)
public class ExecuteFunctionOpRetryTest {

  static final boolean IS_FN_SERIALIZATION_REQUIRED = false;
  static final boolean ALL_SERVERS_SETTING = false;

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
  public void executeDoesNotRetryOnServerOperationException(
      final ExecuteFunctionTestSupport.HAStatus haStatus,
      final ExecuteFunctionTestSupport.FunctionIdentifierType functionIdentifierType,
      final int retryAttempts,
      final int expectTries) {

    runExecuteTest(haStatus, functionIdentifierType, retryAttempts, expectTries,
        ExecuteFunctionTestSupport.FailureMode.THROW_SERVER_OPERATION_EXCEPTION);
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
  public void executeWithServerConnectivityException(
      final ExecuteFunctionTestSupport.HAStatus haStatus,
      final ExecuteFunctionTestSupport.FunctionIdentifierType functionIdentifierType,
      final int retryAttempts,
      final int expectTries) {

    runExecuteTest(haStatus, functionIdentifierType, retryAttempts, expectTries,
        ExecuteFunctionTestSupport.FailureMode.THROW_SERVER_CONNECTIVITY_EXCEPTION);
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
  public void executeWithInternalFunctionInvocationTargetExceptionCallsReexecute(
      final ExecuteFunctionTestSupport.HAStatus haStatus,
      final ExecuteFunctionTestSupport.FunctionIdentifierType functionIdentifierType,
      final int retryAttempts,
      final int expectTries) {

    runExecuteTest(haStatus, functionIdentifierType, retryAttempts, expectTries,
        ExecuteFunctionTestSupport.FailureMode.THROW_INTERNAL_FUNCTION_INVOCATION_TARGET_EXCEPTION);
  }

  private void runExecuteTest(final ExecuteFunctionTestSupport.HAStatus haStatus,
      final ExecuteFunctionTestSupport.FunctionIdentifierType functionIdentifierType,
      final int retryAttempts, final int expectTries,
      final ExecuteFunctionTestSupport.FailureMode failureMode) {

    createMocks(haStatus, failureMode);

    executeFunction(haStatus, functionIdentifierType, retryAttempts,
        testSupport.getExecutablePool(), testSupport.getFunction(), serverFunctionExecutor,
        args, memberMappedArg, testSupport.getResultCollector(), userAttributes, groups);

    verify(testSupport.getExecutablePool(), times(expectTries)).execute(
        ArgumentMatchers.<AbstractOp>any(),
        ArgumentMatchers.anyInt());
  }

  private void executeFunction(final ExecuteFunctionTestSupport.HAStatus haStatus,
      final ExecuteFunctionTestSupport.FunctionIdentifierType functionIdentifierType,
      final int retryAttempts,
      final PoolImpl executablePool,
      final Function<Integer> function,
      final ServerFunctionExecutor serverFunctionExecutor,
      Object args,
      MemberMappedArgument memberMappedArg,
      final ResultCollector<Integer, Collection<Integer>> resultCollector,
      UserAttributes userAttributes, String[] groups) {

    when(executablePool.getRetryAttempts()).thenReturn(retryAttempts);

    switch (functionIdentifierType) {
      case STRING:
        ignoreServerConnectivityException(
            () -> ExecuteFunctionOp.execute(executablePool, FUNCTION_NAME, serverFunctionExecutor,
                args, memberMappedArg, ALL_SERVERS_SETTING, FUNCTION_HAS_RESULT, resultCollector,
                IS_FN_SERIALIZATION_REQUIRED, testSupport.toBoolean(haStatus),
                OPTIMIZE_FOR_WRITE_SETTING, userAttributes, groups));
        break;

      case OBJECT_REFERENCE:
        ignoreServerConnectivityException(
            () -> ExecuteFunctionOp.execute(executablePool, function, serverFunctionExecutor, args,
                memberMappedArg, ALL_SERVERS_SETTING, FUNCTION_HAS_RESULT, resultCollector,
                IS_FN_SERIALIZATION_REQUIRED, userAttributes, groups));
        break;
      default:
        throw new AssertionError("unknown FunctionIdentifierType type: " + functionIdentifierType);
    }

  }

  @SuppressWarnings("unchecked")
  private void createMocks(final ExecuteFunctionTestSupport.HAStatus haStatus,
      final FailureMode failureModeArg) {

    testSupport = new ExecuteFunctionTestSupport(haStatus, failureModeArg,
        (final PoolImpl pool, final FailureMode failureMode) -> {
          /*
           * We know execute() handles three kinds of exception from the pool:
           *
           * InternalFunctionInvocationTargetException
           *
           * keep trying without regard to retry attempt limit
           *
           * ServerOperationException | NoAvailableServersException
           *
           * re-throw
           *
           * ServerConnectivityException
           *
           * keep trying up to retry attempt limit
           */
          final OngoingStubbing<Object> when = when(pool
              .execute(ArgumentMatchers.<AbstractOp>any(), ArgumentMatchers.anyInt()));
          switch (failureModeArg) {
            case NO_FAILURE:
              when.thenReturn(null);
              break;
            case THROW_SERVER_CONNECTIVITY_EXCEPTION:
              when.thenThrow(new ServerConnectivityException("testing"));
              break;
            case THROW_SERVER_OPERATION_EXCEPTION:
              when.thenThrow(new ServerOperationException("testing"));
              break;
            case THROW_NO_AVAILABLE_SERVERS_EXCEPTION:
              when.thenThrow(new NoAvailableServersException("testing"));
              break;
            case THROW_INTERNAL_FUNCTION_INVOCATION_TARGET_EXCEPTION:
              /*
               * The product assumes that InternalFunctionInvocationTargetException will only be
               * encountered
               * when the target cache is going down. That condition is transient so the product
               * assume
               * it's
               * ok to keep trying forever. In order to test this situation (and for the test to not
               * hang)
               * we throw this exception first, then we throw ServerConnectivityException
               */
              when.thenThrow(new InternalFunctionInvocationTargetException("testing"))
                  .thenThrow(new ServerConnectivityException("testing"));
              break;
            default:
              throw new AssertionError("unknown FailureMode type: " + failureModeArg);
          }
        });
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
    } catch (final ServerConnectivityException e) {
      // ok
    }
  }

}
