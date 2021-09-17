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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

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
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.execute.ServerRegionFunctionExecutor;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

/**
 * Test retry counts on single-hop onRegion() function execution (via
 * ExecuteRegionFunctionSingleHop).
 * Ensure they are never infinite!
 */
@Category({ClientServerTest.class})
@RunWith(GeodeParamsRunner.class)

public class ExecuteRegionFunctionSingleHopOpRetryTest {

  private Map<ServerLocation, HashSet<Integer>> serverToFilterMap;
  private ExecuteFunctionTestSupport testSupport;

  @Test
  @Parameters({
      "NOT_HA, OBJECT_REFERENCE, -1, 2",
      "NOT_HA, OBJECT_REFERENCE, 0, 2",
      "NOT_HA, OBJECT_REFERENCE, 3, 2",
      "NOT_HA, STRING, -1, 2",
      "NOT_HA, STRING, 0, 2",
      "NOT_HA, STRING, 3, 2",
      "HA, OBJECT_REFERENCE, -1, 2",
      "HA, OBJECT_REFERENCE, 0, 2",
      "HA, OBJECT_REFERENCE, 3, 2",
      "HA, STRING, -1, 2",
      "HA, STRING, 0, 2",
      "HA, STRING, 3, 2",
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
      "NOT_HA, OBJECT_REFERENCE, -1, 2",
      "NOT_HA, OBJECT_REFERENCE, 0, 2",
      "NOT_HA, OBJECT_REFERENCE, 3, 2",
      "NOT_HA, STRING, -1, 2",
      "NOT_HA, STRING, 0, 2",
      "NOT_HA, STRING, 3, 2",
      "HA, OBJECT_REFERENCE, -1, 2",
      "HA, OBJECT_REFERENCE, 0, 2",
      "HA, OBJECT_REFERENCE, 3, 2",
      "HA, STRING, -1, 2",
      "HA, STRING, 0, 2",
      "HA, STRING, 3, 2",
  })
  @TestCaseName("[{index}] {method}: {params}")
  public void executeDoesNotRetryOnNoServerAvailableException(
      final HAStatus haStatus,
      final FunctionIdentifierType functionIdentifierType,
      final int retryAttempts,
      final int expectTries) {

    runExecuteTest(haStatus, functionIdentifierType, retryAttempts, expectTries,
        FailureMode.THROW_NO_AVAILABLE_SERVERS_EXCEPTION);
  }

  @Test
  @Parameters({
      "NOT_HA, OBJECT_REFERENCE, -1, 2",
      "NOT_HA, OBJECT_REFERENCE, 0, 2",
      "NOT_HA, OBJECT_REFERENCE, 3, 2",
      "NOT_HA, STRING, -1, 2",
      "NOT_HA, STRING, 0, 2",
      "NOT_HA, STRING, 3, 2",
      "HA, OBJECT_REFERENCE, -1, 2",
      "HA, OBJECT_REFERENCE, 0, 2",
      "HA, OBJECT_REFERENCE, 3, 2",
      "HA, STRING, -1, 2",
      "HA, STRING, 0, 2",
      "HA, STRING, 3, 2",
  })
  @TestCaseName("[{index}] {method}: {params}")
  public void executeWithServerConnectivityException(
      final HAStatus haStatus,
      final FunctionIdentifierType functionIdentifierType,
      final int retryAttempts,
      final int expectTries) {

    runExecuteTest(haStatus, functionIdentifierType, retryAttempts, expectTries,
        FailureMode.THROW_SERVER_CONNECTIVITY_EXCEPTION);
  }

  private void createMocks(final HAStatus haStatus,
      final FailureMode failureModeArg, Integer retryAttempts) {

    testSupport = new ExecuteFunctionTestSupport(haStatus, failureModeArg,
        (pool, failureMode) -> ExecuteFunctionTestSupport.thenThrow(when(
            pool.executeOn(
                ArgumentMatchers.any(),
                ArgumentMatchers.any(),
                ArgumentMatchers.anyBoolean(),
                ArgumentMatchers.anyBoolean())),
            failureMode),
        retryAttempts);

    serverToFilterMap = new HashMap<>();
    serverToFilterMap.put(new ServerLocation("host1", 10), new HashSet<>());
    serverToFilterMap.put(new ServerLocation("host2", 10), new HashSet<>());
  }

  private void runExecuteTest(final HAStatus haStatus,
      final FunctionIdentifierType functionIdentifierType,
      final int retryAttempts, final int expectTries,
      final FailureMode failureMode) {

    createMocks(haStatus, failureMode, retryAttempts);

    executeFunctionSingleHopAndValidate(haStatus, functionIdentifierType, retryAttempts,
        testSupport.getExecutablePool(),
        testSupport.getFunction(),
        testSupport.getExecutor(), testSupport.getResultCollector(), expectTries);
  }

  private void executeFunctionSingleHopAndValidate(
      final HAStatus haStatus,
      final FunctionIdentifierType functionIdentifierType,
      final int retryAttempts,
      final PoolImpl executablePool,
      final Function<Integer> function,
      final ServerRegionFunctionExecutor executor,
      final ResultCollector<Integer, Collection<Integer>> resultCollector,
      final int expectTries) {

    switch (functionIdentifierType) {
      case STRING:
        ignoreServerConnectivityException(
            () -> ignoreServerConnectivityException(() -> ExecuteRegionFunctionSingleHopOp.execute(
                executablePool, testSupport.getRegion(),
                executor, resultCollector, serverToFilterMap,
                testSupport.toBoolean(haStatus),
                executor1 -> new ExecuteRegionFunctionSingleHopOp.ExecuteRegionFunctionSingleHopOpImpl(
                    testSupport.getRegion().getFullPath(), FUNCTION_NAME,
                    executor1, resultCollector,
                    FUNCTION_HAS_RESULT, new HashSet<>(),
                    ExecuteFunctionTestSupport.ALL_BUCKETS_SETTING, testSupport.toBoolean(haStatus),
                    OPTIMIZE_FOR_WRITE_SETTING, DEFAULT_CLIENT_FUNCTION_TIMEOUT),
                () -> new ExecuteRegionFunctionOp.ExecuteRegionFunctionOpImpl(
                    testSupport.getRegion().getFullPath(), FUNCTION_NAME,
                    executor, resultCollector, FUNCTION_HAS_RESULT, testSupport.toBoolean(haStatus),
                    OPTIMIZE_FOR_WRITE_SETTING, true, DEFAULT_CLIENT_FUNCTION_TIMEOUT))));
        break;
      case OBJECT_REFERENCE:
        ignoreServerConnectivityException(
            () -> ExecuteRegionFunctionSingleHopOp.execute(executablePool, testSupport.getRegion(),
                executor, resultCollector, serverToFilterMap,
                function.isHA(),
                executor1 -> new ExecuteRegionFunctionSingleHopOp.ExecuteRegionFunctionSingleHopOpImpl(
                    testSupport.getRegion().getFullPath(), function,
                    executor1, resultCollector,
                    FUNCTION_HAS_RESULT, new HashSet<>(),
                    ExecuteFunctionTestSupport.ALL_BUCKETS_SETTING,
                    DEFAULT_CLIENT_FUNCTION_TIMEOUT),
                () -> new ExecuteRegionFunctionOp.ExecuteRegionFunctionOpImpl(
                    testSupport.getRegion().getFullPath(), function,
                    executor, resultCollector, DEFAULT_CLIENT_FUNCTION_TIMEOUT)));
        break;
      default:
        throw new AssertionError("unknown FunctionIdentifierType type: " + functionIdentifierType);
    }

    verify(executablePool, times(expectTries))
        .executeOn(
            ArgumentMatchers.any(),
            ArgumentMatchers.any(),
            ArgumentMatchers.anyBoolean(),
            ArgumentMatchers.anyBoolean());
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
