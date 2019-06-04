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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.sun.tools.javac.util.List;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;

import org.apache.geode.cache.client.NoAvailableServersException;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.client.internal.ExecuteFunctionTestSupport.FailureMode;
import org.apache.geode.cache.client.internal.ExecuteFunctionTestSupport.FunctionIdentifierType;
import org.apache.geode.cache.client.internal.ExecuteFunctionTestSupport.HAStatus;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.execute.InternalFunctionInvocationTargetException;
import org.apache.geode.internal.cache.execute.ServerRegionFunctionExecutor;
import org.apache.geode.test.junit.categories.ClientServerTest;

/**
 * Test retry counts on single-hop on region function execution.
 * Ensure they are never infinite!
 */
@Category({ClientServerTest.class})
@RunWith(JUnitParamsRunner.class)

public class ExecuteRegionFunctionSingleHopOpTest {

  private SingleHopClientExecutor singleHopClientExecutor;
  private Map<ServerLocation, HashSet<Integer>> serverToFilterMap;
  private ExecuteFunctionTestSupport testSupport;
  private ExecutorService executorService;

  @SuppressWarnings("unchecked")
  private void createMocks(final HAStatus haStatus,
      final FailureMode failureMode)
      throws ExecutionException, InterruptedException {

    testSupport = new ExecuteFunctionTestSupport(haStatus, failureMode);

    final Future<Integer> future = (Future<Integer>) mock(Future.class);

    switch (failureMode) {
      case NO_FAILURE:
        when(future.get()).thenReturn(1);
        break;
      case THROW_SERVER_CONNECTIVITY_EXCEPTION:
        when(future.get()).thenThrow(new ServerConnectivityException("testing"));
        break;
      case THROW_SERVER_OPERATION_EXCEPTION:
        when(future.get()).thenThrow(new ServerOperationException("testing"));
        break;
      case THROW_NO_AVAILABLE_SERVERS_EXCEPTION:
        when(future.get()).thenThrow(new NoAvailableServersException("testing"));
        break;
      case THROW_INTERNAL_FUNCTION_INVOCATION_TARGET_EXCEPTION:
        /*
         * The product assumes that InternalFunctionInvocationTargetException will only be
         * encountered
         * when the target cache is going down. That condition is transient so the product assume
         * it's
         * ok to keep trying forever. In order to test this situation (and for the test to not hang)
         * we throw this exception first, then we throw ServerConnectivityException
         */
        when(future.get()).thenThrow(new InternalFunctionInvocationTargetException("testing"))
            .thenThrow(new ServerConnectivityException("testing"));
        break;
      default:
        throw new AssertionError("unknown FailureMode type: " + failureMode);
    }

    executorService = mock(ExecutorService.class);
    when(executorService
        .invokeAll(ArgumentMatchers.<Collection<? extends Callable<Integer>>>any()))
            .thenReturn(List.of(future));

    singleHopClientExecutor = new SingleHopClientExecutorImpl(executorService);

    serverToFilterMap = new HashMap<>();
    serverToFilterMap.put(new ServerLocation("host1", 10), new HashSet<>());
    serverToFilterMap.put(new ServerLocation("host2", 10), new HashSet<>());
  }

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
      final HAStatus haStatus,
      final FunctionIdentifierType functionIdentifierType,
      final int retryAttempts,
      final int expectTries) throws ExecutionException, InterruptedException {

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
      "HA, OBJECT_REFERENCE, -1, 1",
      "HA, OBJECT_REFERENCE, 0, 1",
      "HA, OBJECT_REFERENCE, 3, 1",
      "HA, STRING, -1, 1",
      "HA, STRING, 0, 1",
      "HA, STRING, 3, 1",
  })
  public void executeDoesNotRetryOnNoServerAvailableException(
      final HAStatus haStatus,
      final FunctionIdentifierType functionIdentifierType,
      final int retryAttempts,
      final int expectTries) throws ExecutionException, InterruptedException {

    runExecuteTest(haStatus, functionIdentifierType, retryAttempts, expectTries,
        FailureMode.THROW_NO_AVAILABLE_SERVERS_EXCEPTION);
  }

  @Test
  @Parameters({
      // "NOT_HA, OBJECT_REFERENCE, -1, 1",
      // "NOT_HA, OBJECT_REFERENCE, 0, 1",
      // "NOT_HA, OBJECT_REFERENCE, 3, 1",
      // "NOT_HA, STRING, -1, 1",
      // "NOT_HA, STRING, 0, 1",
      // "NOT_HA, STRING, 3, 1",
      // "HA, OBJECT_REFERENCE, -1, 2",
      // "HA, OBJECT_REFERENCE, 0, 1",
      "HA, OBJECT_REFERENCE, 3, 4", // no interactions w/ pool
      // "HA, STRING, -1, 2",
      // "HA, STRING, 0, 1",
      // "HA, STRING, 3, 4", // no interactions w/ pool
  })
  public void executeWithServerConnectivityException(
      final HAStatus haStatus,
      final FunctionIdentifierType functionIdentifierType,
      final int retryAttempts,
      final int expectTries) throws ExecutionException, InterruptedException {

    runExecuteTest(haStatus, functionIdentifierType, retryAttempts, expectTries,
        FailureMode.THROW_SERVER_CONNECTIVITY_EXCEPTION);
  }

  private void runExecuteTest(final HAStatus haStatus,
      final FunctionIdentifierType functionIdentifierType,
      final int retryAttempts, final int expectTries,
      final FailureMode failureMode)
      throws ExecutionException, InterruptedException {
    createMocks(haStatus, failureMode);

    executeFunctionSingleHopAndValidate(haStatus, functionIdentifierType, retryAttempts,
        testSupport.getExecutablePool(),
        testSupport.getFunction(),
        testSupport.getExecutor(), testSupport.getResultCollector(), expectTries);
  }

  private void executeFunctionSingleHopAndValidate(final HAStatus haStatus,
      final FunctionIdentifierType functionIdentifierType,
      final int retryAttempts,
      final PoolImpl executablePool,
      final Function<Integer> function,
      final ServerRegionFunctionExecutor executor,
      final ResultCollector<Integer, Collection<Integer>> resultCollector,
      final int expectTries)
      throws InterruptedException {
    switch (functionIdentifierType) {
      case STRING:
        ignoreServerConnectivityException(
            () -> ignoreServerConnectivityException(() -> ExecuteRegionFunctionSingleHopOp.execute(
                executablePool, testSupport.getRegion(), FUNCTION_NAME,
                executor, resultCollector, FUNCTION_HAS_RESULT, serverToFilterMap,
                retryAttempts,
                ExecuteFunctionTestSupport.ALL_BUCKETS_SETTING, testSupport.toBoolean(haStatus),
                OPTIMIZE_FOR_WRITE_SETTING,
                singleHopClientExecutor)));
        break;
      case OBJECT_REFERENCE:
        ignoreServerConnectivityException(
            () -> ExecuteRegionFunctionSingleHopOp.execute(executablePool, testSupport.getRegion(),
                function,
                executor, resultCollector, FUNCTION_HAS_RESULT, serverToFilterMap,
                retryAttempts,
                ExecuteFunctionTestSupport.ALL_BUCKETS_SETTING, singleHopClientExecutor));
        break;
      default:
        throw new AssertionError("unknown FunctionIdentifierType type: " + functionIdentifierType);
    }

    verify(executorService, times(1))
        .invokeAll(ArgumentMatchers.<Collection<? extends Callable<Integer>>>any());

    final int extraTries = expectTries - ExecuteFunctionTestSupport.NUMBER_OF_SERVERS;

    if (extraTries > 0) {
      verify(executablePool, times(extraTries))
          .execute(ArgumentMatchers.<AbstractOp>any(),
              ArgumentMatchers.anyInt());
    }
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
