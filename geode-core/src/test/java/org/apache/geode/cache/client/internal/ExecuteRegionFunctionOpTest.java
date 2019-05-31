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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.List;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;

import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionInvocationTargetException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.execute.InternalFunctionInvocationTargetException;
import org.apache.geode.internal.cache.execute.ServerRegionFunctionExecutor;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
@RunWith(JUnitParamsRunner.class)
public class ExecuteRegionFunctionOpTest {

  @Test
  public void addFunctionExceptionWithFunctionTargetInvocationExceptionWrapsInPlainFunctionException() {
    FunctionInvocationTargetException exception = mock(FunctionInvocationTargetException.class);
    ExecuteRegionFunctionOp.ExecuteRegionFunctionOpImpl op =
        new ExecuteRegionFunctionOp.ExecuteRegionFunctionOpImpl();
    op.addFunctionException(exception);
    assertThat(op.getFunctionException()).isInstanceOf(FunctionException.class);
    assertThat(op.getFunctionException()).isNotInstanceOf(FunctionInvocationTargetException.class);
  }

  @Test
  public void addFunctionExceptionWithInternalFunctionTargetInvocationExceptionWrapsInPlainFunctionException() {
    FunctionInvocationTargetException exception =
        mock(InternalFunctionInvocationTargetException.class);
    ExecuteRegionFunctionOp.ExecuteRegionFunctionOpImpl op =
        new ExecuteRegionFunctionOp.ExecuteRegionFunctionOpImpl();
    op.addFunctionException(exception);
    assertThat(op.getFunctionException()).isInstanceOf(FunctionException.class);
    assertThat(op.getFunctionException())
        .isNotInstanceOf(InternalFunctionInvocationTargetException.class);
  }

  @Test
  public void addFunctionExceptionWithCauseFunctionTargetInvocationExceptionAddsToListOfException() {
    FunctionInvocationTargetException cause = mock(FunctionInvocationTargetException.class);
    FunctionException exception = new FunctionException(cause);
    ExecuteRegionFunctionOp.ExecuteRegionFunctionOpImpl op =
        new ExecuteRegionFunctionOp.ExecuteRegionFunctionOpImpl();
    op.addFunctionException(exception);
    assertThat(op.getFunctionException()).isInstanceOf(FunctionException.class);
    assertThat(op.getFunctionException().getExceptions()).contains(cause);
  }

  /*
   * Test retry logic
   */

  private enum HAStatus {
    NOT_HA, HA
  }

  private enum ExecutionTarget {
    REGION_NO_FILTER,
    REGION_WITH_FILTER_1_KEY,
    REGION_WITH_FILTER_2_KEYS,
    SERVER,
  }

  private enum FunctionIdentifierType {
    STRING, OBJECT_REFERENCE
  }

  private enum ClientMetadataStatus {
    CLIENT_HAS_METADATA, CLIENT_MISSING_METADATA
  }

  private enum FailureMode {
    NO_FAILURE,
    THROW_SERVER_CONNECTIVITY_EXCEPTION
  }

  private static final byte FUNCTION_HAS_RESULT = (byte) 1;
  private static final int NUMBER_OF_SERVERS = 2;
  private static final boolean OPTIMIZE_FOR_WRITE_SETTING = false;
  private static final String REGION_NAME = "REGION1";
  private static final String FUNCTION_NAME = "FUNCTION1";

  @Test
  @Parameters({
      "HA, CLIENT_HAS_METADATA, REGION_NO_FILTER, OBJECT_REFERENCE, 0, 1",
      "HA, CLIENT_HAS_METADATA, REGION_NO_FILTER, OBJECT_REFERENCE, -1, 2",
      "HA, CLIENT_HAS_METADATA, REGION_NO_FILTER, OBJECT_REFERENCE, 0, 1",
      "HA, CLIENT_HAS_METADATA, REGION_NO_FILTER, OBJECT_REFERENCE, 1, 2",
  })
  @TestCaseName("[{index}] {method}: {params}")
  @SuppressWarnings("unchecked")
  public void foo(
      final HAStatus haStatus,
      final ClientMetadataStatus _ignoredClientMetadataStatus,
      final ExecutionTarget executionTarget,
      final FunctionIdentifierType functionIdentifierType,
      final int retryAttempts,
      final int expectTries) {

    final FailureMode failureMode = FailureMode.THROW_SERVER_CONNECTIVITY_EXCEPTION;

    final List<ServerLocation> servers = mock(List.class);

    when(servers.size()).thenReturn(NUMBER_OF_SERVERS);

    final ConnectionSource connectionSource = mock(ConnectionSource.class);

    when(connectionSource.getAllServers()).thenReturn(servers);

    /*
     * It would be nice to make this variable have type ExecutablePool (an interface) but that
     * won't work because the method we are testing casts the reference to a PoolImpl.
     */
    final PoolImpl executablePool = mock(PoolImpl.class);

    when(executablePool.getConnectionSource()).thenReturn(connectionSource);

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
    switch (failureMode) {
      case NO_FAILURE:
        when(executablePool.execute(ArgumentMatchers.<AbstractOp>any(), ArgumentMatchers.anyInt()))
            .thenReturn(null);
        break;
      case THROW_SERVER_CONNECTIVITY_EXCEPTION:
        when(executablePool.execute(ArgumentMatchers.<AbstractOp>any(), ArgumentMatchers.anyInt()))
            .thenThrow(new ServerConnectivityException("testing"));
        break;
      default:
        throw new AssertionError("unknown FailureMode type: " + failureMode);
    }

    final Function<Integer> function =
        (Function<Integer>) mock(Function.class);

    when(function.isHA()).thenReturn(toBoolean(haStatus));

    final ServerRegionFunctionExecutor executor = mock(ServerRegionFunctionExecutor.class);
    final ResultCollector<Integer, Collection<Integer>> resultCollector =
        (ResultCollector<Integer, Collection<Integer>>) mock(ResultCollector.class);

    switch (executionTarget) {
      case REGION_NO_FILTER:
        switch (failureMode) {
          case NO_FAILURE:
            executeFunctionForSideEffects(haStatus, functionIdentifierType, retryAttempts,
                failureMode,
                executablePool, function, executor, resultCollector);
            break;
          case THROW_SERVER_CONNECTIVITY_EXCEPTION:
            try {
              executeFunctionForSideEffects(haStatus, functionIdentifierType, retryAttempts,
                  failureMode,
                  executablePool, function, executor, resultCollector);
              throw new AssertionError("expected execute() to throw exception but it didn't");
            } catch (final ServerConnectivityException e) {
              // expected
            }
            break;
          default:
            throw new AssertionError("unknown FailureMode type: " + failureMode);
        }
        break;
      case REGION_WITH_FILTER_1_KEY:
        // TODO
      case REGION_WITH_FILTER_2_KEYS:
        // TODO
      case SERVER:
        // TODO
        throw new AssertionError(
            "execution target type not yet supported by test: " + executionTarget);
      default:
        throw new AssertionError("unknown ExecutionTarget type: " + executionTarget);
    }

    verify(executablePool, times(expectTries)).execute(ArgumentMatchers.<AbstractOp>any(),
        ArgumentMatchers.anyInt());

  }

  private void executeFunctionForSideEffects(final HAStatus haStatus,
      final FunctionIdentifierType functionIdentifierType,
      final int retryAttempts,
      final FailureMode failureMode,
      final PoolImpl executablePool,
      final Function<Integer> function,
      final ServerRegionFunctionExecutor executor,
      final ResultCollector<Integer, Collection<Integer>> resultCollector) {
    switch (functionIdentifierType) {
      case STRING:
        ExecuteRegionFunctionOp.execute(executablePool, REGION_NAME, FUNCTION_NAME,
            executor, resultCollector, FUNCTION_HAS_RESULT, retryAttempts, toBoolean(haStatus),
            OPTIMIZE_FOR_WRITE_SETTING);
        break;
      case OBJECT_REFERENCE:
        ExecuteRegionFunctionOp.execute(executablePool, REGION_NAME, function,
            executor, resultCollector, FUNCTION_HAS_RESULT, retryAttempts);
        break;
      default:
        throw new AssertionError("unknown FunctionIdentifierType type: " + failureMode);
    }
  }

  private boolean toBoolean(final HAStatus haStatus) {
    return haStatus == HAStatus.HA;
  }

}
