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
import java.util.HashSet;
import java.util.List;
import java.util.Map;

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
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
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
  private static final boolean ALL_BUCKETS_SETTING = true;

  private List<ServerLocation> servers;
  private ConnectionSource connectionSource;

  /*
   * It would be nice to make this variable have type ExecutablePool (an interface) but that
   * won't work because the methods we are testing cast the reference to a PoolImpl.
   */
  private PoolImpl executablePool;

  private Function<Integer> function;
  private ServerRegionFunctionExecutor executor;
  private ResultCollector<Integer, Collection<Integer>> resultCollector;

  /*
   * It would be nice to make this variable have type Cache (an interface) but that
   * won't work because the methods we are testing cast the reference to a InternalCache.
   */
  private InternalCache cache;

  /*
   * It would be nice to make this variable have type Region (an interface) but that
   * won't work because the methods we are testing cast the reference to a LocalRegion.
   */
  private LocalRegion region;

  private SingleHopClientExecutor singleHopClientExecutor;
  private Map<ServerLocation, ? extends HashSet> serverToFilterMap;

  @SuppressWarnings("unchecked")
  private void createMocks(final HAStatus haStatus) {

    final FailureMode failureMode = FailureMode.THROW_SERVER_CONNECTIVITY_EXCEPTION;

    servers = (List<ServerLocation>) mock(List.class);
    when(servers.size()).thenReturn(NUMBER_OF_SERVERS);

    connectionSource = mock(ConnectionSource.class);
    when(connectionSource.getAllServers()).thenReturn(servers);

    executablePool = mock(PoolImpl.class);
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

    function = (Function<Integer>) mock(Function.class);
    when(function.isHA()).thenReturn(toBoolean(haStatus));

    executor = mock(ServerRegionFunctionExecutor.class);

    resultCollector = (ResultCollector<Integer, Collection<Integer>>) mock(ResultCollector.class);

    cache = mock(InternalCache.class);
    when(cache.getClientMetadataService()).thenReturn(mock(ClientMetadataService.class));

    region = mock(LocalRegion.class);
    when(region.getCache()).thenReturn(cache);

    singleHopClientExecutor = mock(SingleHopClientExecutor.class);

    serverToFilterMap = (Map<ServerLocation, ? extends HashSet>) mock(Map.class);
  }

  @Test
  @Parameters({
      "HA, CLIENT_HAS_METADATA, REGION_NO_FILTER, OBJECT_REFERENCE, 0, 1",
      "HA, CLIENT_HAS_METADATA, REGION_NO_FILTER, OBJECT_REFERENCE, -1, 2",
      "HA, CLIENT_HAS_METADATA, REGION_NO_FILTER, OBJECT_REFERENCE, 0, 1",
      "HA, CLIENT_HAS_METADATA, REGION_NO_FILTER, OBJECT_REFERENCE, 1, 2",
      "HA, CLIENT_HAS_METADATA, REGION_WITH_FILTER_1_KEY, OBJECT_REFERENCE, 1, 2",
      "HA, CLIENT_HAS_METADATA, REGION_WITH_FILTER_1_KEY, STRING, 1, 2",
  })
  @TestCaseName("[{index}] {method}: {params}")
  @SuppressWarnings("unchecked")
  public void retryTest(
      final HAStatus haStatus,
      final ClientMetadataStatus clientMetadataStatus,
      final ExecutionTarget executionTarget,
      final FunctionIdentifierType functionIdentifierType,
      final int retryAttempts,
      final int expectTries) {

    createMocks(haStatus);

    switch (executionTarget) {
      case REGION_NO_FILTER:
        executeFunctionMultiHopAndValidate(haStatus, functionIdentifierType, retryAttempts,
            executablePool,
            function,
            executor, resultCollector, expectTries);
        break;
      case REGION_WITH_FILTER_1_KEY:
      case REGION_WITH_FILTER_2_KEYS:
        switch (clientMetadataStatus) {
          case CLIENT_HAS_METADATA:
            switch (functionIdentifierType) {
              case STRING:
                ignoreServerConnectivityException(() -> ExecuteRegionFunctionSingleHopOp.execute(
                    executablePool, region, FUNCTION_NAME,
                    executor, resultCollector, FUNCTION_HAS_RESULT, serverToFilterMap,
                    retryAttempts,
                    ALL_BUCKETS_SETTING, toBoolean(haStatus), OPTIMIZE_FOR_WRITE_SETTING,
                    singleHopClientExecutor));
                break;
              case OBJECT_REFERENCE:
                ignoreServerConnectivityException(
                    () -> ExecuteRegionFunctionSingleHopOp.execute(executablePool, region, function,
                        executor, resultCollector, FUNCTION_HAS_RESULT, serverToFilterMap,
                        retryAttempts,
                        ALL_BUCKETS_SETTING, singleHopClientExecutor));
                break;
              default:
                throw new AssertionError(
                    "unknown FunctionIdentifierType type: " + functionIdentifierType);
            }
            verify(singleHopClientExecutor, times(1))
                .submitAllHA(ArgumentMatchers.anyList(), ArgumentMatchers.any(),
                    ArgumentMatchers.anyBoolean(), ArgumentMatchers.any(),
                    ArgumentMatchers.any(), ArgumentMatchers.anyInt(), ArgumentMatchers.any());
            final int extraTries = expectTries - NUMBER_OF_SERVERS;
            if (extraTries > 0) {
              verify(this.executablePool, times(extraTries))
                  .execute(ArgumentMatchers.<AbstractOp>any(),
                      ArgumentMatchers.anyInt());
            }
            break;
          case CLIENT_MISSING_METADATA:
            executeFunctionMultiHopAndValidate(haStatus, functionIdentifierType, retryAttempts,
                executablePool,
                function,
                executor, resultCollector, expectTries);
            break;
          default:
            throw new AssertionError("unknown ClientMetadataStatus type: " + clientMetadataStatus);
        }
        break;
      case SERVER:
        // switch (functionIdentifierType) {
        // case STRING:
        // ExecuteFunctionOp.execute(executablePool,FUNCTION_NAME,executor,new Object(),new
        // MemberMappedArgument(),false,FUNCTION_HAS_RESULT,resultCollector,false,toBoolean(haStatus),OPTIMIZE_FOR_WRITE_SETTING,new
        // UserAttributes(),new String[]{});
        // break;
        // case OBJECT_REFERENCE:
        // ExecuteFunctionOp.execute(executablePool,function,executor,new Object(),new
        // MemberMappedArgument(),false,FUNCTION_HAS_RESULT,resultCollector,false,new
        // UserAttributes(),new String[]{});
        // break;
        // default:
        // throw new AssertionError("unknown FunctionIdentifierType type: " +
        // functionIdentifierType);
        // }
        // break;
        throw new AssertionError("unsupported ExecutionTarget type: " + executionTarget);
      default:
        throw new AssertionError("unknown ExecutionTarget type: " + executionTarget);
    }
  }

  private void executeFunctionMultiHopAndValidate(final HAStatus haStatus,
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
            () -> ExecuteRegionFunctionOp.execute(executablePool, REGION_NAME, FUNCTION_NAME,
                executor, resultCollector, FUNCTION_HAS_RESULT, retryAttempts, toBoolean(haStatus),
                OPTIMIZE_FOR_WRITE_SETTING));
        break;
      case OBJECT_REFERENCE:
        ignoreServerConnectivityException(
            () -> ExecuteRegionFunctionOp.execute(executablePool, REGION_NAME, function,
                executor, resultCollector, FUNCTION_HAS_RESULT, retryAttempts));
        break;
      default:
        throw new AssertionError("unknown FunctionIdentifierType type: " + functionIdentifierType);
    }

    verify(this.executablePool, times(expectTries)).execute(ArgumentMatchers.<AbstractOp>any(),
        ArgumentMatchers.anyInt());
  }

  private boolean toBoolean(final HAStatus haStatus) {
    return haStatus == HAStatus.HA;
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
