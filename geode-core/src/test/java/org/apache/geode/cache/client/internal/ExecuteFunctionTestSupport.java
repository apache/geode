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

import static org.apache.geode.cache.client.internal.ExecuteFunctionTestSupport.HAStatus.HA;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

import org.mockito.ArgumentMatchers;
import org.mockito.stubbing.OngoingStubbing;

import org.apache.geode.cache.client.NoAvailableServersException;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.client.internal.pooling.ConnectionManagerImpl;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.execute.InternalFunctionInvocationTargetException;
import org.apache.geode.internal.cache.execute.ServerRegionFunctionExecutor;

class ExecuteFunctionTestSupport {

  static final boolean OPTIMIZE_FOR_WRITE_SETTING = false;
  static final byte FUNCTION_HAS_RESULT = (byte) 1;
  static final int NUMBER_OF_SERVERS = 2;
  static final String REGION_NAME = "REGION1";
  static final String FUNCTION_NAME = "FUNCTION1";
  static final boolean ALL_BUCKETS_SETTING = true;

  public enum HAStatus {
    NOT_HA, HA
  }

  enum FunctionIdentifierType {
    STRING, OBJECT_REFERENCE
  }

  enum FailureMode {
    NO_FAILURE,
    THROW_SERVER_CONNECTIVITY_EXCEPTION,
    THROW_SERVER_OPERATION_EXCEPTION,
    THROW_NO_AVAILABLE_SERVERS_EXCEPTION,
    THROW_INTERNAL_FUNCTION_INVOCATION_TARGET_EXCEPTION
  }

  private final Function<Integer> function;
  private final ServerRegionFunctionExecutor executor;
  private final ResultCollector<Integer, Collection<Integer>> resultCollector;

  /*
   * It would be nice to make this variable have type Cache (an interface) but that
   * won't work because the methods we are testing cast the reference to a InternalCache.
   */
  private final InternalCache cache;

  /*
   * It would be nice to make this variable have type Region (an interface) but that
   * won't work because the methods we are testing cast the reference to a LocalRegion.
   */
  private final LocalRegion region;

  /*
   * It would be nice to make this variable have type ExecutablePool (an interface) but that
   * won't work because the methods we are testing cast the reference to a PoolImpl.
   */
  private final PoolImpl executablePool;

  /**
   * Add a {@link OngoingStubbing#thenThrow(Throwable...)} behavior to
   * {@param whenPoolExecute}
   *
   * This method has to be {@code static} because it is called before
   * {@link ExecuteFunctionTestSupport} is constructed.
   *
   * @param failureMode is the {@link FailureMode} that determines the kind of exception
   *        to {@code throw}
   */
  static void thenThrow(final OngoingStubbing<Object> whenPoolExecute,
      final FailureMode failureMode) {
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
        whenPoolExecute.thenReturn(null);
        break;
      case THROW_SERVER_CONNECTIVITY_EXCEPTION:
        whenPoolExecute.thenThrow(new ServerConnectivityException(
            ConnectionManagerImpl.SOCKET_TIME_OUT_MSG));
        break;
      case THROW_SERVER_OPERATION_EXCEPTION:
        whenPoolExecute.thenThrow(new ServerOperationException("testing"));
        break;
      case THROW_NO_AVAILABLE_SERVERS_EXCEPTION:
        whenPoolExecute.thenThrow(new NoAvailableServersException("testing"));
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
        whenPoolExecute.thenThrow(new InternalFunctionInvocationTargetException("testing"))
            .thenThrow(new ServerConnectivityException(ConnectionManagerImpl.SOCKET_TIME_OUT_MSG));
        break;
      default:
        throw new AssertionError("unknown FailureMode type: " + failureMode);
    }
  }

  @SuppressWarnings("unchecked")
  ExecuteFunctionTestSupport(
      final HAStatus haStatus,
      final FailureMode failureMode,
      final BiConsumer<PoolImpl, FailureMode> addPoolMockBehavior, Integer retryAttempts) {

    final List<ServerLocation> servers = (List<ServerLocation>) mock(List.class);
    when(servers.size()).thenReturn(ExecuteFunctionTestSupport.NUMBER_OF_SERVERS);

    final ConnectionSource connectionSource = mock(ConnectionSource.class);
    when(connectionSource.getAllServers()).thenReturn(servers);

    function = (Function<Integer>) mock(Function.class);
    when(function.isHA()).thenReturn(toBoolean(haStatus));
    when(function.hasResult()).thenReturn(true);

    executor = mock(ServerRegionFunctionExecutor.class);
    when(executor.withFilter(ArgumentMatchers.<Set<Integer>>any())).thenReturn(executor);

    resultCollector = (ResultCollector<Integer, Collection<Integer>>) mock(ResultCollector.class);

    cache = mock(InternalCache.class);
    when(cache.getClientMetadataService()).thenReturn(mock(ClientMetadataService.class));

    region = mock(LocalRegion.class);
    when(region.getCache()).thenReturn(cache);

    executablePool = mock(PoolImpl.class);
    when(executablePool.getConnectionSource()).thenReturn(connectionSource);
    when(executablePool.getRetryAttempts()).thenReturn(retryAttempts);
    when(executablePool.calculateRetryAttempts(any(ServerConnectivityException.class)))
        .thenCallRealMethod();

    addPoolMockBehavior.accept(executablePool, failureMode);
  }

  Function<Integer> getFunction() {
    return function;
  }

  ServerRegionFunctionExecutor getExecutor() {
    return executor;
  }

  ResultCollector<Integer, Collection<Integer>> getResultCollector() {
    return resultCollector;
  }

  InternalCache getCache() {
    return cache;
  }

  LocalRegion getRegion() {
    return region;
  }

  PoolImpl getExecutablePool() {
    return executablePool;
  }

  boolean toBoolean(final HAStatus haStatus) {
    return haStatus == HA;
  }

}
