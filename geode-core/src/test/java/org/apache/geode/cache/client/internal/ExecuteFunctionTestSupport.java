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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

import org.mockito.ArgumentMatchers;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
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

  // TODO: remove
  enum ExecutionTarget {
    REGION_NO_FILTER,
    REGION_WITH_FILTER_1_KEY,
    REGION_WITH_FILTER_2_KEYS,
    SERVER,
  }

  enum FunctionIdentifierType {
    STRING, OBJECT_REFERENCE
  }

  // TODO: remove
  private enum ClientMetadataStatus {
    CLIENT_HAS_METADATA, CLIENT_MISSING_METADATA
  }

  enum FailureMode {
    NO_FAILURE,
    THROW_SERVER_CONNECTIVITY_EXCEPTION,
    THROW_SERVER_OPERATION_EXCEPTION,
    THROW_NO_AVAILABLE_SERVERS_EXCEPTION,
    THROW_INTERNAL_FUNCTION_INVOCATION_TARGET_EXCEPTION
  }

  private List<ServerLocation> servers;
  private ConnectionSource connectionSource;

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

  /*
   * It would be nice to make this variable have type ExecutablePool (an interface) but that
   * won't work because the methods we are testing cast the reference to a PoolImpl.
   */
  private PoolImpl executablePool;


  @SuppressWarnings("unchecked")
  ExecuteFunctionTestSupport(
      final ExecuteFunctionTestSupport.HAStatus haStatus,
      final ExecuteFunctionTestSupport.FailureMode failureMode,
      final BiConsumer<PoolImpl, FailureMode> addPoolMockBehavior) {

    servers = (List<ServerLocation>) mock(List.class);
    when(servers.size()).thenReturn(ExecuteFunctionTestSupport.NUMBER_OF_SERVERS);

    connectionSource = mock(ConnectionSource.class);
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

    addPoolMockBehavior.accept(executablePool, failureMode);
  }

  List<ServerLocation> getServers() {
    return servers;
  }

  ConnectionSource getConnectionSource() {
    return connectionSource;
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

  public PoolImpl getExecutablePool() {
    return executablePool;
  }

  boolean toBoolean(final ExecuteFunctionTestSupport.HAStatus haStatus) {
    return haStatus == ExecuteFunctionTestSupport.HAStatus.HA;
  }

}
