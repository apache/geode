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
package org.apache.geode.internal.cache.execute;

import java.util.Set;

import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionService;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionService;

/**
 * Provides internal methods for tests
 *
 * @since GemFire 6.1
 */
public class InternalFunctionService extends FunctionService {

  @MakeNotStatic
  private static final InternalFunctionService INSTANCE =
      new InternalFunctionService(new InternalFunctionExecutionServiceImpl());

  private final InternalFunctionExecutionService internalFunctionExecutionService;

  private InternalFunctionService(
      InternalFunctionExecutionService internalFunctionExecutionService) {
    super(internalFunctionExecutionService);
    this.internalFunctionExecutionService = internalFunctionExecutionService;
  }

  /**
   * Returns an {@link Execution} object that can be used to execute a function on the set of
   * {@link Region}s. The function would be executed on the set of members that host data for any of
   * the regions in the set of regions. <br>
   *
   * If the Set provided contains region with : <br>
   * DataPolicy.NORMAL, execute the function on any random member which has DataPolicy.REPLICATE .
   * <br>
   * DataPolicy.EMPTY, execute the function on any random member which has DataPolicy.REPLICATE
   * .<br>
   * DataPolicy.REPLICATE, execute the function locally or any random member which has
   * DataPolicy.REPLICATE .<br>
   * DataPolicy.PARTITION, it executes on members where the primary copy of data is hosted. <br>
   *
   * This API is not supported for cache clients in client server mode
   *
   * For an Execution object obtained from this method, calling the withFilter method throws
   * {@link UnsupportedOperationException}
   *
   * @see MultiRegionFunctionContext
   */
  public static Execution onRegions(Set<Region> regions) {
    return getInternalFunctionExecutionService().onRegions(regions);
  }

  /**
   * Returns an {@link Execution} object that can be used to execute a data independent function on
   * all the servers that the given cache is connected to. If the optional groups parameter is
   * provided, function is executed on all servers that belong to the provided groups.
   * <p>
   * If one of the servers goes down while dispatching or executing the function on the server, an
   * Exception will be thrown.
   *
   * @param regionService obtained from {@link ClientCacheFactory#create} or
   *        {@link ClientCache#createAuthenticatedView(java.util.Properties)}.
   * @param groups optional list of GemFire configuration property "groups" (see
   *        <a href="../../distributed/DistributedSystem.html#groups"> <code>groups</code></a>) on
   *        which to execute the function. Function will be executed on all servers of each group
   * @since GemFire 7.0
   */
  public static Execution onServers(RegionService regionService, String... groups) {
    return getInternalFunctionExecutionService().onServers(regionService, groups);
  }

  /**
   * Returns an {@link Execution} object that can be used to execute a data independent function on
   * a server that the given cache is connected to. If the optional groups parameter is provided,
   * the function is executed on one server of each group.
   * <p>
   * If the server goes down while dispatching or executing the function, an Exception will be
   * thrown.
   *
   * @param regionService obtained from {@link ClientCacheFactory#create()} or
   *        {@link ClientCache#createAuthenticatedView(java.util.Properties)}.
   * @param groups optional list of GemFire configuration property "groups" (see
   *        <a href="../../distributed/DistributedSystem.html#groups"> <code>groups</code></a>) on
   *        which to execute the function. Function will be executed on one server of each group
   * @since GemFire 7.0
   */
  public static Execution onServer(RegionService regionService, String... groups) {
    return getInternalFunctionExecutionService().onServer(regionService, groups);
  }

  /**
   * Returns an {@link Execution} object that can be used to execute a data independent function on
   * all the servers in the provided {@link Pool}. If the optional groups parameter is provided,
   * function is executed on all servers that belong to the provided groups.
   * <p>
   * If one of the servers goes down while dispatching or executing the function on the server, an
   * Exception will be thrown.
   *
   * @param pool the set of servers to execute the function
   * @param groups optional list of GemFire configuration property "groups" (see
   *        <a href="../../distributed/DistributedSystem.html#groups"> <code>groups</code></a>) on
   *        which to execute the function. Function will be executed on all servers of each group
   * @since GemFire 7.0
   */
  public static Execution onServers(Pool pool, String... groups) {
    return getInternalFunctionExecutionService().onServers(pool, groups);
  }

  /**
   * Returns an {@link Execution} object that can be used to execute a data independent function on
   * a server in the provided {@link Pool}. If the optional groups parameter is provided, the
   * function is executed on one server of each group.
   * <p>
   * If the server goes down while dispatching or executing the function, an Exception will be
   * thrown.
   *
   * @param pool from which to chose a server for execution
   * @param groups optional list of GemFire configuration property "groups" (see
   *        <a href="../../distributed/DistributedSystem.html#groups"> <code>groups</code></a>) on
   *        which to execute the function. Function will be executed on one server of each group
   * @since GemFire 7.0
   */
  public static Execution onServer(Pool pool, String... groups) {
    return getInternalFunctionExecutionService().onServer(pool, groups);
  }

  /**
   * Unregisters all functions.
   */
  public static void unregisterAllFunctions() {
    getInternalFunctionExecutionService().unregisterAllFunctions();
  }

  /**
   * Returns non-static reference to {@code InternalFunctionExecutionService} singleton instance.
   */
  public static InternalFunctionExecutionService getInternalFunctionExecutionService() {
    return INSTANCE.internalFunctionExecutionService;
  }
}
