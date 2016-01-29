/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache.execute;

import java.util.Set;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionService;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.internal.FunctionServiceManager;
import com.gemstone.gemfire.internal.cache.AbstractRegion;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * 
 * Provides internal methods for sqlFabric product
 * 
 * @author Yogesh Mahajan
 * 
 * @since 6.1
 * 
 */
public class InternalFunctionService {

  /**
   * Returns an {@link Execution} object that can be used to execute a function
   * on the set of {@link Region}s. The function would be executed on the set of
   * members that host data for any of the regions in the set of regions. <br>
   * 
   * If the Set provided contains region with : <br>
   * DataPolicy.NORMAL, execute the function on any random member which has
   * DataPolicy.REPLICATE . <br>
   * DataPolicy.EMPTY, execute the function on any random member which has
   * DataPolicy.REPLICATE .<br>
   * DataPolicy.REPLICATE, execute the function locally or any random member
   * which has DataPolicy.REPLICATE .<br>
   * DataPolicy.PARTITION, it executes on members where the primary copy of data
   * is hosted. <br>
   * 
   * This API is not supported for cache clients in client server mode
   * 
   * For an Execution object obtained from this method, calling the withFilter
   * method throws {@link UnsupportedOperationException}
   * 
   * @see MultiRegionFunctionContext
   * 
   * @param regions
   * 
   */
  public static Execution onRegions(Set<Region> regions) {
    if (regions == null) {
      throw new IllegalArgumentException(
          LocalizedStrings.ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
              .toLocalizedString("regions set"));
    }
    if (regions.contains(null)) {
      throw new IllegalArgumentException(
          LocalizedStrings.OnRegionsFunctions_THE_REGION_SET_FOR_ONREGIONS_HAS_NULL
              .toLocalizedString());
    }
    if (regions.isEmpty()) {
      throw new IllegalArgumentException(
          LocalizedStrings.OnRegionsFunctions_THE_REGION_SET_IS_EMPTY_FOR_ONREGIONS
              .toLocalizedString());
    }
    for (Region region : regions) {
      if (isClientRegion(region)) {
        throw new UnsupportedOperationException(
            LocalizedStrings.OnRegionsFunctions_NOT_SUPPORTED_FOR_CLIENT_SERVER
                .toLocalizedString());
      }
    }
    return new MultiRegionFunctionExecutor(regions);
  }

  /**
   * @param region
   * @return true if the method is called on a region has a {@link Pool}.
   * @since 6.0
   */
  private static boolean isClientRegion(Region region) {
    LocalRegion localRegion = (LocalRegion)region;
    return localRegion.hasServerProxy();
  }

  private static final FunctionServiceManager funcServiceManager = new FunctionServiceManager();

  /**
   * Returns an {@link Execution} object that can be used to execute a data
   * independent function on all the servers that the given cache is connected
   * to. If the optional groups parameter is provided, function is executed on
   * all servers that belong to the provided groups.
   * <p>
   * If one of the servers goes down while dispatching or executing the function
   * on the server, an Exception will be thrown.
   *
   * @param regionService
   *          obtained from {@link ClientCacheFactory#create} or
   *          {@link ClientCache#createAuthenticatedView(java.util.Properties)}.
   * @param groups
   *          optional list of GemFire configuration property "groups" (see
   *          <a href="../../distributed/DistributedSystem.html#groups">
   *          <code>groups</code></a>) on which to execute the function.
   *          Function will be executed on all servers of each group
   * @return Execution
   * @since 7.0
   */
  public static Execution onServers(RegionService regionService, String... groups) {
    return funcServiceManager.onServers(regionService, groups);
  }

  /**
   * Returns an {@link Execution} object that can be used to execute a data
   * independent function on a server that the given cache is connected to. If
   * the optional groups parameter is provided, the function is executed on one
   * server of each group.
   * <p>
   * If the server goes down while dispatching or executing the function, an
   * Exception will be thrown.
   *
   * @param regionService
   *          obtained from {@link ClientCacheFactory#create()} or
   *          {@link ClientCache#createAuthenticatedView(java.util.Properties)}.
   * @param groups
   *          optional list of GemFire configuration property "groups" (see
   *          <a href="../../distributed/DistributedSystem.html#groups">
   *          <code>groups</code></a>) on which to execute the function.
   *          Function will be executed on one server of each group
   * @return Execution
   * @since 7.0
   */
  public static Execution onServer(RegionService regionService, String... groups) {
    return funcServiceManager.onServer(regionService, groups);
  }

  /**
   * Returns an {@link Execution} object that can be used to execute a data
   * independent function on all the servers in the provided {@link Pool}. If
   * the optional groups parameter is provided, function is executed on all
   * servers that belong to the provided groups.
   * <p>
   * If one of the servers goes down while dispatching or executing the function
   * on the server, an Exception will be thrown.
   *
   * @param pool
   *          the set of servers to execute the function
   * @param groups
   *          optional list of GemFire configuration property "groups" (see <a
   *          href="../../distributed/DistributedSystem.html#groups">
   *          <code>groups</code></a>) on which to execute the function.
   *          Function will be executed on all servers of each group
   * @return Execution
   * @since 7.0
   */
  public static Execution onServers(Pool pool, String... groups) {
    return funcServiceManager.onServers(pool, groups);
  }

  /**
   * Returns an {@link Execution} object that can be used to execute a data
   * independent function on a server in the provided {@link Pool}. If the
   * optional groups parameter is provided, the function is executed on one
   * server of each group.
   * <p>
   * If the server goes down while dispatching or executing the function, an
   * Exception will be thrown.
   *
   * @param pool
   *          from which to chose a server for execution
   * @param groups
   *          optional list of GemFire configuration property "groups" (see <a
   *          href="../../distributed/DistributedSystem.html#groups">
   *          <code>groups</code></a>) on which to execute the function.
   *          Function will be executed on one server of each group
   * @return Execution
   * @since 7.0
   */
  public static Execution onServer(Pool pool, String... groups) {
    return funcServiceManager.onServer(pool, groups);
  }
}
