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

import java.util.Properties;
import java.util.Set;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionService;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;

public interface InternalFunctionExecutionService extends FunctionExecutionService {

  void unregisterAllFunctions();

  /**
   * Returns an {@link Execution} object that can be used to execute a function on the set of {@link
   * Region}s. The function would be executed on the set of members that host data for any of the
   * regions in the set of regions. <br>
   * If the Set provided contains region with : <br>
   * DataPolicy.NORMAL, execute the function on any random member which has DataPolicy.REPLICATE .
   * <br>
   * DataPolicy.EMPTY, execute the function on any random member which has DataPolicy.REPLICATE .
   * <br>
   * DataPolicy.REPLICATE, execute the function locally or any random member which has
   * DataPolicy.REPLICATE .<br>
   * DataPolicy.PARTITION, it executes on members where the primary copy of data is hosted. <br>
   * This API is not supported for cache clients in client server mode
   *
   * <p>
   * For an Execution object obtained from this method, calling the withFilter method throws
   * {@link UnsupportedOperationException}
   *
   * @see MultiRegionFunctionContext
   */
  Execution onRegions(Set<Region> regions);

  /**
   * Returns an {@link Execution} object that can be used to execute a data independent function on
   * all the servers that the given cache is connected to. If one of the servers goes down while
   * dispatching or executing the function on the server, an Exception will be thrown.
   *
   * @param regionService obtained from {@link ClientCacheFactory#create} or
   *        {@link ClientCache#createAuthenticatedView(Properties)} .
   * @throws FunctionException if cache is null, is not on a client, or it does not have a default
   *         pool
   * @since GemFire 6.5
   */
  Execution onServers(RegionService regionService, String... groups);

  /**
   * Returns an {@link Execution} object that can be used to execute a data independent function on
   * a server that the given cache is connected to.
   * <p>
   * If the server goes down while dispatching or executing the function, an Exception will be
   * thrown.
   *
   * @param regionService obtained from {@link ClientCacheFactory#create} or
   *        {@link ClientCache#createAuthenticatedView(Properties)} .
   * @throws FunctionException if cache is null, is not on a client, or it does not have a default
   *         pool
   * @since GemFire 6.5
   */
  Execution onServer(RegionService regionService, String... groups);

  /**
   * Returns an {@link Execution} object that can be used to execute a data independent function on
   * all the servers in the provided {@link Pool}. If one of the servers goes down while dispatching
   * or executing the function on the server, an Exception will be thrown.
   *
   * @param pool the set of servers to execute the function
   * @throws FunctionException if Pool instance passed in is null
   * @since GemFire 6.0
   */
  Execution onServers(Pool pool, String... groups);

  /**
   * Returns an {@link Execution} object that can be used to execute a data independent function on
   * a server in the provided {@link Pool}.
   * <p>
   * If the server goes down while dispatching or executing the function, an Exception will be
   * thrown.
   *
   * @param pool from which to chose a server for execution
   * @throws FunctionException if Pool instance passed in is null
   * @since GemFire 6.0
   */
  Execution onServer(Pool pool, String... groups);

  /**
   * Returns an {@link Execution} object that can be used to execute a data independent function on
   * a {@link DistributedMember} of the {@link DistributedSystem}. If the member is not found in the
   * system, the function execution will throw an Exception. If the member goes down while
   * dispatching or executing the function on the member, an Exception will be thrown.
   *
   * @param system defines the distributed system
   * @param distributedMember defines a member in the distributed system
   * @throws FunctionException if either input parameter is null
   * @since GemFire 6.0
   *
   */
  Execution onMember(DistributedSystem system, DistributedMember distributedMember);

  /**
   * Returns an {@link Execution} object that can be used to execute a data independent function on
   * all members of the {@link DistributedSystem}. If one of the members goes down while dispatching
   * or executing the function on the member, an Exception will be thrown.
   *
   * @param system defines the distributed system
   *
   * @throws FunctionException if DistributedSystem instance passed is null
   * @since GemFire 6.0
   */
  Execution onMembers(DistributedSystem system, String... groups);

  /**
   * Uses {@code RANDOM_onMember} for tests.
   *
   * <p>
   * TODO: maybe merge with {@link #onMembers(DistributedSystem, String...)}
   */
  Execution onMember(DistributedSystem system, String... groups);

  /**
   * Returns an {@link Execution} object that can be used to execute a data independent function on
   * the set of {@link DistributedMember}s of the {@link DistributedSystem}. If one of the members
   * goes down while dispatching or executing the function, an Exception will be thrown.
   *
   * @param system defines the distributed system
   * @param distributedMembers set of distributed members on which {@link Function} to be executed
   * @throws FunctionException if DistributedSystem instance passed is null
   * @since GemFire 6.0
   */
  Execution onMembers(DistributedSystem system, Set<DistributedMember> distributedMembers);
}
