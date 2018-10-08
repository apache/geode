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
package org.apache.geode.cache.execute;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionService;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.execute.internal.FunctionServiceManager;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.InternalDistributedSystem;

/**
 * Provides the entry point into execution of user defined {@linkplain Function}s.
 * <p>
 * Function execution provides a means to route application behaviour to {@linkplain Region data} or
 * more generically to peers in a {@link DistributedSystem} or servers in a {@link Pool}.
 * </p>
 *
 * @since GemFire 6.0
 */
public class FunctionService {
  private static final FunctionServiceManager functionSvcMgr = new FunctionServiceManager();

  FunctionService() {}

  /**
   * Returns an {@link Execution} object that can be used to execute a data dependent function on
   * the specified Region.<br>
   * When invoked from a GemFire client, the method returns an Execution instance that sends a
   * message to one of the connected servers as specified by the {@link Pool} for the region. <br>
   * Depending on the filters setup on the {@link Execution}, the function is executed on all
   * GemFire members that define the data region, or a subset of members.
   * {@link Execution#withFilter(Set)}).
   *
   * For DistributedRegions with DataPolicy.NORMAL, it throws UnsupportedOperationException. For
   * DistributedRegions with DataPolicy.EMPTY, execute the function on any random member which has
   * DataPolicy.REPLICATE <br>
   * . For DistributedRegions with DataPolicy.REPLICATE, execute the function locally. For Regions
   * with DataPolicy.PARTITION, it executes on members where the data resides as specified by the
   * filter.
   *
   * @throws FunctionException if the region passed in is null
   * @since GemFire 6.0
   */
  public static Execution onRegion(Region region) {
    return functionSvcMgr.onRegion(region);
  }

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
  public static Execution onServer(Pool pool) {
    return functionSvcMgr.onServer(pool);
  }

  /**
   * Returns an {@link Execution} object that can be used to execute a data independent function on
   * all the servers in the provided {@link Pool}. If one of the servers goes down while dispatching
   * or executing the function on the server, an Exception will be thrown.
   *
   * @param pool the set of servers to execute the function
   * @throws FunctionException if Pool instance passed in is null
   * @since GemFire 6.0
   */
  public static Execution onServers(Pool pool) {
    return functionSvcMgr.onServers(pool);
  }

  /**
   * Returns an {@link Execution} object that can be used to execute a data independent function on
   * a server that the given cache is connected to.
   * <p>
   * If the server goes down while dispatching or executing the function, an Exception will be
   * thrown.
   *
   * @param regionService obtained from {@link ClientCacheFactory#create} or
   *        {@link ClientCache#createAuthenticatedView(Properties)}.
   * @throws FunctionException if cache is null, is not on a client, or it does not have a default
   *         pool
   * @since GemFire 6.5
   */
  public static Execution onServer(RegionService regionService) {
    return functionSvcMgr.onServer(regionService);
  }

  /**
   * Returns an {@link Execution} object that can be used to execute a data independent function on
   * all the servers that the given cache is connected to. If one of the servers goes down while
   * dispatching or executing the function on the server, an Exception will be thrown.
   *
   * @param regionService obtained from {@link ClientCacheFactory#create} or
   *        {@link ClientCache#createAuthenticatedView(Properties)}.
   * @throws FunctionException if cache is null, is not on a client, or it does not have a default
   *         pool
   * @since GemFire 6.5
   */
  public static Execution onServers(RegionService regionService) {
    return functionSvcMgr.onServers(regionService);
  }

  /**
   * Returns an {@link Execution} object that can be used to execute a data independent function on
   * a {@link DistributedMember}. If the member is not found, executing the function will throw an
   * Exception. If the member goes down while dispatching or executing the function on the member,
   * an Exception will be thrown.
   *
   * @param distributedMember defines a member in the distributed system
   * @throws FunctionException if distributedMember is null
   * @since GemFire 7.0
   *
   */
  public static Execution onMember(DistributedMember distributedMember) {
    return functionSvcMgr.onMember(getDistributedSystem(), distributedMember);
  }

  /**
   * Returns an {@link Execution} object that can be used to execute a data independent function on
   * all peer members. If the optional groups parameter is provided, function is executed on all
   * members that belong to the provided groups.
   * <p>
   * If one of the members goes down while dispatching or executing the function on the member, an
   * Exception will be thrown.
   *
   * @param groups optional list of GemFire configuration property "groups" (see
   *        <a href="../../distributed/DistributedSystem.html#groups"> <code>groups</code></a>) on
   *        which to execute the function. Function will be executed on all members of each group
   *
   * @throws FunctionException if no members are found belonging to the provided groups
   * @since GemFire 7.0
   */
  public static Execution onMembers(String... groups) {
    return functionSvcMgr.onMembers(getDistributedSystem(), groups);
  }

  /**
   * Returns an {@link Execution} object that can be used to execute a data independent function on
   * the set of {@link DistributedMember}s. If one of the members goes down while dispatching or
   * executing the function, an Exception will be thrown.
   *
   * @param distributedMembers set of distributed members on which {@link Function} to be executed
   * @throws FunctionException if distributedMembers is null
   * @since GemFire 7.0
   */
  public static Execution onMembers(Set<DistributedMember> distributedMembers) {
    return functionSvcMgr.onMembers(getDistributedSystem(), distributedMembers);
  }

  /**
   * Returns an {@link Execution} object that can be used to execute a data independent function on
   * one member of each group provided.
   *
   * @param groups list of GemFire configuration property "groups" (see
   *        <a href="../../distributed/DistributedSystem.html#groups"> <code>groups</code></a>) on
   *        which to execute the function. Function will be executed on one member of each group
   *
   * @throws FunctionException if no members are found belonging to the provided groups
   * @since GemFire 7.0
   */
  public static Execution onMember(String... groups) {
    return functionSvcMgr.onMember(getDistributedSystem(), groups);
  }

  /**
   * Returns the {@link Function} defined by the functionId, returns null if no function is found
   * for the specified functionId
   *
   * @throws FunctionException if functionID passed is null
   * @since GemFire 6.0
   */
  public static Function getFunction(String functionId) {
    return functionSvcMgr.getFunction(functionId);
  }

  /**
   * Registers the given {@link Function} with the {@link FunctionService} using
   * {@link Function#getId()}.
   * <p>
   * Registering a function allows execution of the function using
   * {@link Execution#execute(String)}. Every member that could execute a function using its
   * {@link Function#getId()} should register the function.
   * </p>
   *
   * @throws FunctionException if function instance passed is null or Function.getId() returns null
   * @since GemFire 6.0
   */
  public static void registerFunction(Function function) {
    functionSvcMgr.registerFunction(function);
  }

  /**
   * Unregisters the given {@link Function} with the {@link FunctionService} using
   * {@link Function#getId()}.
   * <p>
   *
   * @throws FunctionException if function instance passed is null or Function.getId() returns null
   * @since GemFire 6.0
   */
  public static void unregisterFunction(String functionId) {
    functionSvcMgr.unregisterFunction(functionId);
  }

  /**
   * Returns true if the function is registered to FunctionService
   *
   * @throws FunctionException if function instance passed is null or Function.getId() returns null
   * @since GemFire 6.0
   */
  public static boolean isRegistered(String functionId) {
    return functionSvcMgr.isRegistered(functionId);
  }


  /**
   * Returns all locally registered functions
   *
   * @return A view of registered functions as a Map of {@link Function#getId()} to {@link Function}
   * @since GemFire 6.0
   */
  public static Map<String, Function> getRegisteredFunctions() {
    return functionSvcMgr.getRegisteredFunctions();
  }

  private static DistributedSystem getDistributedSystem() {
    DistributedSystem system = InternalDistributedSystem.getConnectedInstance();
    if (system == null) {
      throw new DistributedSystemDisconnectedException(
          "This connection to a distributed system has been disconnected.");
    }
    return system;
  }
}
