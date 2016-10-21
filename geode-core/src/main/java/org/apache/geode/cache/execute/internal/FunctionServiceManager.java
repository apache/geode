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
package org.apache.geode.cache.execute.internal;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionService;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.ProxyCache;
import org.apache.geode.cache.client.internal.ProxyRegion;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.execute.*;
import org.apache.geode.internal.i18n.LocalizedStrings;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Provides the entry point into execution of user defined {@linkplain Function}s.
 * <p>
 * Function execution provides a means to route application behaviour to {@linkplain Region data} or
 * more generically to peers in a {@link DistributedSystem} or servers in a {@link Pool}.
 * </p>
 * 
 * While {@link FunctionService} is a customer facing interface to this functionality, all of the
 * work is done here. In addition, internal only functionality is exposed in this class.
 * 
 * @since GemFire 7.0
 */
public class FunctionServiceManager {
  private final static ConcurrentHashMap<String, Function> idToFunctionMap =
      new ConcurrentHashMap<String, Function>();

  /**
   * use when the optimization to execute onMember locally is not desired.
   */
  public static final boolean RANDOM_onMember =
      Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "randomizeOnMember");

  public FunctionServiceManager() {}


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
   * @param region
   * @return Execution
   * @throws FunctionException if the region passed in is null
   * @since GemFire 6.0
   */
  public final Execution onRegion(Region region) {
    if (region == null) {
      throw new FunctionException(
          LocalizedStrings.FunctionService_0_PASSED_IS_NULL.toLocalizedString("Region instance "));
    }

    ProxyCache proxyCache = null;
    String poolName = region.getAttributes().getPoolName();
    if (poolName != null) {
      Pool pool = PoolManager.find(poolName);
      if (pool.getMultiuserAuthentication()) {
        if (region instanceof ProxyRegion) {
          ProxyRegion pr = (ProxyRegion) region;
          region = pr.getRealRegion();
          proxyCache = (ProxyCache) pr.getAuthenticatedCache();
        } else {
          throw new UnsupportedOperationException();
        }
      }
    }

    if (isClientRegion(region)) {
      return new ServerRegionFunctionExecutor(region, proxyCache);
    }
    if (PartitionRegionHelper.isPartitionedRegion(region)) {
      return new PartitionedRegionFunctionExecutor(region);
    }
    return new DistributedRegionFunctionExecutor(region);
  }

  /**
   * Returns an {@link Execution} object that can be used to execute a data independent function on
   * a server in the provided {@link Pool}.
   * <p>
   * If the server goes down while dispatching or executing the function, an Exception will be
   * thrown.
   * 
   * @param pool from which to chose a server for execution
   * @return Execution
   * @throws FunctionException if Pool instance passed in is null
   * @since GemFire 6.0
   */
  public final Execution onServer(Pool pool, String... groups) {
    if (pool == null) {
      throw new FunctionException(
          LocalizedStrings.FunctionService_0_PASSED_IS_NULL.toLocalizedString("Pool instance "));
    }

    if (pool.getMultiuserAuthentication()) {
      throw new UnsupportedOperationException();
    }

    return new ServerFunctionExecutor(pool, false, groups);
  }

  /**
   * Returns an {@link Execution} object that can be used to execute a data independent function on
   * all the servers in the provided {@link Pool}. If one of the servers goes down while dispatching
   * or executing the function on the server, an Exception will be thrown.
   * 
   * @param pool the set of servers to execute the function
   * @return Execution
   * @throws FunctionException if Pool instance passed in is null
   * @since GemFire 6.0
   */
  public final Execution onServers(Pool pool, String... groups) {
    if (pool == null) {
      throw new FunctionException(
          LocalizedStrings.FunctionService_0_PASSED_IS_NULL.toLocalizedString("Pool instance "));
    }

    if (pool.getMultiuserAuthentication()) {
      throw new UnsupportedOperationException();
    }

    return new ServerFunctionExecutor(pool, true, groups);
  }

  /**
   * Returns an {@link Execution} object that can be used to execute a data independent function on
   * a server that the given cache is connected to.
   * <p>
   * If the server goes down while dispatching or executing the function, an Exception will be
   * thrown.
   * 
   * @param regionService obtained from {@link ClientCacheFactory#create} or
   *        {@link ClientCache#createAuthenticatedView(Properties)} .
   * @return Execution
   * @throws FunctionException if cache is null, is not on a client, or it does not have a default
   *         pool
   * @since GemFire 6.5
   */
  public final Execution onServer(RegionService regionService, String... groups) {
    if (regionService == null) {
      throw new FunctionException(LocalizedStrings.FunctionService_0_PASSED_IS_NULL
          .toLocalizedString("RegionService instance "));
    }
    if (regionService instanceof GemFireCacheImpl) {
      GemFireCacheImpl gfc = (GemFireCacheImpl) regionService;
      if (!gfc.isClient()) {
        throw new FunctionException("The cache was not a client cache");
      } else if (gfc.getDefaultPool() != null) {
        return onServer(gfc.getDefaultPool(), groups);
      } else {
        throw new FunctionException("The client cache does not have a default pool");
      }
    } else {
      ProxyCache pc = (ProxyCache) regionService;
      return new ServerFunctionExecutor(pc.getUserAttributes().getPool(), false, pc, groups);
    }
  }

  /**
   * Returns an {@link Execution} object that can be used to execute a data independent function on
   * all the servers that the given cache is connected to. If one of the servers goes down while
   * dispatching or executing the function on the server, an Exception will be thrown.
   * 
   * @param regionService obtained from {@link ClientCacheFactory#create} or
   *        {@link ClientCache#createAuthenticatedView(Properties)} .
   * @return Execution
   * @throws FunctionException if cache is null, is not on a client, or it does not have a default
   *         pool
   * @since GemFire 6.5
   */
  public final Execution onServers(RegionService regionService, String... groups) {
    if (regionService == null) {
      throw new FunctionException(LocalizedStrings.FunctionService_0_PASSED_IS_NULL
          .toLocalizedString("RegionService instance "));
    }
    if (regionService instanceof GemFireCacheImpl) {
      GemFireCacheImpl gfc = (GemFireCacheImpl) regionService;
      if (!gfc.isClient()) {
        throw new FunctionException("The cache was not a client cache");
      } else if (gfc.getDefaultPool() != null) {
        return onServers(gfc.getDefaultPool(), groups);
      } else {
        throw new FunctionException("The client cache does not have a default pool");
      }
    } else {
      ProxyCache pc = (ProxyCache) regionService;
      return new ServerFunctionExecutor(pc.getUserAttributes().getPool(), true, pc, groups);
    }
  }

  /**
   * Returns an {@link Execution} object that can be used to execute a data independent function on
   * a {@link DistributedMember} of the {@link DistributedSystem}. If the member is not found in the
   * system, the function execution will throw an Exception. If the member goes down while
   * dispatching or executing the function on the member, an Exception will be thrown.
   * 
   * @param system defines the distributed system
   * @param distributedMember defines a member in the distributed system
   * @return Execution
   * @throws FunctionException if either input parameter is null
   * @since GemFire 6.0
   * 
   */
  public final Execution onMember(DistributedSystem system, DistributedMember distributedMember) {
    if (system == null) {
      throw new FunctionException(LocalizedStrings.FunctionService_0_PASSED_IS_NULL
          .toLocalizedString("DistributedSystem instance "));
    }
    if (distributedMember == null) {
      throw new FunctionException(LocalizedStrings.FunctionService_0_PASSED_IS_NULL
          .toLocalizedString("DistributedMember instance "));
    }
    return new MemberFunctionExecutor(system, distributedMember);
  }

  /**
   * Returns an {@link Execution} object that can be used to execute a data independent function on
   * all members of the {@link DistributedSystem}. If one of the members goes down while dispatching
   * or executing the function on the member, an Exception will be thrown.
   * 
   * @param system defines the distributed system
   * @return Execution
   * 
   * @throws FunctionException if DistributedSystem instance passed is null
   * @since GemFire 6.0
   */
  public final Execution onMembers(DistributedSystem system, String... groups) {
    if (system == null) {
      throw new FunctionException(LocalizedStrings.FunctionService_0_PASSED_IS_NULL
          .toLocalizedString("DistributedSystem instance "));
    }
    if (groups.length == 0) {
      return new MemberFunctionExecutor(system);
    }
    Set<DistributedMember> members = new HashSet<DistributedMember>();
    for (String group : groups) {
      members.addAll(system.getGroupMembers(group));
    }
    if (members.isEmpty()) {
      throw new FunctionException(LocalizedStrings.FunctionService_NO_MEMBERS_FOUND_IN_GROUPS
          .toLocalizedString(Arrays.toString(groups)));
    }
    return new MemberFunctionExecutor(system, members);
  }

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
  public final Execution onMembers(DistributedSystem system,
      Set<DistributedMember> distributedMembers) {
    if (system == null) {
      throw new FunctionException(LocalizedStrings.FunctionService_0_PASSED_IS_NULL
          .toLocalizedString("DistributedSystem instance "));
    }
    if (distributedMembers == null) {
      throw new FunctionException(LocalizedStrings.FunctionService_0_PASSED_IS_NULL
          .toLocalizedString("distributedMembers set "));
    }
    return new MemberFunctionExecutor(system, distributedMembers);
  }

  /**
   * Returns the {@link Function} defined by the functionId, returns null if no function is found
   * for the specified functionId
   * 
   * @param functionId
   * @return Function
   * @throws FunctionException if functionID passed is null
   * @since GemFire 6.0
   */
  public final Function getFunction(String functionId) {
    if (functionId == null) {
      throw new FunctionException(LocalizedStrings.FunctionService_0_PASSED_IS_NULL
          .toLocalizedString("functionId instance "));
    }
    return idToFunctionMap.get(functionId);
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
  public final void registerFunction(Function function) {
    if (function == null) {
      throw new FunctionException(LocalizedStrings.FunctionService_0_PASSED_IS_NULL
          .toLocalizedString("function instance "));
    }
    if (function.getId() == null) {
      throw new FunctionException(
          LocalizedStrings.FunctionService_FUNCTION_GET_ID_RETURNED_NULL.toLocalizedString());
    }
    if (function.isHA() && !function.hasResult()) {
      throw new FunctionException(
          LocalizedStrings.FunctionService_FUNCTION_ATTRIBUTE_MISMATCH.toLocalizedString());
    }

    idToFunctionMap.put(function.getId(), function);
  }

  /**
   * Unregisters the given {@link Function} with the {@link FunctionService} using
   * {@link Function#getId()}.
   * <p>
   * 
   * @throws FunctionException if function instance passed is null or Function.getId() returns null
   * @since GemFire 6.0
   */
  public final void unregisterFunction(String functionId) {
    if (functionId == null) {
      throw new FunctionException(LocalizedStrings.FunctionService_0_PASSED_IS_NULL
          .toLocalizedString("functionId instance "));
    }
    idToFunctionMap.remove(functionId);
  }

  /**
   * Returns true if the function is registered to FunctionService
   * 
   * @throws FunctionException if function instance passed is null or Function.getId() returns null
   * @since GemFire 6.0
   */
  public final boolean isRegistered(String functionId) {
    if (functionId == null) {
      throw new FunctionException(LocalizedStrings.FunctionService_0_PASSED_IS_NULL
          .toLocalizedString("functionId instance "));
    }
    return idToFunctionMap.containsKey(functionId);
  }

  /**
   * Returns all locally registered functions
   * 
   * @return A view of registered functions as a Map of {@link Function#getId()} to {@link Function}
   * @since GemFire 6.0
   */
  public final Map<String, Function> getRegisteredFunctions() {
    // We have to remove the internal functions before returning the map to the users
    final Map<String, Function> tempIdToFunctionMap = new HashMap<String, Function>();
    for (Map.Entry<String, Function> entry : idToFunctionMap.entrySet()) {
      if (!(entry.getValue() instanceof InternalEntity)) {
        tempIdToFunctionMap.put(entry.getKey(), entry.getValue());
      }
    }
    return tempIdToFunctionMap;
  }

  public final void unregisterAllFunctions() {
    // Unregistering all the functions registered with the FunctionService.
    Map<String, Function> functions = new HashMap<String, Function>(idToFunctionMap);
    for (String functionId : idToFunctionMap.keySet()) {
      unregisterFunction(functionId);
    }
  }

  /**
   * @param region
   * @return true if the method is called on a region has a {@link Pool}.
   * @since GemFire 6.0
   */
  private final boolean isClientRegion(Region region) {
    LocalRegion localRegion = (LocalRegion) region;
    return localRegion.hasServerProxy();
  }


  public final Execution onMember(DistributedSystem system, String... groups) {
    if (system == null) {
      throw new FunctionException(LocalizedStrings.FunctionService_0_PASSED_IS_NULL
          .toLocalizedString("DistributedSystem instance "));
    }
    Set<DistributedMember> members = new HashSet<DistributedMember>();
    for (String group : groups) {
      List<DistributedMember> grpMembers =
          new ArrayList<DistributedMember>(system.getGroupMembers(group));
      if (!grpMembers.isEmpty()) {
        if (!RANDOM_onMember && grpMembers.contains(system.getDistributedMember())) {
          members.add(system.getDistributedMember());
        } else {
          Collections.shuffle(grpMembers);
          members.add(grpMembers.get(0));
        }
      }
    }
    if (members.isEmpty()) {
      throw new FunctionException(LocalizedStrings.FunctionService_NO_MEMBERS_FOUND_IN_GROUPS
          .toLocalizedString(Arrays.toString(groups)));
    }
    return new MemberFunctionExecutor(system, members);
  }
}
