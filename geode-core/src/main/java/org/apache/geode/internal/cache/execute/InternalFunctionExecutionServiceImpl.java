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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionService;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.InternalClientCache;
import org.apache.geode.cache.client.internal.ProxyCache;
import org.apache.geode.cache.client.internal.ProxyRegion;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.util.internal.GeodeGlossary;

public class InternalFunctionExecutionServiceImpl
    implements FunctionExecutionService, InternalFunctionExecutionService {

  /**
   * use when the optimization to execute onMember locally is not desired.
   */
  public static final boolean RANDOM_onMember =
      Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "randomizeOnMember");

  private static final String[] EMPTY_GROUPS = new String[0];

  @MakeNotStatic
  private static final ConcurrentHashMap<String, Function> idToFunctionMap =
      new ConcurrentHashMap<>();

  public InternalFunctionExecutionServiceImpl() {
    // nothing
  }

  // FunctionExecutionService API ----------------------------------------------------------------

  @Override
  public Execution onServer(Pool pool) {
    return onServer(pool, EMPTY_GROUPS);
  }

  @Override
  public Execution onServers(Pool pool) {
    return onServers(pool, EMPTY_GROUPS);
  }

  @Override
  public Execution onServer(RegionService regionService) {
    return onServer(regionService, EMPTY_GROUPS);
  }

  @Override
  public Execution onServers(RegionService regionService) {
    return onServers(regionService, EMPTY_GROUPS);
  }

  @Override
  public Execution onMember(DistributedMember distributedMember) {
    return onMember(getDistributedSystem(), distributedMember);
  }

  @Override
  public Execution onMembers(String... groups) {
    return onMembers(getDistributedSystem(), groups);
  }

  @Override
  public Execution onMembers(Set<DistributedMember> distributedMembers) {
    return onMembers(getDistributedSystem(), distributedMembers);
  }

  @Override
  public Execution onMember(String... groups) {
    return onMember(getDistributedSystem(), groups);
  }

  protected Pool findPool(String poolName) {
    return PoolManager.find(poolName);
  }

  @Override
  public Execution onRegion(Region region) {
    if (region == null) {
      throw new FunctionException("Region instance passed is null");
    }

    ProxyCache proxyCache = null;
    String poolName = region.getAttributes().getPoolName();
    if (poolName != null) {
      Pool pool = findPool(poolName);

      if (pool == null) {
        throw new IllegalStateException(String.format("Could not find a pool named %s.", poolName));
      } else {
        if (pool.getMultiuserAuthentication()) {
          if (region instanceof ProxyRegion) {
            ProxyRegion proxyRegion = (ProxyRegion) region;
            region = proxyRegion.getRealRegion();
            proxyCache = proxyRegion.getAuthenticatedCache();
          } else {
            throw new UnsupportedOperationException();
          }
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

  @Override
  public Function getFunction(String functionId) {
    if (functionId == null) {
      throw new FunctionException(String.format("%s passed is null",
          "functionId instance "));
    }
    return idToFunctionMap.get(functionId);
  }

  @Override
  public void registerFunction(Function function) {
    if (function == null) {
      throw new FunctionException(String.format("%s passed is null",
          "function instance "));
    }
    if (function.getId() == null) {
      throw new FunctionException(
          "function.getId() returned null, implement the Function.getId() method properly");
    }
    if (function.isHA() && !function.hasResult()) {
      throw new FunctionException(
          "For Functions with isHA true, hasResult must also be true.");
    }

    idToFunctionMap.put(function.getId(), function);
  }

  @Override
  public void unregisterFunction(String functionId) {
    if (functionId == null) {
      throw new FunctionException(String.format("%s passed is null",
          "functionId instance "));
    }
    idToFunctionMap.remove(functionId);
  }

  @Override
  public boolean isRegistered(String functionId) {
    if (functionId == null) {
      throw new FunctionException(String.format("%s passed is null",
          "functionId instance "));
    }
    return idToFunctionMap.containsKey(functionId);
  }

  @Override
  public Map<String, Function> getRegisteredFunctions() {
    // We have to remove the internal functions before returning the map to the users
    final Map<String, Function> tempIdToFunctionMap = new HashMap<>();
    for (Map.Entry<String, Function> entry : idToFunctionMap.entrySet()) {
      if (!(entry.getValue() instanceof InternalEntity)) {
        tempIdToFunctionMap.put(entry.getKey(), entry.getValue());
      }
    }
    return tempIdToFunctionMap;
  }

  // InternalFunctionExecutionService OnServerGroups API -----------------------------------------

  @Override
  public Execution onServer(Pool pool, String... groups) {
    if (pool == null) {
      throw new FunctionException(
          String.format("%s passed is null", "Pool instance "));
    }

    if (pool.getMultiuserAuthentication()) {
      throw new UnsupportedOperationException();
    }

    return new ServerFunctionExecutor(pool, false, groups);
  }

  @Override
  public Execution onServers(Pool pool, String... groups) {
    if (pool == null) {
      throw new FunctionException(
          String.format("%s passed is null", "Pool instance "));
    }

    if (pool.getMultiuserAuthentication()) {
      throw new UnsupportedOperationException();
    }

    return new ServerFunctionExecutor(pool, true, groups);
  }

  @Override
  public Execution onServer(RegionService regionService, String... groups) {
    if (regionService == null) {
      throw new FunctionException(String.format("%s passed is null",
          "RegionService instance "));
    }
    if (regionService instanceof GemFireCacheImpl) {
      InternalClientCache internalCache = (InternalClientCache) regionService;
      if (!internalCache.isClient()) {
        throw new FunctionException("The cache was not a client cache");
      } else if (internalCache.getDefaultPool() != null) {
        return onServer(internalCache.getDefaultPool(), groups);
      } else {
        throw new FunctionException("The client cache does not have a default pool");
      }
    } else {
      ProxyCache proxyCache = (ProxyCache) regionService;
      return new ServerFunctionExecutor(proxyCache.getUserAttributes().getPool(), false, proxyCache,
          groups);
    }
  }

  @Override
  public Execution onServers(RegionService regionService, String... groups) {
    if (regionService == null) {
      throw new FunctionException(String.format("%s passed is null",
          "RegionService instance "));
    }
    if (regionService instanceof GemFireCacheImpl) {
      InternalClientCache internalCache = (InternalClientCache) regionService;
      if (!internalCache.isClient()) {
        throw new FunctionException("The cache was not a client cache");
      } else if (internalCache.getDefaultPool() != null) {
        return onServers(internalCache.getDefaultPool(), groups);
      } else {
        throw new FunctionException("The client cache does not have a default pool");
      }
    } else {
      ProxyCache proxyCache = (ProxyCache) regionService;
      return new ServerFunctionExecutor(proxyCache.getUserAttributes().getPool(), true, proxyCache,
          groups);
    }
  }

  // InternalFunctionExecutionService InDistributedSystem API ------------------------------------

  @Override
  public Execution onMember(DistributedSystem system, DistributedMember distributedMember) {
    if (system == null) {
      throw new FunctionException(String.format("%s passed is null",
          "DistributedSystem instance "));
    }
    if (distributedMember == null) {
      throw new FunctionException(String.format("%s passed is null",
          "DistributedMember instance "));
    }
    return new MemberFunctionExecutor(system, distributedMember);
  }

  @Override
  public Execution onMembers(DistributedSystem system, String... groups) {
    if (system == null) {
      throw new FunctionException(String.format("%s passed is null",
          "DistributedSystem instance "));
    }
    if (groups.length == 0) {
      return new MemberFunctionExecutor(system);
    }
    Set<DistributedMember> members = new HashSet<>();
    for (String group : groups) {
      members.addAll(system.getGroupMembers(group));
    }
    if (members.isEmpty()) {
      throw new FunctionException(String.format("No members found in group(s) %s",
          Arrays.toString(groups)));
    }
    return new MemberFunctionExecutor(system, members);
  }

  @Override
  public Execution onMember(DistributedSystem system, String... groups) {
    if (system == null) {
      throw new FunctionException(String.format("%s passed is null",
          "DistributedSystem instance "));
    }
    Set<DistributedMember> members = new HashSet<>();
    for (String group : groups) {
      List<DistributedMember> grpMembers = new ArrayList<>(system.getGroupMembers(group));
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
      throw new FunctionException(String.format("No members found in group(s) %s",
          Arrays.toString(groups)));
    }
    return new MemberFunctionExecutor(system, members);
  }

  @Override
  public Execution onMembers(DistributedSystem system, Set<DistributedMember> distributedMembers) {
    if (system == null) {
      throw new FunctionException(String.format("%s passed is null",
          "DistributedSystem instance "));
    }
    if (distributedMembers == null) {
      throw new FunctionException(String.format("%s passed is null",
          "distributedMembers set "));
    }
    return new MemberFunctionExecutor(system, distributedMembers);
  }

  // InternalFunctionExecutionService OnRegions API ----------------------------------------------

  @Override
  public Execution onRegions(Set<Region> regions) {
    if (regions == null) {
      throw new IllegalArgumentException(
          String.format("The input %s for the execute function request is null",
              "regions set"));
    }
    if (regions.contains(null)) {
      throw new IllegalArgumentException(
          "One or more region references added to the regions set is(are) null");
    }
    if (regions.isEmpty()) {
      throw new IllegalArgumentException(
          "Regions set is empty for onRegions function execution");
    }
    for (Region region : regions) {
      if (isClientRegion(region)) {
        throw new UnsupportedOperationException(
            "FunctionService#onRegions() is not supported for cache clients in client server mode");
      }
    }
    return new MultiRegionFunctionExecutor(regions);
  }

  // InternalFunctionExecutionService unregisterAllFunctions API ---------------------------------

  @Override
  public void unregisterAllFunctions() {
    // Unregistering all the functions registered with the FunctionService.
    for (String functionId : idToFunctionMap.keySet()) {
      unregisterFunction(functionId);
    }
  }

  /**
   * @return true if the method is called on a region has a {@link Pool}.
   * @since GemFire 6.0
   */
  private boolean isClientRegion(Region region) {
    return ((InternalRegion) region).hasServerProxy();
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
