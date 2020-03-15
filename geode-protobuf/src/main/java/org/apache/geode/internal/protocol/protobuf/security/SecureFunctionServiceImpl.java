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
package org.apache.geode.internal.protocol.protobuf.security;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.cache.InternalCache;

public class SecureFunctionServiceImpl implements SecureFunctionService {

  private final Security security;
  private final InternalCache internalCache;

  public SecureFunctionServiceImpl(InternalCache internalCache, Security security) {
    this.security = security;
    this.internalCache = internalCache;
  }

  @Override
  public List<Object> executeFunctionOnRegion(String functionID, String regionName,
      Object arguments, Set<?> keyFilter) {

    Function<Object> function = authorizeAndGetFunction(regionName, functionID, arguments);
    Region<?, ?> region = getRegion(regionName);
    @SuppressWarnings("unchecked")
    Execution<Object, Object, List<Object>> execution = FunctionService.onRegion(region);
    if (keyFilter != null) {
      execution = execution.withFilter(keyFilter);
    }
    return executeFunction(execution, functionID, function, arguments);
  }

  private List<Object> executeFunction(Execution<Object, Object, List<Object>> execution,
      String functionID, Function<Object> function,
      Object arguments) {
    if (arguments != null) {
      execution = execution.setArguments(arguments);
    }
    ResultCollector<Object, List<Object>> collector = execution.execute(functionID);
    if (function.hasResult()) {
      return collector.getResult();
    } else {
      return Collections.emptyList();
    }
  }

  private <T> Function<T> authorizeAndGetFunction(String regionName, String functionID,
      Object arguments) {
    @SuppressWarnings("unchecked")
    final Function<T> function = FunctionService.getFunction(functionID);
    if (function == null) {
      throw new IllegalArgumentException(
          String.format("Function named %s is not registered to FunctionService",
              functionID));
    }

    function.getRequiredPermissions(regionName, arguments).forEach(security::authorize);
    return function;
  }

  @Override
  public List<Object> executeFunctionOnMember(String functionID, Object arguments,
      List<String> memberNameList) {

    Function<Object> function = authorizeAndGetFunction(null, functionID, arguments);
    @SuppressWarnings("unchecked")
    Execution<Object, Object, List<Object>> execution =
        FunctionService.onMembers(getMemberIDs(functionID, memberNameList));
    return executeFunction(execution, functionID, function, arguments);
  }

  @Override
  public List<Object> executeFunctionOnGroups(String functionID, Object arguments,
      List<String> groupNameList) {
    Function<Object> function = authorizeAndGetFunction(null, functionID, arguments);
    @SuppressWarnings("unchecked")
    Execution<Object, Object, List<Object>> execution =
        FunctionService.onMember(groupNameList.toArray(new String[0]));
    return executeFunction(execution, functionID, function, arguments);
  }

  private Set<DistributedMember> getMemberIDs(String functionID, List<String> memberNameList) {
    Set<DistributedMember> memberIds = new HashSet<>(memberNameList.size());
    DistributionManager distributionManager = internalCache.getDistributionManager();
    for (String name : memberNameList) {
      DistributedMember member = distributionManager.getMemberWithName(name);
      if (member == null) {
        throw new IllegalArgumentException(
            "Member " + name + " not found to execute \"" + functionID + "\"");
      }
      memberIds.add(member);
    }
    if (memberIds.isEmpty()) {
      throw new IllegalArgumentException("No members found to execute \"" + functionID + "\"");
    }

    return memberIds;
  }

  private <K, V> Region<K, V> getRegion(String regionName) {
    Region<K, V> region = internalCache.getRegion(regionName);
    if (region == null) {
      throw new RegionDestroyedException("Region not found " + regionName, regionName);
    }
    return region;
  }
}
