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
package org.apache.geode.management.internal.util;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.AbstractExecution;
import org.apache.geode.internal.serialization.Version;

public class ManagementUtils {
  /**
   * Returns a set of all the members of the distributed system excluding locators.
   */
  @SuppressWarnings("unchecked")
  public static Set<DistributedMember> getAllNormalMembers(InternalCache cache) {
    return new HashSet<DistributedMember>(
        cache.getDistributionManager().getNormalDistributionManagerIds());
  }

  /**
   * Returns a set of all the members of the distributed system of a specific version excluding
   * locators.
   */
  @SuppressWarnings("unchecked")
  public static Set<DistributedMember> getNormalMembersWithSameOrNewerVersion(InternalCache cache,
      Version version) {
    return getAllNormalMembers(cache).stream().filter(
        member -> ((InternalDistributedMember) member).getVersionObject().compareTo(version) >= 0)
        .collect(Collectors.toSet());
  }

  /**
   * Returns a set of all the members of the distributed system including locators.
   */
  @SuppressWarnings("unchecked")
  public static Set<DistributedMember> getAllMembers(InternalCache cache) {
    return getAllMembers(cache.getInternalDistributedSystem());
  }

  @SuppressWarnings("unchecked")
  public static Set<DistributedMember> getAllMembers(InternalDistributedSystem internalDS) {
    return new HashSet<DistributedMember>(
        internalDS.getDistributionManager().getDistributionManagerIds());
  }

  /***
   * Executes a function with arguments on a set of members, ignoring the departed members.
   *
   * @param function Function to be executed.
   * @param args Arguments passed to the function, pass null if you wish to pass no arguments to the
   *        function.
   * @param targetMembers Set of members on which the function is to be executed.
   *
   */
  public static ResultCollector<?, ?> executeFunction(final Function function, Object args,
      final Set<DistributedMember> targetMembers) {
    Execution execution;

    if (args != null) {
      execution = FunctionService.onMembers(targetMembers).setArguments(args);
    } else {
      execution = FunctionService.onMembers(targetMembers);
    }

    ((AbstractExecution) execution).setIgnoreDepartedMembers(true);
    return execution.execute(function);
  }
}
