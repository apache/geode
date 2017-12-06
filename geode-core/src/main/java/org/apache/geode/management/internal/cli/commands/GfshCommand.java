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
package org.apache.geode.management.internal.cli.commands;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.springframework.shell.core.CommandMarker;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterConfigurationService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.exceptions.EntityNotFoundException;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.shell.Gfsh;

/**
 * Encapsulates common functionality for implementing command classes for the Geode shell (gfsh).
 * this provides wrapper around the static methods in CliUtils for easy mock of the commands
 *
 * this class should not have much implementation of its own other then those tested in
 * GfshCommandJUnitTest
 */
@SuppressWarnings("unused")
public interface GfshCommand extends CommandMarker {

  default boolean isConnectedAndReady() {
    return getGfsh() != null && getGfsh().isConnectedAndReady();
  }

  default ClusterConfigurationService getSharedConfiguration() {
    InternalLocator locator = InternalLocator.getLocator();
    return locator == null ? null : locator.getSharedConfiguration();
  }

  default void persistClusterConfiguration(Result result, Runnable runnable) {
    if (result == null) {
      throw new IllegalArgumentException("Result should not be null");
    }
    ClusterConfigurationService sc = getSharedConfiguration();
    if (sc == null) {
      result.setCommandPersisted(false);
    } else {
      runnable.run();
      result.setCommandPersisted(true);
    }
  }

  default boolean isDebugging() {
    return getGfsh() != null && getGfsh().getDebug();
  }

  default boolean isLogging() {
    return getGfsh() != null;
  }

  default InternalCache getCache() {
    return (InternalCache) CacheFactory.getAnyInstance();
  }

  default SecurityService getSecurityService() {
    return getCache().getSecurityService();
  }

  default Gfsh getGfsh() {
    return Gfsh.getCurrentInstance();
  }

  /**
   * this either returns a non-null member or throw an exception if member is not found.
   */
  default DistributedMember getMember(final String memberName) {
    DistributedMember member = findMember(memberName);

    if (member == null) {
      throw new EntityNotFoundException(
          CliStrings.format(CliStrings.MEMBER_NOT_FOUND_ERROR_MESSAGE, memberName));
    }
    return member;
  }

  /**
   * this will return the member found or null if no member with that name
   */
  default DistributedMember findMember(final String memberName) {
    return CliUtil.getDistributedMemberByNameOrId(memberName);
  }

  /**
   * Gets all members in the GemFire distributed system/cache, including locators
   */
  default Set<DistributedMember> getAllMembers(final InternalCache cache) {
    return CliUtil.getAllMembers(cache);
  }

  /**
   * Get All members, excluding locators
   */
  default Set<DistributedMember> getAllNormalMembers(InternalCache cache) {
    return CliUtil.getAllNormalMembers(cache);
  }

  default Execution getMembersFunctionExecutor(final Set<DistributedMember> members) {
    return FunctionService.onMembers(members);
  }

  /**
   * if no members matches these names, an empty set would return
   */
  default Set<DistributedMember> findMembers(String[] groups, String[] members) {
    return CliUtil.findMembers(groups, members);
  }

  /**
   * if no members matches these names, a UserErrorException will be thrown
   */
  default Set<DistributedMember> getMembers(String[] groups, String[] members) {
    Set<DistributedMember> matchingMembers = findMembers(groups, members);
    if (matchingMembers.size() == 0) {
      throw new EntityNotFoundException(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }
    return matchingMembers;
  }

  /**
   * if no members matches these names, an empty set would return
   */
  default Set<DistributedMember> findMembersIncludingLocators(String[] groups, String[] members) {
    return CliUtil.findMembersIncludingLocators(groups, members);
  }

  /**
   * if no members matches these names, a UserErrorException will be thrown
   */
  default Set<DistributedMember> getMembersIncludingLocators(String[] groups, String[] members) {
    Set<DistributedMember> matchingMembers = findMembersIncludingLocators(groups, members);
    if (matchingMembers.size() == 0) {
      throw new EntityNotFoundException(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }
    return matchingMembers;
  }

  default ManagementService getManagementService() {
    return ManagementService.getExistingManagementService(getCache());
  }

  default Set<DistributedMember> findMembersForRegion(InternalCache cache, String regionPath) {
    return CliUtil.getRegionAssociatedMembers(regionPath, cache, true);
  }

  default ResultCollector<?, ?> executeFunction(Function function, Object args,
      final Set<DistributedMember> targetMembers) {
    return CliUtil.executeFunction(function, args, targetMembers);
  }

  default ResultCollector<?, ?> executeFunction(Function function, Object args,
      final DistributedMember targetMember) {
    return executeFunction(function, args, Collections.singleton(targetMember));
  }

  default List<CliFunctionResult> executeAndGetFunctionResult(Function function, Object args,
      Set<DistributedMember> targetMembers) {
    ResultCollector rc = executeFunction(function, args, targetMembers);
    return CliFunctionResult.cleanResults((List<?>) rc.getResult());
  }

  default Set<DistributedMember> findAnyMembersForRegion(InternalCache cache, String regionPath) {
    return CliUtil.getRegionAssociatedMembers(regionPath, cache, false);
  }

  default Set<DistributedMember> findMembersWithAsyncEventQueue(String queueId) {
    return CliUtil.getMembersWithAsyncEventQueue(getCache(), queueId);
  }
}
