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
import java.util.Objects;
import java.util.Set;

import org.springframework.shell.core.CommandMarker;

import org.apache.geode.cache.Cache;
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
import org.apache.geode.management.internal.configuration.domain.XmlEntity;

/**
 * Encapsulates common functionality for implementing command classes for the Geode shell (gfsh).
 * this provides wrapper around the static methods in CliUtils for easy mock of the commands
 *
 * this class should not have much implementation of its own other then those tested in
 * GfshCommandJUnitTest
 */
@SuppressWarnings("unused")
public abstract class GfshCommand implements CommandMarker {
  private InternalCache cache;

  public void setCache(Cache cache) {
    this.cache = (InternalCache) cache;
  }

  public InternalCache getCache() {
    return cache;
  }

  public static final String EXPERIMENTAL = "(Experimental) ";

  public boolean isConnectedAndReady() {
    return getGfsh() != null && getGfsh().isConnectedAndReady();
  }

  public ClusterConfigurationService getSharedConfiguration() {
    InternalLocator locator = InternalLocator.getLocator();
    return locator == null ? null : locator.getSharedConfiguration();
  }

  public void persistClusterConfiguration(Result result, Runnable runnable) {
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

  public XmlEntity findXmlEntity(List<CliFunctionResult> functionResults) {
    return functionResults.stream().filter(CliFunctionResult::isSuccessful)
        .map(CliFunctionResult::getXmlEntity).filter(Objects::nonNull).findFirst().orElse(null);
  }

  public boolean isDebugging() {
    return getGfsh() != null && getGfsh().getDebug();
  }

  public boolean isLogging() {
    return getGfsh() != null;
  }

  public SecurityService getSecurityService() {
    return getCache().getSecurityService();
  }

  public Gfsh getGfsh() {
    return Gfsh.getCurrentInstance();
  }

  /**
   * this either returns a non-null member or throw an exception if member is not found.
   */
  public DistributedMember getMember(final String memberName) {
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
  public DistributedMember findMember(final String memberName) {
    return CliUtil.getDistributedMemberByNameOrId(memberName, getCache());
  }

  /**
   * Gets all members in the GemFire distributed system/cache, including locators
   */
  public Set<DistributedMember> getAllMembers() {
    return CliUtil.getAllMembers(cache);
  }

  /**
   * Get All members, excluding locators
   */
  public Set<DistributedMember> getAllNormalMembers() {
    return CliUtil.getAllNormalMembers(cache);
  }

  public Execution getMembersFunctionExecutor(final Set<DistributedMember> members) {
    return FunctionService.onMembers(members);
  }

  /**
   * if no members matches these names, an empty set would return, this does not include locators
   */
  public Set<DistributedMember> findMembers(String[] groups, String[] members) {
    return CliUtil.findMembers(groups, members, getCache());
  }

  /**
   * if no members matches these names, a UserErrorException will be thrown
   */
  public Set<DistributedMember> getMembers(String[] groups, String[] members) {
    Set<DistributedMember> matchingMembers = findMembers(groups, members);
    if (matchingMembers.size() == 0) {
      throw new EntityNotFoundException(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }
    return matchingMembers;
  }

  /**
   * if no members matches these names, an empty set would return
   */
  public Set<DistributedMember> findMembersIncludingLocators(String[] groups, String[] members) {
    return CliUtil.findMembersIncludingLocators(groups, members, getCache());
  }

  /**
   * if no members matches these names, a UserErrorException will be thrown
   */
  public Set<DistributedMember> getMembersIncludingLocators(String[] groups, String[] members) {
    Set<DistributedMember> matchingMembers = findMembersIncludingLocators(groups, members);
    if (matchingMembers.size() == 0) {
      throw new EntityNotFoundException(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }
    return matchingMembers;
  }

  public ManagementService getManagementService() {
    return ManagementService.getExistingManagementService(getCache());
  }

  public Set<DistributedMember> findMembersForRegion(String regionPath) {
    return CliUtil.getRegionAssociatedMembers(regionPath, cache, true);
  }

  public Set<DistributedMember> findAnyMembersForRegion(String regionPath) {
    return CliUtil.getRegionAssociatedMembers(regionPath, cache, false);
  }

  public ResultCollector<?, ?> executeFunction(Function function, Object args,
      final Set<DistributedMember> targetMembers) {
    return CliUtil.executeFunction(function, args, targetMembers);
  }

  public ResultCollector<?, ?> executeFunction(Function function, Object args,
      final DistributedMember targetMember) {
    return executeFunction(function, args, Collections.singleton(targetMember));
  }

  public List<CliFunctionResult> executeAndGetFunctionResult(Function function, Object args,
      Set<DistributedMember> targetMembers) {
    ResultCollector rc = executeFunction(function, args, targetMembers);
    return CliFunctionResult.cleanResults((List<?>) rc.getResult());
  }

  public Set<DistributedMember> findMembersWithAsyncEventQueue(String queueId) {
    return CliUtil.getMembersWithAsyncEventQueue(getCache(), queueId);
  }
}
