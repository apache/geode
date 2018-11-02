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
package org.apache.geode.management.cli;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.shiro.subject.Subject;
import org.springframework.shell.core.CommandMarker;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.exceptions.EntityNotFoundException;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.security.ResourcePermission;

@Experimental
public abstract class GfshCommand implements CommandMarker {
  public static final String EXPERIMENTAL = "(Experimental) ";
  private InternalCache cache;

  public boolean isConnectedAndReady() {
    Gfsh gfsh = Gfsh.getCurrentInstance();
    return gfsh != null && gfsh.isConnectedAndReady();
  }

  public boolean isOnlineCommandAvailable() {
    Gfsh gfsh = Gfsh.getCurrentInstance();
    // command should always be available on the server
    if (gfsh == null) {
      return true;
    }
    // if in gfshVM, only when gfsh is connected and ready
    return gfsh.isConnectedAndReady();
  }

  public void authorize(ResourcePermission.Resource resource,
      ResourcePermission.Operation operation, ResourcePermission.Target target) {
    cache.getSecurityService().authorize(resource, operation, target);
  }

  public void authorize(ResourcePermission.Resource resource,
      ResourcePermission.Operation operation, String target) {
    cache.getSecurityService().authorize(resource, operation, target);
  }

  public void authorize(ResourcePermission.Resource resource,
      ResourcePermission.Operation operation, String target, String key) {
    cache.getSecurityService().authorize(resource, operation, target, key);
  }

  public Cache getCache() {
    return cache;
  }

  public <T extends ManagementService> T getManagementService() {
    return (T) ManagementService.getExistingManagementService(cache);
  }

  public ConfigurationPersistenceService getConfigurationPersistenceService() {
    InternalLocator locator = InternalLocator.getLocator();
    return locator == null ? null : locator.getConfigurationPersistenceService();
  }


  public void setCache(Cache cache) {
    this.cache = (InternalCache) cache;
  }

  public boolean isSharedConfigurationRunning() {
    InternalLocator locator = InternalLocator.getLocator();
    return locator != null && locator.isSharedConfigurationRunning();
  }

  public Subject getSubject() {
    return cache.getSecurityService().getSubject();
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
    return CliUtil.getDistributedMemberByNameOrId(memberName, (InternalCache) getCache());
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

  /**
   * Get All members >= a specific version, excluding locators
   */
  public Set<DistributedMember> getNormalMembersWithSameOrNewerVersion(Version version) {
    return CliUtil.getNormalMembersWithSameOrNewerVersion(cache, version);
  }

  public Execution getMembersFunctionExecutor(final Set<DistributedMember> members) {
    return FunctionService.onMembers(members);
  }

  /**
   * if no members matches these names, an empty set would return, this does not include locators
   */
  public Set<DistributedMember> findMembers(String[] groups, String[] members) {
    return CliUtil.findMembers(groups, members, cache);
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
    return CliUtil.findMembersIncludingLocators(groups, members, (InternalCache) getCache());
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

  public CliFunctionResult executeFunctionAndGetFunctionResult(Function function, Object args,
      final DistributedMember targetMember) {
    ResultCollector rc = executeFunction(function, args, Collections.singleton(targetMember));
    List<CliFunctionResult> results = CliFunctionResult.cleanResults((List<?>) rc.getResult());
    return results.size() > 0 ? results.get(0) : null;
  }

  public List<CliFunctionResult> executeAndGetFunctionResult(Function function, Object args,
      Set<DistributedMember> targetMembers) {
    ResultCollector rc = executeFunction(function, args, targetMembers);
    return CliFunctionResult.cleanResults((List<?>) rc.getResult());
  }
}
