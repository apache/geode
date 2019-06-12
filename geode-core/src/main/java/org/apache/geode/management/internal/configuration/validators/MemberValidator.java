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

package org.apache.geode.management.internal.configuration.validators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.configuration.mutators.ConfigurationManager;
import org.apache.geode.management.internal.exceptions.EntityExistsException;

/**
 * this is used to validate all the common attributes of CacheElement, eg. name and group
 */
public class MemberValidator {
  private ConfigurationPersistenceService persistenceService;
  private InternalCache cache;

  public MemberValidator(InternalCache cache, ConfigurationPersistenceService persistenceService) {
    this.cache = cache;
    this.persistenceService = persistenceService;
  }

  public void validateCreate(CacheElement config, ConfigurationManager manager) {

    String[] groupWithThisElement = findGroupsWithThisElement(config, manager);
    if (groupWithThisElement.length == 0) {
      return;
    }

    Set<DistributedMember> membersOfExistingGroups = findMembers(groupWithThisElement);
    Set<DistributedMember> membersOfNewGroup = findMembers(config.getConfigGroup());
    Collection<DistributedMember> intersection =
        CollectionUtils.intersection(membersOfExistingGroups, membersOfNewGroup);
    if (intersection.size() > 0) {
      String members =
          intersection.stream().map(DistributedMember::getName).collect(Collectors.joining(", "));
      throw new EntityExistsException(
          "Member(s) " + members + " already has this element created.");
    }
  }

  public String[] findGroupsWithThisElement(CacheElement config, ConfigurationManager manager) {
    // if the same element exists in some groups already, make sure the groups has no common members
    List<String> groupWithThisElement = new ArrayList<>();
    for (String group : persistenceService.getGroups()) {
      CacheConfig cacheConfig = persistenceService.getCacheConfig(group);
      if (cacheConfig != null && manager.get(config.getId(), cacheConfig) != null) {
        groupWithThisElement.add(group);
      }
    }
    return groupWithThisElement.toArray(new String[0]);
  }

  /**
   * @param groups should not be null contains no element
   */
  public Set<DistributedMember> findMembers(String... groups) {
    if (groups == null || groups.length == 0) {
      throw new IllegalArgumentException("groups cannot be empty");
    }

    Set<DistributedMember> allMembers = getAllServers();

    // if groups contains "cluster" group, return all members
    if (Arrays.stream(groups).filter(g -> g.equals("cluster")).findFirst().isPresent()) {
      return allMembers;
    }

    Set<DistributedMember> matchingMembers = new HashSet<>();
    for (String group : groups) {
      matchingMembers.addAll(
          allMembers.stream().filter(m -> m.getGroups() != null && m.getGroups().contains(group))
              .collect(Collectors.toSet()));
    }
    return matchingMembers;
  }

  Set<DistributedMember> getAllServers() {
    return cache.getDistributionManager().getNormalDistributionManagerIds()
        .stream().map(DistributedMember.class::cast).collect(Collectors.toSet());
  }
}
