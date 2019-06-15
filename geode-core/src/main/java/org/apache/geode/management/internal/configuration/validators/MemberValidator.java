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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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

    Map<String, CacheElement> existingElementsAndTheirGroups =
        findCacheElement(config.getId(), manager);
    if (existingElementsAndTheirGroups.size() == 0) {
      return;
    }

    Set<DistributedMember> membersOfExistingGroups =
        findMembers(existingElementsAndTheirGroups.keySet().toArray(new String[0]));
    Set<DistributedMember> membersOfNewGroup = findMembers(config.getConfigGroup());
    Set<DistributedMember> intersection = new HashSet<>(membersOfExistingGroups);
    intersection.retainAll(membersOfNewGroup);
    if (intersection.size() > 0) {
      String members =
          intersection.stream().map(DistributedMember::getName).collect(Collectors.joining(", "));
      throw new EntityExistsException(
          "Member(s) " + members + " already has this element created.");
    }

    // if there is no common member, we still need to verify if the new config is compatible with
    // the existing ones.
    for (Map.Entry<String, CacheElement> existing : existingElementsAndTheirGroups.entrySet()) {
      manager.checkCompatibility(config, existing.getKey(), existing.getValue());
    }
  }

  public String[] findGroupsWithThisElement(String id, ConfigurationManager manager) {
    return findCacheElement(id, manager).keySet().toArray(new String[0]);
  }

  /**
   * this returns a map of CacheElement with this id, with the group as the key of the map
   */
  public Map<String, CacheElement> findCacheElement(String id, ConfigurationManager manager) {
    Map<String, CacheElement> results = new HashMap<>();
    for (String group : persistenceService.getGroups()) {
      CacheConfig cacheConfig = persistenceService.getCacheConfig(group);
      if (cacheConfig == null) {
        continue;
      }
      CacheElement existing = manager.get(id, cacheConfig);
      if (existing != null) {
        results.put(group, existing);
      }
    }
    return results;
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
    if (Arrays.asList(groups).contains(CacheElement.CLUSTER)) {
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
