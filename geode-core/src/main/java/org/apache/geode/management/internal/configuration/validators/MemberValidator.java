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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.configuration.AbstractConfiguration;
import org.apache.geode.management.configuration.GroupableConfiguration;
import org.apache.geode.management.configuration.RegionScoped;
import org.apache.geode.management.internal.configuration.mutators.CacheConfigurationManager;
import org.apache.geode.management.internal.exceptions.EntityExistsException;

/**
 * this is used to validate all the common attributes of CacheElement, eg. name and group
 */
public class MemberValidator {
  private final ConfigurationPersistenceService persistenceService;
  private final InternalCache cache;

  public MemberValidator(InternalCache cache, ConfigurationPersistenceService persistenceService) {
    this.cache = cache;
    this.persistenceService = persistenceService;
  }

  public void validateCreate(AbstractConfiguration config, CacheConfigurationManager manager) {

    Map<String, AbstractConfiguration> existingElementsAndTheirGroups =
        findCacheElement(config, manager);
    if (existingElementsAndTheirGroups.size() == 0) {
      return;
    }

    // if the configuration is not groupable and already exists, throw exception
    if (!(config instanceof GroupableConfiguration)) {
      throw new EntityExistsException(
          config.getClass().getSimpleName() + " '" + config.getId()
              + "' already exists");
    }

    // if configuration is groupable, then check if it's already in the group
    String configGroup = AbstractConfiguration.getGroupName(config.getGroup());
    if (existingElementsAndTheirGroups.containsKey(configGroup)) {
      throw new EntityExistsException(
          config.getClass().getSimpleName() + " '" + config.getId()
              + "' already exists in group " + configGroup);
    }

    // if other group and this new group has common members, then throw exception
    String[] groups = existingElementsAndTheirGroups.keySet().toArray(new String[0]);
    Set<DistributedMember> membersOfExistingGroups = findServers(groups);
    Set<DistributedMember> membersOfNewGroup = findServers(config.getGroup());
    Set<DistributedMember> intersection = new HashSet<>(membersOfExistingGroups);
    intersection.retainAll(membersOfNewGroup);
    if (intersection.size() > 0) {
      String members =
          intersection.stream().map(DistributedMember::getName).collect(Collectors.joining(", "));
      throw new EntityExistsException(
          config.getClass().getSimpleName() + " '" + config.getId()
              + "' already exists on member(s) " + members + ".");
    }

    // if there is no common member, we still need to verify if the new config is compatible with
    // the existing ones.
    for (Map.Entry<String, AbstractConfiguration> existing : existingElementsAndTheirGroups
        .entrySet()) {
      manager.checkCompatibility(config, existing.getKey(), existing.getValue());
    }
  }

  public String[] findGroupsWithThisElement(AbstractConfiguration config,
      CacheConfigurationManager manager) {
    return findCacheElement(config, manager).keySet().toArray(new String[0]);
  }

  /**
   * this returns a map of CacheElement with this id, with the group as the key of the map
   */
  public Map<String, AbstractConfiguration> findCacheElement(AbstractConfiguration config,
      CacheConfigurationManager manager) {
    Map<String, AbstractConfiguration> results = new HashMap<>();
    for (String group : persistenceService.getGroups()) {
      CacheConfig cacheConfig = persistenceService.getCacheConfig(group, true);
      AbstractConfiguration existing = manager.get(config, cacheConfig);
      if (existing != null) {
        results.put(group, existing);
      }
    }
    return results;
  }

  public Set<String> findGroups(String regionName) {
    Set<String> results = new HashSet<>();
    Set<String> groups = persistenceService.getGroups();
    for (String group : groups) {
      CacheConfig existing = persistenceService.getCacheConfig(group, false);
      if (existing != null && existing.findRegionConfiguration(regionName) != null) {
        results.add(group);
      }
    }
    return results;
  }

  /**
   * @param groups should not be null contains no element
   */
  public Set<DistributedMember> findServers(String... groups) {
    return findMembers(false, groups);
  }


  public Set<DistributedMember> findServers(AbstractConfiguration configuration) {
    if (configuration instanceof RegionScoped) {
      Set<String> groups = findGroups(((RegionScoped) configuration).getRegionName());
      if (groups.size() == 0) {
        return Collections.emptySet();
      }
      return findServers(groups.toArray(new String[0]));
    }

    return findServers(configuration.getGroup());
  }

  /**
   * if id is specified, find the member with that id, otherwise find members in the groups
   */
  public Set<DistributedMember> findMembers(String id, String... groups) {
    if (StringUtils.isNotBlank(id)) {
      return getAllServersAndLocators().stream().filter(m -> m.getName().equals(id))
          .collect(Collectors.toSet());
    }

    return findMembers(true, groups);
  }

  /**
   * find members within these groups
   *
   * @param includeLocators whether to include locators in this search or not
   */
  public Set<DistributedMember> findMembers(boolean includeLocators, String... groups) {
    if (groups == null) {
      groups = new String[] {AbstractConfiguration.CLUSTER};
    }

    groups = Arrays.stream(groups).filter(Objects::nonNull).filter(s -> s.length() > 0)
        .toArray(String[]::new);

    if (groups.length == 0) {
      groups = new String[] {AbstractConfiguration.CLUSTER};
    }

    Set<DistributedMember> all = includeLocators ? getAllServersAndLocators() : getAllServers();

    if (Arrays.stream(groups).anyMatch(AbstractConfiguration::isCluster)) {
      return all;
    }

    Set<DistributedMember> matchingMembers = new HashSet<>();
    for (String group : groups) {
      matchingMembers.addAll(
          all.stream().filter(m -> m.getGroups() != null && m.getGroups().contains(group))
              .collect(Collectors.toSet()));
    }
    return matchingMembers;
  }

  Set<DistributedMember> getAllServers() {
    return cache.getDistributionManager().getNormalDistributionManagerIds()
        .stream().map(DistributedMember.class::cast).collect(Collectors.toSet());
  }

  Set<DistributedMember> getAllServersAndLocators() {
    return cache.getDistributionManager().getDistributionManagerIds()
        .stream().map(DistributedMember.class::cast).collect(Collectors.toSet());
  }
}
