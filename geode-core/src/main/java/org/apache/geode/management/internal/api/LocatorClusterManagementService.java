/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.management.internal.api;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.configuration.MemberConfig;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.UpdateCacheFunction;
import org.apache.geode.management.internal.configuration.mutators.ConfigurationManager;
import org.apache.geode.management.internal.configuration.mutators.MemberConfigManager;
import org.apache.geode.management.internal.configuration.mutators.RegionConfigManager;
import org.apache.geode.management.internal.configuration.validators.CacheElementValidator;
import org.apache.geode.management.internal.configuration.validators.ConfigurationValidator;
import org.apache.geode.management.internal.configuration.validators.RegionConfigValidator;
import org.apache.geode.management.internal.exceptions.EntityExistsException;

public class LocatorClusterManagementService implements ClusterManagementService {
  private static final Logger logger = LogService.getLogger();
  private InternalCache cache;
  private ConfigurationPersistenceService persistenceService;
  private HashMap<Class, ConfigurationManager> managers;
  private HashMap<Class, ConfigurationValidator> validators;

  public LocatorClusterManagementService(InternalCache cache,
      ConfigurationPersistenceService persistenceService) {
    this(cache, persistenceService, new HashMap(), new HashMap());
    // initialize the list of managers
    managers.put(RegionConfig.class, new RegionConfigManager(cache));
    managers.put(MemberConfig.class, new MemberConfigManager(cache));

    // initialize the list of validators
    validators.put(CacheElement.class, new CacheElementValidator());
    validators.put(RegionConfig.class, new RegionConfigValidator(cache));
  }

  @VisibleForTesting
  public LocatorClusterManagementService(InternalCache cache,
      ConfigurationPersistenceService persistenceService, HashMap managers, HashMap validators) {
    this.cache = cache;
    this.persistenceService = persistenceService;
    this.managers = managers;
    this.validators = validators;
  }

  @Override
  public ClusterManagementResult create(CacheElement config) {
    String group = config.getConfigGroup();

    if (persistenceService == null) {
      return new ClusterManagementResult(false,
          "Cluster configuration service needs to be enabled");
    }

    // first validate common attributes of all configuration object
    validators.get(CacheElement.class).validate(config);

    ConfigurationValidator validator = validators.get(config.getClass());
    if (validator != null) {
      validator.validate(config);
      // exit early if config element already exists in cache config
      CacheConfig currentPersistedConfig = persistenceService.getCacheConfig(group, true);
      if (validator.exists(config, currentPersistedConfig)) {
        throw new EntityExistsException("cache element " + config.getId() + " already exists.");
      }
    }

    // validate that user used the correct config object type
    ConfigurationManager configurationManager = managers.get(config.getClass());
    if (configurationManager == null) {
      throw new IllegalArgumentException(String.format("Configuration type %s is not supported.",
          config.getClass().getSimpleName()));
    }

    // execute function on all members
    Set<DistributedMember> targetedMembers = findMembers(group);

    if (targetedMembers.size() == 0) {
      return new ClusterManagementResult(false,
          "no members found in " + group + " to create cache element");
    }

    ClusterManagementResult result = new ClusterManagementResult();

    List<CliFunctionResult> functionResults = executeAndGetFunctionResult(
        new UpdateCacheFunction(),
        Arrays.asList(config, UpdateCacheFunction.CacheElementOperation.ADD),
        targetedMembers);
    functionResults
        .forEach(functionResult -> result.addMemberStatus(functionResult.getMemberIdOrName(),
            functionResult.isSuccessful(),
            functionResult.getStatusMessage()));

    // if any false result is added to the member list
    if (result.getStatusCode() != ClusterManagementResult.StatusCode.OK) {
      result.setStatus(false, "Failed to apply the update on all members.");
      return result;
    }

    // persist configuration in cache config
    final String finalGroup = group; // the below lambda requires a reference that is final
    persistenceService.updateCacheConfig(finalGroup, cacheConfigForGroup -> {
      try {
        configurationManager.add(config, cacheConfigForGroup);
        result.setStatus(true,
            "successfully persisted config for " + finalGroup);
      } catch (Exception e) {
        String message = "failed to update cluster config for " + finalGroup;
        logger.error(message, e);
        result.setStatus(ClusterManagementResult.StatusCode.FAIL_TO_PERSIST, message);
        return null;
      }
      return cacheConfigForGroup;
    });
    return result;
  }

  @Override
  public ClusterManagementResult delete(CacheElement config) {
    throw new NotImplementedException("Not implemented");
  }

  @Override
  public ClusterManagementResult update(CacheElement config) {
    throw new NotImplementedException("Not implemented");
  }

  @Override
  public ClusterManagementResult list(CacheElement filter) {
    ConfigurationManager manager = managers.get(filter.getClass());
    ClusterManagementResult result = new ClusterManagementResult();

    if (filter instanceof MemberConfig) {
      List<CacheElement> listResults = manager.list(filter, null);
      result.setResult(listResults);
      return result;
    }

    if (persistenceService == null) {
      return new ClusterManagementResult(false,
          "Cluster configuration service needs to be enabled");
    }

    List<CacheElement> elements = new ArrayList<>();

    boolean filterHasGroup = StringUtils.isNotBlank(filter.getGroup());
    for (String group : persistenceService.getGroups()) {
      // simply skip if filter group does not equal to current group
      if (filterHasGroup && !group.equals(filter.getConfigGroup())) {
        continue;
      }

      CacheConfig currentPersistedConfig = persistenceService.getCacheConfig(group, true);
      List<CacheElement> listInGroup = manager.list(filter, currentPersistedConfig);

      for (CacheElement element : listInGroup) {
        if (filterHasGroup) {
          elements.add(element);
        }
        // in case different groups have the same entity, we will need to consolidate the
        // group information. this should only happen if we are not filtering on specific
        // group.
        else {
          // group information is only shown when we are not filtering by group
          if (!"cluster".equals(group)) {
            element.setGroup(group);
          }
          CacheElement exist = CacheElement.findElement(elements, element.getId());
          if (exist != null) {
            // combine the group names
            String combined = exist.getConfigGroup() + "," + element.getConfigGroup();
            exist.setGroup(sortedCommaSeparatedList(combined));
          } else {
            elements.add(element);
          }
        }
      }
    }

    result.setResult(elements);
    return result;
  }

  private static String sortedCommaSeparatedList(String combined) {
    return Arrays
        .stream(combined.split(",")).sorted().collect(Collectors.joining(","));
  }

  @Override
  public boolean isConnected() {
    return true;
  }

  @VisibleForTesting
  Set<DistributedMember> findMembers(String group) {
    Stream<DistributedMember> stream =
        cache.getDistributionManager().getNormalDistributionManagerIds()
            .stream().map(DistributedMember.class::cast);
    if (!"cluster".equals(group)) {
      stream = stream.filter(m -> m.getGroups().contains(group));
    }
    return stream.collect(Collectors.toSet());
  }

  @VisibleForTesting
  List<CliFunctionResult> executeAndGetFunctionResult(Function function, Object args,
      Set<DistributedMember> targetMembers) {
    ResultCollector rc = CliUtil.executeFunction(function, args, targetMembers);
    return CliFunctionResult.cleanResults((List<?>) rc.getResult());
  }
}
