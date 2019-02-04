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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.UpdateCacheFunction;
import org.apache.geode.management.internal.configuration.mutators.ConfigurationMutator;
import org.apache.geode.management.internal.configuration.mutators.RegionConfigMutator;
import org.apache.geode.management.internal.configuration.validators.ConfigurationValidator;
import org.apache.geode.management.internal.configuration.validators.RegionConfigValidator;
import org.apache.geode.management.internal.exceptions.EntityExistsException;

public class LocatorClusterManagementService implements ClusterManagementService {
  private static final Logger logger = LogService.getLogger();
  private DistributionManager distributionManager;
  private ConfigurationPersistenceService persistenceService;
  private HashMap<Class, ConfigurationMutator> mutators;
  private HashMap<Class, ConfigurationValidator> validators;

  public LocatorClusterManagementService(DistributionManager distributionManager,
      ConfigurationPersistenceService persistenceService) {
    this(distributionManager, persistenceService, new HashMap(), new HashMap());
    // initialize the list of mutators
    mutators.put(RegionConfig.class, new RegionConfigMutator());

    // initialize the list of validators
    validators.put(RegionConfig.class, new RegionConfigValidator());
  }

  @VisibleForTesting
  public LocatorClusterManagementService(DistributionManager distributionManager,
      ConfigurationPersistenceService persistenceService, HashMap mutators, HashMap validators) {
    this.distributionManager = distributionManager;
    this.persistenceService = persistenceService;
    this.mutators = mutators;
    this.validators = validators;
  }

  @Override
  public ClusterManagementResult create(CacheElement config, String group) {
    if (group == null) {
      group = "cluster";
    }

    if (persistenceService == null) {
      return new ClusterManagementResult(false,
          "Cluster configuration service needs to be enabled");
    }

    ClusterManagementResult result = new ClusterManagementResult();
    ConfigurationMutator configurationMutator = mutators.get(config.getClass());

    ConfigurationValidator validator = validators.get(config.getClass());
    if (validator != null) {
      try {
        validator.validate(config);
      } catch (IllegalArgumentException e) {
        return new ClusterManagementResult(false, e.getMessage());
      }
    }

    // exit early if config element already exists in cache config
    CacheConfig currentPersistedConfig = persistenceService.getCacheConfig(group, true);
    if (configurationMutator.exists(config, currentPersistedConfig)) {
      throw new EntityExistsException("cache element " + config.getId() + " already exists.");
    }

    // execute function on all members
    Set<DistributedMember> targetedMembers = findMembers(null, null);
    if (targetedMembers.size() == 0) {
      return new ClusterManagementResult(false, "no members found to create cache element");
    }

    List<CliFunctionResult> functionResults = executeAndGetFunctionResult(
        new UpdateCacheFunction(),
        Arrays.asList(config, UpdateCacheFunction.CacheElementOperation.ADD),
        targetedMembers);
    functionResults
        .forEach(functionResult -> result.addMemberStatus(functionResult.getMemberIdOrName(),
            functionResult.isSuccessful(),
            functionResult.getStatusMessage()));

    if (!result.isSuccessfullyAppliedOnMembers()) {
      result.setClusterConfigPersisted(false, "Failed to apply the update on all members.");
      return result;
    }

    // persist configuration in cache config
    String finalGroup = group;
    persistenceService.updateCacheConfig(finalGroup, cacheConfigForGroup -> {
      try {
        configurationMutator.add(config, cacheConfigForGroup);
        result.setClusterConfigPersisted(true,
            "successfully persisted config for " + finalGroup);
      } catch (Exception e) {
        String message = "failed to update cluster config for " + finalGroup;
        logger.error(message, e);
        result.setClusterConfigPersisted(false, message);
        return null;
      }
      return cacheConfigForGroup;
    });
    return result;
  }

  @Override
  public ClusterManagementResult delete(CacheElement config, String group) {
    throw new NotImplementedException();
  }

  @Override
  public ClusterManagementResult update(CacheElement config, String group) {
    throw new NotImplementedException();
  }

  @VisibleForTesting
  Set<DistributedMember> findMembers(String[] groups, String[] members) {
    return CliUtil.findMembers(groups, members, distributionManager);
  }

  @VisibleForTesting
  List<CliFunctionResult> executeAndGetFunctionResult(Function function, Object args,
      Set<DistributedMember> targetMembers) {
    ResultCollector rc = CliUtil.executeFunction(function, args, targetMembers);
    return CliFunctionResult.cleanResults((List<?>) rc.getResult());
  }
}
