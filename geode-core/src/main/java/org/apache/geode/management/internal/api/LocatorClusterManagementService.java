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

import org.apache.commons.lang3.NotImplementedException;
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
    managers.put(RegionConfig.class, new RegionConfigManager());
    managers.put(MemberConfig.class, new MemberConfigManager(cache));

    // initialize the list of validators
    validators.put(RegionConfig.class, new RegionConfigValidator());
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
  public ClusterManagementResult create(CacheElement config, String group) {
    if (group == null) {
      group = "cluster";
    }

    if (persistenceService == null) {
      return new ClusterManagementResult(false,
          "Cluster configuration service needs to be enabled");
    }

    ClusterManagementResult result = new ClusterManagementResult();
    ConfigurationManager configurationMutator = managers.get(config.getClass());

    ConfigurationValidator validator = validators.get(config.getClass());
    if (validator != null) {
      validator.validate(config);
    }


    // exit early if config element already exists in cache config
    CacheConfig currentPersistedConfig = persistenceService.getCacheConfig(group, true);
    if (validator.exists(config, currentPersistedConfig)) {
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

    // if any false result is added to the member list
    if (result.getStatusCode() != ClusterManagementResult.StatusCode.OK) {
      result.setStatus(false, "Failed to apply the update on all members.");
      return result;
    }

    // persist configuration in cache config
    String finalGroup = group;
    persistenceService.updateCacheConfig(finalGroup, cacheConfigForGroup -> {
      try {
        configurationMutator.add(config, cacheConfigForGroup);
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
  public ClusterManagementResult delete(CacheElement config, String group) {
    throw new NotImplementedException("Not implemented");
  }

  @Override
  public ClusterManagementResult update(CacheElement config, String group) {
    throw new NotImplementedException("Not implemented");
  }

  @Override
  public ClusterManagementResult list(CacheElement filter) {
    ConfigurationManager manager = managers.get(filter.getClass());
    List<CacheElement> listResults = manager.list(filter, null);

    ClusterManagementResult result = new ClusterManagementResult();
    result.setResult(listResults);

    return result;
  }

  @Override
  public boolean isConnected() {
    return true;
  }

  @VisibleForTesting
  Set<DistributedMember> findMembers(String[] groups, String[] members) {
    return CliUtil.findMembers(groups, members, cache);
  }

  @VisibleForTesting
  List<CliFunctionResult> executeAndGetFunctionResult(Function function, Object args,
      Set<DistributedMember> targetMembers) {
    ResultCollector rc = CliUtil.executeFunction(function, args, targetMembers);
    return CliFunctionResult.cleanResults((List<?>) rc.getResult());
  }
}
