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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.GatewayReceiverConfig;
import org.apache.geode.cache.configuration.PdxType;
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
import org.apache.geode.management.configuration.MultiGroupCacheElement;
import org.apache.geode.management.internal.CacheElementOperation;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.UpdateCacheFunction;
import org.apache.geode.management.internal.configuration.mutators.ConfigurationManager;
import org.apache.geode.management.internal.configuration.mutators.GatewayReceiverConfigManager;
import org.apache.geode.management.internal.configuration.mutators.MemberConfigManager;
import org.apache.geode.management.internal.configuration.mutators.PdxManager;
import org.apache.geode.management.internal.configuration.mutators.RegionConfigManager;
import org.apache.geode.management.internal.configuration.validators.CacheElementValidator;
import org.apache.geode.management.internal.configuration.validators.ConfigurationValidator;
import org.apache.geode.management.internal.configuration.validators.GatewayReceiverConfigValidator;
import org.apache.geode.management.internal.configuration.validators.MemberValidator;
import org.apache.geode.management.internal.configuration.validators.RegionConfigValidator;
import org.apache.geode.management.internal.exceptions.EntityNotFoundException;

public class LocatorClusterManagementService implements ClusterManagementService {
  private static final Logger logger = LogService.getLogger();
  private ConfigurationPersistenceService persistenceService;
  private Map<Class, ConfigurationManager<? extends CacheElement>> managers;
  private Map<Class, ConfigurationValidator> validators;
  private MemberValidator memberValidator;

  public LocatorClusterManagementService(InternalCache cache,
      ConfigurationPersistenceService persistenceService) {
    this(persistenceService, new HashMap(), new HashMap(), null);
    // initialize the list of managers
    managers.put(RegionConfig.class, new RegionConfigManager(cache));
    managers.put(MemberConfig.class, new MemberConfigManager(cache));
    managers.put(PdxType.class, new PdxManager());
    managers.put(GatewayReceiverConfig.class, new GatewayReceiverConfigManager(cache));

    // initialize the list of validators
    validators.put(CacheElement.class, new CacheElementValidator());
    validators.put(RegionConfig.class, new RegionConfigValidator(cache));
    validators.put(GatewayReceiverConfig.class, new GatewayReceiverConfigValidator());
    memberValidator = new MemberValidator(cache, persistenceService);
  }

  @VisibleForTesting
  public LocatorClusterManagementService(ConfigurationPersistenceService persistenceService,
      Map managers, Map validators, MemberValidator memberValidator) {
    this.persistenceService = persistenceService;
    this.managers = managers;
    this.validators = validators;
    this.memberValidator = memberValidator;
  }

  @Override
  public ClusterManagementResult create(CacheElement config) {
    // validate that user used the correct config object type
    ConfigurationManager configurationManager = getConfigurationManager(config);

    if (persistenceService == null) {
      return new ClusterManagementResult(false,
          "Cluster configuration service needs to be enabled");
    }

    // first validate common attributes of all configuration object
    validators.get(CacheElement.class).validate(CacheElementOperation.CREATE, config);


    String group = config.getConfigGroup();
    ConfigurationValidator validator = validators.get(config.getClass());
    if (validator != null) {
      validator.validate(CacheElementOperation.CREATE, config);
    }

    // check if this config already exists on all/some members of this group
    memberValidator.validateCreate(config, configurationManager);

    // execute function on all members
    Set<DistributedMember> targetedMembers = memberValidator.findMembers(group);

    ClusterManagementResult result = new ClusterManagementResult();

    List<CliFunctionResult> functionResults = executeAndGetFunctionResult(
        new UpdateCacheFunction(),
        Arrays.asList(config, CacheElementOperation.CREATE),
        targetedMembers);
    functionResults
        .forEach(functionResult -> result.addMemberStatus(functionResult.getMemberIdOrName(),
            functionResult.isSuccessful(),
            functionResult.getStatusMessage()));

    // if any false result is added to the member list
    if (result.getStatusCode() != ClusterManagementResult.StatusCode.OK) {
      result.setStatus(false, "Failed to apply the update on all members");
      return result;
    }

    // persist configuration in cache config
    final String finalGroup = group; // the below lambda requires a reference that is final
    persistenceService.updateCacheConfig(finalGroup, cacheConfigForGroup -> {
      try {
        configurationManager.add(config, cacheConfigForGroup);
        result.setStatus(true,
            "Successfully updated config for " + finalGroup);
      } catch (Exception e) {
        String message = "Failed to update cluster config for " + finalGroup;
        logger.error(message, e);
        result.setStatus(ClusterManagementResult.StatusCode.FAIL_TO_PERSIST, message);
        return null;
      }
      return cacheConfigForGroup;
    });

    // add the config object which includes the HATOS information of the element created
    if (result.isSuccessful()) {
      result.setResult(Collections.singletonList(config));
    }
    return result;
  }

  @Override
  public ClusterManagementResult delete(CacheElement config) {
    // validate that user used the correct config object type
    ConfigurationManager configurationManager = getConfigurationManager(config);

    if (persistenceService == null) {
      return new ClusterManagementResult(false,
          "Cluster configuration service needs to be enabled");
    }

    // first validate common attributes of all configuration object
    validators.get(CacheElement.class).validate(CacheElementOperation.DELETE, config);

    ConfigurationValidator validator = validators.get(config.getClass());
    if (validator != null) {
      validator.validate(CacheElementOperation.DELETE, config);
    }

    String[] groupsWithThisElement =
        memberValidator.findGroupsWithThisElement(config.getId(), configurationManager);
    if (groupsWithThisElement.length == 0) {
      throw new EntityNotFoundException("Cache element '" + config.getId() + "' does not exist");
    }

    // execute function on all members
    ClusterManagementResult result = new ClusterManagementResult();

    List<CliFunctionResult> functionResults = executeAndGetFunctionResult(
        new UpdateCacheFunction(),
        Arrays.asList(config, CacheElementOperation.DELETE),
        memberValidator.findMembers(groupsWithThisElement));
    functionResults
        .forEach(functionResult -> result.addMemberStatus(functionResult.getMemberIdOrName(),
            functionResult.isSuccessful(),
            functionResult.getStatusMessage()));

    // if any false result is added to the member list
    if (result.getStatusCode() != ClusterManagementResult.StatusCode.OK) {
      result.setStatus(false, "Failed to apply the update on all members");
      return result;
    }

    // persist configuration in cache config
    List<String> updatedGroups = new ArrayList<>();
    List<String> failedGroups = new ArrayList<>();
    for (String finalGroup : groupsWithThisElement) {
      persistenceService.updateCacheConfig(finalGroup, cacheConfigForGroup -> {
        try {
          configurationManager.delete(config, cacheConfigForGroup);
          updatedGroups.add(finalGroup);
        } catch (Exception e) {
          logger.error("Failed to update cluster config for " + finalGroup, e);
          failedGroups.add(finalGroup);
          return null;
        }
        return cacheConfigForGroup;
      });
    }

    if (failedGroups.isEmpty()) {
      result.setStatus(true, "Successfully removed config for " + updatedGroups);
    } else {
      String message = "Failed to update cluster config for " + failedGroups;
      result.setStatus(ClusterManagementResult.StatusCode.FAIL_TO_PERSIST, message);
    }

    return result;
  }

  @Override
  public ClusterManagementResult update(CacheElement config) {
    throw new NotImplementedException("Not implemented");
  }

  @Override
  public <T extends CacheElement> ClusterManagementResult list(T filter) {
    ConfigurationManager<T> manager = (ConfigurationManager<T>) managers.get(filter.getClass());

    ClusterManagementResult result = new ClusterManagementResult();

    if (filter instanceof MemberConfig) {
      List<? extends T> listResults = manager.list(filter, null);
      result.setResult(listResults);
      return result;
    }

    if (persistenceService == null) {
      return new ClusterManagementResult(false,
          "Cluster configuration service needs to be enabled");
    }

    List<T> resultList = new ArrayList<>();
    for (String group : persistenceService.getGroups()) {
      CacheConfig currentPersistedConfig = persistenceService.getCacheConfig(group, true);
      List<? extends T> listInGroup = manager.list(filter, currentPersistedConfig);
      for (T element : listInGroup) {
        if (group.equals(element.getConfigGroup()) || element instanceof MultiGroupCacheElement) {
          element.setGroup(group);
          resultList.add(element);
        }
      }
    }

    // if empty result, return immediately
    if (resultList.size() == 0) {
      return result;
    }

    // right now the list contains [{regionA, group1}, {regionA, group2}...], if the elements are
    // MultiGroupCacheElement, we need to consolidate the list into [{regionA, [group1, group2]}
    if (resultList.get(0) instanceof MultiGroupCacheElement) {
      List<MultiGroupCacheElement> multiGroupList = new ArrayList<>();
      for (T element : resultList) {
        int index = multiGroupList.indexOf(element);
        if (index >= 0) {
          MultiGroupCacheElement exist = multiGroupList.get(index);
          exist.getGroups().add(element.getGroup());
        } else {
          multiGroupList.add((MultiGroupCacheElement) element);
        }
      }
      if (StringUtils.isNotBlank(filter.getGroup())) {
        multiGroupList = multiGroupList.stream()
            .filter(e -> e.getGroups().contains(filter.getConfigGroup()))
            .collect(Collectors.toList());
      }
      // if "cluster" is the only group, clear it
      for (MultiGroupCacheElement element : multiGroupList) {
        if (element.getGroups().size() == 1 && "cluster".equals(element.getGroup())) {
          element.getGroups().clear();
        }
      }
      resultList =
          (List<T>) multiGroupList.stream().map(CacheElement.class::cast)
              .collect(Collectors.toList());
    } else {
      // for non-MultiGroup CacheElement, just clear out the "cluster" group
      for (T element : resultList) {
        if ("cluster".equals(element.getGroup())) {
          element.setGroup(null);
        }
      }
    }

    result.setResult(resultList);
    return result;
  }

  @Override
  public ClusterManagementResult get(CacheElement config) {
    ClusterManagementResult list = list(config);
    List<CacheElement> result = list.getResult(CacheElement.class);
    if (result.size() == 0) {
      throw new EntityNotFoundException(
          config.getClass().getSimpleName() + " with id = " + config.getId() + " not found.");
    }

    if (result.size() > 1) {
      throw new IllegalStateException(
          "Expect only one matching " + config.getClass().getSimpleName());
    }
    return list;
  }


  @Override
  public boolean isConnected() {
    return true;
  }

  @VisibleForTesting
  List<CliFunctionResult> executeAndGetFunctionResult(Function function, Object args,
      Set<DistributedMember> targetMembers) {
    if (targetMembers.size() == 0) {
      return Collections.emptyList();
    }

    ResultCollector rc = CliUtil.executeFunction(function, args, targetMembers);
    return CliFunctionResult.cleanResults((List<?>) rc.getResult());
  }

  private ConfigurationManager getConfigurationManager(CacheElement config) {
    ConfigurationManager configurationManager = managers.get(config.getClass());
    if (configurationManager == null) {
      throw new IllegalArgumentException(String.format("Configuration type %s is not supported",
          config.getClass().getSimpleName()));
    }

    return configurationManager;
  }
}
