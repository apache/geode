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

import static org.apache.geode.management.api.ClusterManagementResult.StatusCode.ENTITY_EXISTS;
import static org.apache.geode.management.api.ClusterManagementResult.StatusCode.ENTITY_NOT_FOUND;
import static org.apache.geode.management.api.ClusterManagementResult.StatusCode.ERROR;
import static org.apache.geode.management.api.ClusterManagementResult.StatusCode.ILLEGAL_ARGUMENT;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.GatewayReceiverConfig;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.AbstractExecution;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.api.ClusterManagementException;
import org.apache.geode.management.api.ClusterManagementListOperationsResult;
import org.apache.geode.management.api.ClusterManagementListResult;
import org.apache.geode.management.api.ClusterManagementOperation;
import org.apache.geode.management.api.ClusterManagementOperationResult;
import org.apache.geode.management.api.ClusterManagementRealizationException;
import org.apache.geode.management.api.ClusterManagementRealizationResult;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementResult.StatusCode;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.api.ConfigurationResult;
import org.apache.geode.management.api.CorrespondWith;
import org.apache.geode.management.api.RealizationResult;
import org.apache.geode.management.api.RestfulEndpoint;
import org.apache.geode.management.configuration.MemberConfig;
import org.apache.geode.management.configuration.Pdx;
import org.apache.geode.management.internal.CacheElementOperation;
import org.apache.geode.management.internal.ClusterManagementOperationStatusResult;
import org.apache.geode.management.internal.cli.functions.CacheRealizationFunction;
import org.apache.geode.management.internal.configuration.mutators.ConfigurationManager;
import org.apache.geode.management.internal.configuration.mutators.GatewayReceiverConfigManager;
import org.apache.geode.management.internal.configuration.mutators.PdxManager;
import org.apache.geode.management.internal.configuration.mutators.RegionConfigManager;
import org.apache.geode.management.internal.configuration.validators.CacheElementValidator;
import org.apache.geode.management.internal.configuration.validators.ConfigurationValidator;
import org.apache.geode.management.internal.configuration.validators.GatewayReceiverConfigValidator;
import org.apache.geode.management.internal.configuration.validators.MemberValidator;
import org.apache.geode.management.internal.configuration.validators.RegionConfigValidator;
import org.apache.geode.management.internal.exceptions.EntityExistsException;
import org.apache.geode.management.internal.operation.OperationHistoryManager;
import org.apache.geode.management.internal.operation.OperationHistoryManager.OperationInstance;
import org.apache.geode.management.internal.operation.OperationManager;
import org.apache.geode.management.internal.operation.TaggedWithOperator;
import org.apache.geode.management.runtime.OperationResult;
import org.apache.geode.management.runtime.RuntimeInfo;

public class LocatorClusterManagementService implements ClusterManagementService {
  private static final Logger logger = LogService.getLogger();
  private final ConfigurationPersistenceService persistenceService;
  private final Map<Class, ConfigurationManager> managers;
  private final Map<Class, ConfigurationValidator> validators;
  private final OperationManager operationManager;
  private final MemberValidator memberValidator;
  private final CacheElementValidator commonValidator;

  public LocatorClusterManagementService(InternalCache cache,
      ConfigurationPersistenceService persistenceService) {
    this(persistenceService, new ConcurrentHashMap<>(), new ConcurrentHashMap<>(),
        new MemberValidator(cache, persistenceService), new CacheElementValidator(),
        new OperationManager(cache, new OperationHistoryManager()));
    // initialize the list of managers
    managers.put(RegionConfig.class, new RegionConfigManager());
    managers.put(Pdx.class, new PdxManager());
    managers.put(GatewayReceiverConfig.class, new GatewayReceiverConfigManager(cache));

    // initialize the list of validators
    validators.put(RegionConfig.class, new RegionConfigValidator(cache));
    validators.put(GatewayReceiverConfig.class, new GatewayReceiverConfigValidator());
  }

  @VisibleForTesting
  public LocatorClusterManagementService(ConfigurationPersistenceService persistenceService,
      Map<Class, ConfigurationManager> managers,
      Map<Class, ConfigurationValidator> validators,
      MemberValidator memberValidator,
      CacheElementValidator commonValidator,
      OperationManager operationManager) {
    this.persistenceService = persistenceService;
    this.managers = managers;
    this.validators = validators;
    this.memberValidator = memberValidator;
    this.commonValidator = commonValidator;
    this.operationManager = operationManager;
  }

  @Override
  public <T extends CacheElement> ClusterManagementRealizationResult create(T config) {
    // validate that user used the correct config object type
    ConfigurationManager configurationManager = getConfigurationManager(config);

    if (persistenceService == null) {
      return assertSuccessful(new ClusterManagementRealizationResult(false,
          "Cluster configuration service needs to be enabled"));
    }

    String group = config.getConfigGroup();
    try {
      // first validate common attributes of all configuration object
      commonValidator.validate(CacheElementOperation.CREATE, config);

      ConfigurationValidator validator = validators.get(config.getClass());
      if (validator != null) {
        validator.validate(CacheElementOperation.CREATE, config);
      }

      // check if this config already exists on all/some members of this group
      memberValidator.validateCreate(config, configurationManager);
      // execute function on all members
    } catch (EntityExistsException e) {
      raise(ENTITY_EXISTS, e.getMessage());
    } catch (IllegalArgumentException e) {
      raise(ILLEGAL_ARGUMENT, e.getMessage());
    }

    Set<DistributedMember> targetedMembers = memberValidator.findServers(group);
    ClusterManagementRealizationResult result = new ClusterManagementRealizationResult();

    List<RealizationResult> functionResults = executeAndGetFunctionResult(
        new CacheRealizationFunction(),
        Arrays.asList(config, CacheElementOperation.CREATE),
        targetedMembers);

    functionResults.forEach(result::addMemberStatus);

    // if any false result is added to the member list
    if (result.getStatusCode() != StatusCode.OK) {
      result.setStatus(false, "Failed to apply the update on all members");
      return assertSuccessful(result);
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
        result.setStatus(StatusCode.FAIL_TO_PERSIST, message);
        return null;
      }
      return cacheConfigForGroup;
    });

    // add the config object which includes the HATOS information of the element created
    if (result.isSuccessful() && config instanceof RestfulEndpoint) {
      result.setUri(((RestfulEndpoint) config).getUri());
    }
    return assertSuccessful(result);
  }

  @Override
  public <T extends CacheElement> ClusterManagementRealizationResult delete(
      T config) {
    // validate that user used the correct config object type
    ConfigurationManager configurationManager = getConfigurationManager(config);

    if (persistenceService == null) {
      return assertSuccessful(new ClusterManagementRealizationResult(false,
          "Cluster configuration service needs to be enabled"));
    }

    try {
      // first validate common attributes of all configuration object
      commonValidator.validate(CacheElementOperation.DELETE, config);

      ConfigurationValidator validator = validators.get(config.getClass());
      if (validator != null) {
        validator.validate(CacheElementOperation.DELETE, config);
      }
    } catch (IllegalArgumentException e) {
      raise(ILLEGAL_ARGUMENT, e.getMessage());
    }

    String[] groupsWithThisElement =
        memberValidator.findGroupsWithThisElement(config.getId(), configurationManager);
    if (groupsWithThisElement.length == 0) {
      raise(ENTITY_NOT_FOUND, "Cache element '" + config.getId() + "' does not exist");
    }

    // execute function on all members
    ClusterManagementRealizationResult result = new ClusterManagementRealizationResult();

    List<RealizationResult> functionResults = executeAndGetFunctionResult(
        new CacheRealizationFunction(),
        Arrays.asList(config, CacheElementOperation.DELETE),
        memberValidator.findServers(groupsWithThisElement));
    functionResults.forEach(result::addMemberStatus);

    // if any false result is added to the member list
    if (result.getStatusCode() != StatusCode.OK) {
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
      result.setStatus(StatusCode.FAIL_TO_PERSIST, message);
    }

    return assertSuccessful(result);
  }

  @Override
  public <T extends CacheElement> ClusterManagementRealizationResult update(
      T config) {
    throw new NotImplementedException("Not implemented");
  }

  @Override
  public <T extends CacheElement & CorrespondWith<R>, R extends RuntimeInfo> ClusterManagementListResult<T, R> list(
      T filter) {
    ClusterManagementListResult<T, R> result = new ClusterManagementListResult<>();

    if (persistenceService == null) {
      return assertSuccessful(new ClusterManagementListResult<>(false,
          "Cluster configuration service needs to be enabled"));
    }

    List<T> resultList = new ArrayList<>();

    if (filter instanceof MemberConfig) {
      resultList.add(filter);
    } else {
      ConfigurationManager<T> manager = getConfigurationManager(filter);
      // gather elements on all the groups, consolidate the group information and then do the filter
      // so that when we filter by a specific group, we still show that a particular element might
      // also belong to another group.
      for (String group : persistenceService.getGroups()) {
        CacheConfig currentPersistedConfig = persistenceService.getCacheConfig(group, true);
        List<T> listInGroup = manager.list(filter, currentPersistedConfig);
        for (T element : listInGroup) {
          element.setGroup(group);
          resultList.add(element);
        }
      }

      // if empty result, return immediately
      if (resultList.size() == 0) {
        return result;
      }

      // right now the list contains [{regionA, group1}, {regionA, group2}...], if the elements are
      // MultiGroupCacheElement, we need to consolidate the list into [{regionA, [group1, group2]}
      List<T> consolidatedResultList = new ArrayList<>();
      for (T element : resultList) {
        int index = consolidatedResultList.indexOf(element);
        if (index >= 0) {
          T exist = consolidatedResultList.get(index);
          exist.addGroup(element.getGroup());
        } else {
          consolidatedResultList.add(element);
        }
      }
      if (StringUtils.isNotBlank(filter.getGroup())) {
        consolidatedResultList = consolidatedResultList.stream()
            .filter(e -> (e.getGroups().contains(filter.getConfigGroup())))
            .collect(Collectors.toList());
      }
      resultList = consolidatedResultList;
    }

    // gather the runtime info for each configuration objects
    List<ConfigurationResult<T, R>> responses = new ArrayList<>();
    boolean hasRuntimeInfo = filter.hasRuntimeInfo();

    for (T element : resultList) {
      List<String> groups = element.getGroups();
      ConfigurationResult<T, R> response = new ConfigurationResult<>(element);

      // if "cluster" is the only group, clear it, so that the returning json does not show
      // "cluster" as a group value
      if (element.getGroups().size() == 1 && CacheElement.CLUSTER.equals(element.getGroup())) {
        element.getGroups().clear();
      }

      responses.add(response);
      // do not gather runtime if this type of CacheElement is RespondWith<RuntimeInfo>
      if (!hasRuntimeInfo) {
        continue;
      }

      Set<DistributedMember> members;

      if (filter instanceof MemberConfig) {
        members =
            memberValidator.findMembers(filter.getId(), filter.getGroups().toArray(new String[0]));
      } else {
        members = memberValidator.findServers(groups.toArray(new String[0]));
      }

      // no member belongs to these groups
      if (members.size() == 0) {
        continue;
      }

      // if this cacheElement's runtime info only contains global info (no per member info), we will
      // only need to issue get function on any member instead of all of them.
      if (element.isGlobalRuntime()) {
        members = Collections.singleton(members.iterator().next());
      }

      List<R> runtimeInfos = executeAndGetFunctionResult(new CacheRealizationFunction(),
          Arrays.asList(element, CacheElementOperation.GET),
          members);
      response.setRuntimeInfo(runtimeInfos);
    }

    result.setResult(responses);
    return assertSuccessful(result);
  }

  @Override
  public <T extends CacheElement & CorrespondWith<R>, R extends RuntimeInfo> ClusterManagementListResult<T, R> get(
      T config) {
    ClusterManagementListResult<T, R> list = list(config);
    List<ConfigurationResult<T, R>> result = list.getResult();

    if (result.size() == 0) {
      raise(ENTITY_NOT_FOUND,
          config.getClass().getSimpleName() + " with id = " + config.getId() + " not found.");
    }

    if (result.size() > 1) {
      raise(ERROR, "Expect only one matching " + config.getClass().getSimpleName());
    }
    return assertSuccessful(list);
  }

  @Override
  public <A extends ClusterManagementOperation<V>, V extends OperationResult> ClusterManagementOperationResult<V> start(
      A op) {
    OperationInstance<A, V> operationInstance = operationManager.submit(op);
    if (op instanceof TaggedWithOperator) {
      operationInstance.setOperator(((TaggedWithOperator) op).getOperator());
    }

    ClusterManagementResult result = new ClusterManagementResult(
        StatusCode.ACCEPTED,
        "async operation started (GET uri to check status)");

    return assertSuccessful(toClusterManagementListOperationsResult(result, operationInstance));
  }

  @Override
  public <A extends ClusterManagementOperation<V>, V extends OperationResult> ClusterManagementListOperationsResult<V> list(
      A opType) {
    return assertSuccessful(new ClusterManagementListOperationsResult<>(
        operationManager.listOperationInstances(opType).stream()
            .map(this::toClusterManagementListOperationsResult).collect(Collectors.toList())));
  }

  /**
   * builds a base status from the state of a future result
   */
  private static <V extends OperationResult> ClusterManagementResult getStatus(
      CompletableFuture<V> future) {
    if (future.isCompletedExceptionally()) {
      return new ClusterManagementResult(StatusCode.ERROR, "failed");
    } else if (future.isDone()) {
      return new ClusterManagementResult(StatusCode.OK, "finished successfully");
    } else {
      return new ClusterManagementResult(StatusCode.IN_PROGRESS, "in progress");
    }
  }

  /**
   * builds a result object from a base status and an operation instance
   */
  private <A extends ClusterManagementOperation<V>, V extends OperationResult> ClusterManagementOperationResult<V> toClusterManagementListOperationsResult(
      ClusterManagementResult status, OperationInstance<A, V> operationInstance) {
    ClusterManagementOperationResult<V> result = new ClusterManagementOperationResult<>(status,
        operationInstance.getFutureResult(), operationInstance.getOperationStart(),
        operationInstance.getFutureOperationEnded(), operationInstance.getOperator());
    result.setUri(RestfulEndpoint.URI_CONTEXT + RestfulEndpoint.URI_VERSION
        + operationInstance.getOperation().getEndpoint() + "/" + operationInstance.getId());
    return result;
  }

  /**
   * builds a result object from an operation instance
   */
  private <A extends ClusterManagementOperation<V>, V extends OperationResult> ClusterManagementOperationResult<V> toClusterManagementListOperationsResult(
      OperationInstance<A, V> operationInstance) {
    return toClusterManagementListOperationsResult(getStatus(operationInstance.getFutureResult()),
        operationInstance);
  }

  /**
   * this is intended for use by the REST controller. for Java usage, please use
   * {@link ClusterManagementOperationResult#getFutureResult()}
   */
  public <V extends OperationResult> ClusterManagementOperationStatusResult<V> checkStatus(
      String opId) {
    final OperationInstance<?, V> operationInstance = operationManager.getOperationInstance(opId);
    if (operationInstance == null) {
      raise(ENTITY_NOT_FOUND, "Operation id = " + opId + " not found");
    }
    final CompletableFuture<V> status = operationInstance.getFutureResult();
    ClusterManagementOperationStatusResult<V> result =
        new ClusterManagementOperationStatusResult<>();
    result.setOperator(operationInstance.getOperator());
    result.setOperationStart(operationInstance.getOperationStart());
    if (!status.isDone()) {
      result.setStatus(StatusCode.IN_PROGRESS, "in progress");
    } else {
      try {
        result.setOperationEnded(operationInstance.getFutureOperationEnded().get());
        result.setResult(status.get());
        result.setStatus(StatusCode.OK, "finished successfully");
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
    return result;
  }

  private <T extends ClusterManagementResult> T assertSuccessful(T result) {
    if (!result.isSuccessful()) {
      if (result instanceof ClusterManagementRealizationResult) {
        throw new ClusterManagementRealizationException(
            (ClusterManagementRealizationResult) result);
      } else {
        throw new ClusterManagementException(result);
      }
    }
    return result;
  }

  private static void raise(StatusCode statusCode, String statusMessage) {
    throw new ClusterManagementException(new ClusterManagementResult(statusCode, statusMessage));
  }

  public boolean isConnected() {
    return true;
  }

  @Override
  public void close() {
    operationManager.close();
  }

  private <T extends CacheElement> ConfigurationManager<T> getConfigurationManager(
      T config) {
    ConfigurationManager configurationManager = managers.get(config.getClass());
    if (configurationManager == null) {
      raise(ILLEGAL_ARGUMENT, String.format("Configuration type %s is not supported",
          config.getClass().getSimpleName()));
    }

    return configurationManager;
  }

  @VisibleForTesting
  <R> List<R> executeAndGetFunctionResult(Function function, Object args,
      Set<DistributedMember> targetMembers) {
    if (targetMembers.size() == 0) {
      return Collections.emptyList();
    }

    Execution execution = FunctionService.onMembers(targetMembers).setArguments(args);
    ((AbstractExecution) execution).setIgnoreDepartedMembers(true);
    ResultCollector rc = execution.execute(function);

    return (List<R>) rc.getResult();
  }
}
