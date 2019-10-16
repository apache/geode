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


import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
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
import org.apache.geode.management.api.ClusterManagementGetResult;
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
import org.apache.geode.management.api.RealizationResult;
import org.apache.geode.management.configuration.AbstractConfiguration;
import org.apache.geode.management.configuration.GatewayReceiver;
import org.apache.geode.management.configuration.GroupableConfiguration;
import org.apache.geode.management.configuration.Index;
import org.apache.geode.management.configuration.Links;
import org.apache.geode.management.configuration.Member;
import org.apache.geode.management.configuration.Pdx;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.internal.CacheElementOperation;
import org.apache.geode.management.internal.ClusterManagementOperationStatusResult;
import org.apache.geode.management.internal.cli.functions.CacheRealizationFunction;
import org.apache.geode.management.internal.configuration.mutators.ConfigurationManager;
import org.apache.geode.management.internal.configuration.mutators.GatewayReceiverConfigManager;
import org.apache.geode.management.internal.configuration.mutators.IndexConfigManager;
import org.apache.geode.management.internal.configuration.mutators.PdxManager;
import org.apache.geode.management.internal.configuration.mutators.RegionConfigManager;
import org.apache.geode.management.internal.configuration.validators.CommonConfigurationValidator;
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
  private final CommonConfigurationValidator commonValidator;

  public LocatorClusterManagementService(InternalCache cache,
      ConfigurationPersistenceService persistenceService) {
    this(persistenceService, new ConcurrentHashMap<>(), new ConcurrentHashMap<>(),
        new MemberValidator(cache, persistenceService), new CommonConfigurationValidator(),
        new OperationManager(cache, new OperationHistoryManager()));
    // initialize the list of managers
    managers.put(Region.class, new RegionConfigManager());
    managers.put(Pdx.class, new PdxManager());
    managers.put(GatewayReceiver.class, new GatewayReceiverConfigManager());
    managers.put(Index.class, new IndexConfigManager());

    // initialize the list of validators
    validators.put(Region.class, new RegionConfigValidator(cache));
    validators.put(GatewayReceiver.class, new GatewayReceiverConfigValidator());
  }

  @VisibleForTesting
  public LocatorClusterManagementService(ConfigurationPersistenceService persistenceService,
      Map<Class, ConfigurationManager> managers,
      Map<Class, ConfigurationValidator> validators,
      MemberValidator memberValidator,
      CommonConfigurationValidator commonValidator,
      OperationManager operationManager) {
    this.persistenceService = persistenceService;
    this.managers = managers;
    this.validators = validators;
    this.memberValidator = memberValidator;
    this.commonValidator = commonValidator;
    this.operationManager = operationManager;
  }

  @Override
  public <T extends AbstractConfiguration<?>> ClusterManagementRealizationResult create(T config) {
    // validate that user used the correct config object type
    ConfigurationManager configurationManager = getConfigurationManager(config);

    if (persistenceService == null) {
      return assertSuccessful(new ClusterManagementRealizationResult(StatusCode.ERROR,
          "Cluster configuration service needs to be enabled."));
    }

    String group = config.getGroup();
    final String groupName =
        AbstractConfiguration.isCluster(group) ? AbstractConfiguration.CLUSTER : group;
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
      raise(StatusCode.ENTITY_EXISTS, e);
    } catch (IllegalArgumentException e) {
      raise(StatusCode.ILLEGAL_ARGUMENT, e);
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
      result.setStatus(StatusCode.ERROR, "Failed to create on all members.");
      return assertSuccessful(result);
    }

    // persist configuration in cache config
    persistenceService.updateCacheConfig(groupName, cacheConfigForGroup -> {
      try {
        configurationManager.add(config, cacheConfigForGroup);
        result.setStatus(StatusCode.OK,
            "Successfully updated configuration for " + groupName + ".");
      } catch (Exception e) {
        String message = "Failed to update cluster configuration for " + groupName + ".";
        logger.error(message, e);
        result.setStatus(StatusCode.FAIL_TO_PERSIST, message);
        return null;
      }
      return cacheConfigForGroup;
    });

    // add the config object which includes the HATEOAS information of the element created
    if (result.isSuccessful()) {
      result.setLinks(config.getLinks());
    }
    return assertSuccessful(result);
  }

  @Override
  public <T extends AbstractConfiguration<?>> ClusterManagementRealizationResult delete(
      T config) {
    // validate that user used the correct config object type
    ConfigurationManager configurationManager = getConfigurationManager(config);

    if (persistenceService == null) {
      return assertSuccessful(new ClusterManagementRealizationResult(StatusCode.ERROR,
          "Cluster configuration service needs to be enabled."));
    }

    try {
      // first validate common attributes of all configuration object
      commonValidator.validate(CacheElementOperation.DELETE, config);

      ConfigurationValidator validator = validators.get(config.getClass());
      if (validator != null) {
        validator.validate(CacheElementOperation.DELETE, config);
      }
    } catch (IllegalArgumentException e) {
      raise(StatusCode.ILLEGAL_ARGUMENT, e);
    }

    String[] groupsWithThisElement =
        memberValidator.findGroupsWithThisElement(config.getId(), configurationManager);
    if (groupsWithThisElement.length == 0) {
      raise(StatusCode.ENTITY_NOT_FOUND,
          config.getClass().getSimpleName() + " '" + config.getId() + "' does not exist.");
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
      result.setStatus(StatusCode.ERROR, "Failed to delete on all members.");
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
          logger.error("Failed to update cluster configuration for " + finalGroup + ".", e);
          failedGroups.add(finalGroup);
          return null;
        }
        return cacheConfigForGroup;
      });
    }

    if (failedGroups.isEmpty()) {
      result.setStatus(StatusCode.OK,
          "Successfully removed configuration for " + updatedGroups + ".");
    } else {
      String message = "Failed to update cluster configuration for " + failedGroups + ".";
      result.setStatus(StatusCode.FAIL_TO_PERSIST, message);
    }

    return assertSuccessful(result);
  }

  @Override
  public <T extends AbstractConfiguration<?>> ClusterManagementRealizationResult update(
      T config) {
    throw new NotImplementedException("Not implemented");
  }

  @Override
  public <T extends AbstractConfiguration<R>, R extends RuntimeInfo> ClusterManagementListResult<T, R> list(
      T filter) {
    ClusterManagementListResult<T, R> result = new ClusterManagementListResult<>();

    if (persistenceService == null) {
      return assertSuccessful(new ClusterManagementListResult<>(StatusCode.ERROR,
          "Cluster configuration service needs to be enabled."));
    }

    List<T> resultList = new ArrayList<>();

    if (filter instanceof Member) {
      resultList.add(filter);
    } else {
      ConfigurationManager<T> manager = getConfigurationManager(filter);
      Set<String> groups;
      if (StringUtils.isNotBlank(filter.getGroup())) {
        groups = Collections.singleton(filter.getGroup());
      } else {
        groups = persistenceService.getGroups();
      }

      for (String group : groups) {
        CacheConfig currentPersistedConfig =
            persistenceService.getCacheConfig(
                AbstractConfiguration.isCluster(group) ? AbstractConfiguration.CLUSTER : group,
                true);
        List<T> listInGroup = manager.list(filter, currentPersistedConfig);
        if (!AbstractConfiguration.isCluster(group)) {
          listInGroup.forEach(t -> {
            if (t instanceof GroupableConfiguration) {
              ((GroupableConfiguration<?>) t).setGroup(group);
            }
          });
        }
        resultList.addAll(listInGroup);
      }
    }

    // gather the runtime info for each configuration objects
    List<ConfigurationResult<T, R>> responses = new ArrayList<>();
    boolean hasRuntimeInfo = hasRuntimeInfo(filter.getClass());

    for (T element : resultList) {
      ConfigurationResult<T, R> response = new ConfigurationResult<>(element);

      responses.add(response);
      // do not gather runtime if this type of CacheElement is RespondWith<RuntimeInfo>
      if (!hasRuntimeInfo) {
        continue;
      }

      Set<DistributedMember> members;

      if (filter instanceof Member) {
        members =
            memberValidator.findMembers(filter.getId(), filter.getGroup());
      } else {
        members = memberValidator.findServers(element.getGroup());
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
  public <T extends AbstractConfiguration<R>, R extends RuntimeInfo> ClusterManagementGetResult<T, R> get(
      T config) {
    ClusterManagementListResult<T, R> list = list(config);
    List<ConfigurationResult<T, R>> result = list.getResult();

    int size = result.size();
    if (config instanceof Member) {
      size = result.get(0).getRuntimeInfo().size();
    }

    if (size == 0) {
      raise(StatusCode.ENTITY_NOT_FOUND,
          config.getClass().getSimpleName() + " '" + config.getId() + "' does not exist.");
    }

    if (size > 1) {
      raise(StatusCode.ERROR,
          "Expect only one matching " + config.getClass().getSimpleName() + ".");
    }

    return assertSuccessful(new ClusterManagementGetResult<>(list));
  }

  @Override
  public <A extends ClusterManagementOperation<V>, V extends OperationResult> ClusterManagementOperationResult<V> start(
      A op) {
    OperationInstance<A, V> operationInstance = operationManager.submit(op);
    if (op instanceof TaggedWithOperator) {
      operationInstance.setOperator(((TaggedWithOperator) op).getOperator());
    }

    ClusterManagementResult result = new ClusterManagementResult(
        StatusCode.ACCEPTED, "Operation started.  Use the URI to check its status.");

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
      String error = "Operation failed.";
      try {
        future.get();
      } catch (InterruptedException ignore) {
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        error = e.getMessage();
      }
      return new ClusterManagementResult(StatusCode.ERROR, error);
    } else if (future.isDone()) {
      return new ClusterManagementResult(StatusCode.OK, "Operation finished successfully.");
    } else {
      return new ClusterManagementResult(StatusCode.IN_PROGRESS, "Operation in progress.");
    }
  }

  /**
   * builds a result object from a base status and an operation instance
   */
  private <A extends ClusterManagementOperation<V>, V extends OperationResult> ClusterManagementOperationResult<V> toClusterManagementListOperationsResult(
      ClusterManagementResult status, OperationInstance<A, V> operationInstance) {
    ClusterManagementOperationResult<V> result = new ClusterManagementOperationResult<>(status,
        operationInstance.getFutureResult(), operationInstance.getOperationStart(),
        operationInstance.getFutureOperationEnded(), operationInstance.getOperator(),
        operationInstance.getId());
    result.setLinks(
        new Links(operationInstance.getId(), operationInstance.getOperation().getEndpoint()));
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
      raise(StatusCode.ENTITY_NOT_FOUND, "Operation '" + opId + "' does not exist.");
    }
    final CompletableFuture<V> status = operationInstance.getFutureResult();
    ClusterManagementOperationStatusResult<V> result =
        new ClusterManagementOperationStatusResult<>(getStatus(status));
    result.setOperator(operationInstance.getOperator());
    result.setOperationStart(operationInstance.getOperationStart());
    if (status.isDone() && !status.isCompletedExceptionally()) {
      try {
        result.setOperationEnded(operationInstance.getFutureOperationEnded().get());
        result.setResult(status.get());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (ExecutionException ignore) {
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

  private static void raise(StatusCode statusCode, Exception e) {
    throw new ClusterManagementException(new ClusterManagementResult(statusCode, e.getMessage()),
        e);
  }

  public boolean isConnected() {
    return true;
  }

  @Override
  public void close() {
    operationManager.close();
  }

  private <T extends AbstractConfiguration> ConfigurationManager<T> getConfigurationManager(
      T config) {
    ConfigurationManager configurationManager = managers.get(config.getClass());
    if (configurationManager == null) {
      raise(StatusCode.ILLEGAL_ARGUMENT, String.format("%s is not supported.",
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


  /**
   * for internal use only
   */
  @VisibleForTesting
  Class<?> getRuntimeClass(Class<?> configClass) {
    Type genericSuperclass = configClass.getGenericSuperclass();

    if (genericSuperclass instanceof ParameterizedType) {
      return (Class<?>) ((ParameterizedType) genericSuperclass).getActualTypeArguments()[0];
    }

    return null;
  }

  @VisibleForTesting
  boolean hasRuntimeInfo(Class<?> configClass) {
    return !RuntimeInfo.class.equals(getRuntimeClass(configClass));
  }

}
