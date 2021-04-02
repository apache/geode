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
import java.util.HashSet;
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
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.AbstractExecution;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.logging.internal.log4j.api.LogService;
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
import org.apache.geode.management.api.EntityGroupInfo;
import org.apache.geode.management.api.EntityInfo;
import org.apache.geode.management.api.RealizationResult;
import org.apache.geode.management.configuration.AbstractConfiguration;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.management.configuration.GatewayReceiver;
import org.apache.geode.management.configuration.GroupableConfiguration;
import org.apache.geode.management.configuration.Index;
import org.apache.geode.management.configuration.Links;
import org.apache.geode.management.configuration.Member;
import org.apache.geode.management.configuration.Pdx;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.configuration.RegionScoped;
import org.apache.geode.management.internal.CacheElementOperation;
import org.apache.geode.management.internal.ClusterManagementOperationStatusResult;
import org.apache.geode.management.internal.configuration.mutators.CacheConfigurationManager;
import org.apache.geode.management.internal.configuration.mutators.ConfigurationManager;
import org.apache.geode.management.internal.configuration.mutators.DeploymentManager;
import org.apache.geode.management.internal.configuration.mutators.GatewayReceiverConfigManager;
import org.apache.geode.management.internal.configuration.mutators.IndexConfigManager;
import org.apache.geode.management.internal.configuration.mutators.PdxManager;
import org.apache.geode.management.internal.configuration.mutators.RegionConfigManager;
import org.apache.geode.management.internal.configuration.validators.CommonConfigurationValidator;
import org.apache.geode.management.internal.configuration.validators.ConfigurationValidator;
import org.apache.geode.management.internal.configuration.validators.GatewayReceiverConfigValidator;
import org.apache.geode.management.internal.configuration.validators.IndexValidator;
import org.apache.geode.management.internal.configuration.validators.MemberValidator;
import org.apache.geode.management.internal.configuration.validators.PdxValidator;
import org.apache.geode.management.internal.configuration.validators.RegionConfigValidator;
import org.apache.geode.management.internal.exceptions.EntityExistsException;
import org.apache.geode.management.internal.functions.CacheRealizationFunction;
import org.apache.geode.management.internal.operation.OperationHistoryManager;
import org.apache.geode.management.internal.operation.OperationHistoryManager.OperationInstance;
import org.apache.geode.management.internal.operation.OperationManager;
import org.apache.geode.management.internal.operation.TaggedWithOperator;
import org.apache.geode.management.runtime.OperationResult;
import org.apache.geode.management.runtime.RuntimeInfo;

public class LocatorClusterManagementService implements ClusterManagementService {
  private static final Logger logger = LogService.getLogger();
  private final InternalConfigurationPersistenceService persistenceService;
  private final Map<Class, ConfigurationManager> managers;
  private final Map<Class, ConfigurationValidator> validators;
  private final OperationManager operationManager;
  private final MemberValidator memberValidator;
  private final CommonConfigurationValidator commonValidator;

  public LocatorClusterManagementService(InternalCache cache,
      InternalConfigurationPersistenceService persistenceService) {
    this(persistenceService, new ConcurrentHashMap<>(), new ConcurrentHashMap<>(),
        new MemberValidator(cache, persistenceService), new CommonConfigurationValidator(),
        new OperationManager(cache, new OperationHistoryManager()));
    // initialize the list of managers
    managers.put(Region.class, new RegionConfigManager(persistenceService));
    managers.put(Pdx.class, new PdxManager(persistenceService));
    managers.put(GatewayReceiver.class, new GatewayReceiverConfigManager(persistenceService));
    managers.put(Index.class, new IndexConfigManager(persistenceService));
    managers.put(Deployment.class, new DeploymentManager(persistenceService));

    // initialize the list of validators
    validators.put(Region.class, new RegionConfigValidator(cache));
    validators.put(GatewayReceiver.class, new GatewayReceiverConfigValidator());
    validators.put(Pdx.class, new PdxValidator());
    validators.put(Index.class, new IndexValidator());
  }

  @VisibleForTesting
  public LocatorClusterManagementService(InternalConfigurationPersistenceService persistenceService,
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

    try {
      // first validate common attributes of all configuration object
      commonValidator.validate(CacheElementOperation.CREATE, config);

      ConfigurationValidator validator = validators.get(config.getClass());
      if (validator != null) {
        validator.validate(CacheElementOperation.CREATE, config);
      }

      // check if this config already exists
      if (configurationManager instanceof CacheConfigurationManager) {
        memberValidator.validateCreate(config, (CacheConfigurationManager) configurationManager);
      }
    } catch (EntityExistsException e) {
      raise(StatusCode.ENTITY_EXISTS, e);
    } catch (IllegalArgumentException e) {
      raise(StatusCode.ILLEGAL_ARGUMENT, e);
    }

    // find the targeted members
    Set<String> groups = new HashSet<>();
    Set<DistributedMember> targetedMembers;
    if (config instanceof RegionScoped) {
      String regionName = ((RegionScoped) config).getRegionName();
      groups = memberValidator.findGroups(regionName);
      if (groups.isEmpty()) {
        raise(StatusCode.ENTITY_NOT_FOUND, "Region provided does not exist: " + regionName);
      }
      targetedMembers = memberValidator.findServers(groups.toArray(new String[0]));
    } else {
      final String groupName =
          AbstractConfiguration.isCluster(config.getGroup()) ? AbstractConfiguration.CLUSTER
              : config.getGroup();
      groups.add(groupName);
      targetedMembers = memberValidator.findServers(groupName);
    }


    ClusterManagementRealizationResult result = new ClusterManagementRealizationResult();

    // execute function on all targeted members
    List<RealizationResult> functionResults = executeAndGetFunctionResult(
        Arrays.asList(config, CacheElementOperation.CREATE),
        targetedMembers);

    functionResults.forEach(result::addMemberStatus);

    // if any false result is added to the member list
    if (result.getStatusCode() != StatusCode.OK) {
      result.setStatus(StatusCode.ERROR, "Failed to create on all members.");
      return assertSuccessful(result);
    }

    // persist configuration in cache config
    for (String groupName : groups) {
      configurationManager.add(config, groupName);
    }
    result.setStatus(StatusCode.OK,
        "Successfully updated configuration for " + String.join(", ", groups) + ".");

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
    CacheConfigurationManager configurationManager =
        (CacheConfigurationManager) getConfigurationManager(config);

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
        memberValidator.findGroupsWithThisElement(config, configurationManager);
    if (groupsWithThisElement.length == 0) {
      raise(StatusCode.ENTITY_NOT_FOUND,
          config.getClass().getSimpleName() + " '" + config.getId() + "' does not exist.");
    }

    // execute function on all members
    ClusterManagementRealizationResult result = new ClusterManagementRealizationResult();

    List<RealizationResult> functionResults = executeAndGetFunctionResult(
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
      try {
        configurationManager.delete(config, finalGroup);
        updatedGroups.add(finalGroup);
      } catch (Exception e) {
        logger.error(e.getMessage(), e);
        failedGroups.add(finalGroup);
      }
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
        List<T> list = manager.list(filter, group);
        if (!AbstractConfiguration.isCluster(group)) {
          list.forEach(t -> {
            if (t instanceof GroupableConfiguration) {
              ((GroupableConfiguration<?>) t).setGroup(group);
            }
          });
        }
        list.stream().filter(t -> !resultList.contains(t))
            .forEach(resultList::add);
      }
    }

    // gather the runtime info for each configuration objects
    List<EntityGroupInfo<T, R>> responses = new ArrayList<>();
    boolean hasRuntimeInfo = hasRuntimeInfo(filter.getClass());

    for (T element : resultList) {
      EntityGroupInfo<T, R> response = new EntityGroupInfo<>(element);

      responses.add(response);
      if (!hasRuntimeInfo) {
        continue;
      }

      Set<DistributedMember> members;

      if (filter instanceof Member) {
        members =
            memberValidator.findMembers(filter.getId(), filter.getGroup());
      } else {
        members = memberValidator.findServers(element);
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

      List<R> runtimeInfos = executeAndGetFunctionResult(
          Arrays.asList(element, CacheElementOperation.GET),
          members);
      response.setRuntimeInfo(runtimeInfos);
    }

    if (filter instanceof Member) {
      // for members, we have exactly one response that holds the filter
      // and all the members in the runtimeInfo section
      List<R> members = responses.get(0).getRuntimeInfo();
      for (R memberInfo : members) {
        Member member = new Member();
        member.setId(memberInfo.getMemberName());
        EntityInfo<T, R> entityInfo = new EntityInfo<>(memberInfo.getMemberName(),
            Collections.singletonList(
                new EntityGroupInfo<>((T) member, Collections.singletonList(memberInfo))));
        result.addEntityInfo(entityInfo);
      }
    } else {
      result.setEntityGroupInfo(responses);
    }
    return assertSuccessful(result);
  }

  @Override
  public <T extends AbstractConfiguration<R>, R extends RuntimeInfo> ClusterManagementGetResult<T, R> get(
      T config) {
    ClusterManagementListResult<T, R> list = list(config);

    List<EntityInfo<T, R>> result = list.getResult();
    if (result.size() == 0) {
      raise(StatusCode.ENTITY_NOT_FOUND,
          config.getClass().getSimpleName() + " '" + config.getId() + "' does not exist.");
    }

    return new ClusterManagementGetResult<>(result.get(0));
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
  <R> List<R> executeAndGetFunctionResult(Object args,
      Set<DistributedMember> targetMembers) {
    Set<DistributedMember> targetMemberPRE1_12_0 = new HashSet<>();
    Set<DistributedMember> targetMemberPOST1_12_0 = new HashSet<>();
    if (targetMembers.size() == 0) {
      return Collections.emptyList();
    }

    targetMembers.stream().forEach(member -> {
      if (((InternalDistributedMember) member).getVersionOrdinal() < Version.GEODE_1_12_0
          .ordinal()) {
        targetMemberPRE1_12_0.add(member);
      } else {
        targetMemberPOST1_12_0.add(member);
      }
    });

    List<R> functionResults = new ArrayList<>();
    if (targetMemberPRE1_12_0.size() > 0) {
      Function<?> function =
          new org.apache.geode.management.internal.cli.functions.CacheRealizationFunction();
      Execution execution = FunctionService.onMembers(targetMemberPRE1_12_0).setArguments(args);
      ((AbstractExecution) execution).setIgnoreDepartedMembers(true);
      functionResults.addAll(cleanResults((List<?>) execution.execute(function).getResult()));
    }
    if (targetMemberPOST1_12_0.size() > 0) {
      Function<?> function = new CacheRealizationFunction();
      Execution execution = FunctionService.onMembers(targetMemberPOST1_12_0).setArguments(args);
      ((AbstractExecution) execution).setIgnoreDepartedMembers(true);
      functionResults.addAll(cleanResults((List<?>) execution.execute(function).getResult()));
    }

    return functionResults;
  }

  <R> List<R> cleanResults(List<?> functionResults) {
    List<R> results = new ArrayList<>();
    for (Object functionResult : functionResults) {
      if (functionResult == null) {
        continue;
      }
      if (functionResult instanceof Throwable) {
        // log the exception and continue
        logger.warn("Error executing CacheRealizationFunction.", (Throwable) functionResult);
        continue;
      }
      results.add((R) functionResult);
    }
    return results;
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
