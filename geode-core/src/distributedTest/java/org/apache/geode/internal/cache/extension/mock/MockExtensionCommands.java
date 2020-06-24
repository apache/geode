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

package org.apache.geode.internal.cache.extension.mock;

import java.util.List;
import java.util.Set;

import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.management.internal.util.ManagementUtils;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

/**
 * Mock Extension gfsh commands.
 *
 * @since GemFire 8.1
 */
public class MockExtensionCommands implements CommandMarker {

  public static final String OPTION_VALUE = "value";

  public static final String OPTION_REGION_NAME = "region-name";

  public static final String CREATE_MOCK_REGION_EXTENSION = "create mock region extension";

  public static final String ALTER_MOCK_REGION_EXTENSION = "alter mock region extension";

  public static final String DESTROY_MOCK_REGION_EXTENSION = "destroy mock region extension";

  public static final String CREATE_MOCK_CACHE_EXTENSION = "create mock cache extension";

  public static final String ALTER_MOCK_CACHE_EXTENSION = "alter mock cache extension";

  public static final String DESTROY_MOCK_CACHE_EXTENSION = "destroy mock cache extension";

  /**
   * Creates a {@link MockRegionExtension} on the given <code>regionName</code>.
   *
   * @param regionName {@link Region} name on which to create {@link MockRegionExtension} .
   * @param value {@link String} value to set on {@link MockRegionExtension#setValue(String)}.
   * @return {@link Result}
   * @since GemFire 8.1
   */
  @CliCommand(value = CREATE_MOCK_REGION_EXTENSION)
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result createMockRegionExtension(
      @CliOption(key = OPTION_REGION_NAME, mandatory = true) final String regionName,
      @CliOption(key = OPTION_VALUE, mandatory = true) final String value) {
    return executeFunctionOnAllMembersTabulateResultPersist(
        new CreateMockRegionExtensionFunction(), true,
        CreateMockRegionExtensionFunction.toArgs(regionName, value));
  }

  /**
   * Alters a {@link MockRegionExtension} on the given <code>regionName</code>.
   *
   * @param regionName {@link Region} name on which to create {@link MockRegionExtension} .
   * @param value {@link String} value to set on {@link MockRegionExtension#setValue(String)}.
   * @return {@link Result}
   * @since GemFire 8.1
   */
  @CliCommand(value = ALTER_MOCK_REGION_EXTENSION)
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result alterMockRegionExtension(
      @CliOption(key = OPTION_REGION_NAME, mandatory = true) final String regionName,
      @CliOption(key = OPTION_VALUE, mandatory = true) final String value) {
    return executeFunctionOnAllMembersTabulateResultPersist(
        new AlterMockRegionExtensionFunction(), true,
        AlterMockRegionExtensionFunction.toArgs(regionName, value));
  }

  /**
   * Destroy the {@link MockRegionExtension} on the given <code>regionName</code>.
   *
   * @param regionName {@link Region} name on which to create {@link MockRegionExtension} .
   * @return {@link Result}
   * @since GemFire 8.1
   */
  @CliCommand(value = DESTROY_MOCK_REGION_EXTENSION)
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result destroyMockRegionExtension(
      @CliOption(key = OPTION_REGION_NAME, mandatory = true) final String regionName) {
    return executeFunctionOnAllMembersTabulateResultPersist(
        new DestroyMockRegionExtensionFunction(), true,
        DestroyMockRegionExtensionFunction.toArgs(regionName));
  }

  /**
   * Creates a {@link MockCacheExtension}.
   *
   * @param value {@link String} value to set on {@link MockCacheExtension#setValue(String)}.
   * @return {@link Result}
   * @since GemFire 8.1
   */
  @CliCommand(value = CREATE_MOCK_CACHE_EXTENSION)
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result createMockCacheExtension(
      @CliOption(key = OPTION_VALUE, mandatory = true) final String value) {
    return executeFunctionOnAllMembersTabulateResultPersist(
        new CreateMockCacheExtensionFunction(), true,
        CreateMockCacheExtensionFunction.toArgs(value));
  }

  /**
   * Alter a {@link MockCacheExtension}.
   *
   * @param value {@link String} value to set on {@link MockCacheExtension#setValue(String)}.
   * @return {@link Result}
   * @since GemFire 8.1
   */
  @CliCommand(value = ALTER_MOCK_CACHE_EXTENSION)
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result alterMockCacheExtension(
      @CliOption(key = OPTION_VALUE, mandatory = true) final String value) {
    return executeFunctionOnAllMembersTabulateResultPersist(
        new AlterMockCacheExtensionFunction(), true,
        AlterMockCacheExtensionFunction.toArgs(value));
  }

  /**
   * Destroy a {@link MockCacheExtension}.
   *
   * @return {@link Result}
   * @since GemFire 8.1
   */
  @CliCommand(value = DESTROY_MOCK_CACHE_EXTENSION)
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result destroyMockCacheExtension() {
    return executeFunctionOnAllMembersTabulateResultPersist(
        new DestroyMockCacheExtensionFunction(), false);
  }

  /**
   * Call <code>function</code> with <code>args</code> on all members, tabulate results and persist
   * shared config if changed.
   *
   * @param function {@link Function} to execute.
   * @param addXmlElement If <code>true</code> then add result {@link XmlEntity} to the config,
   *        otherwise delete it.
   * @param args Arguments to pass to function.
   * @since GemFire 8.1
   */
  protected Result executeFunctionOnAllMembersTabulateResultPersist(final Function function,
      final boolean addXmlElement, final Object... args) {
    InternalCache cache = GemFireCacheImpl.getInstance();
    final Set<DistributedMember> members = ManagementUtils.getAllNormalMembers(cache);

    @SuppressWarnings("unchecked")
    final ResultCollector<CliFunctionResult, List<CliFunctionResult>> resultCollector =
        (ResultCollector<CliFunctionResult, List<CliFunctionResult>>) ManagementUtils
            .executeFunction(function, args, members);
    final List<CliFunctionResult> functionResults = resultCollector.getResult();

    ResultModel resultModel = ResultModel.createMemberStatusResult(functionResults);

    XmlEntity xmlEntity = functionResults.stream().filter(CliFunctionResult::isSuccessful)
        .map(CliFunctionResult::getXmlEntity)
        .findFirst().orElse(null);

    InternalConfigurationPersistenceService ccService =
        InternalLocator.getLocator().getConfigurationPersistenceService();
    System.out.println("MockExtensionCommands: persisting xmlEntity=" + xmlEntity);
    if (xmlEntity != null) {
      if (addXmlElement) {
        ccService.addXmlEntity(xmlEntity, null);
      } else {
        ccService.deleteXmlEntity(xmlEntity, null);
      }
    }

    return new CommandResult(resultModel);
  }
}
