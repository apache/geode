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

package org.apache.geode.management.internal.cli.commands;

import java.util.List;
import java.util.Set;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.internal.cli.functions.CreateDefinedIndexesFunction;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class CreateDefinedIndexesCommand extends SingleGfshCommand {
  public static final String CREATE_DEFINED_INDEXES_SECTION = "create-defined-indexes";
  @Immutable
  private static final CreateDefinedIndexesFunction createDefinedIndexesFunction =
      new CreateDefinedIndexesFunction();

  @CliCommand(value = CliStrings.CREATE_DEFINED_INDEXES, help = CliStrings.CREATE_DEFINED__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_DATA})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.QUERY)
  public ResultModel createDefinedIndexes(

      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.CREATE_DEFINED_INDEXES__MEMBER__HELP) final String[] memberNameOrID,

      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.CREATE_DEFINED_INDEXES__GROUP__HELP) final String[] groups) {

    ResultModel result = new ResultModel();

    if (IndexDefinition.indexDefinitions.isEmpty()) {
      return ResultModel.createInfo(CliStrings.DEFINE_INDEX__FAILURE__MSG);
    }

    Set<DistributedMember> targetMembers = findMembers(groups, memberNameOrID);
    if (targetMembers.isEmpty()) {
      return ResultModel.createError(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }

    List<CliFunctionResult> functionResults = executeAndGetFunctionResult(
        createDefinedIndexesFunction, IndexDefinition.indexDefinitions, targetMembers);
    result.addTableAndSetStatus(CREATE_DEFINED_INDEXES_SECTION, functionResults, false, true);
    result.setConfigObject(IndexDefinition.indexDefinitions);

    return result;
  }

  @Override
  public boolean updateConfigForGroup(String group, CacheConfig config, Object configObject) {
    @SuppressWarnings("unchecked")
    Set<RegionConfig.Index> updatedIndexes = (Set<RegionConfig.Index>) configObject;
    if (updatedIndexes == null) {
      return false;
    }

    for (RegionConfig.Index index : updatedIndexes) {
      RegionConfig regionConfig = getValidRegionConfig(index.getFromClause(), config);
      if (regionConfig == null) {
        throw new IllegalStateException("RegionConfig is null");
      }

      regionConfig.getIndexes().add(index);
    }
    return true;
  }

  RegionConfig getValidRegionConfig(String regionPath, CacheConfig cacheConfig) {
    // Check to see if the region path contains an alias e.g "/region1 r1"
    // Then the first string will be the regionPath
    String[] regionPathTokens = regionPath.trim().split(" ");
    regionPath = regionPathTokens[0];
    // check to see if the region path is in the form of "--region=region.entrySet() z"
    RegionConfig regionConfig = cacheConfig.findRegionConfiguration(regionPath);
    while (regionPath.contains(".") && (regionConfig) == null) {
      regionPath = regionPath.substring(0, regionPath.lastIndexOf("."));
      regionConfig = cacheConfig.findRegionConfiguration(regionPath);
    }
    return regionConfig;
  }
}
