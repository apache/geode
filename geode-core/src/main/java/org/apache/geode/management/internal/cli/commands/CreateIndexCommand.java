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

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.internal.cli.exceptions.EntityNotFoundException;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.CreateIndexFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class CreateIndexCommand extends SingleGfshCommand {
  private static final CreateIndexFunction createIndexFunction = new CreateIndexFunction();

  @CliCommand(value = CliStrings.CREATE_INDEX, help = CliStrings.CREATE_INDEX__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_DATA})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.QUERY)
  public ResultModel createIndex(@CliOption(key = CliStrings.CREATE_INDEX__NAME, mandatory = true,
      help = CliStrings.CREATE_INDEX__NAME__HELP) final String indexName,

      @CliOption(key = CliStrings.CREATE_INDEX__EXPRESSION, mandatory = true,
          help = CliStrings.CREATE_INDEX__EXPRESSION__HELP) final String indexedExpression,

      @CliOption(key = CliStrings.CREATE_INDEX__REGION, mandatory = true,
          optionContext = ConverterHint.REGION_PATH,
          help = CliStrings.CREATE_INDEX__REGION__HELP) String regionPath,

      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.CREATE_INDEX__MEMBER__HELP) final String[] memberNameOrID,

      @CliOption(key = CliStrings.CREATE_INDEX__TYPE, unspecifiedDefaultValue = "range",
          optionContext = ConverterHint.INDEX_TYPE,
          help = CliStrings.CREATE_INDEX__TYPE__HELP) final IndexType indexType,

      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.CREATE_INDEX__GROUP__HELP) final String[] groups) {

    final Set<DistributedMember> targetMembers = findMembers(groups, memberNameOrID);

    if (targetMembers.isEmpty()) {
      return ResultModel.createError(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }

    RegionConfig.Index index = new RegionConfig.Index();
    index.setName(indexName);
    index.setExpression(indexedExpression);
    index.setFromClause(regionPath);
    if (indexType == IndexType.PRIMARY_KEY) {
      index.setKeyIndex(true);
    } else {
      index.setKeyIndex(false);
      index.setType(indexType.getName());
    }

    List<CliFunctionResult> functionResults =
        executeAndGetFunctionResult(createIndexFunction, index, targetMembers);
    ResultModel result = ResultModel.createMemberStatusResult(functionResults);
    result.setConfigObject(index);
    return result;
  }

  String getValidRegionName(String regionPath, CacheConfig cacheConfig) {
    // Check to see if the region path contains an alias e.g "/region1 r1"
    // Then the first string will be the regionPath
    String[] regionPathTokens = regionPath.trim().split(" ");
    regionPath = regionPathTokens[0];
    // check to see if the region path is in the form of "--region=region.entrySet() z"
    while (regionPath.contains(".") && cacheConfig.findRegionConfiguration(regionPath) == null) {
      regionPath = regionPath.substring(0, regionPath.lastIndexOf("."));
    }
    return regionPath;
  }

  @Override
  public boolean updateConfigForGroup(String group, CacheConfig config, Object element) {
    RegionConfig.Index index = (RegionConfig.Index) element;
    String regionPath = getValidRegionName(index.getFromClause(), config);

    RegionConfig regionConfig = config.findRegionConfiguration(regionPath);
    if (regionConfig == null) {
      throw new EntityNotFoundException("Region " + index.getFromClause() + " not found.");
    }
    regionConfig.getIndexes().add(index);
    return true;
  }
}
