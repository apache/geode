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

import static org.apache.geode.management.internal.cli.remote.CommandExecutor.RUN_ON_MEMBER_CHANGE_NOT_PERSISTED;
import static org.apache.geode.management.internal.cli.remote.CommandExecutor.SERVICE_NOT_RUNNING_CHANGE_NOT_PERSISTED;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.internal.cli.functions.CreateIndexFunction;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class CreateIndexCommand extends GfshCommand {
  @Immutable
  private static final CreateIndexFunction createIndexFunction = new CreateIndexFunction();

  @CliCommand(value = CliStrings.CREATE_INDEX, help = CliStrings.CREATE_INDEX__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_DATA})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.QUERY)
  @SuppressWarnings("deprecation")
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
          help = CliStrings.CREATE_INDEX__TYPE__HELP) final org.apache.geode.cache.query.IndexType indexType,

      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.CREATE_INDEX__GROUP__HELP) String[] groups) {

    // first find out what groups this region belongs to when using cluster configuration
    InternalConfigurationPersistenceService ccService = getConfigurationPersistenceService();
    ClusterManagementService cms = getClusterManagementService();
    final Set<DistributedMember> targetMembers;
    ResultModel resultModel = new ResultModel();
    InfoResultModel info = resultModel.addInfo();
    String regionName = null;
    // if cluster management service is enabled and user did not specify a member id, then
    // we will find the applicable members based on the what group this region is on
    if (ccService != null && memberNameOrID == null) {
      regionName = getValidRegionName(regionPath, cms);
      Set<String> calculatedGroups = getGroupsContainingRegion(cms, regionName);
      if (calculatedGroups.isEmpty()) {
        return ResultModel.createError("Region " + regionName + " does not exist.");
      }
      if (groups != null && !calculatedGroups.containsAll(Arrays.asList(groups))) {
        return ResultModel
            .createError("Region " + regionName + " does not exist in some of the groups.");
      }
      if (groups == null) {
        // the calculatedGroups will have null value to indicate the "cluster" level, in thise case
        // we want the groups to an empty array
        groups = calculatedGroups.stream().filter(Objects::nonNull).toArray(String[]::new);
      }
      targetMembers = findMembers(groups, null);
    }
    // otherwise use the group/members specified in the option to find the applicable members.
    else {
      targetMembers = findMembers(groups, memberNameOrID);
    }

    if (targetMembers.isEmpty()) {
      return ResultModel.createError(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }

    RegionConfig.Index index = new RegionConfig.Index();
    index.setName(indexName);
    index.setExpression(indexedExpression);
    index.setFromClause(regionPath);
    if (indexType == org.apache.geode.cache.query.IndexType.PRIMARY_KEY) {
      index.setKeyIndex(true);
    } else {
      index.setKeyIndex(false);
      index.setType(indexType.getName());
    }

    List<CliFunctionResult> functionResults =
        executeAndGetFunctionResult(createIndexFunction, index, targetMembers);
    resultModel.addTableAndSetStatus("createIndex", functionResults, true, false);

    if (!resultModel.isSuccessful()) {
      return resultModel;
    }

    // update the cluster configuration. Can't use SingleGfshCommand to do the update since in some
    // cases
    // groups information is inferred by the region, and the --group option might have the wrong
    // group
    // information.
    if (ccService == null) {
      info.addLine(SERVICE_NOT_RUNNING_CHANGE_NOT_PERSISTED);
      return resultModel;
    }
    if (memberNameOrID != null) {
      info.addLine(RUN_ON_MEMBER_CHANGE_NOT_PERSISTED);
      return resultModel;
    }

    final InfoResultModel groupStatus = resultModel.addInfo("groupStatus");
    String finalRegionName = regionName;
    // at this point, groups should be the regionConfig's groups
    if (groups.length == 0) {
      groups = new String[] {"cluster"};
    }
    for (String group : groups) {
      ccService.updateCacheConfig(group, cacheConfig -> {
        RegionConfig regionConfig = cacheConfig.findRegionConfiguration(finalRegionName);
        regionConfig.getIndexes().add(index);
        groupStatus
            .addLine("Cluster configuration for group '" + group + "' is updated.");
        return cacheConfig;
      });
    }
    return resultModel;
  }

  // find a valid regionName when regionPath passed in is in the form of
  // "/region1.fieldName.fieldName x"
  // this also handles the possibility when regionName has "." in it, like "/A.B". It's stripping
  // . part one by one and check if the remaining part is a valid region name or not. If we
  // could not find a region with any part of the name, (like, couldn't find A.B or A), then A is
  // returned.
  String getValidRegionName(String regionPath, ClusterManagementService cms) {
    // Check to see if the region path contains an alias e.g "/region1 r1"
    // Then the first string will be the regionPath
    String regionName = regionPath.trim().split(" ")[0];
    // check to see if the region path is in the form of "--region=region.entrySet() z"
    while (regionName.contains(".")) {
      Set<String> groupsContainingRegion = getGroupsContainingRegion(cms, regionName);
      if (!groupsContainingRegion.isEmpty()) {
        break;
      }
      // otherwise, strip one more . part off the regionName
      else {
        regionName = regionName.substring(0, regionName.lastIndexOf("."));
      }
    }
    return regionName;
  }

  // if region belongs to "cluster" level, it will return a set of one null value
  Set<String> getGroupsContainingRegion(ClusterManagementService cms,
      String regionName) {
    Region regionConfig = new Region();
    regionConfig.setName(regionName);
    List<Region> regions = cms.list(regionConfig).getConfigResult();
    return regions.stream().map(Region::getGroup)
        .collect(Collectors.toSet());
  }


}
