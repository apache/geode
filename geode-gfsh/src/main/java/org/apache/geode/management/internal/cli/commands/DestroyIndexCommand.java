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

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.lang.Identifiable;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.internal.cli.functions.DestroyIndexFunction;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.exceptions.EntityNotFoundException;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class DestroyIndexCommand extends SingleGfshCommand {
  @Immutable
  private static final DestroyIndexFunction destroyIndexFunction = new DestroyIndexFunction();

  @CliCommand(value = CliStrings.DESTROY_INDEX, help = CliStrings.DESTROY_INDEX__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_DATA})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.QUERY)
  public ResultModel destroyIndex(
      @CliOption(key = CliStrings.DESTROY_INDEX__NAME, unspecifiedDefaultValue = "",
          help = CliStrings.DESTROY_INDEX__NAME__HELP) final String indexName,
      @CliOption(key = CliStrings.DESTROY_INDEX__REGION, optionContext = ConverterHint.REGION_PATH,
          help = CliStrings.DESTROY_INDEX__REGION__HELP) final String regionPath,
      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.DESTROY_INDEX__MEMBER__HELP) final String[] memberNameOrID,
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.DESTROY_INDEX__GROUP__HELP) final String[] group,
      @CliOption(key = CliStrings.IFEXISTS, specifiedDefaultValue = "true",
          unspecifiedDefaultValue = "false", help = CliStrings.IFEXISTS_HELP) boolean ifExists) {

    if (isBlank(indexName) && isBlank(regionPath)
        && ArrayUtils.isEmpty(group) && ArrayUtils.isEmpty(memberNameOrID)) {
      return ResultModel.createError(
          CliStrings.format(CliStrings.PROVIDE_ATLEAST_ONE_OPTION, CliStrings.DESTROY_INDEX));
    }

    String regionName = null;
    if (regionPath != null) {
      regionName = regionPath.startsWith("/") ? regionPath.substring(1) : regionPath;
    }

    RegionConfig.Index indexInfo = new RegionConfig.Index();
    indexInfo.setName(indexName);
    indexInfo.setFromClause(regionName);

    Set<DistributedMember> targetMembers = findMembers(group, memberNameOrID);

    if (targetMembers.isEmpty()) {
      return ResultModel.createError(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }

    List<CliFunctionResult> funcResults =
        executeAndGetFunctionResult(destroyIndexFunction, indexInfo, targetMembers);
    ResultModel result = ResultModel.createMemberStatusResult(funcResults, ifExists);

    result.setConfigObject(indexInfo);

    return result;
  }

  @Override
  public boolean updateConfigForGroup(String group, CacheConfig config, Object element) {
    RegionConfig.Index indexFromCommand = (RegionConfig.Index) element;
    String indexName = indexFromCommand.getName();

    String regionName = indexFromCommand.getFromClause();
    if (regionName != null) {
      RegionConfig regionConfig = config.findRegionConfiguration(regionName);
      if (regionConfig == null) {
        String errorMessage = "Region " + regionName + " not found";
        if (!ConfigurationPersistenceService.CLUSTER_CONFIG.equals(group)) {
          errorMessage += " in group " + group;
        }
        throw new EntityNotFoundException(errorMessage);
      }

      if (indexName.isEmpty()) {
        regionConfig.getIndexes().clear();
      } else {
        Identifiable.remove(regionConfig.getIndexes(), indexName);
      }
    } else {
      // Need to search for the index name as region was not specified
      for (RegionConfig r : config.getRegions()) {
        Identifiable.remove(r.getIndexes(), indexName);
      }
    }
    return true;
  }

}
