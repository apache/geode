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

import org.apache.commons.lang.ArrayUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalClusterConfigurationService;
import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.domain.IndexInfo;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.DestroyIndexFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class DestroyIndexCommand extends InternalGfshCommand {
  private static final DestroyIndexFunction destroyIndexFunction = new DestroyIndexFunction();

  @CliCommand(value = CliStrings.DESTROY_INDEX, help = CliStrings.DESTROY_INDEX__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_DATA})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.QUERY)
  public Result destroyIndex(
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

    if (StringUtils.isBlank(indexName) && StringUtils.isBlank(regionPath)
        && ArrayUtils.isEmpty(group) && ArrayUtils.isEmpty(memberNameOrID)) {
      return ResultBuilder.createUserErrorResult(
          CliStrings.format(CliStrings.PROVIDE_ATLEAST_ONE_OPTION, CliStrings.DESTROY_INDEX));
    }

    String regionName = null;
    if (regionPath != null) {
      regionName = regionPath.startsWith("/") ? regionPath.substring(1) : regionPath;
    }
    IndexInfo indexInfo = new IndexInfo(indexName, regionName);
    indexInfo.setIfExists(ifExists);
    Set<DistributedMember> targetMembers = findMembers(group, memberNameOrID);

    if (targetMembers.isEmpty()) {
      return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }

    List<CliFunctionResult> funcResults =
        executeAndGetFunctionResult(destroyIndexFunction, indexInfo, targetMembers);
    Result result = ResultBuilder.buildResult(funcResults);
    XmlEntity xmlEntity = findXmlEntity(funcResults);

    if (xmlEntity != null) {
      persistClusterConfiguration(result,
          () -> ((InternalClusterConfigurationService) getConfigurationService())
              .deleteXmlEntity(xmlEntity, group));
    }
    return result;
  }
}
