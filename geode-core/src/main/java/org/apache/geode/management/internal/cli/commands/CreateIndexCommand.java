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

import org.apache.geode.cache.query.IndexType;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalClusterConfigurationService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.domain.IndexInfo;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.CreateIndexFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class CreateIndexCommand extends InternalGfshCommand {
  private static final CreateIndexFunction createIndexFunction = new CreateIndexFunction();

  @CliCommand(value = CliStrings.CREATE_INDEX, help = CliStrings.CREATE_INDEX__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_DATA})
  // TODO : Add optionContext for indexName
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.QUERY)
  public Result createIndex(@CliOption(key = CliStrings.CREATE_INDEX__NAME, mandatory = true,
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
          help = CliStrings.CREATE_INDEX__GROUP__HELP) final String[] group) {

    Result result;
    final Set<DistributedMember> targetMembers = findMembers(group, memberNameOrID);

    if (targetMembers.isEmpty()) {
      return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }

    IndexInfo indexInfo = new IndexInfo(indexName, indexedExpression, regionPath, indexType);
    List<CliFunctionResult> functionResults =
        executeAndGetFunctionResult(createIndexFunction, indexInfo, targetMembers);
    result = ResultBuilder.buildResult(functionResults);
    XmlEntity xmlEntity = findXmlEntity(functionResults);

    if (xmlEntity != null) {
      persistClusterConfiguration(result,
          () -> ((InternalClusterConfigurationService) getConfigurationService())
              .addXmlEntity(xmlEntity, group));
    }
    return result;
  }
}
