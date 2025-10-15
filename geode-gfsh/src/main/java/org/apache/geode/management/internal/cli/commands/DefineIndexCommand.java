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

import java.util.Set;

import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.functions.ManageIndexDefinitionFunction;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class DefineIndexCommand extends GfshCommand {
  @ShellMethod(value = CliStrings.DEFINE_INDEX__HELP, key = CliStrings.DEFINE_INDEX)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_DATA})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.QUERY)
  public ResultModel defineIndex(
      @ShellOption(value = CliStrings.DEFINE_INDEX_NAME,
          help = CliStrings.DEFINE_INDEX__HELP) final String indexName,
      @ShellOption(value = CliStrings.DEFINE_INDEX__EXPRESSION,
          help = CliStrings.DEFINE_INDEX__EXPRESSION__HELP) final String indexedExpression,
      @ShellOption(value = CliStrings.DEFINE_INDEX__REGION,
          help = CliStrings.DEFINE_INDEX__REGION__HELP) String regionPath,
      @SuppressWarnings("deprecation") @ShellOption(value = CliStrings.DEFINE_INDEX__TYPE,
          defaultValue = "range",
          help = CliStrings.DEFINE_INDEX__TYPE__HELP) final org.apache.geode.cache.query.IndexType indexType) {

    ResultModel result = new ResultModel();

    RegionConfig.Index indexInfo = new RegionConfig.Index();
    indexInfo.setName(indexName);
    indexInfo.setExpression(indexedExpression);
    indexInfo.setFromClause(regionPath);
    indexInfo.setType(indexType.getName());

    IndexDefinition.indexDefinitions.add(indexInfo);

    // send the indexDefinition to the other locators to keep in memory
    Set<DistributedMember> allOtherLocators = findAllOtherLocators();
    if (allOtherLocators.size() > 0) {
      executeAndGetFunctionResult(new ManageIndexDefinitionFunction(),
          indexInfo, allOtherLocators);
    }

    InfoResultModel infoResult = result.addInfo();
    infoResult.addLine(CliStrings.DEFINE_INDEX__SUCCESS__MSG);
    infoResult.addLine(CliStrings.format(CliStrings.DEFINE_INDEX__NAME__MSG, indexName));
    infoResult
        .addLine(CliStrings.format(CliStrings.DEFINE_INDEX__EXPRESSION__MSG, indexedExpression));
    infoResult.addLine(CliStrings.format(CliStrings.DEFINE_INDEX__REGIONPATH__MSG, regionPath));

    return result;
  }
}
