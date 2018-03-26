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

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.query.IndexType;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.domain.IndexInfo;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.InfoResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class DefineIndexCommand extends InternalGfshCommand {
  @CliCommand(value = CliStrings.DEFINE_INDEX, help = CliStrings.DEFINE_INDEX__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_DATA})
  // TODO : Add optionContext for indexName
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.QUERY)
  public Result defineIndex(
      @CliOption(key = CliStrings.DEFINE_INDEX_NAME, mandatory = true,
          help = CliStrings.DEFINE_INDEX__HELP) final String indexName,
      @CliOption(key = CliStrings.DEFINE_INDEX__EXPRESSION, mandatory = true,
          help = CliStrings.DEFINE_INDEX__EXPRESSION__HELP) final String indexedExpression,
      @CliOption(key = CliStrings.DEFINE_INDEX__REGION, mandatory = true,
          optionContext = ConverterHint.REGION_PATH,
          help = CliStrings.DEFINE_INDEX__REGION__HELP) String regionPath,
      @CliOption(key = CliStrings.DEFINE_INDEX__TYPE, unspecifiedDefaultValue = "range",
          optionContext = ConverterHint.INDEX_TYPE,
          help = CliStrings.DEFINE_INDEX__TYPE__HELP) final IndexType indexType) {

    Result result;

    IndexInfo indexInfo = new IndexInfo(indexName, indexedExpression, regionPath, indexType);
    IndexDefinition.indexDefinitions.add(indexInfo);

    final InfoResultData infoResult = ResultBuilder.createInfoResultData();
    infoResult.addLine(CliStrings.DEFINE_INDEX__SUCCESS__MSG);
    infoResult.addLine(CliStrings.format(CliStrings.DEFINE_INDEX__NAME__MSG, indexName));
    infoResult
        .addLine(CliStrings.format(CliStrings.DEFINE_INDEX__EXPRESSION__MSG, indexedExpression));
    infoResult.addLine(CliStrings.format(CliStrings.DEFINE_INDEX__REGIONPATH__MSG, regionPath));
    result = ResultBuilder.buildResult(infoResult);

    return result;
  }
}
