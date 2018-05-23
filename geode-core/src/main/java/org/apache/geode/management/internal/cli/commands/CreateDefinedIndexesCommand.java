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
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.CreateDefinedIndexesFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class CreateDefinedIndexesCommand extends SingleGfshCommand {
  public static final String CREATE_DEFINED_INDEXES_SECTION = "create-defined-indexes";
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
      result.addInfo().addLine(CliStrings.DEFINE_INDEX__FAILURE__MSG);
      return result;
    }

    Set<DistributedMember> targetMembers = findMembers(groups, memberNameOrID);
    if (targetMembers.isEmpty()) {
      result.addInfo().addLine(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
      result.setStatus(Result.Status.ERROR);
      return result;
    }

    // This command is very horrible because there is no definitive correlation between where
    // indexes might have been created and where errors occurred. Thus it may be very difficult to
    // determine what to update for the cluster configuration. We try to alleviate this problem by
    // applying changes iteratively.
    // It may be better to limit this command to EITHER executing on a list of members OR
    // to execute on a SINGLE group.

    // First let's do the explicitly provided members. This implies changes to the whole cluster.
    // However this is also not always a correct assumption as the user could be creating an index
    // on a REPLICATED region on a single member. (Indexes created on PARTITIONED regions are
    // automatically distributed).

    Result.Status status;
    TabularResultModel table = result.addTable(CREATE_DEFINED_INDEXES_SECTION);
    ResultCollector<?, ?> rc = executeFunction(createDefinedIndexesFunction,
        IndexDefinition.indexDefinitions, targetMembers);

    status = accumulateResults(table, (List<CliFunctionResult>) rc.getResult());
    if (status == Result.Status.ERROR) {
      result.setStatus(status);
    } else {
      result.setConfigObject(IndexDefinition.indexDefinitions);
    }

    return result;
  }

  private Result.Status accumulateResults(TabularResultModel table,
      List<CliFunctionResult> functionResults) {
    Result.Status resultStatus = Result.Status.OK;

    for (CliFunctionResult cliFunctionResult : functionResults) {
      if (cliFunctionResult.getResultObject() instanceof List) {
        for (Object singleResult : (List<?>) cliFunctionResult.getResultObject()) {
          table.accumulate("member", cliFunctionResult.getMemberIdOrName());
          table.accumulate("status", cliFunctionResult.getStatus());

          if (singleResult instanceof String) {
            table.accumulate("message", "Created index " + singleResult.toString());
          } else {
            table.accumulate("message", ((Throwable) singleResult).getMessage());
          }
        }
      } else if (cliFunctionResult.getResultObject() instanceof Throwable) {
        table.accumulate("member", cliFunctionResult.getMemberIdOrName());
        table.accumulate("status", cliFunctionResult.getStatus());
        table.accumulate("message", ((Throwable) cliFunctionResult.getResultObject()).getMessage());
      } else {
        throw new IllegalStateException("create defined indexes returned function result of "
            + cliFunctionResult.getResultObject().getClass().getSimpleName());
      }

      if (!cliFunctionResult.isSuccessful()) {
        resultStatus = Result.Status.ERROR;
      }
    }

    return resultStatus;
  }

  @Override
  public void updateClusterConfig(String group, CacheConfig config, Object configObject) {
    Set<RegionConfig.Index> updatedIndexes = (Set<RegionConfig.Index>) configObject;
    if (updatedIndexes == null) {
      return;
    }

    for (RegionConfig.Index index : updatedIndexes) {
      RegionConfig region = config.findRegionConfiguration(index.getFromClause());
      if (region == null) {
        throw new IllegalStateException("RegionConfig is null");
      }

      region.getIndexes().add(index);
    }
  }
}
