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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalClusterConfigurationService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.CreateDefinedIndexesFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ErrorResultData;
import org.apache.geode.management.internal.cli.result.InfoResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class CreateDefinedIndexesCommand extends InternalGfshCommand {
  private static final CreateDefinedIndexesFunction createDefinedIndexesFunction =
      new CreateDefinedIndexesFunction();

  @CliCommand(value = CliStrings.CREATE_DEFINED_INDEXES, help = CliStrings.CREATE_DEFINED__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_DATA})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.QUERY)
  // TODO : Add optionContext for indexName
  public Result createDefinedIndexes(

      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.CREATE_DEFINED_INDEXES__MEMBER__HELP) final String[] memberNameOrID,

      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.CREATE_DEFINED_INDEXES__GROUP__HELP) final String[] group) {

    Result result;
    List<XmlEntity> xmlEntities = new ArrayList<>();

    if (IndexDefinition.indexDefinitions.isEmpty()) {
      final InfoResultData infoResult = ResultBuilder.createInfoResultData();
      infoResult.addLine(CliStrings.DEFINE_INDEX__FAILURE__MSG);
      return ResultBuilder.buildResult(infoResult);
    }

    try {
      final Set<DistributedMember> targetMembers = findMembers(group, memberNameOrID);

      if (targetMembers.isEmpty()) {
        return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
      }

      final ResultCollector<?, ?> rc = executeFunction(createDefinedIndexesFunction,
          IndexDefinition.indexDefinitions, targetMembers);

      final List<Object> funcResults = (List<Object>) rc.getResult();
      final Set<String> successfulMembers = new TreeSet<>();
      final Map<String, Set<String>> indexOpFailMap = new HashMap<>();

      for (final Object funcResult : funcResults) {
        if (funcResult instanceof CliFunctionResult) {
          final CliFunctionResult cliFunctionResult = (CliFunctionResult) funcResult;

          if (cliFunctionResult.isSuccessful()) {
            successfulMembers.add(cliFunctionResult.getMemberIdOrName());

            // Only add the XmlEntity if it wasn't previously added from the result of another
            // successful member.
            XmlEntity resultEntity = cliFunctionResult.getXmlEntity();
            if ((null != resultEntity) && (!xmlEntities.contains(resultEntity))) {
              xmlEntities.add(cliFunctionResult.getXmlEntity());
            }
          } else {
            final String exceptionMessage = cliFunctionResult.getMessage();
            Set<String> failedMembers = indexOpFailMap.get(exceptionMessage);

            if (failedMembers == null) {
              failedMembers = new TreeSet<>();
            }

            failedMembers.add(cliFunctionResult.getMemberIdOrName());
            indexOpFailMap.put(exceptionMessage, failedMembers);
          }
        }
      }

      // TODO: GEODE-3916.
      // The index creation might succeed in some members and fail in others, the current logic only
      // reports to the user the members on which the operation was successful, giving no details
      // about the failures. We should report the exact details of what failed/succeeded, and
      // where/why.
      if (!successfulMembers.isEmpty()) {
        final InfoResultData infoResult = ResultBuilder.createInfoResultData();
        infoResult.addLine(CliStrings.CREATE_DEFINED_INDEXES__SUCCESS__MSG);

        int num = 0;

        for (final String memberId : successfulMembers) {
          ++num;
          infoResult.addLine(CliStrings
              .format(CliStrings.CREATE_DEFINED_INDEXES__NUMBER__AND__MEMBER, num, memberId));
        }
        result = ResultBuilder.buildResult(infoResult);

      } else {
        // Group members by the exception thrown.
        final ErrorResultData erd = ResultBuilder.createErrorResultData();

        final Set<String> exceptionMessages = indexOpFailMap.keySet();

        for (final String exceptionMessage : exceptionMessages) {
          erd.addLine(exceptionMessage);
          erd.addLine(CliStrings.CREATE_INDEX__EXCEPTION__OCCURRED__ON);
          final Set<String> memberIds = indexOpFailMap.get(exceptionMessage);

          int num = 0;
          for (final String memberId : memberIds) {
            ++num;
            erd.addLine(CliStrings.format(CliStrings.CREATE_DEFINED_INDEXES__NUMBER__AND__MEMBER,
                num, memberId));
          }
        }
        result = ResultBuilder.buildResult(erd);
      }
    } catch (Exception e) {
      result = ResultBuilder.createGemFireErrorResult(e.getMessage());
    }

    for (XmlEntity xmlEntity : xmlEntities) {
      persistClusterConfiguration(result,
          () -> ((InternalClusterConfigurationService) getConfigurationService())
              .addXmlEntity(xmlEntity, group));
    }

    return result;
  }
}
