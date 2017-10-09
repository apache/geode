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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.domain.IndexInfo;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.CreateIndexFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ErrorResultData;
import org.apache.geode.management.internal.cli.result.InfoResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class CreateIndexCommand implements GfshCommand {
  private static final CreateIndexFunction createIndexFunction = new CreateIndexFunction();

  @CliCommand(value = CliStrings.CREATE_INDEX, help = CliStrings.CREATE_INDEX__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_DATA})
  // TODO : Add optionContext for indexName
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.QUERY)
  public Result executeCommand(@CliOption(key = CliStrings.CREATE_INDEX__NAME, mandatory = true,
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
    AtomicReference<XmlEntity> xmlEntity = new AtomicReference<>();

    if (!regionPath.startsWith(Region.SEPARATOR)) {
      regionPath = Region.SEPARATOR + regionPath;
    }

    final Set<DistributedMember> targetMembers = findMembers(group, memberNameOrID);

    if (targetMembers.isEmpty()) {
      return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }

    final ResultCollector<?, ?> rc =
        createIndexOnMember(indexName, indexedExpression, regionPath, indexType, targetMembers);

    final List<Object> funcResults = (List<Object>) rc.getResult();
    final Set<String> successfulMembers = new TreeSet<>();
    final Map<String, Set<String>> indexOpFailMap = new HashMap<>();

    for (final Object funcResult : funcResults) {
      if (funcResult instanceof CliFunctionResult) {
        final CliFunctionResult cliFunctionResult = (CliFunctionResult) funcResult;

        if (cliFunctionResult.isSuccessful()) {
          successfulMembers.add(cliFunctionResult.getMemberIdOrName());

          if (xmlEntity.get() == null) {
            xmlEntity.set(cliFunctionResult.getXmlEntity());
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

    if (!successfulMembers.isEmpty()) {
      final InfoResultData infoResult = ResultBuilder.createInfoResultData();
      infoResult.addLine(CliStrings.CREATE_INDEX__SUCCESS__MSG);
      infoResult.addLine(CliStrings.format(CliStrings.CREATE_INDEX__NAME__MSG, indexName));
      infoResult
          .addLine(CliStrings.format(CliStrings.CREATE_INDEX__EXPRESSION__MSG, indexedExpression));
      infoResult.addLine(CliStrings.format(CliStrings.CREATE_INDEX__REGIONPATH__MSG, regionPath));
      infoResult.addLine(CliStrings.CREATE_INDEX__MEMBER__MSG);

      int num = 0;

      for (final String memberId : successfulMembers) {
        ++num;
        infoResult.addLine(
            CliStrings.format(CliStrings.CREATE_INDEX__NUMBER__AND__MEMBER, num, memberId));
      }
      result = ResultBuilder.buildResult(infoResult);

    } else {
      // Group members by the exception thrown.
      final ErrorResultData erd = ResultBuilder.createErrorResultData();
      erd.addLine(CliStrings.format(CliStrings.CREATE_INDEX__FAILURE__MSG, indexName));
      final Set<String> exceptionMessages = indexOpFailMap.keySet();

      for (final String exceptionMessage : exceptionMessages) {
        erd.addLine(exceptionMessage);
        erd.addLine(CliStrings.CREATE_INDEX__EXCEPTION__OCCURRED__ON);
        final Set<String> memberIds = indexOpFailMap.get(exceptionMessage);
        int num = 0;
        for (final String memberId : memberIds) {
          ++num;
          erd.addLine(
              CliStrings.format(CliStrings.CREATE_INDEX__NUMBER__AND__MEMBER, num, memberId));
        }
      }
      result = ResultBuilder.buildResult(erd);
    }

    if (xmlEntity.get() != null) {
      persistClusterConfiguration(result,
          () -> getSharedConfiguration().addXmlEntity(xmlEntity.get(), group));
    }
    return result;
  }

  ResultCollector<?, ?> createIndexOnMember(String indexName, String indexedExpression,
      String regionPath, IndexType indexType, Set<DistributedMember> targetMembers) {
    IndexInfo indexInfo = new IndexInfo(indexName, indexedExpression, regionPath, indexType);
    return CliUtil.executeFunction(createIndexFunction, indexInfo, targetMembers);
  }
}
