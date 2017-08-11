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

import org.apache.commons.lang.ArrayUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.domain.IndexInfo;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.DestroyIndexFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ErrorResultData;
import org.apache.geode.management.internal.cli.result.InfoResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class DestroyIndexCommand implements GfshCommand {
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
          help = CliStrings.DESTROY_INDEX__GROUP__HELP) final String[] group) {

    Result result;

    if (StringUtils.isBlank(indexName) && StringUtils.isBlank(regionPath)
        && ArrayUtils.isEmpty(group) && ArrayUtils.isEmpty(memberNameOrID)) {
      return ResultBuilder.createUserErrorResult(
          CliStrings.format(CliStrings.PROVIDE_ATLEAST_ONE_OPTION, CliStrings.DESTROY_INDEX));
    }

    final Cache cache = CacheFactory.getAnyInstance();
    String regionName = null;
    if (regionPath != null) {
      regionName = regionPath.startsWith("/") ? regionPath.substring(1) : regionPath;
    }
    IndexInfo indexInfo = new IndexInfo(indexName, regionName);
    Set<DistributedMember> targetMembers = CliUtil.findMembers(group, memberNameOrID);

    if (targetMembers.isEmpty()) {
      return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }

    ResultCollector rc = CliUtil.executeFunction(destroyIndexFunction, indexInfo, targetMembers);
    List<Object> funcResults = (List<Object>) rc.getResult();

    Set<String> successfulMembers = new TreeSet<>();
    Map<String, Set<String>> indexOpFailMap = new HashMap<>();

    AtomicReference<XmlEntity> xmlEntity = new AtomicReference<>();
    for (Object funcResult : funcResults) {
      if (!(funcResult instanceof CliFunctionResult)) {
        continue;
      }
      CliFunctionResult cliFunctionResult = (CliFunctionResult) funcResult;

      if (cliFunctionResult.isSuccessful()) {
        successfulMembers.add(cliFunctionResult.getMemberIdOrName());
        if (xmlEntity.get() == null) {
          xmlEntity.set(cliFunctionResult.getXmlEntity());
        }
      } else {
        String exceptionMessage = cliFunctionResult.getMessage();
        Set<String> failedMembers = indexOpFailMap.get(exceptionMessage);

        if (failedMembers == null) {
          failedMembers = new TreeSet<>();
        }
        failedMembers.add(cliFunctionResult.getMemberIdOrName());
        indexOpFailMap.put(exceptionMessage, failedMembers);
      }
    }
    if (!successfulMembers.isEmpty()) {
      InfoResultData infoResult = ResultBuilder.createInfoResultData();
      if (StringUtils.isNotBlank(indexName)) {
        if (StringUtils.isNotBlank(regionPath)) {
          infoResult.addLine(CliStrings.format(CliStrings.DESTROY_INDEX__ON__REGION__SUCCESS__MSG,
              indexName, regionPath));
        } else {
          infoResult.addLine(CliStrings.format(CliStrings.DESTROY_INDEX__SUCCESS__MSG, indexName));
        }
      } else {
        if (StringUtils.isNotBlank(regionPath)) {
          infoResult.addLine(CliStrings
              .format(CliStrings.DESTROY_INDEX__ON__REGION__ONLY__SUCCESS__MSG, regionPath));
        } else {
          infoResult.addLine(CliStrings.DESTROY_INDEX__ON__MEMBERS__ONLY__SUCCESS__MSG);
        }
      }
      int num = 0;
      for (String memberId : successfulMembers) {
        infoResult.addLine(CliStrings.format(
            CliStrings.format(CliStrings.DESTROY_INDEX__NUMBER__AND__MEMBER, ++num, memberId)));
      }
      result = ResultBuilder.buildResult(infoResult);
    } else {
      ErrorResultData erd = ResultBuilder.createErrorResultData();
      if (StringUtils.isNotBlank(indexName)) {
        erd.addLine(CliStrings.format(CliStrings.DESTROY_INDEX__FAILURE__MSG, indexName));
      } else {
        erd.addLine("Indexes could not be destroyed for following reasons");
      }

      Set<String> exceptionMessages = indexOpFailMap.keySet();

      for (String exceptionMessage : exceptionMessages) {
        erd.addLine(CliStrings.format(CliStrings.DESTROY_INDEX__REASON_MESSAGE, exceptionMessage));
        erd.addLine(CliStrings.DESTROY_INDEX__EXCEPTION__OCCURRED__ON);

        Set<String> memberIds = indexOpFailMap.get(exceptionMessage);
        int num = 0;

        for (String memberId : memberIds) {
          erd.addLine(CliStrings.format(
              CliStrings.format(CliStrings.DESTROY_INDEX__NUMBER__AND__MEMBER, ++num, memberId)));
        }
        erd.addLine("");
      }
      result = ResultBuilder.buildResult(erd);
    }
    if (xmlEntity.get() != null) {
      persistClusterConfiguration(result,
          () -> getSharedConfiguration().deleteXmlEntity(xmlEntity.get(), group));
    }
    return result;
  }
}
