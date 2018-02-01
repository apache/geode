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
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.execute.FunctionInvocationTargetException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.domain.RegionInformation;
import org.apache.geode.management.internal.cli.functions.GetRegionsFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class ListRegionCommand implements GfshCommand {
  private static final GetRegionsFunction getRegionsFunction = new GetRegionsFunction();

  @CliCommand(value = {CliStrings.LIST_REGION}, help = CliStrings.LIST_REGION__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_REGION)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public Result listRegion(
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.LIST_REGION__GROUP__HELP) String[] group,
      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.LIST_REGION__MEMBER__HELP) String[] memberNameOrId) {
    Result result = null;
    try {
      Set<RegionInformation> regionInfoSet = new LinkedHashSet<>();
      ResultCollector<?, ?> rc;
      Set<DistributedMember> targetMembers = CliUtil.findMembers(group, memberNameOrId);

      if (targetMembers.isEmpty()) {
        return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
      }

      TabularResultData resultData = ResultBuilder.createTabularResultData();
      rc = CliUtil.executeFunction(getRegionsFunction, null, targetMembers);
      ArrayList<?> resultList = (ArrayList<?>) rc.getResult();

      if (resultList != null) {
        // Gather all RegionInformation into a flat set.
        regionInfoSet.addAll(resultList.stream().filter(Objects::nonNull)
            .filter(Object[].class::isInstance).map(Object[].class::cast).flatMap(Arrays::stream)
            .filter(RegionInformation.class::isInstance).map(RegionInformation.class::cast)
            .collect(Collectors.toSet()));

        Set<String> regionNames = new TreeSet<>();

        for (RegionInformation regionInfo : regionInfoSet) {
          regionNames.add(regionInfo.getName());
          Set<String> subRegionNames = regionInfo.getSubRegionNames();

          regionNames.addAll(subRegionNames);
        }

        for (String regionName : regionNames) {
          resultData.accumulate("List of regions", regionName);
        }

        if (!regionNames.isEmpty()) {
          result = ResultBuilder.buildResult(resultData);

        } else {
          result = ResultBuilder.createInfoResult(CliStrings.LIST_REGION__MSG__NOT_FOUND);
        }
      }
    } catch (FunctionInvocationTargetException e) {
      result = ResultBuilder.createGemFireErrorResult(CliStrings
          .format(CliStrings.COULD_NOT_EXECUTE_COMMAND_TRY_AGAIN, CliStrings.LIST_REGION));
    } catch (Exception e) {
      result = ResultBuilder
          .createGemFireErrorResult(CliStrings.LIST_REGION__MSG__ERROR + " : " + e.getMessage());
    }
    return result;
  }
}
