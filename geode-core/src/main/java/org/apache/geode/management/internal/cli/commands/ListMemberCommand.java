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
import java.util.TreeSet;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class ListMemberCommand implements GfshCommand {
  @CliCommand(value = {CliStrings.LIST_MEMBER}, help = CliStrings.LIST_MEMBER__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_SERVER)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public Result listMember(@CliOption(key = {CliStrings.GROUP}, unspecifiedDefaultValue = "",
      optionContext = ConverterHint.MEMBERGROUP,
      help = CliStrings.LIST_MEMBER__GROUP__HELP) String group) {
    Result result;

    // TODO: Add the code for identifying the system services
    try {
      Set<DistributedMember> memberSet = new TreeSet<>();
      InternalCache cache = getCache();

      // default get all the members in the DS
      if (group.isEmpty()) {
        memberSet.addAll(CliUtil.getAllMembers(cache));
      } else {
        memberSet.addAll(cache.getDistributedSystem().getGroupMembers(group));
      }

      if (memberSet.isEmpty()) {
        result = ResultBuilder.createInfoResult(CliStrings.LIST_MEMBER__MSG__NO_MEMBER_FOUND);
      } else {
        TabularResultData resultData = ResultBuilder.createTabularResultData();
        for (DistributedMember member : memberSet) {
          resultData.accumulate("Name", member.getName());
          resultData.accumulate("Id", member.getId());
        }

        result = ResultBuilder.buildResult(resultData);
      }
    } catch (Exception e) {
      result = ResultBuilder
          .createGemFireErrorResult("Could not fetch the list of members. " + e.getMessage());
      LogWrapper.getInstance().warning(e.getMessage(), e);
    }
    return result;
  }
}
