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

import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.domain.MemberResult;
import org.apache.geode.management.internal.cli.functions.CloseDurableClientFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class CloseDurableClientCommand implements GfshCommand {
  DurableClientCommandsResultBuilder builder = new DurableClientCommandsResultBuilder();

  @CliCommand(value = CliStrings.CLOSE_DURABLE_CLIENTS,
      help = CliStrings.CLOSE_DURABLE_CLIENTS__HELP)
  @CliMetaData()
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.QUERY)
  public Result closeDurableClient(
      @CliOption(key = CliStrings.CLOSE_DURABLE_CLIENTS__CLIENT__ID, mandatory = true,
          help = CliStrings.CLOSE_DURABLE_CLIENTS__CLIENT__ID__HELP) final String durableClientId,
      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          help = CliStrings.CLOSE_DURABLE_CLIENTS__MEMBER__HELP,
          optionContext = ConverterHint.MEMBERIDNAME) final String[] memberNameOrId,
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          help = CliStrings.COUNT_DURABLE_CQ_EVENTS__GROUP__HELP,
          optionContext = ConverterHint.MEMBERGROUP) final String[] group) {

    Result result;
    try {

      Set<DistributedMember> targetMembers = CliUtil.findMembers(group, memberNameOrId);

      if (targetMembers.isEmpty()) {
        return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
      }

      final ResultCollector<?, ?> rc =
          CliUtil.executeFunction(new CloseDurableClientFunction(), durableClientId, targetMembers);
      final List<MemberResult> results = (List<MemberResult>) rc.getResult();
      String failureHeader =
          CliStrings.format(CliStrings.CLOSE_DURABLE_CLIENTS__FAILURE__HEADER, durableClientId);
      String successHeader =
          CliStrings.format(CliStrings.CLOSE_DURABLE_CLIENTS__SUCCESS, durableClientId);
      result = builder.buildResult(results, successHeader, failureHeader);
    } catch (Exception e) {
      result = ResultBuilder.createGemFireErrorResult(e.getMessage());
    }
    return result;
  }
}
