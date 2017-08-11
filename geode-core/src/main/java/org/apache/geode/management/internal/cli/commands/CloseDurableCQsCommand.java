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
import org.apache.geode.management.internal.cli.functions.CloseDurableCqFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class CloseDurableCQsCommand implements GfshCommand {
  DurableClientCommandsResultBuilder builder = new DurableClientCommandsResultBuilder();

  @CliCommand(value = CliStrings.CLOSE_DURABLE_CQS, help = CliStrings.CLOSE_DURABLE_CQS__HELP)
  @CliMetaData()
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.QUERY)
  public Result closeDurableCqs(@CliOption(key = CliStrings.CLOSE_DURABLE_CQS__DURABLE__CLIENT__ID,
      mandatory = true,
      help = CliStrings.CLOSE_DURABLE_CQS__DURABLE__CLIENT__ID__HELP) final String durableClientId,

      @CliOption(key = CliStrings.CLOSE_DURABLE_CQS__NAME, mandatory = true,
          help = CliStrings.CLOSE_DURABLE_CQS__NAME__HELP) final String cqName,

      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          help = CliStrings.CLOSE_DURABLE_CQS__MEMBER__HELP,
          optionContext = ConverterHint.MEMBERIDNAME) final String[] memberNameOrId,

      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          help = CliStrings.CLOSE_DURABLE_CQS__GROUP__HELP,
          optionContext = ConverterHint.MEMBERGROUP) final String[] group) {
    Result result;
    try {
      Set<DistributedMember> targetMembers = CliUtil.findMembers(group, memberNameOrId);

      if (targetMembers.isEmpty()) {
        return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
      }

      String[] params = new String[2];
      params[0] = durableClientId;
      params[1] = cqName;

      final ResultCollector<?, ?> rc =
          CliUtil.executeFunction(new CloseDurableCqFunction(), params, targetMembers);
      final List<MemberResult> results = (List<MemberResult>) rc.getResult();
      String failureHeader =
          CliStrings.format(CliStrings.CLOSE_DURABLE_CQS__FAILURE__HEADER, cqName, durableClientId);
      String successHeader =
          CliStrings.format(CliStrings.CLOSE_DURABLE_CQS__SUCCESS, cqName, durableClientId);
      result = builder.buildResult(results, successHeader, failureHeader);
    } catch (Exception e) {
      result = ResultBuilder.createGemFireErrorResult(e.getMessage());
    }
    return result;
  }
}
