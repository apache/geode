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
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.functions.ListDurableCqNamesFunction;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class ListDurableClientCQsCommand extends GfshCommand {

  @CliCommand(value = CliStrings.LIST_DURABLE_CQS, help = CliStrings.LIST_DURABLE_CQS__HELP)
  @CliMetaData()
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public ResultModel listDurableClientCQs(
      @CliOption(key = CliStrings.LIST_DURABLE_CQS__DURABLECLIENTID, mandatory = true,
          help = CliStrings.LIST_DURABLE_CQS__DURABLECLIENTID__HELP) final String durableClientId,

      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          help = CliStrings.LIST_DURABLE_CQS__MEMBER__HELP,
          optionContext = ConverterHint.MEMBERIDNAME) final String[] memberNameOrId,

      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          help = CliStrings.LIST_DURABLE_CQS__GROUP__HELP,
          optionContext = ConverterHint.MEMBERGROUP) final String[] group) {

    Set<DistributedMember> targetMembers = findMembers(group, memberNameOrId);

    if (targetMembers.isEmpty()) {
      return ResultModel.createInfo(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }

    final ResultCollector<?, ?> rc =
        executeFunction(new ListDurableCqNamesFunction(), durableClientId, targetMembers);
    @SuppressWarnings("unchecked")
    final List<List<CliFunctionResult>> results = (List<List<CliFunctionResult>>) rc.getResult();

    ResultModel result = new ResultModel();
    TabularResultModel table = result.addTable("list-durable-client-cqs");

    for (List<CliFunctionResult> perMemberList : results) {
      for (CliFunctionResult oneResult : perMemberList) {
        table.accumulate("Member", oneResult.getMemberIdOrName());
        table.accumulate("Status", oneResult.getStatus());
        table.accumulate("CQ Name", oneResult.getStatusMessage());

        if (!(oneResult.isSuccessful() || oneResult.isIgnorableFailure())) {
          result.setStatus(Result.Status.ERROR);
        }
      }
    }

    return result;
  }
}
