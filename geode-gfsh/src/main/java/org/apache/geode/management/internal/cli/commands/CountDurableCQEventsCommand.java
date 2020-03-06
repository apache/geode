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
import org.apache.geode.management.internal.cli.functions.GetSubscriptionQueueSizeFunction;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class CountDurableCQEventsCommand extends GfshCommand {

  @CliCommand(value = CliStrings.COUNT_DURABLE_CQ_EVENTS,
      help = CliStrings.COUNT_DURABLE_CQ_EVENTS__HELP)
  @CliMetaData()
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public ResultModel countDurableCqEvents(
      @CliOption(key = CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE__CLIENT__ID, mandatory = true,
          help = CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE__CLIENT__ID__HELP) final String durableClientId,
      @CliOption(key = CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE__CQ__NAME,
          help = CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE__CQ__NAME__HELP) final String cqName,
      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          help = CliStrings.COUNT_DURABLE_CQ_EVENTS__MEMBER__HELP,
          optionContext = ConverterHint.MEMBERIDNAME) final String[] memberNameOrId,
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          help = CliStrings.COUNT_DURABLE_CQ_EVENTS__GROUP__HELP,
          optionContext = ConverterHint.MEMBERGROUP) final String[] group) {

    Set<DistributedMember> targetMembers = findMembers(group, memberNameOrId);

    if (targetMembers.isEmpty()) {
      return ResultModel.createError(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }

    String[] params = new String[2];
    params[0] = durableClientId;
    params[1] = cqName;
    final ResultCollector<?, ?> rc =
        executeFunction(new GetSubscriptionQueueSizeFunction(), params, targetMembers);
    @SuppressWarnings("unchecked")
    final List<CliFunctionResult> funcResults = (List<CliFunctionResult>) rc.getResult();

    ResultModel result = new ResultModel();
    TabularResultModel table = result.addTable("subscription-queue-size");
    for (CliFunctionResult oneResult : funcResults) {
      table.accumulate("Member", oneResult.getMemberIdOrName());
      table.accumulate("Status", oneResult.getStatus());
      table.accumulate("Queue Size", oneResult.getStatusMessage());

      if (!oneResult.isSuccessful()) {
        result.setStatus(Result.Status.ERROR);
      }
    }
    return result;
  }

}
