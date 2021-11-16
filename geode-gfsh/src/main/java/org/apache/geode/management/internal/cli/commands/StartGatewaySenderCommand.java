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
import java.util.List;
import java.util.Set;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.functions.StartGatewaySenderFunction;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class StartGatewaySenderCommand extends GfshCommand {
  private final StartGatewaySenderFunction startGatewaySenderFunction =
      new StartGatewaySenderFunction();

  @CliCommand(value = CliStrings.START_GATEWAYSENDER, help = CliStrings.START_GATEWAYSENDER__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_WAN)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.GATEWAY)
  public ResultModel startGatewaySender(@CliOption(key = CliStrings.START_GATEWAYSENDER__ID,
      mandatory = true, optionContext = ConverterHint.GATEWAY_SENDER_ID,
      help = CliStrings.START_GATEWAYSENDER__ID__HELP) String senderId,

      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.START_GATEWAYSENDER__GROUP__HELP) String[] onGroup,

      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.START_GATEWAYSENDER__MEMBER__HELP) String[] onMember,

      @CliOption(key = CliStrings.START_GATEWAYSENDER__CLEAN_QUEUE,
          unspecifiedDefaultValue = "false",
          specifiedDefaultValue = "true",
          help = CliStrings.START_GATEWAYSENDER__CLEAN_QUEUE__HELP) final Boolean cleanQueues) {

    Set<DistributedMember> dsMembers = findMembers(onGroup, onMember);

    if (dsMembers.isEmpty()) {
      return ResultModel.createError(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }

    List<Object> args = new ArrayList<>(2);
    args.add(senderId);
    args.add(cleanQueues);

    List<CliFunctionResult> gatewaySenderStartResults =
        executeAndGetFunctionResult(startGatewaySenderFunction, args,
            dsMembers);

    ResultModel resultModel = new ResultModel();
    TabularResultModel resultData = resultModel.addTable(CliStrings.START_GATEWAYSENDER);

    for (CliFunctionResult result : gatewaySenderStartResults) {
      resultData.addMemberStatusResultRow(result.getMemberIdOrName(), result.getStatus(),
          result.getStatusMessage());
    }

    return resultModel;
  }
}
