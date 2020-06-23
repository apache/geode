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

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.functions.ResumeAsyncEventQueueDispatcherFunction;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;


public class ResumeAsyncEventQueueDispatcherCommand extends GfshCommand {

  @CliCommand(value = CliStrings.RESUME_ASYNCEVENTQUEUE,
      help = CliStrings.RESUME_ASYNCEVENTQUEUE__HELP)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)
  public ResultModel resumeAsyncEventQueueDispatcher(
      @CliOption(key = CliStrings.RESUME_ASYNCEVENTQUEUE__ID,
          mandatory = true, help = CliStrings.RESUME_ASYNCEVENTQUEUE__ID__HELP) String queueId,

      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.RESUME_ASYNCEVENTQUEUE__GROUP__HELP) String[] onGroup,
      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.RESUME_ASYNCEVENTQUEUE__MEMBER__HELP) String[] onMember) {

    if (queueId != null) {
      queueId = queueId.trim();
    }

    Set<DistributedMember> targetMembers = findMembers(onGroup, onMember);

    List<CliFunctionResult> results =
        executeAndGetFunctionResult(new ResumeAsyncEventQueueDispatcherFunction(), queueId,
            targetMembers);

    return constructResultModel(results);
  }

  ResultModel constructResultModel(List<CliFunctionResult> results) {
    return ResultModel.createMemberStatusResult(results);
  }
}
