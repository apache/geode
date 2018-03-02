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

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.SystemFailure;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.ListFunctionFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class ListFunctionCommand implements GfshCommand {
  private final ListFunctionFunction listFunctionFunction = new ListFunctionFunction();

  @CliCommand(value = CliStrings.LIST_FUNCTION, help = CliStrings.LIST_FUNCTION__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_FUNCTION})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public Result listFunction(
      @CliOption(key = CliStrings.LIST_FUNCTION__MATCHES,
          help = CliStrings.LIST_FUNCTION__MATCHES__HELP) String matches,
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.LIST_FUNCTION__GROUP__HELP) String[] groups,
      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.LIST_FUNCTION__MEMBER__HELP) String[] members) {
    TabularResultData tabularData = ResultBuilder.createTabularResultData();
    boolean accumulatedData = false;

    Set<DistributedMember> targetMembers = findMembers(groups, members, getCache());

    if (targetMembers.isEmpty()) {
      return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }

    try {
      ResultCollector<?, ?> rc =
          executeFunction(this.listFunctionFunction, new Object[] {matches}, targetMembers);
      List<CliFunctionResult> results = CliFunctionResult.cleanResults((List<?>) rc.getResult());

      for (CliFunctionResult result : results) {
        if (result.getThrowable() != null) {
          tabularData.accumulate("Member", result.getMemberIdOrName());
          tabularData.accumulate("Function", "<ERROR: " + result.getThrowable().getMessage() + ">");
          accumulatedData = true;
          tabularData.setStatus(Result.Status.ERROR);
        } else if (result.isSuccessful()) {
          String[] strings = (String[]) result.getSerializables();
          Arrays.sort(strings);
          for (String string : strings) {
            tabularData.accumulate("Member", result.getMemberIdOrName());
            tabularData.accumulate("Function", string);
            accumulatedData = true;
          }
        }
      }

      if (!accumulatedData) {
        return ResultBuilder
            .createInfoResult(CliStrings.LIST_FUNCTION__NO_FUNCTIONS_FOUND_ERROR_MESSAGE);
      }
      return ResultBuilder.buildResult(tabularData);
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable th) {
      SystemFailure.checkFailure();
      return ResultBuilder.createGemFireErrorResult(
          "Exception while attempting to list functions: " + th.getMessage());
    }
  }
}
