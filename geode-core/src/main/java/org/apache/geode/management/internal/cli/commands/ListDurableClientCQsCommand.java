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
import java.util.TreeMap;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.domain.DurableCqNamesResult;
import org.apache.geode.management.internal.cli.functions.ListDurableCqNamesFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class ListDurableClientCQsCommand implements GfshCommand {
  DurableClientCommandsResultBuilder builder = new DurableClientCommandsResultBuilder();

  @CliCommand(value = CliStrings.LIST_DURABLE_CQS, help = CliStrings.LIST_DURABLE_CQS__HELP)
  @CliMetaData()
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public Result listDurableClientCQs(
      @CliOption(key = CliStrings.LIST_DURABLE_CQS__DURABLECLIENTID, mandatory = true,
          help = CliStrings.LIST_DURABLE_CQS__DURABLECLIENTID__HELP) final String durableClientId,

      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          help = CliStrings.LIST_DURABLE_CQS__MEMBER__HELP,
          optionContext = ConverterHint.MEMBERIDNAME) final String[] memberNameOrId,

      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          help = CliStrings.LIST_DURABLE_CQS__GROUP__HELP,
          optionContext = ConverterHint.MEMBERGROUP) final String[] group) {
    Result result;
    try {

      boolean noResults = true;
      Set<DistributedMember> targetMembers = findMembers(group, memberNameOrId, getCache());

      if (targetMembers.isEmpty()) {
        return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
      }

      final ResultCollector<?, ?> rc =
          executeFunction(new ListDurableCqNamesFunction(), durableClientId, targetMembers);
      final List<DurableCqNamesResult> results = (List<DurableCqNamesResult>) rc.getResult();
      Map<String, List<String>> memberCqNamesMap = new TreeMap<>();
      Map<String, List<String>> errorMessageNodes = new HashMap<>();
      Map<String, List<String>> exceptionMessageNodes = new HashMap<>();

      for (DurableCqNamesResult memberResult : results) {
        if (memberResult != null) {
          if (memberResult.isSuccessful()) {
            memberCqNamesMap.put(memberResult.getMemberNameOrId(), memberResult.getCqNamesList());
          } else {
            if (memberResult.isOpPossible()) {
              builder.groupByMessage(memberResult.getExceptionMessage(),
                  memberResult.getMemberNameOrId(), exceptionMessageNodes);
            } else {
              builder.groupByMessage(memberResult.getErrorMessage(),
                  memberResult.getMemberNameOrId(), errorMessageNodes);
            }
          }
        }
      }

      if (!memberCqNamesMap.isEmpty()) {
        TabularResultData table = ResultBuilder.createTabularResultData();
        Set<String> members = memberCqNamesMap.keySet();

        for (String member : members) {
          boolean isFirst = true;
          List<String> cqNames = memberCqNamesMap.get(member);
          for (String cqName : cqNames) {
            if (isFirst) {
              isFirst = false;
              table.accumulate(CliStrings.MEMBER, member);
            } else {
              table.accumulate(CliStrings.MEMBER, "");
            }
            table.accumulate(CliStrings.LIST_DURABLE_CQS__NAME, cqName);
          }
        }
        result = ResultBuilder.buildResult(table);
      } else {
        String errorHeader =
            CliStrings.format(CliStrings.LIST_DURABLE_CQS__FAILURE__HEADER, durableClientId);
        result = ResultBuilder.buildResult(
            builder.buildFailureData(null, exceptionMessageNodes, errorMessageNodes, errorHeader));
      }
    } catch (Exception e) {
      result = ResultBuilder.createGemFireErrorResult(e.getMessage());
    }
    return result;
  }
}
