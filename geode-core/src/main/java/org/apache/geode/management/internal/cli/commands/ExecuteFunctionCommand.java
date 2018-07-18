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

import static org.apache.geode.internal.security.IntegratedSecurityService.CREDENTIALS_SESSION_ATTRIBUTE;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.shiro.subject.Subject;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.internal.cli.CliAroundInterceptor;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.UserFunctionExecution;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;

public class ExecuteFunctionCommand extends InternalGfshCommand {
  @CliCommand(value = CliStrings.EXECUTE_FUNCTION, help = CliStrings.EXECUTE_FUNCTION__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_FUNCTION},
      interceptor = "org.apache.geode.management.internal.cli.commands.ExecuteFunctionCommand$ExecuteFunctionCommandInterceptor")
  public ResultModel executeFunction(
      @CliOption(key = CliStrings.EXECUTE_FUNCTION__ID, mandatory = true,
          help = CliStrings.EXECUTE_FUNCTION__ID__HELP) String functionId,
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.EXECUTE_FUNCTION__ONGROUPS__HELP) String[] onGroups,
      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.EXECUTE_FUNCTION__ONMEMBER__HELP) String[] onMembers,
      @CliOption(key = CliStrings.EXECUTE_FUNCTION__ONREGION,
          optionContext = ConverterHint.REGION_PATH,
          help = CliStrings.EXECUTE_FUNCTION__ONREGION__HELP) String onRegion,
      @CliOption(key = CliStrings.EXECUTE_FUNCTION__ARGUMENTS,
          help = CliStrings.EXECUTE_FUNCTION__ARGUMENTS__HELP) String[] arguments,
      @CliOption(key = CliStrings.EXECUTE_FUNCTION__RESULTCOLLECTOR,
          help = CliStrings.EXECUTE_FUNCTION__RESULTCOLLECTOR__HELP) String resultCollector,
      @CliOption(key = CliStrings.EXECUTE_FUNCTION__FILTER,
          help = CliStrings.EXECUTE_FUNCTION__FILTER__HELP) String filterString) {

    ResultModel resultModel = new ResultModel();
    TabularResultModel resultTable = resultModel.addTable("Table1");
    String headerText = "Execution summary";
    resultTable.setHeader(headerText);

    // when here, the options are already parsed and validated
    // find out the members this function need to be executed on
    Set<DistributedMember> dsMembers;
    if (onRegion == null) {
      // find the members based on the groups or members
      dsMembers = findMembers(onGroups, onMembers);
    } else {
      dsMembers = findAnyMembersForRegion(onRegion);
    }

    if (dsMembers.size() == 0) {
      return new ResultModel().createError("No members found.");
    }

    // Build up our argument list
    Object[] args = new Object[6];
    args[0] = functionId;
    if (filterString != null) {
      args[1] = filterString;
    }
    if (resultCollector != null) {
      args[2] = resultCollector;
    }
    if (arguments != null && arguments.length > 0) {
      args[3] = "";
      for (String str : arguments) {
        // send via CSV separated value format
        if (str != null) {
          args[3] = args[3] + str + ",";
        }
      }
    }
    args[4] = onRegion;

    Subject currentUser = getSubject();
    if (currentUser != null) {
      args[5] = currentUser.getSession().getAttribute(CREDENTIALS_SESSION_ATTRIBUTE);
    } else {
      args[5] = null;
    }

    // Execute function and aggregate results
    List<CliFunctionResult> results =
        executeAndGetFunctionResult(new UserFunctionExecution(), args, dsMembers);

    return ResultModel.createMemberStatusResult(results, false, false);
  }

  public static class ExecuteFunctionCommandInterceptor implements CliAroundInterceptor {
    @Override
    public ResultModel preExecution(GfshParseResult parseResult) {
      String onRegion = parseResult.getParamValueAsString(CliStrings.EXECUTE_FUNCTION__ONREGION);
      String onMember = parseResult.getParamValueAsString(CliStrings.MEMBER);
      String onGroup = parseResult.getParamValueAsString(CliStrings.GROUP);
      String filter = parseResult.getParamValueAsString(CliStrings.EXECUTE_FUNCTION__FILTER);

      boolean moreThanOne =
          Stream.of(onRegion, onMember, onGroup).filter(Objects::nonNull).count() > 1;

      ResultModel result = new ResultModel();
      if (moreThanOne) {
        return result.createError(CliStrings.EXECUTE_FUNCTION__MSG__OPTIONS);
      }

      if (onRegion == null && filter != null) {
        return result.createError(
            CliStrings.EXECUTE_FUNCTION__MSG__MEMBER_SHOULD_NOT_HAVE_FILTER_FOR_EXECUTION);
      }

      return result;
    }
  }
}
