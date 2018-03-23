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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.functions.UnregisterFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ErrorResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class DestroyFunctionCommand extends GfshCommand {
  @CliCommand(value = CliStrings.DESTROY_FUNCTION, help = CliStrings.DESTROY_FUNCTION__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_FUNCTION},
      interceptor = "org.apache.geode.management.internal.cli.commands.DestroyFunctionCommand$Interceptor")
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.DEPLOY)
  // TODO: Add optioncontext for functionId
  public Result destroyFunction(
      @CliOption(key = CliStrings.DESTROY_FUNCTION__ID, mandatory = true,
          help = CliStrings.DESTROY_FUNCTION__HELP) String functionId,
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.DESTROY_FUNCTION__ONGROUPS__HELP) String[] groups,
      @CliOption(key = CliStrings.MEMBER, optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.DESTROY_FUNCTION__ONMEMBER__HELP) String memberId) {
    try {
      Cache cache = getCache();
      Set<DistributedMember> dsMembers = new HashSet<>();
      if (groups != null && memberId != null) {
        return ResultBuilder
            .createUserErrorResult(CliStrings.DESTROY_FUNCTION__MSG__PROVIDE_OPTION);
      } else if (groups != null && groups.length > 0) {
        // execute on group members
        for (String grp : groups) {
          dsMembers.addAll(cache.getDistributedSystem().getGroupMembers(grp));
        }
        @SuppressWarnings("unchecked")
        Result results = executeFunction(cache, dsMembers, functionId);
        return results;
      } else if (memberId != null) {
        // execute on member
        dsMembers.add(getMember(memberId));
        @SuppressWarnings("unchecked")
        Result results = executeFunction(cache, dsMembers, functionId);
        return results;
      } else {
        // no option provided.
        @SuppressWarnings("unchecked")
        Result results = executeFunction(cache, cache.getMembers(), functionId);
        return results;
      }
    } catch (Exception e) {
      ErrorResultData errorResultData = ResultBuilder.createErrorResultData()
          .setErrorCode(ResultBuilder.ERRORCODE_DEFAULT).addLine(e.getMessage());
      return ResultBuilder.buildResult(errorResultData);
    }
  }

  private Result executeFunction(Cache cache, Set<DistributedMember> DsMembers, String functionId) {
    // unregister on a set of of members
    Function unregisterFunction = new UnregisterFunction();
    FunctionService.registerFunction(unregisterFunction);
    List resultList;

    if (DsMembers.isEmpty()) {
      return ResultBuilder.createInfoResult("No members for execution");
    }
    Object[] obj = new Object[1];
    obj[0] = functionId;

    Execution execution = FunctionService.onMembers(DsMembers).setArguments(obj);

    if (execution == null) {
      cache.getLogger().error("executeUnregister execution is null");
      ErrorResultData errorResultData =
          ResultBuilder.createErrorResultData().setErrorCode(ResultBuilder.ERRORCODE_DEFAULT)
              .addLine(CliStrings.DESTROY_FUNCTION__MSG__CANNOT_EXECUTE);
      return (ResultBuilder.buildResult(errorResultData));
    }
    try {
      resultList = (ArrayList) execution.execute(unregisterFunction).getResult();
    } catch (FunctionException ex) {
      ErrorResultData errorResultData = ResultBuilder.createErrorResultData()
          .setErrorCode(ResultBuilder.ERRORCODE_DEFAULT).addLine(ex.getMessage());
      return (ResultBuilder.buildResult(errorResultData));
    }
    String resultStr = ((String) resultList.get(0));
    if (resultStr.equals("Succeeded in unregistering")) {
      StringBuilder members = new StringBuilder();
      for (DistributedMember member : DsMembers) {
        members.append(member.getId());
        members.append(",");
      }
      return ResultBuilder.createInfoResult("Destroyed " + functionId + " Successfully on "
          + members.toString().substring(0, members.toString().length() - 1));
    } else {
      return ResultBuilder.createInfoResult("Failed in unregistering");
    }
  }

  /**
   * Interceptor used by gfsh to intercept execution of destroy.
   */
  public static class Interceptor extends AbstractCliAroundInterceptor {
    @Override
    public Result preExecution(GfshParseResult parseResult) {
      String onGroup = parseResult.getParamValueAsString(CliStrings.GROUP);
      String onMember = parseResult.getParamValueAsString(CliStrings.MEMBER);

      String functionId = parseResult.getParamValueAsString(CliStrings.DESTROY_FUNCTION__ID);

      if ((onGroup == null && onMember == null)) {
        Response response = readYesNo(
            "Do you really want to destroy " + functionId + " on entire DS?", Response.NO);
        if (response == Response.NO) {
          return ResultBuilder
              .createShellClientAbortOperationResult("Aborted destroy of " + functionId);
        } else {
          return ResultBuilder.createInfoResult("Destroying " + functionId);
        }
      } else {
        return ResultBuilder.createInfoResult("Destroying " + functionId);
      }
    }
  }
}
