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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.functions.UnregisterFunction;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class DestroyFunctionCommand extends GfshCommand {
  @CliCommand(value = CliStrings.DESTROY_FUNCTION, help = CliStrings.DESTROY_FUNCTION__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_FUNCTION},
      interceptor = "org.apache.geode.management.internal.cli.commands.DestroyFunctionCommand$Interceptor")
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.DEPLOY)
  // TODO: Add optioncontext for functionId
  public ResultModel destroyFunction(
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
        return ResultModel.createError(CliStrings.DESTROY_FUNCTION__MSG__PROVIDE_OPTION);
      } else if (groups != null && groups.length > 0) {
        // execute on group members
        for (String grp : groups) {
          dsMembers.addAll(cache.getDistributedSystem().getGroupMembers(grp));
        }
        return executeFunction(cache, dsMembers, functionId);
      } else if (memberId != null) {
        // execute on member
        dsMembers.add(getMember(memberId));
        return executeFunction(cache, dsMembers, functionId);
      } else {
        // no option provided.
        return executeFunction(cache, cache.getMembers(), functionId);
      }
    } catch (Exception e) {
      return ResultModel.createError(e.getMessage());
    }
  }

  private ResultModel executeFunction(Cache cache, Set<DistributedMember> DsMembers,
      String functionId) {
    // unregister on a set of of members
    UnregisterFunction unregisterFunction = new UnregisterFunction();
    FunctionService.registerFunction(unregisterFunction);

    if (DsMembers.isEmpty()) {
      return ResultModel.createInfo("No members for execution");
    }

    @SuppressWarnings("unchecked")
    Execution<Object[], String, List<String>> execution =
        FunctionService.onMembers(DsMembers).setArguments(new Object[] {functionId});

    if (execution == null) {
      cache.getLogger().error("executeUnregister execution is null");
      return ResultModel.createError(CliStrings.DESTROY_FUNCTION__MSG__CANNOT_EXECUTE);
    }

    final List<String> resultList;
    try {
      resultList = execution.execute(unregisterFunction).getResult();
    } catch (FunctionException ex) {
      return ResultModel.createError(ex.getMessage());
    }

    String resultStr = resultList.get(0);
    if (resultStr.equals("Succeeded in unregistering")) {
      StringBuilder members = new StringBuilder();
      for (DistributedMember member : DsMembers) {
        members.append(member.getId());
        members.append(",");
      }
      return ResultModel.createInfo("Destroyed " + functionId + " Successfully on "
          + members.toString().substring(0, members.toString().length() - 1));
    } else {
      return ResultModel.createInfo("Failed in unregistering");
    }
  }

  /**
   * Interceptor used by gfsh to intercept execution of destroy.
   */
  public static class Interceptor extends AbstractCliAroundInterceptor {
    @Override
    public ResultModel preExecution(GfshParseResult parseResult) {
      String onGroup = parseResult.getParamValueAsString(CliStrings.GROUP);
      String onMember = parseResult.getParamValueAsString(CliStrings.MEMBER);

      String functionId = parseResult.getParamValueAsString(CliStrings.DESTROY_FUNCTION__ID);

      if ((onGroup == null && onMember == null)) {
        Response response = readYesNo(
            "Do you really want to destroy " + functionId + " on entire DS?", Response.NO);
        if (response == Response.NO) {
          return ResultModel.createError("Aborted destroy of " + functionId);
        } else {
          return ResultModel.createInfo("Destroying " + functionId);
        }
      } else {
        return ResultModel.createInfo("Destroying " + functionId);
      }
    }
  }
}
