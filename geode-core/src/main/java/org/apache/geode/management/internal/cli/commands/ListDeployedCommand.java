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

import org.apache.geode.SystemFailure;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.ListDeployedFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class ListDeployedCommand extends GfshCommand {
  private final ListDeployedFunction listDeployedFunction = new ListDeployedFunction();

  /**
   * List all currently deployed JARs for members of a group or for all members.
   *
   * @param group Group for which to list JARs or null for all members
   * @return List of deployed JAR files
   */
  @CliCommand(value = {CliStrings.LIST_DEPLOYED}, help = CliStrings.LIST_DEPLOYED__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_CONFIG})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public Result listDeployed(@CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
      help = CliStrings.LIST_DEPLOYED__GROUP__HELP) String[] group) {

    try {
      TabularResultData tabularData = ResultBuilder.createTabularResultData();
      boolean accumulatedData = false;

      Set<DistributedMember> targetMembers = findMembers(group, null);

      if (targetMembers.isEmpty()) {
        return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
      }

      ResultCollector<?, ?> rc =
          CliUtil.executeFunction(this.listDeployedFunction, null, targetMembers);
      List<CliFunctionResult> results = CliFunctionResult.cleanResults((List<?>) rc.getResult());

      for (CliFunctionResult result : results) {
        if (result.getThrowable() != null) {
          tabularData.accumulate("Member", result.getMemberIdOrName());
          tabularData.accumulate("JAR", "");
          tabularData.accumulate("JAR Location",
              "ERROR: " + result.getThrowable().getClass().getName() + ": "
                  + result.getThrowable().getMessage());
          accumulatedData = true;
          tabularData.setStatus(Result.Status.ERROR);
        } else {
          String[] strings = (String[]) result.getSerializables();
          for (int i = 0; i < strings.length; i += 2) {
            tabularData.accumulate("Member", result.getMemberIdOrName());
            tabularData.accumulate("JAR", strings[i]);
            tabularData.accumulate("JAR Location", strings[i + 1]);
            accumulatedData = true;
          }
        }
      }

      if (!accumulatedData) {
        return ResultBuilder.createInfoResult(CliStrings.LIST_DEPLOYED__NO_JARS_FOUND_MESSAGE);
      }
      return ResultBuilder.buildResult(tabularData);

    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable th) {
      SystemFailure.checkFailure();
      return ResultBuilder.createGemFireErrorResult("Exception while attempting to list deployed: "
          + th.getClass().getName() + ": " + th.getMessage());
    }
  }
}
