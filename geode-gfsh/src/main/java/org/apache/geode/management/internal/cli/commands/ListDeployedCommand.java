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
import java.util.Map;
import java.util.Set;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.functions.ListDeployedFunction;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
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
  public ResultModel listDeployed(@CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
      help = CliStrings.LIST_DEPLOYED__GROUP__HELP) String[] group) {

    Set<DistributedMember> targetMembers = findMembers(group, null);
    if (targetMembers.isEmpty()) {
      return ResultModel.createInfo(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }

    ResultModel result = new ResultModel();
    TabularResultModel tabularData = result.addTable("jars");

    List<CliFunctionResult> functionResults = executeAndGetFunctionResult(this.listDeployedFunction,
        null, targetMembers);

    for (CliFunctionResult cliResult : functionResults) {
      @SuppressWarnings("unchecked")
      Map<String, String> strings = (Map<String, String>) cliResult.getResultObject();
      if (strings == null) {
        continue;
      }
      for (Map.Entry<String, String> jar : strings.entrySet()) {
        tabularData.accumulate("Member", cliResult.getMemberIdOrName());
        tabularData.accumulate("JAR", jar.getKey());
        tabularData.accumulate("JAR Location", jar.getValue());
      }
    }

    if (tabularData.getRowSize() == 0) {
      return ResultModel.createInfo(CliStrings.LIST_DEPLOYED__NO_JARS_FOUND_MESSAGE);
    }

    return result;
  }
}
