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
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.functions.UndeployFunction;
import org.apache.geode.management.internal.cli.remote.CommandExecutor;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class UndeployCommand extends GfshCommand {
  private final UndeployFunction undeployFunction = new UndeployFunction();

  /**
   * Undeploy one or more JAR files from members of a group or all members.
   *
   * @param groups Group(s) to undeploy the JAR from or null for all members
   * @param jars JAR(s) to undeploy (separated by comma)
   * @return The result of the attempt to undeploy
   */
  @CliCommand(value = {CliStrings.UNDEPLOY}, help = CliStrings.UNDEPLOY__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_CONFIG})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.DEPLOY)
  public ResultModel undeploy(
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          help = CliStrings.UNDEPLOY__GROUP__HELP,
          optionContext = ConverterHint.MEMBERGROUP) String[] groups,
      @CliOption(key = {CliStrings.JAR, CliStrings.JARS},
          help = CliStrings.UNDEPLOY__JAR__HELP) String[] jars) {

    Set<DistributedMember> targetMembers = findMembers(groups, null);

    if (targetMembers.isEmpty()) {
      return ResultModel.createError(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }

    List<CliFunctionResult> results =
        executeAndGetFunctionResult(this.undeployFunction, new Object[] {jars}, targetMembers);

    ResultModel result = new ResultModel();
    TabularResultModel tabularData = result.addTable("jars");
    for (CliFunctionResult cliResult : results) {
      if (!cliResult.isSuccessful()) {
        result.setStatus(Result.Status.ERROR);
      }

      @SuppressWarnings("unchecked")
      Map<String, String> undeployedJars = (Map<String, String>) cliResult.getResultObject();
      if (undeployedJars == null) {
        continue;
      }

      for (String key : undeployedJars.keySet()) {
        tabularData.accumulate("Member", cliResult.getMemberIdOrName());
        tabularData.accumulate("Un-Deployed JAR", key);
        tabularData.accumulate("Un-Deployed From JAR Location", undeployedJars.get(key));
      }
    }

    if (tabularData.getRowSize() == 0) {
      return ResultModel.createInfo(CliStrings.UNDEPLOY__NO_JARS_FOUND_MESSAGE);
    }

    if (result.getStatus() != Result.Status.OK) {
      return result;
    }
    if (getConfigurationPersistenceService() == null) {
      result.addInfo().addLine(CommandExecutor.SERVICE_NOT_RUNNING_CHANGE_NOT_PERSISTED);
    } else {
      ((InternalConfigurationPersistenceService) getConfigurationPersistenceService())
          .removeJars(jars, groups);
    }

    return result;
  }
}
