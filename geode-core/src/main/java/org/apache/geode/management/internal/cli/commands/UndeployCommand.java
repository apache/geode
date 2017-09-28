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
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.UndeployFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class UndeployCommand implements GfshCommand {
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
  public Result undeploy(
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          help = CliStrings.UNDEPLOY__GROUP__HELP,
          optionContext = ConverterHint.MEMBERGROUP) String[] groups,
      @CliOption(key = {CliStrings.JAR, CliStrings.JARS},
          help = CliStrings.UNDEPLOY__JAR__HELP) String[] jars) {

    try {
      TabularResultData tabularData = ResultBuilder.createTabularResultData();
      boolean accumulatedData = false;

      Set<DistributedMember> targetMembers = CliUtil.findMembers(groups, null);

      if (targetMembers.isEmpty()) {
        return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
      }

      ResultCollector<?, ?> rc =
          CliUtil.executeFunction(this.undeployFunction, new Object[] {jars}, targetMembers);
      List<CliFunctionResult> results = CliFunctionResult.cleanResults((List<?>) rc.getResult());

      for (CliFunctionResult result : results) {

        if (result.getThrowable() != null) {
          tabularData.accumulate("Member", result.getMemberIdOrName());
          tabularData.accumulate("Un-Deployed JAR", "");
          tabularData.accumulate("Un-Deployed JAR Location",
              "ERROR: " + result.getThrowable().getClass().getName() + ": "
                  + result.getThrowable().getMessage());
          accumulatedData = true;
          tabularData.setStatus(Result.Status.ERROR);
        } else {
          String[] strings = (String[]) result.getSerializables();
          for (int i = 0; i < strings.length; i += 2) {
            tabularData.accumulate("Member", result.getMemberIdOrName());
            tabularData.accumulate("Un-Deployed JAR", strings[i]);
            tabularData.accumulate("Un-Deployed From JAR Location", strings[i + 1]);
            accumulatedData = true;
          }
        }
      }

      if (!accumulatedData) {
        return ResultBuilder.createInfoResult(CliStrings.UNDEPLOY__NO_JARS_FOUND_MESSAGE);
      }

      Result result = ResultBuilder.buildResult(tabularData);
      if (tabularData.getStatus().equals(Result.Status.OK)) {
        persistClusterConfiguration(result,
            () -> getSharedConfiguration().removeJars(jars, groups));
      }
      return result;
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable th) {
      SystemFailure.checkFailure();
      return ResultBuilder.createGemFireErrorResult("Exception while attempting to un-deploy: "
          + th.getClass().getName() + ": " + th.getMessage());
    }
  }
}
