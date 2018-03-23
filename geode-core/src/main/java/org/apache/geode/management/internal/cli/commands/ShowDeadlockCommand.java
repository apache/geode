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

import java.text.MessageFormat;
import java.util.Collection;
import java.util.Set;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.deadlock.DeadlockDetector;
import org.apache.geode.distributed.internal.deadlock.Dependency;
import org.apache.geode.distributed.internal.deadlock.DependencyGraph;
import org.apache.geode.distributed.internal.deadlock.GemFireDeadlockDetector;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.InfoResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class ShowDeadlockCommand extends InternalGfshCommand {
  @CliCommand(value = CliStrings.SHOW_DEADLOCK, help = CliStrings.SHOW_DEADLOCK__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DEBUG_UTIL})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public Result showDeadlock(@CliOption(key = CliStrings.SHOW_DEADLOCK__DEPENDENCIES__FILE,
      help = CliStrings.SHOW_DEADLOCK__DEPENDENCIES__FILE__HELP,
      mandatory = true) String filename) {

    Result result;
    try {
      if (!filename.endsWith(".txt")) {
        return ResultBuilder
            .createUserErrorResult(CliStrings.format(CliStrings.INVALID_FILE_EXTENSION, ".txt"));
      }
      Set<DistributedMember> allMembers = getAllMembers();
      GemFireDeadlockDetector gfeDeadLockDetector = new GemFireDeadlockDetector(allMembers);
      DependencyGraph dependencyGraph = gfeDeadLockDetector.find();
      Collection<Dependency> deadlock = dependencyGraph.findCycle();
      DependencyGraph deepest = null;
      if (deadlock == null) {
        deepest = dependencyGraph.findLongestCallChain();
        if (deepest != null) {
          deadlock = deepest.getEdges();
        }
      }
      Set<Dependency> dependencies = (Set<Dependency>) dependencyGraph.getEdges();

      InfoResultData resultData = ResultBuilder.createInfoResultData();

      if (deadlock != null) {
        if (deepest != null) {
          resultData.addLine(CliStrings.SHOW_DEADLOCK__DEEPEST_FOUND);
        } else {
          resultData.addLine(CliStrings.SHOW_DEADLOCK__DEADLOCK__DETECTED);
        }
        resultData.addLine(DeadlockDetector.prettyFormat(deadlock));
      } else {
        resultData.addLine(CliStrings.SHOW_DEADLOCK__NO__DEADLOCK);
      }
      resultData.addAsFile(filename, DeadlockDetector.prettyFormat(dependencies),
          MessageFormat.format(CliStrings.SHOW_DEADLOCK__DEPENDENCIES__REVIEW, filename), false);
      result = ResultBuilder.buildResult(resultData);

    } catch (Exception e) {
      result = ResultBuilder
          .createGemFireErrorResult(CliStrings.SHOW_DEADLOCK__ERROR + " : " + e.getMessage());
    }
    return result;
  }
}
