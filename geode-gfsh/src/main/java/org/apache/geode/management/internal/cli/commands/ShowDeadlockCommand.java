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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
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
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.CliAroundInterceptor;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class ShowDeadlockCommand extends GfshCommand {
  @CliCommand(value = CliStrings.SHOW_DEADLOCK, help = CliStrings.SHOW_DEADLOCK__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DEBUG_UTIL},
      interceptor = "org.apache.geode.management.internal.cli.commands.ShowDeadlockCommand$Interceptor")
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public ResultModel showDeadlock(@CliOption(key = CliStrings.SHOW_DEADLOCK__DEPENDENCIES__FILE,
      help = CliStrings.SHOW_DEADLOCK__DEPENDENCIES__FILE__HELP,
      mandatory = true) String filename) {

    ResultModel result = new ResultModel();
    try {
      if (!filename.endsWith(".txt")) {
        return ResultModel.createError(
            CliStrings.format(CliStrings.INVALID_FILE_EXTENSION, ".txt"));
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

      InfoResultModel infoResult = result.addInfo();

      if (deadlock != null) {
        if (deepest != null) {
          infoResult.addLine(CliStrings.SHOW_DEADLOCK__DEEPEST_FOUND);
        } else {
          infoResult.addLine(CliStrings.SHOW_DEADLOCK__DEADLOCK__DETECTED);
        }
        infoResult.addLine(DeadlockDetector.prettyFormat(deadlock));
      } else {
        infoResult.addLine(CliStrings.SHOW_DEADLOCK__NO__DEADLOCK);
      }
      result.addFile(filename, DeadlockDetector.prettyFormat(dependencies));
    } catch (Exception e) {
      result = ResultModel.createError(CliStrings.SHOW_DEADLOCK__ERROR + " : " + e.getMessage());
    }

    return result;
  }

  public static class Interceptor implements CliAroundInterceptor {
    @Override
    public ResultModel postExecution(GfshParseResult parseResult, ResultModel resultModel,
        Path tempFile) throws IOException {
      String saveAs =
          parseResult.getParamValueAsString(CliStrings.SHOW_DEADLOCK__DEPENDENCIES__FILE);

      File file = new File(saveAs).getAbsoluteFile();
      resultModel.saveFileTo(file.getParentFile());
      return resultModel;
    }
  }

}
