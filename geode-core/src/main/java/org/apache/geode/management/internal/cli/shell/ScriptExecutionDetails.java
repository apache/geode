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
package org.apache.geode.management.internal.cli.shell;

import java.util.ArrayList;
import java.util.List;

import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;

class ScriptExecutionDetails {
  private final String filePath;
  private final List<CommandAndStatus> commandAndStatusList;

  ScriptExecutionDetails(String filePath) {
    this.filePath = filePath;
    this.commandAndStatusList = new ArrayList<CommandAndStatus>();
  }

  void addCommandAndStatus(String command, String status) {
    this.commandAndStatusList.add(new CommandAndStatus(command, status));
  }

  Result getResult() {
    CompositeResultData compositeResultData = ResultBuilder.createCompositeResultData();
    compositeResultData.setHeader(
        "************************* Execution Summary ***********************\nScript file: "
            + filePath);

    for (int i = 0; i < this.commandAndStatusList.size(); i++) {
      int commandSrNo = i + 1;
      CompositeResultData.SectionResultData section = compositeResultData.addSection("" + (i + 1));
      CommandAndStatus commandAndStatus = commandAndStatusList.get(i);
      section.addData("Command-" + String.valueOf(commandSrNo), commandAndStatus.command);
      section.addData("Status", commandAndStatus.status);
      if (commandAndStatus.status.equals("FAILED")) {
        compositeResultData.setStatus(Result.Status.ERROR);
      }
      if (i != this.commandAndStatusList.size()) {
        section.setFooter(Gfsh.LINE_SEPARATOR);
      }
    }

    return ResultBuilder.buildResult(compositeResultData);
  }

  void logScriptExecutionInfo(LogWrapper logWrapper, Result result) {
    logWrapper.info(ResultBuilder.resultAsString(result));
  }

  static class CommandAndStatus {
    private final String command;
    private final String status;

    public CommandAndStatus(String command, String status) {
      this.command = command;
      this.status = status;
    }

    @Override
    public String toString() {
      return command + "     " + status;
    }
  }
}
