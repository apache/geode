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
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.model.DataResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;

class ScriptExecutionDetails {
  private final String filePath;
  private final List<CommandAndStatus> commandAndStatusList;

  ScriptExecutionDetails(String filePath) {
    this.filePath = filePath;
    commandAndStatusList = new ArrayList<>();
  }

  void addCommandAndStatus(String command, String status) {
    commandAndStatusList.add(new CommandAndStatus(command, status));
  }

  ResultModel getResult() {
    ResultModel result = new ResultModel();
    result.setHeader(
        "************************* Execution Summary ***********************\nScript file: "
            + filePath);

    for (int i = 0; i < commandAndStatusList.size(); i++) {
      int commandSrNo = i + 1;
      DataResultModel commandDetail = result.addData("command" + commandSrNo);
      CommandAndStatus commandAndStatus = commandAndStatusList.get(i);
      commandDetail.addData("Command-" + commandSrNo, commandAndStatus.command);
      commandDetail.addData("Status", commandAndStatus.status);
      if (commandAndStatus.status.equals("FAILED")) {
        result.setStatus(Result.Status.ERROR);
      }
      if (i != commandAndStatusList.size()) {
        result.setFooter(Gfsh.LINE_SEPARATOR);
      }
    }

    return result;
  }

  void logScriptExecutionInfo(LogWrapper logWrapper, ResultModel result) {
    logWrapper.info(new CommandResult(result).asString());
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
