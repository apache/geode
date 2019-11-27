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

import org.springframework.shell.core.annotation.CliCommand;

import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.cli.shell.OperationInvoker;
import org.apache.geode.management.internal.i18n.CliStrings;

public class DisconnectCommand extends OfflineGfshCommand {
  @CliCommand(value = {CliStrings.DISCONNECT}, help = CliStrings.DISCONNECT__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GFSH, CliStrings.TOPIC_GEODE_JMX,
      CliStrings.TOPIC_GEODE_MANAGER})
  public ResultModel disconnect() {


    if (getGfsh() != null && !getGfsh().isConnectedAndReady()) {
      return ResultModel.createInfo("Not connected.");
    }

    ResultModel result = new ResultModel();
    InfoResultModel infoResultData = result.addInfo();

    Gfsh gfshInstance = getGfsh();
    if (gfshInstance.isConnectedAndReady()) {
      OperationInvoker operationInvoker = gfshInstance.getOperationInvoker();
      Gfsh.println("Disconnecting from: " + operationInvoker);
      operationInvoker.stop();
      infoResultData.addLine(CliStrings.format(CliStrings.DISCONNECT__MSG__DISCONNECTED,
          operationInvoker.toString()));
      LogWrapper.getInstance().info(CliStrings
          .format(CliStrings.DISCONNECT__MSG__DISCONNECTED, operationInvoker.toString()));
      if (!gfshInstance.isHeadlessMode()) {
        gfshInstance.setPromptPath(gfshInstance.getEnvAppContextPath());
      }
    } else {
      infoResultData.addLine(CliStrings.DISCONNECT__MSG__NOTCONNECTED);
    }

    return result;
  }
}
