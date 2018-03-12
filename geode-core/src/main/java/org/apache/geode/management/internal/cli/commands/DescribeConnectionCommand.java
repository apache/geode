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
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ErrorResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.cli.shell.OperationInvoker;

public class DescribeConnectionCommand extends GfshCommand {
  @CliCommand(value = {CliStrings.DESCRIBE_CONNECTION}, help = CliStrings.DESCRIBE_CONNECTION__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GFSH, CliStrings.TOPIC_GEODE_JMX})
  public Result describeConnection() {
    Result result;
    try {
      TabularResultData tabularResultData = ResultBuilder.createTabularResultData();
      Gfsh gfshInstance = getGfsh();
      if (gfshInstance.isConnectedAndReady()) {
        OperationInvoker operationInvoker = gfshInstance.getOperationInvoker();
        // tabularResultData.accumulate("Monitored GemFire DS", operationInvoker.toString());
        tabularResultData.accumulate("Connection Endpoints", operationInvoker.toString());
      } else {
        tabularResultData.accumulate("Connection Endpoints", "Not connected");
      }
      result = ResultBuilder.buildResult(tabularResultData);
    } catch (Exception e) {
      ErrorResultData errorResultData = ResultBuilder.createErrorResultData()
          .setErrorCode(ResultBuilder.ERRORCODE_DEFAULT).addLine(e.getMessage());
      result = ResultBuilder.buildResult(errorResultData);
    }
    return result;
  }
}
