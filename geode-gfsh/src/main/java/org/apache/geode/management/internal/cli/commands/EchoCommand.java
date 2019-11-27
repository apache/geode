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

import java.util.Map;
import java.util.Set;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.i18n.CliStrings;

public class EchoCommand extends OfflineGfshCommand {
  @CliCommand(value = {CliStrings.ECHO}, help = CliStrings.ECHO__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GFSH})
  public ResultModel echo(@CliOption(key = {CliStrings.ECHO__STR, ""}, specifiedDefaultValue = "",
      mandatory = true, help = CliStrings.ECHO__STR__HELP) String stringToEcho) {

    if (stringToEcho.equals("$*")) {
      Gfsh gfshInstance = getGfsh();
      Map<String, String> envMap = gfshInstance.getEnv();
      Set<Map.Entry<String, String>> setEnvMap = envMap.entrySet();
      return buildResultForEcho(setEnvMap);
    } else {
      return ResultModel.createInfo(stringToEcho);
    }
  }

  private ResultModel buildResultForEcho(Set<Map.Entry<String, String>> propertyMap) {
    ResultModel result = new ResultModel();
    TabularResultModel echoResult = result.addTable("echoResult");

    echoResult.setColumnHeader("Property", "Value");
    for (Map.Entry<String, String> setEntry : propertyMap) {
      echoResult.addRow(setEntry.getKey(), String.valueOf(setEntry.getValue()));
    }
    return result;
  }
}
