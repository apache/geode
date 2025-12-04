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

import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.i18n.CliStrings;

@org.springframework.shell.standard.ShellComponent
public class DebugCommand extends OfflineGfshCommand {
  @ShellMethod(value = CliStrings.DEBUG__HELP, key = {CliStrings.DEBUG})
  @CliMetaData(shellOnly = true,
      relatedTopic = {CliStrings.TOPIC_GFSH, CliStrings.TOPIC_GEODE_DEBUG_UTIL})
  public ResultModel debug(
      @ShellOption(value = CliStrings.DEBUG__STATE, defaultValue = "OFF",
          help = CliStrings.DEBUG__STATE__HELP) String state) {
    Gfsh gfshInstance = Gfsh.getCurrentInstance();
    if (gfshInstance != null) {
      // Handle state
      if (state.equalsIgnoreCase("ON")) {
        gfshInstance.setDebug(true);
      } else if (state.equalsIgnoreCase("OFF")) {
        gfshInstance.setDebug(false);
      } else {
        return ResultModel.createError(
            CliStrings.format(CliStrings.DEBUG__MSG_0_INVALID_STATE_VALUE, state));
      }
    } else {
      return ResultModel.createError(CliStrings.ECHO__MSG__NO_GFSH_INSTANCE);
    }
    return ResultModel.createInfo(CliStrings.DEBUG__MSG_DEBUG_STATE_IS + state);
  }
}
