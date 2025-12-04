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
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;

@org.springframework.shell.standard.ShellComponent
public class SleepCommand extends OfflineGfshCommand {
  @ShellMethod(value = CliStrings.SLEEP__HELP, key = {CliStrings.SLEEP})
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GFSH})
  public ResultModel sleep(@ShellOption(value = {CliStrings.SLEEP__TIME}, defaultValue = "3",
      help = CliStrings.SLEEP__TIME__HELP) double time) {
    try {
      LogWrapper.getInstance().fine("Sleeping for " + time + "seconds.");
      Thread.sleep(Math.round(time * 1000));
    } catch (InterruptedException ignored) {
    }
    return ResultModel.createInfo("");
  }
}
