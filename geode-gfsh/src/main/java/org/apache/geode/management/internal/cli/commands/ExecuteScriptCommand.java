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

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.i18n.CliStrings;

public class ExecuteScriptCommand extends OfflineGfshCommand {
  @CliCommand(value = {CliStrings.RUN}, help = CliStrings.RUN__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GFSH})
  public ResultModel executeScript(
      @CliOption(key = CliStrings.RUN__FILE, optionContext = ConverterHint.FILE, mandatory = true,
          help = CliStrings.RUN__FILE__HELP) File file,
      @CliOption(key = {CliStrings.RUN__QUIET}, specifiedDefaultValue = "true",
          unspecifiedDefaultValue = "false", help = CliStrings.RUN__QUIET__HELP) boolean quiet,
      @CliOption(key = {CliStrings.RUN__CONTINUEONERROR}, specifiedDefaultValue = "true",
          unspecifiedDefaultValue = "false",
          help = CliStrings.RUN__CONTINUEONERROR__HELP) boolean continueOnError) {
    Gfsh gfsh = Gfsh.getCurrentInstance();
    return gfsh.executeScript(file, quiet, continueOnError);
  }

}
