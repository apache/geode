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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.internal.lang.SystemUtils;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.i18n.CliStrings;

public class ShCommand extends OfflineGfshCommand {
  @CliCommand(value = {CliStrings.SH}, help = CliStrings.SH__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GFSH})
  public ResultModel sh(
      @CliOption(key = {"", CliStrings.SH__COMMAND}, mandatory = true,
          help = CliStrings.SH__COMMAND__HELP) String command,
      @CliOption(key = CliStrings.SH__USE_CONSOLE, specifiedDefaultValue = "true",
          unspecifiedDefaultValue = "false",
          help = CliStrings.SH__USE_CONSOLE__HELP) boolean useConsole)
      throws IOException {
    ResultModel result = new ResultModel();
    InfoResultModel info = result.getInfoSection("info");

    executeCommand(info, Gfsh.getCurrentInstance(), command, useConsole);

    return result;
  }

  private void executeCommand(InfoResultModel result, Gfsh gfsh, String userCommand,
      boolean useConsole)
      throws IOException {

    String cmdToExecute = userCommand;
    String cmdExecutor = "/bin/sh";
    String cmdExecutorOpt = "-c";
    if (SystemUtils.isWindows()) {
      cmdExecutor = "cmd";
      cmdExecutorOpt = "/c";
    } else if (useConsole) {
      cmdToExecute = cmdToExecute + " </dev/tty >/dev/tty";
    }
    String[] commandArray = {cmdExecutor, cmdExecutorOpt, cmdToExecute};

    ProcessBuilder builder = new ProcessBuilder();
    builder.command(commandArray);
    builder.directory();
    builder.redirectErrorStream();
    Process proc = builder.start();

    try (InputStreamReader inputStreamReader = new InputStreamReader(proc.getInputStream());
        BufferedReader input = new BufferedReader(inputStreamReader)) {

      String lineRead;
      while ((lineRead = input.readLine()) != null) {
        result.addLine(lineRead);
      }
    }
    proc.getOutputStream().close();

    try {
      if (proc.waitFor() != 0) {
        gfsh.logWarning("The command '" + userCommand + "' did not complete successfully", null);
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e.getMessage(), e);
    }
  }
}
