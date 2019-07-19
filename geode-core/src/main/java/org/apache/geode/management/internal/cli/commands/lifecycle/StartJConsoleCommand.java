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
package org.apache.geode.management.internal.cli.commands.lifecycle;

import static org.apache.geode.internal.process.ProcessStreamReader.waitAndCaptureProcessStandardErrorStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.management.remote.JMXServiceURL;

import org.apache.commons.lang3.StringUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.internal.util.IOUtils;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.internal.cli.GfshParser;
import org.apache.geode.management.internal.cli.commands.OfflineGfshCommand;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.util.JdkTool;

public class StartJConsoleCommand extends OfflineGfshCommand {

  @CliCommand(value = CliStrings.START_JCONSOLE, help = CliStrings.START_JCONSOLE__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GEODE_MANAGER,
      CliStrings.TOPIC_GEODE_JMX, CliStrings.TOPIC_GEODE_M_AND_M})
  public ResultModel startJConsole(
      @CliOption(key = CliStrings.START_JCONSOLE__INTERVAL, unspecifiedDefaultValue = "4",
          help = CliStrings.START_JCONSOLE__INTERVAL__HELP) final int interval,
      @CliOption(key = CliStrings.START_JCONSOLE__NOTILE, specifiedDefaultValue = "true",
          unspecifiedDefaultValue = "false",
          help = CliStrings.START_JCONSOLE__NOTILE__HELP) final boolean notile,
      @CliOption(key = CliStrings.START_JCONSOLE__PLUGINPATH,
          help = CliStrings.START_JCONSOLE__PLUGINPATH__HELP) final String pluginpath,
      @CliOption(key = CliStrings.START_JCONSOLE__VERSION, specifiedDefaultValue = "true",
          unspecifiedDefaultValue = "false",
          help = CliStrings.START_JCONSOLE__VERSION__HELP) final boolean version,
      @CliOption(key = CliStrings.START_JCONSOLE__J, optionContext = GfshParser.J_OPTION_CONTEXT,
          help = CliStrings.START_JCONSOLE__J__HELP) final String[] jvmArgs)
      throws InterruptedException, IOException {
    String[] jconsoleCommandLine =
        createJConsoleCommandLine(interval, notile, pluginpath, version, jvmArgs);

    ResultModel resultModel = new ResultModel();
    InfoResultModel infoResult = resultModel.addInfo();
    if (isDebugging()) {
      getGfsh().printAsInfo(
          String.format("JConsole command-line ($1%s)", Arrays.toString(jconsoleCommandLine)));
    }

    Process jconsoleProcess = getProcess(jconsoleCommandLine);

    StringBuilder message;

    if (version) {
      jconsoleProcess.waitFor();
      message = getErrorStringBuilder(jconsoleProcess);
    } else {
      message = new StringBuilder();
      getGfsh().printAsInfo(CliStrings.START_JCONSOLE__RUN);

      String jconsoleProcessOutput = getProcessOutput(jconsoleProcess);

      if (StringUtils.isNotBlank(jconsoleProcessOutput)) {
        message.append(System.lineSeparator());
        message.append(jconsoleProcessOutput);
      }
    }

    infoResult.addLine(message.toString());

    return resultModel;
  }

  StringBuilder getErrorStringBuilder(Process jconsoleProcess) throws IOException {
    StringBuilder message;
    message = new StringBuilder();
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(jconsoleProcess.getErrorStream()));

    for (String line = reader.readLine(); line != null; line = reader.readLine()) {
      message.append(line);
      message.append(System.lineSeparator());
    }

    IOUtils.close(reader);
    return message;
  }

  String getProcessOutput(Process jconsoleProcess) {
    return waitAndCaptureProcessStandardErrorStream(jconsoleProcess);
  }

  Process getProcess(String[] jconsoleCommandLine) throws IOException {
    return Runtime.getRuntime().exec(jconsoleCommandLine);
  }

  protected String[] createJConsoleCommandLine(final int interval,
      final boolean notile, final String pluginpath,
      final boolean version,
      final String[] jvmArgs) {
    List<String> commandLine = new ArrayList<>();

    commandLine.add(JdkTool.getJConsolePathname());

    if (version) {
      commandLine.add("-version");
    } else {
      commandLine.add("-interval=" + interval);

      if (notile) {
        commandLine.add("-notile");
      }

      if (StringUtils.isNotBlank(pluginpath)) {
        commandLine.add("-pluginpath " + pluginpath);
      }

      if (jvmArgs != null) {
        for (final String arg : jvmArgs) {
          commandLine.add("-J" + arg);
        }
      }

      JMXServiceURL jmxServiceUrl = getJmxServiceUrl();
      if (isConnectedAndReady() && jmxServiceUrl != null) {
        commandLine.add(jmxServiceUrl.toString());
      }
    }

    return commandLine.toArray(new String[0]);
  }
}
