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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.internal.cli.GfshParser;
import org.apache.geode.management.internal.cli.commands.OfflineGfshCommand;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.util.JdkTool;
import org.apache.geode.management.internal.i18n.CliStrings;

public class StartJVisualVMCommand extends OfflineGfshCommand {
  @CliCommand(value = CliStrings.START_JVISUALVM, help = CliStrings.START_JVISUALVM__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GEODE_MANAGER,
      CliStrings.TOPIC_GEODE_JMX, CliStrings.TOPIC_GEODE_M_AND_M})
  public ResultModel startJVisualVM(
      @CliOption(key = CliStrings.START_JCONSOLE__J, optionContext = GfshParser.J_OPTION_CONTEXT,
          help = CliStrings.START_JCONSOLE__J__HELP) final String[] jvmArgs)
      throws IOException {

    String jvisualvmProcessOutput = getProcessOutput(jvmArgs);

    ResultModel result = new ResultModel();
    InfoResultModel infoResultModel = result.addInfo();

    if (StringUtils.isNotBlank(jvisualvmProcessOutput)) {
      infoResultModel.addLine(System.lineSeparator());
      infoResultModel.addLine(jvisualvmProcessOutput);
    }

    return result;

  }

  String getProcessOutput(String[] jvmArgs) throws IOException {
    String[] jvisualvmCommandLine = createJVisualVMCommandLine(jvmArgs);

    if (isDebugging()) {
      getGfsh().printAsInfo(
          String.format("JVisualVM command-line (%1$s)", Arrays.toString(jvisualvmCommandLine)));
    }

    Process jvisualvmProcess = Runtime.getRuntime().exec(jvisualvmCommandLine);
    getGfsh().printAsInfo(CliStrings.START_JVISUALVM__RUN);
    return waitAndCaptureProcessStandardErrorStream(jvisualvmProcess);
  }

  String[] createJVisualVMCommandLine(final String[] jvmArgs) {
    List<String> commandLine = new ArrayList<>();

    commandLine.add(JdkTool.getJVisualVMPathname());

    if (jvmArgs != null) {
      for (final String arg : jvmArgs) {
        commandLine.add("-J" + arg);
      }
    }

    return commandLine.toArray(new String[0]);
  }
}
