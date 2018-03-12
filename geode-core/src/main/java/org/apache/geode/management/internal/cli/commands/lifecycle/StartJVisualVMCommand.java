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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.GemFireException;
import org.apache.geode.SystemFailure;
import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.GfshParser;
import org.apache.geode.management.internal.cli.commands.GfshCommand;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.InfoResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.util.JdkTool;

public class StartJVisualVMCommand extends GfshCommand {
  @CliCommand(value = CliStrings.START_JVISUALVM, help = CliStrings.START_JVISUALVM__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GEODE_MANAGER,
      CliStrings.TOPIC_GEODE_JMX, CliStrings.TOPIC_GEODE_M_AND_M})
  public Result startJVisualVM(
      @CliOption(key = CliStrings.START_JCONSOLE__J, optionContext = GfshParser.J_OPTION_CONTEXT,
          help = CliStrings.START_JCONSOLE__J__HELP) final String[] jvmArgs) {
    try {
      String[] jvisualvmCommandLine = createJVisualVMCommandLine(jvmArgs);

      if (isDebugging()) {
        getGfsh().printAsInfo(
            String.format("JVisualVM command-line (%1$s)", Arrays.toString(jvisualvmCommandLine)));
      }

      Process jvisualvmProcess = Runtime.getRuntime().exec(jvisualvmCommandLine);

      getGfsh().printAsInfo(CliStrings.START_JVISUALVM__RUN);

      String jvisualvmProcessOutput = waitAndCaptureProcessStandardErrorStream(jvisualvmProcess);

      InfoResultData infoResultData = ResultBuilder.createInfoResultData();

      if (StringUtils.isNotBlank(jvisualvmProcessOutput)) {
        infoResultData.addLine(StringUtils.LINE_SEPARATOR);
        infoResultData.addLine(jvisualvmProcessOutput);
      }

      return ResultBuilder.buildResult(infoResultData);
    } catch (GemFireException | IllegalStateException | IllegalArgumentException e) {
      return ResultBuilder.createShellClientErrorResult(e.getMessage());
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      SystemFailure.checkFailure();
      return ResultBuilder.createShellClientErrorResult(
          String.format(CliStrings.START_JVISUALVM__ERROR_MESSAGE, t.getMessage()));
    }
  }

  protected String[] createJVisualVMCommandLine(final String[] jvmArgs) {
    List<String> commandLine = new ArrayList<>();

    commandLine.add(JdkTool.getJVisualVMPathname());

    if (jvmArgs != null) {
      for (final String arg : jvmArgs) {
        commandLine.add("-J" + arg);
      }
    }

    return commandLine.toArray(new String[commandLine.size()]);
  }
}
