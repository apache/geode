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

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.GemFireException;
import org.apache.geode.SystemFailure;
import org.apache.geode.internal.lang.ObjectUtils;
import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.internal.util.IOUtils;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.GfshParser;
import org.apache.geode.management.internal.cli.commands.InternalGfshCommand;
import org.apache.geode.management.internal.cli.converters.ConnectionEndpointConverter;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.shell.JmxOperationInvoker;
import org.apache.geode.management.internal.cli.util.ConnectionEndpoint;
import org.apache.geode.management.internal.cli.util.JdkTool;

public class StartJConsoleCommand extends InternalGfshCommand {

  @CliCommand(value = CliStrings.START_JCONSOLE, help = CliStrings.START_JCONSOLE__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GEODE_MANAGER,
      CliStrings.TOPIC_GEODE_JMX, CliStrings.TOPIC_GEODE_M_AND_M})
  public Result startJConsole(
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
          help = CliStrings.START_JCONSOLE__J__HELP) final String[] jvmArgs) {
    try {
      String[] jconsoleCommandLine =
          createJConsoleCommandLine(null, interval, notile, pluginpath, version, jvmArgs);

      if (isDebugging()) {
        getGfsh().printAsInfo(
            String.format("JConsole command-line ($1%s)", Arrays.toString(jconsoleCommandLine)));
      }

      Process jconsoleProcess = Runtime.getRuntime().exec(jconsoleCommandLine);

      StringBuilder message = new StringBuilder();

      if (version) {
        jconsoleProcess.waitFor();

        BufferedReader reader =
            new BufferedReader(new InputStreamReader(jconsoleProcess.getErrorStream()));

        for (String line = reader.readLine(); line != null; line = reader.readLine()) {
          message.append(line);
          message.append(StringUtils.LINE_SEPARATOR);
        }

        IOUtils.close(reader);
      } else {
        getGfsh().printAsInfo(CliStrings.START_JCONSOLE__RUN);

        String jconsoleProcessOutput = waitAndCaptureProcessStandardErrorStream(jconsoleProcess);

        if (StringUtils.isNotBlank(jconsoleProcessOutput)) {
          message.append(StringUtils.LINE_SEPARATOR);
          message.append(jconsoleProcessOutput);
        }
      }

      return ResultBuilder.createInfoResult(message.toString());
    } catch (GemFireException | IllegalStateException | IllegalArgumentException e) {
      return ResultBuilder.createShellClientErrorResult(e.getMessage());
    } catch (IOException e) {
      return ResultBuilder
          .createShellClientErrorResult(CliStrings.START_JCONSOLE__IO_EXCEPTION_MESSAGE);
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      SystemFailure.checkFailure();
      return ResultBuilder.createShellClientErrorResult(
          String.format(CliStrings.START_JCONSOLE__CATCH_ALL_ERROR_MESSAGE, t.getMessage()));
    }
  }

  protected String[] createJConsoleCommandLine(final String member, final int interval,
      final boolean notile, final String pluginpath, final boolean version,
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

      String jmxServiceUrl = getJmxServiceUrlAsString(member);

      if (StringUtils.isNotBlank(jmxServiceUrl)) {
        commandLine.add(jmxServiceUrl);
      }
    }

    return commandLine.toArray(new String[commandLine.size()]);
  }

  protected String getJmxServiceUrlAsString(final String member) {
    if (StringUtils.isNotBlank(member)) {
      ConnectionEndpointConverter converter = new ConnectionEndpointConverter();

      try {
        ConnectionEndpoint connectionEndpoint =
            converter.convertFromText(member, ConnectionEndpoint.class, null);
        String hostAndPort = connectionEndpoint.getHost() + ":" + connectionEndpoint.getPort();
        return String.format("service:jmx:rmi://%s/jndi/rmi://%s/jmxrmi", hostAndPort, hostAndPort);
      } catch (Exception e) {
        throw new IllegalArgumentException(
            CliStrings.START_JCONSOLE__CONNECT_BY_MEMBER_NAME_ID_ERROR_MESSAGE);
      }
    } else {
      if (isConnectedAndReady()
          && (getGfsh().getOperationInvoker() instanceof JmxOperationInvoker)) {
        JmxOperationInvoker jmxOperationInvoker =
            (JmxOperationInvoker) getGfsh().getOperationInvoker();

        return ObjectUtils.toString(jmxOperationInvoker.getJmxServiceUrl());
      }
    }

    return null;
  }
}
