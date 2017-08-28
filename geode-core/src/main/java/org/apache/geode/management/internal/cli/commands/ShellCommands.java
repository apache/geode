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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.springframework.shell.core.ExitShellRequest;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.lang.SystemUtils;
import org.apache.geode.internal.util.IOUtils;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.GfshParser;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ErrorResultData;
import org.apache.geode.management.internal.cli.result.InfoResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.cli.shell.OperationInvoker;
import org.apache.geode.management.internal.cli.shell.jline.GfshHistory;

/**
 *
 * @since GemFire 7.0
 */
public class ShellCommands implements GfshCommand {
  static Properties loadProperties(URL url) {
    Properties properties = new Properties();

    if (url == null) {
      return properties;
    }

    try (InputStream inputStream = url.openStream()) {
      properties.load(inputStream);
    } catch (IOException io) {
      throw new RuntimeException(
          CliStrings.format(CliStrings.CONNECT__MSG__COULD_NOT_READ_CONFIG_FROM_0,
              CliUtil.decodeWithDefaultCharSet(url.getPath())),
          io);
    }

    return properties;
  }

  static Properties loadProperties(File propertyFile) {
    try {
      return loadProperties(propertyFile.toURI().toURL());
    } catch (MalformedURLException e) {
      throw new RuntimeException(
          CliStrings.format("Failed to load configuration properties from pathname (%1$s)!",
              propertyFile.getAbsolutePath()),
          e);
    }
  }

  /**
   * try to find the file in the current dir or the user home or the classpath in this order
   * 
   * @param fileName the name of the file
   * @return URL if the file is found, otherwise null
   */
  public static URL searchFile(String fileName) {
    File file = new File(fileName);

    if (file.exists()) {
      try {
        return IOUtils.tryGetCanonicalFileElseGetAbsoluteFile(file).toURI().toURL();
      } catch (MalformedURLException ignore) {
      }
    }

    file = new File(System.getProperty("user.home"), fileName);

    if (file.exists()) {
      try {
        return IOUtils.tryGetCanonicalFileElseGetAbsoluteFile(file).toURI().toURL();
      } catch (MalformedURLException ignore) {
      }
    }

    return ClassPathLoader.getLatest().getResource(ShellCommands.class, fileName);
  }

  private static InfoResultData executeCommand(Gfsh gfsh, String userCommand, boolean useConsole)
      throws IOException {
    InfoResultData infoResultData = ResultBuilder.createInfoResultData();

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

    BufferedReader input = new BufferedReader(new InputStreamReader(proc.getInputStream()));

    String lineRead = "";
    while ((lineRead = input.readLine()) != null) {
      infoResultData.addLine(lineRead);
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
    return infoResultData;
  }

  @CliCommand(value = {CliStrings.EXIT, "quit"}, help = CliStrings.EXIT__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GFSH})
  public ExitShellRequest exit() {
    Gfsh gfshInstance = getGfsh();

    gfshInstance.stop();

    ExitShellRequest exitShellRequest = gfshInstance.getExitShellRequest();
    if (exitShellRequest == null) {
      // shouldn't really happen, but we'll fallback to this anyway
      exitShellRequest = ExitShellRequest.NORMAL_EXIT;
    }

    return exitShellRequest;
  }



  @CliCommand(value = {CliStrings.DISCONNECT}, help = CliStrings.DISCONNECT__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GFSH, CliStrings.TOPIC_GEODE_JMX,
      CliStrings.TOPIC_GEODE_MANAGER})
  public Result disconnect() {
    Result result = null;

    if (getGfsh() != null && !getGfsh().isConnectedAndReady()) {
      result = ResultBuilder.createInfoResult("Not connected.");
    } else {
      InfoResultData infoResultData = ResultBuilder.createInfoResultData();
      try {
        Gfsh gfshInstance = getGfsh();
        if (gfshInstance.isConnectedAndReady()) {
          OperationInvoker operationInvoker = gfshInstance.getOperationInvoker();
          Gfsh.println("Disconnecting from: " + operationInvoker);
          operationInvoker.stop();
          infoResultData.addLine(CliStrings.format(CliStrings.DISCONNECT__MSG__DISCONNECTED,
              operationInvoker.toString()));
          LogWrapper.getInstance().info(CliStrings.format(CliStrings.DISCONNECT__MSG__DISCONNECTED,
              operationInvoker.toString()));
          gfshInstance.setPromptPath(
              org.apache.geode.management.internal.cli.converters.RegionPathConverter.DEFAULT_APP_CONTEXT_PATH);
        } else {
          infoResultData.addLine(CliStrings.DISCONNECT__MSG__NOTCONNECTED);
        }
        result = ResultBuilder.buildResult(infoResultData);
      } catch (Exception e) {
        result = ResultBuilder.createConnectionErrorResult(
            CliStrings.format(CliStrings.DISCONNECT__MSG__ERROR, e.getMessage()));
      }
    }

    return result;
  }

  @CliCommand(value = {CliStrings.DESCRIBE_CONNECTION}, help = CliStrings.DESCRIBE_CONNECTION__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GFSH, CliStrings.TOPIC_GEODE_JMX})
  public Result describeConnection() {
    Result result = null;
    try {
      TabularResultData tabularResultData = ResultBuilder.createTabularResultData();
      Gfsh gfshInstance = getGfsh();
      if (gfshInstance.isConnectedAndReady()) {
        OperationInvoker operationInvoker = gfshInstance.getOperationInvoker();
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

  @CliCommand(value = {CliStrings.ECHO}, help = CliStrings.ECHO__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GFSH})
  public Result echo(@CliOption(key = {CliStrings.ECHO__STR, ""}, specifiedDefaultValue = "",
      mandatory = true, help = CliStrings.ECHO__STR__HELP) String stringToEcho) {
    Result result = null;

    if (stringToEcho.equals("$*")) {
      Gfsh gfshInstance = getGfsh();
      Map<String, String> envMap = gfshInstance.getEnv();
      Set<Entry<String, String>> setEnvMap = envMap.entrySet();
      TabularResultData resultData = buildResultForEcho(setEnvMap);

      result = ResultBuilder.buildResult(resultData);
    } else {
      result = ResultBuilder.createInfoResult(stringToEcho);
    }

    return result;
  }

  TabularResultData buildResultForEcho(Set<Entry<String, String>> propertyMap) {
    TabularResultData resultData = ResultBuilder.createTabularResultData();

    for (Entry<String, String> setEntry : propertyMap) {
      resultData.accumulate("Property", setEntry.getKey());
      resultData.accumulate("Value", String.valueOf(setEntry.getValue()));
    }
    return resultData;
  }

  @CliCommand(value = {CliStrings.SET_VARIABLE}, help = CliStrings.SET_VARIABLE__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GFSH})
  public Result setVariable(
      @CliOption(key = CliStrings.SET_VARIABLE__VAR, mandatory = true,
          help = CliStrings.SET_VARIABLE__VAR__HELP) String var,
      @CliOption(key = CliStrings.SET_VARIABLE__VALUE, mandatory = true,
          help = CliStrings.SET_VARIABLE__VALUE__HELP) String value) {
    Result result = null;
    try {
      getGfsh().setEnvProperty(var, String.valueOf(value));
      result =
          ResultBuilder.createInfoResult("Value for variable " + var + " is now: " + value + ".");
    } catch (IllegalArgumentException e) {
      ErrorResultData errorResultData = ResultBuilder.createErrorResultData();
      errorResultData.addLine(e.getMessage());
      result = ResultBuilder.buildResult(errorResultData);
    }

    return result;
  }

  @CliCommand(value = {CliStrings.DEBUG}, help = CliStrings.DEBUG__HELP)
  @CliMetaData(shellOnly = true,
      relatedTopic = {CliStrings.TOPIC_GFSH, CliStrings.TOPIC_GEODE_DEBUG_UTIL})
  public Result debug(
      @CliOption(key = CliStrings.DEBUG__STATE, unspecifiedDefaultValue = "OFF", mandatory = true,
          optionContext = "debug", help = CliStrings.DEBUG__STATE__HELP) String state) {
    Gfsh gfshInstance = Gfsh.getCurrentInstance();
    if (gfshInstance != null) {
      // Handle state
      if (state.equalsIgnoreCase("ON")) {
        gfshInstance.setDebug(true);
      } else if (state.equalsIgnoreCase("OFF")) {
        gfshInstance.setDebug(false);
      } else {
        return ResultBuilder.createUserErrorResult(
            CliStrings.format(CliStrings.DEBUG__MSG_0_INVALID_STATE_VALUE, state));
      }

    } else {
      ErrorResultData errorResultData =
          ResultBuilder.createErrorResultData().setErrorCode(ResultBuilder.ERRORCODE_DEFAULT)
              .addLine(CliStrings.ECHO__MSG__NO_GFSH_INSTANCE);
      return ResultBuilder.buildResult(errorResultData);
    }
    return ResultBuilder.createInfoResult(CliStrings.DEBUG__MSG_DEBUG_STATE_IS + state);
  }

  @CliCommand(value = CliStrings.HISTORY, help = CliStrings.HISTORY__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GFSH})
  public Result history(
      @CliOption(key = {CliStrings.HISTORY__FILE},
          help = CliStrings.HISTORY__FILE__HELP) String saveHistoryTo,
      @CliOption(key = {CliStrings.HISTORY__CLEAR}, specifiedDefaultValue = "true",
          unspecifiedDefaultValue = "false",
          help = CliStrings.HISTORY__CLEAR__HELP) Boolean clearHistory) {

    // process clear history
    if (clearHistory) {
      return executeClearHistory();
    } else {
      // Process file option
      Gfsh gfsh = Gfsh.getCurrentInstance();
      ErrorResultData errorResultData = null;
      StringBuilder contents = new StringBuilder();
      Writer output = null;

      int historySize = gfsh.getHistorySize();
      String historySizeString = String.valueOf(historySize);
      int historySizeWordLength = historySizeString.length();

      GfshHistory gfshHistory = gfsh.getGfshHistory();
      Iterator<?> it = gfshHistory.entries();
      boolean flagForLineNumbers = !(saveHistoryTo != null && saveHistoryTo.length() > 0);
      long lineNumber = 0;

      while (it.hasNext()) {
        String line = it.next().toString();
        if (!line.isEmpty()) {
          if (flagForLineNumbers) {
            lineNumber++;
            contents.append(String.format("%" + historySizeWordLength + "s  ", lineNumber));
          }
          contents.append(line);
          contents.append(GfshParser.LINE_SEPARATOR);
        }
      }

      try {
        // write to a user file
        if (saveHistoryTo != null && saveHistoryTo.length() > 0) {
          File saveHistoryToFile = new File(saveHistoryTo);
          output = new BufferedWriter(new FileWriter(saveHistoryToFile));

          if (!saveHistoryToFile.exists()) {
            errorResultData =
                ResultBuilder.createErrorResultData().setErrorCode(ResultBuilder.ERRORCODE_DEFAULT)
                    .addLine(CliStrings.HISTORY__MSG__FILE_DOES_NOT_EXISTS);
            return ResultBuilder.buildResult(errorResultData);
          }
          if (!saveHistoryToFile.isFile()) {
            errorResultData =
                ResultBuilder.createErrorResultData().setErrorCode(ResultBuilder.ERRORCODE_DEFAULT)
                    .addLine(CliStrings.HISTORY__MSG__FILE_SHOULD_NOT_BE_DIRECTORY);
            return ResultBuilder.buildResult(errorResultData);
          }
          if (!saveHistoryToFile.canWrite()) {
            errorResultData =
                ResultBuilder.createErrorResultData().setErrorCode(ResultBuilder.ERRORCODE_DEFAULT)
                    .addLine(CliStrings.HISTORY__MSG__FILE_CANNOT_BE_WRITTEN);
            return ResultBuilder.buildResult(errorResultData);
          }

          output.write(contents.toString());
        }

      } catch (IOException ex) {
        return ResultBuilder
            .createInfoResult("File error " + ex.getMessage() + " for file " + saveHistoryTo);
      } finally {
        try {
          if (output != null) {
            output.close();
          }
        } catch (IOException e) {
          errorResultData = ResultBuilder.createErrorResultData()
              .setErrorCode(ResultBuilder.ERRORCODE_DEFAULT).addLine("exception in closing file");
          return ResultBuilder.buildResult(errorResultData);
        }
      }
      if (saveHistoryTo != null && saveHistoryTo.length() > 0) {
        // since written to file no need to display the content
        return ResultBuilder.createInfoResult("Wrote successfully to file " + saveHistoryTo);
      } else {
        return ResultBuilder.createInfoResult(contents.toString());
      }
    }

  }

  Result executeClearHistory() {
    try {
      Gfsh gfsh = Gfsh.getCurrentInstance();
      gfsh.clearHistory();
    } catch (Exception e) {
      LogWrapper.getInstance().info(CliUtil.stackTraceAsString(e));
      return ResultBuilder
          .createGemFireErrorResult("Exception occurred while clearing history " + e.getMessage());
    }
    return ResultBuilder.createInfoResult(CliStrings.HISTORY__MSG__CLEARED_HISTORY);

  }

  @CliCommand(value = {CliStrings.RUN}, help = CliStrings.RUN__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GFSH})
  public Result executeScript(
      @CliOption(key = CliStrings.RUN__FILE, optionContext = ConverterHint.FILE, mandatory = true,
          help = CliStrings.RUN__FILE__HELP) File file,
      @CliOption(key = {CliStrings.RUN__QUIET}, specifiedDefaultValue = "true",
          unspecifiedDefaultValue = "false", help = CliStrings.RUN__QUIET__HELP) boolean quiet,
      @CliOption(key = {CliStrings.RUN__CONTINUEONERROR}, specifiedDefaultValue = "true",
          unspecifiedDefaultValue = "false",
          help = CliStrings.RUN__CONTINUEONERROR__HELP) boolean continueOnError) {
    Result result = null;

    Gfsh gfsh = Gfsh.getCurrentInstance();
    try {
      result = gfsh.executeScript(file, quiet, continueOnError);
    } catch (IllegalArgumentException e) {
      result = ResultBuilder.createShellClientErrorResult(e.getMessage());
    } // let CommandProcessingException go to the caller

    return result;
  }

  @CliCommand(value = {CliStrings.VERSION}, help = CliStrings.VERSION__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GFSH})
  public Result version(@CliOption(key = {CliStrings.VERSION__FULL}, specifiedDefaultValue = "true",
      unspecifiedDefaultValue = "false", help = CliStrings.VERSION__FULL__HELP) boolean full) {
    Gfsh gfsh = Gfsh.getCurrentInstance();

    return ResultBuilder.createInfoResult(gfsh.getVersion(full));
  }

  @CliCommand(value = {CliStrings.SLEEP}, help = CliStrings.SLEEP__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GFSH})
  public Result sleep(@CliOption(key = {CliStrings.SLEEP__TIME}, unspecifiedDefaultValue = "3",
      help = CliStrings.SLEEP__TIME__HELP) double time) {
    try {
      LogWrapper.getInstance().fine("Sleeping for " + time + "seconds.");
      Thread.sleep(Math.round(time * 1000));
    } catch (InterruptedException ignored) {
    }
    return ResultBuilder.createInfoResult("");
  }

  @CliCommand(value = {CliStrings.SH}, help = CliStrings.SH__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GFSH})
  public Result sh(
      @CliOption(key = {"", CliStrings.SH__COMMAND}, mandatory = true,
          help = CliStrings.SH__COMMAND__HELP) String command,
      @CliOption(key = CliStrings.SH__USE_CONSOLE, specifiedDefaultValue = "true",
          unspecifiedDefaultValue = "false",
          help = CliStrings.SH__USE_CONSOLE__HELP) boolean useConsole) {
    Result result = null;
    try {
      result =
          ResultBuilder.buildResult(executeCommand(Gfsh.getCurrentInstance(), command, useConsole));
    } catch (IllegalStateException | IOException e) {
      result = ResultBuilder.createUserErrorResult(e.getMessage());
      LogWrapper.getInstance()
          .warning("Unable to execute command \"" + command + "\". Reason:" + e.getMessage() + ".");
    }
    return result;
  }
}
