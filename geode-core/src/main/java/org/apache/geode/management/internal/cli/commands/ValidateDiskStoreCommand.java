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
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.util.DiskStoreValidater;

public class ValidateDiskStoreCommand extends InternalGfshCommand {
  @CliCommand(value = CliStrings.VALIDATE_DISK_STORE, help = CliStrings.VALIDATE_DISK_STORE__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GEODE_DISKSTORE})
  public ResultModel validateDiskStore(
      @CliOption(key = CliStrings.VALIDATE_DISK_STORE__NAME, mandatory = true,
          help = CliStrings.VALIDATE_DISK_STORE__NAME__HELP) String diskStoreName,
      @CliOption(key = CliStrings.VALIDATE_DISK_STORE__DISKDIRS, mandatory = true,
          help = CliStrings.VALIDATE_DISK_STORE__DISKDIRS__HELP) String[] diskDirs,
      @CliOption(key = CliStrings.VALIDATE_DISK_STORE__J,
          help = CliStrings.VALIDATE_DISK_STORE__J__HELP) String[] jvmProps) {

    ResultModel result = new ResultModel();
    InfoResultModel infoResult = result.addInfo();
    LogWrapper logWrapper = LogWrapper.getInstance(getCache());
    Process validateDiskStoreProcess = null;

    try {
      // create a new process ...bug 46075
      StringBuilder dirList = new StringBuilder();
      for (String diskDir : diskDirs) {
        dirList.append(diskDir);
        dirList.append(";");
      }

      List<String> commandList = new ArrayList<>();
      commandList.add(System.getProperty("java.home") + File.separatorChar + "bin"
          + File.separatorChar + "java");

      DiskStoreCommandsUtils.configureLogging(commandList);

      if (jvmProps != null && jvmProps.length != 0) {
        commandList.addAll(Arrays.asList(jvmProps));
      }

      // Pass any java options on to the command
      String opts = System.getenv("JAVA_OPTS");
      if (opts != null) {
        commandList.add(opts);
      }
      commandList.add("-classpath");
      commandList.add(System.getProperty("java.class.path", "."));
      commandList.add(DiskStoreValidater.class.getName());
      commandList.add(diskStoreName);
      commandList.add(dirList.toString());

      ProcessBuilder procBuilder = new ProcessBuilder(commandList);
      procBuilder.redirectErrorStream(true);

      validateDiskStoreProcess = procBuilder.redirectErrorStream(true).start();
      InputStream inputStream = validateDiskStoreProcess.getInputStream();
      BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));

      String line;
      while ((line = br.readLine()) != null) {
        infoResult.addLine(line);
      }

      validateDiskStoreProcess.waitFor(2, TimeUnit.SECONDS);
      if (validateDiskStoreProcess.exitValue() != 0) {
        result.setStatus(Result.Status.ERROR);
      }
    } catch (Exception e) {
      infoResult.addLine(
          String.format("Error compacting disk store %s: %s", diskStoreName, e.getMessage()));
      result.setStatus(Result.Status.ERROR);
      logWrapper.warning(e.getMessage(), e);
    } finally {
      if (validateDiskStoreProcess != null) {
        try {
          // just to check whether the process has exited
          // Process.exitValue() throws IllegalThreadStateException if Process
          // is alive
          validateDiskStoreProcess.exitValue();
        } catch (IllegalThreadStateException ise) {
          // not yet terminated, destroy the process
          validateDiskStoreProcess.destroy();
        }
      }
    }

    return result;
  }
}
