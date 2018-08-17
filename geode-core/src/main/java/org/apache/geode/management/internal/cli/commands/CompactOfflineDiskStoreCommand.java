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
import org.apache.geode.management.internal.cli.util.DiskStoreCompacter;

public class CompactOfflineDiskStoreCommand extends InternalGfshCommand {
  @CliCommand(value = CliStrings.COMPACT_OFFLINE_DISK_STORE,
      help = CliStrings.COMPACT_OFFLINE_DISK_STORE__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GEODE_DISKSTORE})
  public ResultModel compactOfflineDiskStore(
      @CliOption(key = CliStrings.COMPACT_OFFLINE_DISK_STORE__NAME, mandatory = true,
          help = CliStrings.COMPACT_OFFLINE_DISK_STORE__NAME__HELP) String diskStoreName,
      @CliOption(key = CliStrings.COMPACT_OFFLINE_DISK_STORE__DISKDIRS, mandatory = true,
          help = CliStrings.COMPACT_OFFLINE_DISK_STORE__DISKDIRS__HELP) String[] diskDirs,
      @CliOption(key = CliStrings.COMPACT_OFFLINE_DISK_STORE__MAXOPLOGSIZE,
          unspecifiedDefaultValue = "-1",
          help = CliStrings.COMPACT_OFFLINE_DISK_STORE__MAXOPLOGSIZE__HELP) long maxOplogSize,
      @CliOption(key = CliStrings.COMPACT_OFFLINE_DISK_STORE__J,
          help = CliStrings.COMPACT_OFFLINE_DISK_STORE__J__HELP) String[] jvmProps) {
    ResultModel result = new ResultModel();
    InfoResultModel infoResult = result.addInfo();
    LogWrapper logWrapper = LogWrapper.getInstance(getCache());

    Process compactorProcess = null;

    try {
      String validatedDirectories = DiskStoreCommandsUtils.validatedDirectories(diskDirs);
      if (validatedDirectories != null) {
        throw new IllegalArgumentException(
            "Could not find " + CliStrings.COMPACT_OFFLINE_DISK_STORE__DISKDIRS + ": \""
                + validatedDirectories + "\"");
      }

      List<String> commandList = new ArrayList<>();
      commandList.add(System.getProperty("java.home") + File.separatorChar + "bin"
          + File.separatorChar + "java");

      DiskStoreCommandsUtils.configureLogging(commandList);

      if (jvmProps != null && jvmProps.length != 0) {
        commandList.addAll(Arrays.asList(jvmProps));
      }
      commandList.add("-classpath");
      commandList.add(System.getProperty("java.class.path", "."));
      commandList.add(DiskStoreCompacter.class.getName());

      commandList.add(CliStrings.COMPACT_OFFLINE_DISK_STORE__NAME + "=" + diskStoreName);

      if (diskDirs.length != 0) {
        StringBuilder builder = new StringBuilder();
        int arrayLength = diskDirs.length;
        for (int i = 0; i < arrayLength; i++) {
          if (File.separatorChar == '\\') {
            builder.append(diskDirs[i].replace("\\", "/")); // see 46120
          } else {
            builder.append(diskDirs[i]);
          }
          if (i + 1 != arrayLength) {
            builder.append(',');
          }
        }
        commandList.add(CliStrings.COMPACT_OFFLINE_DISK_STORE__DISKDIRS + "=" + builder.toString());
      }
      // -1 is ignore as maxOplogSize
      commandList.add(CliStrings.COMPACT_OFFLINE_DISK_STORE__MAXOPLOGSIZE + "=" + maxOplogSize);

      ProcessBuilder procBuilder = new ProcessBuilder(commandList);
      procBuilder.redirectErrorStream(true);
      compactorProcess = procBuilder.start();

      InputStream inputStream = compactorProcess.getInputStream();
      BufferedReader inputReader = new BufferedReader(new InputStreamReader(inputStream));

      String line;
      while ((line = inputReader.readLine()) != null) {
        infoResult.addLine(line);
      }

      compactorProcess.waitFor(2, TimeUnit.SECONDS);
      if (compactorProcess.exitValue() != 0) {
        result.setStatus(Result.Status.ERROR);
      }
    } catch (Exception e) {
      infoResult.addLine(
          String.format("Error compacting disk store %s: %s", diskStoreName, e.getMessage()));
      result.setStatus(Result.Status.ERROR);
      logWrapper.warning(e.getMessage(), e);
    } finally {
      if (compactorProcess != null) {
        try {
          // just to check whether the process has exited
          // Process.exitValue() throws IllegalThreadStateException if Process
          // is alive
          compactorProcess.exitValue();
        } catch (IllegalThreadStateException ise) {
          // not yet terminated, destroy the process
          compactorProcess.destroy();
        }
      }
    }

    return result;
  }
}
