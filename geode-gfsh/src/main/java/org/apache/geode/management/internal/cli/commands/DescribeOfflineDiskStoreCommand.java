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

import static org.apache.geode.cache.Region.SEPARATOR;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;

import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;

@SuppressWarnings("deprecation")
public class DescribeOfflineDiskStoreCommand extends GfshCommand {
  @ShellMethod(value = CliStrings.DESCRIBE_OFFLINE_DISK_STORE__HELP,
      key = CliStrings.DESCRIBE_OFFLINE_DISK_STORE)
  @CliMetaData(shellOnly = true, relatedTopic = CliStrings.TOPIC_GEODE_DISKSTORE)
  public ResultModel describeOfflineDiskStore(
      @ShellOption(value = CliStrings.DESCRIBE_OFFLINE_DISK_STORE__DISKSTORENAME,
          help = CliStrings.DESCRIBE_OFFLINE_DISK_STORE__DISKSTORENAME__HELP) String diskStoreName,
      @ShellOption(value = CliStrings.DESCRIBE_OFFLINE_DISK_STORE__DISKDIRS,
          help = CliStrings.DESCRIBE_OFFLINE_DISK_STORE__DISKDIRS__HELP) String[] diskDirs,
      @ShellOption(value = CliStrings.DESCRIBE_OFFLINE_DISK_STORE__PDX_TYPES,
          help = CliStrings.DESCRIBE_OFFLINE_DISK_STORE__PDX_TYPES__HELP) Boolean listPdxTypes,
      @ShellOption(value = CliStrings.DESCRIBE_OFFLINE_DISK_STORE__REGIONNAME,
          help = CliStrings.DESCRIBE_OFFLINE_DISK_STORE__REGIONNAME__HELP) String regionName) {

    String validatedDirectories = DiskStoreCommandsUtils.validatedDirectories(diskDirs);
    if (validatedDirectories != null) {
      throw new IllegalArgumentException(
          "Could not find " + CliStrings.DESCRIBE_OFFLINE_DISK_STORE__DISKDIRS + ": \""
              + validatedDirectories + "\"");
    }

    try {
      final File[] dirs = new File[diskDirs.length];
      for (int i = 0; i < diskDirs.length; i++) {
        dirs[i] = new File((diskDirs[i]));
      }

      if (SEPARATOR.equals(regionName)) {
        return ResultModel.createError(CliStrings.INVALID_REGION_NAME);
      }

      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      PrintStream printStream = new PrintStream(outputStream);

      DiskStoreImpl.dumpInfo(printStream, diskStoreName, dirs, regionName, listPdxTypes);
      return ResultModel.createInfo(outputStream.toString());
    } catch (VirtualMachineError e) {
      org.apache.geode.SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable th) {
      org.apache.geode.SystemFailure.checkFailure();
      if (th.getMessage() == null) {
        return ResultModel.createError(
            "An error occurred while describing offline disk stores: " + th);
      }
      return ResultModel.createError(
          "An error occurred while describing offline disk stores: " + th.getMessage());
    }
  }
}
