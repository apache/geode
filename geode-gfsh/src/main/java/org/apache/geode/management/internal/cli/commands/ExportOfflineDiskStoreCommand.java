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

import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;

public class ExportOfflineDiskStoreCommand extends SingleGfshCommand {
  @CliCommand(value = CliStrings.EXPORT_OFFLINE_DISK_STORE,
      help = CliStrings.EXPORT_OFFLINE_DISK_STORE__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GEODE_DISKSTORE})
  @SuppressWarnings("deprecation")
  public ResultModel exportOfflineDiskStore(
      @CliOption(key = CliStrings.EXPORT_OFFLINE_DISK_STORE__DISKSTORENAME, mandatory = true,
          help = CliStrings.EXPORT_OFFLINE_DISK_STORE__DISKSTORENAME__HELP) String diskStoreName,
      @CliOption(key = CliStrings.EXPORT_OFFLINE_DISK_STORE__DISKDIRS, mandatory = true,
          help = CliStrings.EXPORT_OFFLINE_DISK_STORE__DISKDIRS__HELP) String[] diskDirs,
      @CliOption(key = CliStrings.EXPORT_OFFLINE_DISK_STORE__DIR, mandatory = true,
          help = CliStrings.EXPORT_OFFLINE_DISK_STORE__DIR__HELP) String dir) {

    try {
      final File[] dirs = new File[diskDirs.length];
      for (int i = 0; i < diskDirs.length; i++) {
        dirs[i] = new File((diskDirs[i]));
      }

      File output = new File(dir);

      // Note, this can consume a lot of memory, so this should
      // not be moved to a separate process unless we provide a way for the user
      // to configure the size of that process.
      DiskStoreImpl.exportOfflineSnapshot(diskStoreName, dirs, output);

      return ResultModel.createInfo(
          CliStrings.format(CliStrings.EXPORT_OFFLINE_DISK_STORE__SUCCESS, diskStoreName, dir));
    } catch (VirtualMachineError e) {
      org.apache.geode.SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable th) {
      org.apache.geode.SystemFailure.checkFailure();
      LogWrapper.getInstance(getCache()).warning(th.getMessage(), th);
      return ResultModel.createError(CliStrings.format(CliStrings.EXPORT_OFFLINE_DISK_STORE__ERROR,
          diskStoreName, th.toString()));
    }
  }
}
