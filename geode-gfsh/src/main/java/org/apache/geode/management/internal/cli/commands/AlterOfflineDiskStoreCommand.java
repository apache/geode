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

import java.io.File;

import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import org.apache.geode.cache.CacheExistsException;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;

public class AlterOfflineDiskStoreCommand extends GfshCommand {
  @ShellMethod(value = CliStrings.ALTER_DISK_STORE__HELP, key = CliStrings.ALTER_DISK_STORE)
  @CliMetaData(shellOnly = true, relatedTopic = CliStrings.TOPIC_GEODE_DISKSTORE)
  public ResultModel alterOfflineDiskStore(
      @ShellOption(value = CliStrings.ALTER_DISK_STORE__DISKSTORENAME,
          help = CliStrings.ALTER_DISK_STORE__DISKSTORENAME__HELP) String diskStoreName,
      @ShellOption(value = CliStrings.ALTER_DISK_STORE__REGIONNAME,
          help = CliStrings.ALTER_DISK_STORE__REGIONNAME__HELP) String regionName,
      @ShellOption(value = CliStrings.ALTER_DISK_STORE__DISKDIRS,
          help = CliStrings.ALTER_DISK_STORE__DISKDIRS__HELP) String[] diskDirs,
      @ShellOption(value = CliStrings.ALTER_DISK_STORE__COMPRESSOR,
          help = CliStrings.ALTER_DISK_STORE__COMPRESSOR__HELP) String compressorClassName,
      @ShellOption(value = CliStrings.ALTER_DISK_STORE__CONCURRENCY__LEVEL,
          help = CliStrings.ALTER_DISK_STORE__CONCURRENCY__LEVEL__HELP) Integer concurrencyLevel,
      @ShellOption(value = CliStrings.ALTER_DISK_STORE__STATISTICS__ENABLED,
          help = CliStrings.ALTER_DISK_STORE__STATISTICS__ENABLED__HELP) Boolean statisticsEnabled,
      @ShellOption(value = CliStrings.ALTER_DISK_STORE__INITIAL__CAPACITY,
          help = CliStrings.ALTER_DISK_STORE__INITIAL__CAPACITY__HELP) Integer initialCapacity,
      @ShellOption(value = CliStrings.ALTER_DISK_STORE__LOAD__FACTOR,
          help = CliStrings.ALTER_DISK_STORE__LOAD__FACTOR__HELP) Float loadFactor,
      @ShellOption(value = CliStrings.ALTER_DISK_STORE__LRU__EVICTION__ACTION,
          help = CliStrings.ALTER_DISK_STORE__LRU__EVICTION__ACTION__HELP) String lruEvictionAction,
      @ShellOption(value = CliStrings.ALTER_DISK_STORE__LRU__EVICTION__ALGORITHM,
          help = CliStrings.ALTER_DISK_STORE__LRU__EVICTION__ALGORITHM__HELP) String lruEvictionAlgo,
      @ShellOption(value = CliStrings.ALTER_DISK_STORE__LRU__EVICTION__LIMIT,
          help = CliStrings.ALTER_DISK_STORE__LRU__EVICTION__LIMIT__HELP) Integer lruEvictionLimit,
      @ShellOption(value = CliStrings.ALTER_DISK_STORE__OFF_HEAP,
          help = CliStrings.ALTER_DISK_STORE__OFF_HEAP__HELP) Boolean offHeap,
      @ShellOption(value = CliStrings.ALTER_DISK_STORE__REMOVE,
          help = CliStrings.ALTER_DISK_STORE__REMOVE__HELP,
          defaultValue = "false") boolean remove) {

    String validatedDirectories = DiskStoreCommandsUtils.validatedDirectories(diskDirs);
    if (validatedDirectories != null) {
      throw new IllegalArgumentException(
          "Could not find " + CliStrings.ALTER_DISK_STORE__DISKDIRS + ": \""
              + validatedDirectories + "\"");
    }

    try {
      File[] dirs = null;

      if (diskDirs != null) {
        dirs = new File[diskDirs.length];
        for (int i = 0; i < diskDirs.length; i++) {
          dirs[i] = new File((diskDirs[i]));
        }
      }

      if (regionName.equals(SEPARATOR)) {
        return ResultModel.createError(CliStrings.INVALID_REGION_NAME);
      }

      if ((lruEvictionAlgo != null) || (lruEvictionAction != null) || (lruEvictionLimit != null)
          || (concurrencyLevel != null) || (initialCapacity != null) || (loadFactor != null)
          || (compressorClassName != null) || (offHeap != null) || (statisticsEnabled != null)) {
        if (!remove) {
          String lruEvictionLimitString =
              lruEvictionLimit == null ? null : lruEvictionLimit.toString();
          String concurrencyLevelString =
              concurrencyLevel == null ? null : concurrencyLevel.toString();
          String initialCapacityString =
              initialCapacity == null ? null : initialCapacity.toString();
          String loadFactorString = loadFactor == null ? null : loadFactor.toString();
          String statisticsEnabledString =
              statisticsEnabled == null ? null : statisticsEnabled.toString();
          String offHeapString = offHeap == null ? null : offHeap.toString();

          if ("none".equals(compressorClassName)) {
            compressorClassName = "";
          }

          String resultMessage =
              DiskStoreImpl.modifyRegion(diskStoreName, dirs, SEPARATOR + regionName,
                  lruEvictionAlgo, lruEvictionAction, lruEvictionLimitString,
                  concurrencyLevelString,
                  initialCapacityString, loadFactorString, compressorClassName,
                  statisticsEnabledString,
                  offHeapString, false);

          return ResultModel.createInfo(resultMessage);
        } else {
          return ResultModel.createError(
              "Cannot use the --remove=true parameter with any other parameters");
        }
      } else {
        if (remove) {
          DiskStoreImpl.destroyRegion(diskStoreName, dirs, SEPARATOR + regionName);
          return ResultModel.createInfo("The region " + regionName
              + " was successfully removed from the disk store " + diskStoreName);
        } else {
          // Please provide an option
          return ResultModel.createInfo("Please provide a relevant parameter");
        }
      }
      // Catch the IllegalArgumentException thrown by the modifyDiskStore function and sent the
    } catch (IllegalArgumentException e) {
      return ResultModel.createError("Please check the parameters. " + e.getMessage());
    } catch (CacheExistsException e) {
      // Indicates that the command is being used when a cache is open
      return ResultModel.createError("Cannot execute " + CliStrings.ALTER_DISK_STORE
          + " when a cache exists (Offline command)");
    } catch (Exception e) {
      return ResultModel.createError(e.getMessage());
    }
  }
}
