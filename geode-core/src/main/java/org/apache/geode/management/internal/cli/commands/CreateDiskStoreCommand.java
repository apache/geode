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
import java.util.List;
import java.util.Set;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.DiskDirType;
import org.apache.geode.cache.configuration.DiskDirsType;
import org.apache.geode.cache.configuration.DiskStoreType;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.DiskStoreAttributes;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.CreateDiskStoreFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class CreateDiskStoreCommand extends SingleGfshCommand {
  @CliCommand(value = CliStrings.CREATE_DISK_STORE, help = CliStrings.CREATE_DISK_STORE__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DISKSTORE})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.DISK)
  public ResultModel createDiskStore(
      @CliOption(key = CliStrings.CREATE_DISK_STORE__NAME, mandatory = true,
          optionContext = ConverterHint.DISKSTORE,
          help = CliStrings.CREATE_DISK_STORE__NAME__HELP) String name,
      @CliOption(key = CliStrings.CREATE_DISK_STORE__ALLOW_FORCE_COMPACTION,
          specifiedDefaultValue = "true", unspecifiedDefaultValue = "false",
          help = CliStrings.CREATE_DISK_STORE__ALLOW_FORCE_COMPACTION__HELP) boolean allowForceCompaction,
      @CliOption(key = CliStrings.CREATE_DISK_STORE__AUTO_COMPACT, specifiedDefaultValue = "true",
          unspecifiedDefaultValue = "true",
          help = CliStrings.CREATE_DISK_STORE__AUTO_COMPACT__HELP) boolean autoCompact,
      @CliOption(key = CliStrings.CREATE_DISK_STORE__COMPACTION_THRESHOLD,
          unspecifiedDefaultValue = "50",
          help = CliStrings.CREATE_DISK_STORE__COMPACTION_THRESHOLD__HELP) int compactionThreshold,
      @CliOption(key = CliStrings.CREATE_DISK_STORE__MAX_OPLOG_SIZE,
          unspecifiedDefaultValue = "1024",
          help = CliStrings.CREATE_DISK_STORE__MAX_OPLOG_SIZE__HELP) int maxOplogSize,
      @CliOption(key = CliStrings.CREATE_DISK_STORE__QUEUE_SIZE, unspecifiedDefaultValue = "0",
          help = CliStrings.CREATE_DISK_STORE__QUEUE_SIZE__HELP) int queueSize,
      @CliOption(key = CliStrings.CREATE_DISK_STORE__TIME_INTERVAL,
          unspecifiedDefaultValue = "1000",
          help = CliStrings.CREATE_DISK_STORE__TIME_INTERVAL__HELP) long timeInterval,
      @CliOption(key = CliStrings.CREATE_DISK_STORE__WRITE_BUFFER_SIZE,
          unspecifiedDefaultValue = "32768",
          help = CliStrings.CREATE_DISK_STORE__WRITE_BUFFER_SIZE__HELP) int writeBufferSize,
      @CliOption(key = CliStrings.CREATE_DISK_STORE__DIRECTORY_AND_SIZE, mandatory = true,
          help = CliStrings.CREATE_DISK_STORE__DIRECTORY_AND_SIZE__HELP) String[] directoriesAndSizes,
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          help = CliStrings.CREATE_DISK_STORE__GROUP__HELP,
          optionContext = ConverterHint.MEMBERGROUP) String[] groups,
      @CliOption(key = CliStrings.CREATE_DISK_STORE__DISK_USAGE_WARNING_PCT,
          unspecifiedDefaultValue = "90",
          help = CliStrings.CREATE_DISK_STORE__DISK_USAGE_WARNING_PCT__HELP) float diskUsageWarningPercentage,
      @CliOption(key = CliStrings.CREATE_DISK_STORE__DISK_USAGE_CRITICAL_PCT,
          unspecifiedDefaultValue = "99",
          help = CliStrings.CREATE_DISK_STORE__DISK_USAGE_CRITICAL_PCT__HELP) float diskUsageCriticalPercentage) {

    DiskStoreAttributes diskStoreAttributes = new DiskStoreAttributes();
    diskStoreAttributes.allowForceCompaction = allowForceCompaction;
    diskStoreAttributes.autoCompact = autoCompact;
    diskStoreAttributes.compactionThreshold = compactionThreshold;
    diskStoreAttributes.maxOplogSizeInBytes = maxOplogSize * (1024L * 1024L);
    diskStoreAttributes.queueSize = queueSize;
    diskStoreAttributes.timeInterval = timeInterval;
    diskStoreAttributes.writeBufferSize = writeBufferSize;
    diskStoreAttributes.name = name;

    File[] directories = new File[directoriesAndSizes.length];
    int[] sizes = new int[directoriesAndSizes.length];
    for (int i = 0; i < directoriesAndSizes.length; i++) {
      final int hashPosition = directoriesAndSizes[i].indexOf('#');
      if (hashPosition == -1) {
        directories[i] = new File(directoriesAndSizes[i]);
        sizes[i] = Integer.MAX_VALUE;
      } else {
        directories[i] = new File(directoriesAndSizes[i].substring(0, hashPosition));
        sizes[i] = Integer.parseInt(directoriesAndSizes[i].substring(hashPosition + 1));
      }
    }
    diskStoreAttributes.diskDirs = directories;
    diskStoreAttributes.diskDirSizes = sizes;

    diskStoreAttributes.setDiskUsageWarningPercentage(diskUsageWarningPercentage);
    diskStoreAttributes.setDiskUsageCriticalPercentage(diskUsageCriticalPercentage);

    Set<DistributedMember> targetMembers = findMembers(groups, null);

    if (targetMembers.isEmpty()) {
      return ResultModel.createError(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }

    List<CliFunctionResult> functionResults = executeAndGetFunctionResult(
        new CreateDiskStoreFunction(), new Object[] {name, diskStoreAttributes}, targetMembers);

    ResultModel result = ResultModel.createMemberStatusResult(functionResults);
    result.setConfigObject(createDiskStoreType(name, diskStoreAttributes));

    return result;
  }

  private DiskStoreType createDiskStoreType(String name, DiskStoreAttributes diskStoreAttributes) {
    DiskStoreType diskStoreType = new DiskStoreType();
    diskStoreType.setAllowForceCompaction(diskStoreAttributes.getAllowForceCompaction());
    diskStoreType.setAutoCompact(diskStoreAttributes.getAutoCompact());
    diskStoreType
        .setCompactionThreshold(Integer.toString(diskStoreAttributes.getCompactionThreshold()));

    DiskDirsType diskDirsType = new DiskDirsType();
    List<DiskDirType> diskDirs = diskDirsType.getDiskDirs();
    for (int i = 0; i < diskStoreAttributes.getDiskDirs().length; i++) {
      DiskDirType diskDir = new DiskDirType();
      diskDir.setContent(diskStoreAttributes.getDiskDirs()[i].getName());
      diskDir.setDirSize(Integer.toString(diskStoreAttributes.getDiskDirSizes()[i]));

      diskDirs.add(diskDir);
    }
    diskStoreType.setDiskDirs(diskDirsType);
    diskStoreType.setDiskUsageCriticalPercentage(
        Integer.toString((int) diskStoreAttributes.getDiskUsageCriticalPercentage()));
    diskStoreType.setDiskUsageWarningPercentage(
        Integer.toString((int) diskStoreAttributes.getDiskUsageWarningPercentage()));
    diskStoreType.setMaxOplogSize(Integer.toString((int) diskStoreAttributes.getMaxOplogSize()));
    diskStoreType.setName(diskStoreAttributes.getName());
    diskStoreType.setQueueSize(Integer.toString(diskStoreAttributes.getQueueSize()));
    diskStoreType.setTimeInterval(Integer.toString((int) diskStoreAttributes.getTimeInterval()));
    diskStoreType.setWriteBufferSize(Integer.toString(diskStoreAttributes.getWriteBufferSize()));

    return diskStoreType;
  }

  @Override
  public boolean updateConfigForGroup(String group, CacheConfig config, Object configObject) {
    DiskStoreType diskStoreType = (DiskStoreType) configObject;
    config.getDiskStores().add(diskStoreType);
    return true;
  }
}
