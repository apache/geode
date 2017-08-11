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
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.SystemFailure;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.DiskStoreAttributes;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.CreateDiskStoreFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class CreateDiskStoreCommand implements GfshCommand {
  @CliCommand(value = CliStrings.CREATE_DISK_STORE, help = CliStrings.CREATE_DISK_STORE__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DISKSTORE})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.DISK)
  public Result createDiskStore(
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

    try {
      DiskStoreAttributes diskStoreAttributes = new DiskStoreAttributes();
      diskStoreAttributes.allowForceCompaction = allowForceCompaction;
      diskStoreAttributes.autoCompact = autoCompact;
      diskStoreAttributes.compactionThreshold = compactionThreshold;
      diskStoreAttributes.maxOplogSizeInBytes = maxOplogSize * (1024 * 1024);
      diskStoreAttributes.queueSize = queueSize;
      diskStoreAttributes.timeInterval = timeInterval;
      diskStoreAttributes.writeBufferSize = writeBufferSize;

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

      TabularResultData tabularData = ResultBuilder.createTabularResultData();
      boolean accumulatedData = false;

      Set<DistributedMember> targetMembers = CliUtil.findMembers(groups, null);

      if (targetMembers.isEmpty()) {
        return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
      }

      ResultCollector<?, ?> rc = CliUtil.executeFunction(new CreateDiskStoreFunction(),
          new Object[] {name, diskStoreAttributes}, targetMembers);
      List<CliFunctionResult> results = CliFunctionResult.cleanResults((List<?>) rc.getResult());

      AtomicReference<XmlEntity> xmlEntity = new AtomicReference<>();
      for (CliFunctionResult result : results) {
        if (result.getThrowable() != null) {
          tabularData.accumulate("Member", result.getMemberIdOrName());
          tabularData.accumulate("Result", "ERROR: " + result.getThrowable().getClass().getName()
              + ": " + result.getThrowable().getMessage());
          accumulatedData = true;
          tabularData.setStatus(Result.Status.ERROR);
        } else if (result.isSuccessful()) {
          tabularData.accumulate("Member", result.getMemberIdOrName());
          tabularData.accumulate("Result", result.getMessage());
          accumulatedData = true;

          if (xmlEntity.get() == null) {
            xmlEntity.set(result.getXmlEntity());
          }
        }
      }

      if (!accumulatedData) {
        return ResultBuilder.createInfoResult("Unable to create disk store(s).");
      }

      Result result = ResultBuilder.buildResult(tabularData);

      if (xmlEntity.get() != null) {
        persistClusterConfiguration(result,
            () -> getSharedConfiguration().addXmlEntity(xmlEntity.get(), groups));
      }

      return ResultBuilder.buildResult(tabularData);
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable th) {
      SystemFailure.checkFailure();
      return ResultBuilder.createGemFireErrorResult(
          CliStrings.format(CliStrings.CREATE_DISK_STORE__ERROR_WHILE_CREATING_REASON_0,
              new Object[] {th.getMessage()}));
    }
  }
}
