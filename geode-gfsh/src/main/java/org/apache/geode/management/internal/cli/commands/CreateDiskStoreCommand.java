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

import static org.apache.geode.management.internal.i18n.CliStrings.CREATE_DISK_STORE__DIR_SIZE_IS_NEGATIVE;
import static org.apache.geode.management.internal.i18n.CliStrings.CREATE_DISK_STORE__DIR_SIZE_NOT_A_NUMBER;
import static org.apache.geode.management.internal.i18n.CliStrings.CREATE_DISK_STORE__DIR_SIZE_TOO_BIG_ERROR;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.DiskDirType;
import org.apache.geode.cache.configuration.DiskStoreType;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.DiskStoreAttributes;
import org.apache.geode.internal.cache.execute.AbstractExecution;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.internal.cli.domain.DiskStoreDetails;
import org.apache.geode.management.internal.cli.functions.CreateDiskStoreFunction;
import org.apache.geode.management.internal.cli.functions.ListDiskStoresFunction;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class CreateDiskStoreCommand extends SingleGfshCommand {
  private static final int MBEAN_CREATION_WAIT_TIME = 10000;

  @CliCommand(value = CliStrings.CREATE_DISK_STORE, help = CliStrings.CREATE_DISK_STORE__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DISKSTORE})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.DISK)
  public ResultModel createDiskStore(
      @CliOption(key = CliStrings.CREATE_DISK_STORE__DIRECTORY_AND_SIZE, mandatory = true,
          help = CliStrings.CREATE_DISK_STORE__DIRECTORY_AND_SIZE__HELP) String[] directoriesAndSizes,
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
        String dirSizeString = directoriesAndSizes[i].substring(hashPosition + 1);
        verifyDirSizeConstraints(dirSizeString);
        sizes[i] = Integer.parseInt(dirSizeString);
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

    Pair<Boolean, String> validationResult =
        validateDiskstoreAttributes(diskStoreAttributes, targetMembers);
    if (validationResult.getLeft().equals(Boolean.FALSE)) {
      return ResultModel.createError(validationResult.getRight());
    }

    List<CliFunctionResult> functionResults = executeAndGetFunctionResult(
        new CreateDiskStoreFunction(), new Object[] {name, diskStoreAttributes}, targetMembers);

    ResultModel result = ResultModel.createMemberStatusResult(functionResults);
    result.setConfigObject(createDiskStoreType(name, diskStoreAttributes));

    if (!waitForDiskStoreMBeanCreation(name, targetMembers)) {
      result.addInfo()
          .addLine("Did not complete waiting for Disk Store MBean proxy creation");
    }

    return result;
  }

  @VisibleForTesting
  void verifyDirSizeConstraints(String dirSize) {
    long dirSizeLongValue;
    try {
      dirSizeLongValue = Long.parseLong(dirSize);
    } catch (NumberFormatException exc) {
      throw new IllegalArgumentException(
          String.format(CREATE_DISK_STORE__DIR_SIZE_NOT_A_NUMBER, dirSize));
    }
    if (dirSizeLongValue > (long) Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          String.format(CREATE_DISK_STORE__DIR_SIZE_TOO_BIG_ERROR, dirSize));
    }
    if (dirSizeLongValue < 0) {
      throw new IllegalArgumentException(
          String.format(CREATE_DISK_STORE__DIR_SIZE_IS_NEGATIVE, dirSize));
    }
  }

  @VisibleForTesting
  boolean waitForDiskStoreMBeanCreation(String diskStore,
      Set<DistributedMember> membersToCreateDiskStoreOn) {
    DistributedSystemMXBean dsMXBean = getManagementService().getDistributedSystemMXBean();

    return poll(MBEAN_CREATION_WAIT_TIME, TimeUnit.MILLISECONDS,
        () -> membersToCreateDiskStoreOn.stream()
            .allMatch(
                m -> DiskStoreCommandsUtils.diskStoreBeanAndMemberBeanDiskStoreExists(dsMXBean,
                    m.getName(), diskStore)));
  }

  @VisibleForTesting
  Pair<Boolean, String> validateDiskstoreAttributes(
      DiskStoreAttributes diskStoreAttributes,
      Set<DistributedMember> targetMembers) {
    List<DiskStoreDetails> currentDiskstores = getDiskStoreListing(targetMembers);

    for (DiskStoreDetails detail : currentDiskstores) {
      if (detail.getName().equals(diskStoreAttributes.getName())) {
        return Pair.of(Boolean.FALSE,
            String.format("Error: Disk store %s already exists", diskStoreAttributes.getName()));
      }
    }

    return Pair.of(Boolean.TRUE, null);
  }

  private DiskStoreType createDiskStoreType(String name, DiskStoreAttributes diskStoreAttributes) {
    DiskStoreType diskStoreType = new DiskStoreType();
    diskStoreType.setAllowForceCompaction(diskStoreAttributes.getAllowForceCompaction());
    diskStoreType.setAutoCompact(diskStoreAttributes.getAutoCompact());
    diskStoreType
        .setCompactionThreshold(Integer.toString(diskStoreAttributes.getCompactionThreshold()));

    List<DiskDirType> diskDirs = new ArrayList<>();
    for (int i = 0; i < diskStoreAttributes.getDiskDirs().length; i++) {
      DiskDirType diskDir = new DiskDirType();
      File diskDirFile = diskStoreAttributes.getDiskDirs()[i];
      diskDir.setContent(diskDirFile.toString());
      diskDir.setDirSize(Integer.toString(diskStoreAttributes.getDiskDirSizes()[i]));
      diskDirs.add(diskDir);
    }
    diskStoreType.setDiskDirs(diskDirs);
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

  @SuppressWarnings("unchecked")
  private List<DiskStoreDetails> getDiskStoreListing(Set<DistributedMember> members) {
    final Execution membersFunctionExecutor = getMembersFunctionExecutor(members);
    if (membersFunctionExecutor instanceof AbstractExecution) {
      ((AbstractExecution) membersFunctionExecutor).setIgnoreDepartedMembers(true);
    }

    final ResultCollector<?, ?> resultCollector =
        membersFunctionExecutor.execute(new ListDiskStoresFunction());

    final List<?> results = (List<?>) resultCollector.getResult();
    final List<DiskStoreDetails> distributedSystemMemberDiskStores =
        new ArrayList<>(results.size());

    for (final Object result : results) {
      if (result instanceof Set) {
        distributedSystemMemberDiskStores.addAll((Set<DiskStoreDetails>) result);
      }
    }

    Collections.sort(distributedSystemMemberDiskStores);

    return distributedSystemMemberDiskStores;
  }


  @Override
  public boolean updateConfigForGroup(String group, CacheConfig config, Object configObject) {
    DiskStoreType diskStoreType = (DiskStoreType) configObject;
    config.getDiskStores().add(diskStoreType);
    return true;
  }
}
