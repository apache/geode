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

import java.util.Collections;
import java.util.List;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.lang.ClassUtils;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.domain.DiskStoreDetails;
import org.apache.geode.management.internal.cli.functions.DescribeDiskStoreFunction;
import org.apache.geode.management.internal.cli.result.model.DataResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.exceptions.EntityNotFoundException;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class DescribeDiskStoreCommand extends GfshCommand {
  public static final String DISK_STORE_SECTION = "disk-store";
  public static final String DISK_DIR_SECTION = "disk-dir";
  public static final String REGION_SECTION = "region";
  public static final String CACHE_SERVER_SECTION = "cache-server";
  public static final String GATEWAY_SECTION = "gateway";
  public static final String ASYNC_EVENT_QUEUE_SECTION = "async-event-queue";

  @CliCommand(value = CliStrings.DESCRIBE_DISK_STORE, help = CliStrings.DESCRIBE_DISK_STORE__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DISKSTORE})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public ResultModel describeDiskStore(
      @CliOption(key = CliStrings.MEMBER, mandatory = true,
          optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.DESCRIBE_DISK_STORE__MEMBER__HELP) final String memberName,
      @CliOption(key = CliStrings.DESCRIBE_DISK_STORE__NAME, mandatory = true,
          optionContext = ConverterHint.DISKSTORE,
          help = CliStrings.DESCRIBE_DISK_STORE__NAME__HELP) final String diskStoreName) {

    return toResultModel(getDiskStoreDescription(memberName, diskStoreName));

  }

  DiskStoreDetails getDiskStoreDescription(final String memberName, final String diskStoreName) {
    final DistributedMember member = getMember(memberName);

    @SuppressWarnings("unchecked")
    final ResultCollector<?, ?> resultCollector =
        getMembersFunctionExecutor(Collections.singleton(member)).setArguments(diskStoreName)
            .execute(new DescribeDiskStoreFunction());

    final Object result = ((List<?>) resultCollector.getResult()).get(0);

    if (result instanceof DiskStoreDetails) { // disk store details in hand...
      return (DiskStoreDetails) result;
    } else if (result instanceof EntityNotFoundException) { // bad disk store name...
      throw (EntityNotFoundException) result;
    } else { // unknown and unexpected return type...
      final Throwable cause = (result instanceof Throwable ? (Throwable) result : null);

      throw new RuntimeException(
          CliStrings.format(CliStrings.UNEXPECTED_RETURN_TYPE_EXECUTING_COMMAND_ERROR_MESSAGE,
              ClassUtils.getClassName(result), CliStrings.DESCRIBE_DISK_STORE),
          cause);
    }
  }

  private ResultModel toResultModel(final DiskStoreDetails diskStoreDetails) {
    ResultModel result = new ResultModel();

    DataResultModel diskStoreSection = result.addData(DISK_STORE_SECTION);

    diskStoreSection.addData("Disk Store ID", diskStoreDetails.getId());
    diskStoreSection.addData("Disk Store Name", diskStoreDetails.getName());
    diskStoreSection.addData("Member ID", diskStoreDetails.getMemberId());
    diskStoreSection.addData("Member Name", diskStoreDetails.getMemberName());
    diskStoreSection.addData("Allow Force Compaction",
        diskStoreDetails.isAllowForceCompaction() ? "Yes" : "No");
    diskStoreSection.addData("Auto Compaction", diskStoreDetails.isAutoCompact() ? "Yes" : "No");
    diskStoreSection.addData("Compaction Threshold", diskStoreDetails.getCompactionThreshold());
    diskStoreSection.addData("Max Oplog Size", diskStoreDetails.getMaxOplogSize());
    diskStoreSection.addData("Queue Size", diskStoreDetails.getQueueSize());
    diskStoreSection.addData("Time Interval", diskStoreDetails.getTimeInterval());
    diskStoreSection.addData("Write Buffer Size", diskStoreDetails.getWriteBufferSize());
    diskStoreSection.addData("Disk Usage Warning Percentage",
        diskStoreDetails.getDiskUsageWarningPercentage());
    diskStoreSection.addData("Disk Usage Critical Percentage",
        diskStoreDetails.getDiskUsageCriticalPercentage());
    diskStoreSection.addData("PDX Serialization Meta-Data Stored",
        diskStoreDetails.isPdxSerializationMetaDataStored() ? "Yes" : "No");

    final TabularResultModel diskDirTable = result.addTable(DISK_DIR_SECTION);

    for (DiskStoreDetails.DiskDirDetails diskDirDetails : diskStoreDetails) {
      diskDirTable.accumulate("Disk Directory", diskDirDetails.getAbsolutePath());
      diskDirTable.accumulate("Size", Integer.toString(diskDirDetails.getSize()));
    }

    final TabularResultModel regionTable = result.addTable(REGION_SECTION);

    for (DiskStoreDetails.RegionDetails regionDetails : diskStoreDetails.iterateRegions()) {
      regionTable.accumulate("Region Path", regionDetails.getFullPath());
      regionTable.accumulate("Region Name", regionDetails.getName());
      regionTable.accumulate("Persistent", regionDetails.isPersistent() ? "Yes" : "No");
      regionTable.accumulate("Overflow To Disk", regionDetails.isOverflowToDisk() ? "Yes" : "No");
    }

    final TabularResultModel cacheServerTable = result.addTable(CACHE_SERVER_SECTION);

    for (DiskStoreDetails.CacheServerDetails cacheServerDetails : diskStoreDetails
        .iterateCacheServers()) {
      cacheServerTable.accumulate("Bind Address", cacheServerDetails.getBindAddress());
      cacheServerTable.accumulate("Hostname for Clients", cacheServerDetails.getHostName());
      cacheServerTable.accumulate("Port", Integer.toString(cacheServerDetails.getPort()));
    }

    final TabularResultModel gatewayTable = result.addTable(GATEWAY_SECTION);

    for (DiskStoreDetails.GatewayDetails gatewayDetails : diskStoreDetails.iterateGateways()) {
      gatewayTable.accumulate("Gateway ID", gatewayDetails.getId());
      gatewayTable.accumulate("Persistent", gatewayDetails.isPersistent() ? "Yes" : "No");
    }

    final TabularResultModel asyncEventQueueTable = result.addTable(ASYNC_EVENT_QUEUE_SECTION);

    for (DiskStoreDetails.AsyncEventQueueDetails asyncEventQueueDetails : diskStoreDetails
        .iterateAsyncEventQueues()) {
      asyncEventQueueTable.accumulate("Async Event Queue ID", asyncEventQueueDetails.getId());
    }

    return result;
  }
}
