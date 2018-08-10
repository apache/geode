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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.springframework.shell.core.annotation.CliCommand;

import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.AbstractExecution;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.internal.cli.domain.DiskStoreDetails;
import org.apache.geode.management.internal.cli.functions.ListDiskStoresFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultDataException;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class ListDiskStoresCommand extends InternalGfshCommand {
  @CliCommand(value = CliStrings.LIST_DISK_STORE, help = CliStrings.LIST_DISK_STORE__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DISKSTORE})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public ResultModel listDiskStores() {
    Set<DistributedMember> dataMembers =
        DiskStoreCommandsUtils.getNormalMembers((InternalCache) getCache());

    if (dataMembers.isEmpty()) {
      return ResultModel.createInfo(CliStrings.NO_CACHING_MEMBERS_FOUND_MESSAGE);
    }

    return toTabularResult(getDiskStoreListing(dataMembers));
  }

  @SuppressWarnings("unchecked")
  List<DiskStoreDetails> getDiskStoreListing(Set<DistributedMember> members) {
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

  private ResultModel toTabularResult(final List<DiskStoreDetails> diskStoreList)
      throws ResultDataException {
    if (!diskStoreList.isEmpty()) {
      ResultModel result = new ResultModel();
      TabularResultModel diskStoreData =
          result.addTable(DescribeDiskStoreCommand.DISK_STORE_SECTION);

      for (final DiskStoreDetails diskStoreDetails : diskStoreList) {
        diskStoreData.accumulate("Member Name", diskStoreDetails.getMemberName());
        diskStoreData.accumulate("Member Id", diskStoreDetails.getMemberId());
        diskStoreData.accumulate("Disk Store Name", diskStoreDetails.getName());
        diskStoreData.accumulate("Disk Store ID", diskStoreDetails.getId().toString());
      }

      return result;
    } else {
      return ResultModel.createInfo(CliStrings.LIST_DISK_STORE__DISK_STORES_NOT_FOUND_MESSAGE);
    }
  }
}
