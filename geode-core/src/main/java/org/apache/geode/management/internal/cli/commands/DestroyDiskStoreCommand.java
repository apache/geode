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

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.DestroyDiskStoreFunction;
import org.apache.geode.management.internal.cli.functions.DestroyDiskStoreFunctionArgs;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class DestroyDiskStoreCommand implements GfshCommand {
  @CliCommand(value = CliStrings.DESTROY_DISK_STORE, help = CliStrings.DESTROY_DISK_STORE__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DISKSTORE})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.DISK)
  public Result destroyDiskStore(
      @CliOption(key = CliStrings.DESTROY_DISK_STORE__NAME, mandatory = true,
          help = CliStrings.DESTROY_DISK_STORE__NAME__HELP) String name,
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          help = CliStrings.DESTROY_DISK_STORE__GROUP__HELP,
          optionContext = ConverterHint.MEMBERGROUP) String[] groups,
      @CliOption(key = CliStrings.IFEXISTS, help = CliStrings.IFEXISTS_HELP,
          specifiedDefaultValue = "true", unspecifiedDefaultValue = "false") boolean ifExist) {

    TabularResultData tabularData = ResultBuilder.createTabularResultData();

    Set<DistributedMember> targetMembers = findMembers(groups, null);

    if (targetMembers.isEmpty()) {
      return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }

    DestroyDiskStoreFunctionArgs functionArgs = new DestroyDiskStoreFunctionArgs(name, ifExist);

    List<CliFunctionResult> results =
        executeAndGetFunctionResult(new DestroyDiskStoreFunction(), functionArgs, targetMembers);

    AtomicReference<XmlEntity> xmlEntity = new AtomicReference<>();
    for (CliFunctionResult result : results) {
      if (result.isSuccessful()) {
        if (xmlEntity.get() == null) {
          xmlEntity.set(result.getXmlEntity());
        }
      } else {
        tabularData.setStatus(Result.Status.ERROR);
      }
      tabularData.accumulate("Member", result.getMemberIdOrName());
      tabularData.accumulate("Result", result.getMessage());
    }

    Result result = ResultBuilder.buildResult(tabularData);

    if (xmlEntity.get() != null) {
      persistClusterConfiguration(result,
          () -> getSharedConfiguration().deleteXmlEntity(xmlEntity.get(), groups));
    }

    return result;
  }
}
