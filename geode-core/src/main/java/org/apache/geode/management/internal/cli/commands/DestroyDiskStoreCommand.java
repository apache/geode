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

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.DestroyDiskStoreFunction;
import org.apache.geode.management.internal.cli.functions.DestroyDiskStoreFunctionArgs;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class DestroyDiskStoreCommand extends SingleGfshCommand {
  @CliCommand(value = CliStrings.DESTROY_DISK_STORE, help = CliStrings.DESTROY_DISK_STORE__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DISKSTORE})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.DISK)
  public ResultModel destroyDiskStore(
      @CliOption(key = CliStrings.DESTROY_DISK_STORE__NAME, mandatory = true,
          help = CliStrings.DESTROY_DISK_STORE__NAME__HELP) String name,
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          help = CliStrings.DESTROY_DISK_STORE__GROUP__HELP,
          optionContext = ConverterHint.MEMBERGROUP) String[] groups,
      @CliOption(key = CliStrings.IFEXISTS, help = CliStrings.IFEXISTS_HELP,
          specifiedDefaultValue = "true", unspecifiedDefaultValue = "false") boolean ifExist) {

    Set<DistributedMember> targetMembers = findMembers(groups, null);

    if (targetMembers.isEmpty()) {
      return ResultModel.createError(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }

    DestroyDiskStoreFunctionArgs functionArgs = new DestroyDiskStoreFunctionArgs(name, ifExist);

    List<CliFunctionResult> results =
        executeAndGetFunctionResult(new DestroyDiskStoreFunction(), functionArgs, targetMembers);

    ResultModel result = ResultModel.createMemberStatusResult(results, ifExist);
    result.setConfigObject(name);

    return result;
  }

  @Override
  public void updateClusterConfig(String group, CacheConfig config, Object configObject) {
    String diskStoreName = (String) configObject;
    CacheElement.removeElement(config.getDiskStores(), diskStoreName);
  }
}
