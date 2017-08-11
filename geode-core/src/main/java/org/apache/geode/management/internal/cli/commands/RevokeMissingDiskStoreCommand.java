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

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.SystemFailure;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class RevokeMissingDiskStoreCommand implements GfshCommand {
  @CliCommand(value = CliStrings.REVOKE_MISSING_DISK_STORE,
      help = CliStrings.REVOKE_MISSING_DISK_STORE__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DISKSTORE})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.DISK)
  public Result revokeMissingDiskStore(@CliOption(key = CliStrings.REVOKE_MISSING_DISK_STORE__ID,
      mandatory = true, help = CliStrings.REVOKE_MISSING_DISK_STORE__ID__HELP) String id) {

    try {
      DistributedSystemMXBean dsMXBean =
          ManagementService.getManagementService(getCache()).getDistributedSystemMXBean();
      if (dsMXBean.revokeMissingDiskStores(id)) {
        return ResultBuilder.createInfoResult("Missing disk store successfully revoked");
      }

      return ResultBuilder.createUserErrorResult("Unable to find missing disk store to revoke");
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable th) {
      SystemFailure.checkFailure();
      if (th.getMessage() == null) {
        return ResultBuilder.createGemFireErrorResult(
            "An error occurred while revoking missing disk stores: " + th);
      }
      return ResultBuilder.createGemFireErrorResult(
          "An error occurred while revoking missing disk stores: " + th.getMessage());
    }
  }
}
