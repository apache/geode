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
package org.apache.geode.connectors.jdbc.internal.cli;

import static org.apache.geode.connectors.jdbc.internal.cli.ListDriversCommand.NO_MEMBERS_FOUND;

import java.util.List;
import java.util.Set;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

@Experimental
public class RegisterDriverCommand extends GfshCommand {

  static final String REGISTER_DRIVER = "register driver";
  static final String REGISTER_DRIVER__HELP = EXPERIMENTAL
      + "Register a driver with the cluster's Driver Manager using the name of a driver class contained within a currenly deployed jar.";
  static final String DRIVER_CLASS_NAME = "driver-class";
  static final String DRIVER_CLASS_NAME_HELP =
      "The name of the driver class contained in a currently deployed jar to be registered with the cluster's Driver Manager.";

  @CliCommand(value = REGISTER_DRIVER, help = REGISTER_DRIVER__HELP)
  @CliMetaData(relatedTopic = CliStrings.DEFAULT_TOPIC_GEODE)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)
  public ResultModel registerDriver(
      @CliOption(key = DRIVER_CLASS_NAME, help = DRIVER_CLASS_NAME_HELP,
          mandatory = true) String driverClassName) {
    try {
      Set<DistributedMember> targetMembers = findMembers(null, null);

      if (targetMembers.size() > 0) {
        Object[] arguments = new Object[] {driverClassName};
        List<CliFunctionResult> registerDriverResults = executeAndGetFunctionResult(
            new RegisterDriverFunction(), arguments, targetMembers);
        return ResultModel.createMemberStatusResult(registerDriverResults, EXPERIMENTAL, null,
            false, true);
      } else {
        return ResultModel.createInfo(EXPERIMENTAL + "\n" + NO_MEMBERS_FOUND);
      }
    } catch (Exception ex) {
      return ResultModel
          .createError("Failed to register driver \"" + driverClassName + "\": " + ex.getMessage());
    }
  }
}
