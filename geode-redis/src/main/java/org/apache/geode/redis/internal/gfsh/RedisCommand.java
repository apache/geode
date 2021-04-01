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

package org.apache.geode.redis.internal.gfsh;

import java.util.List;
import java.util.Set;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class RedisCommand extends GfshCommand {
  public static final String REDIS = "redis";
  public static final String REDIS__HELP =
      "Commands related to the Geode APIs compatible with Redis.";
  public static final String REDIS__ENABLE_UNSUPPORTED_COMMANDS = "enable-unsupported-commands";
  public static final String REDIS__ENABLE_UNSUPPORTED_COMMANDS__HELP =
      "Boolean value to determine "
          + "whether or not to enable unsupported commands. Unsupported commands have not been fully tested.";

  @CliCommand(value = {REDIS}, help = REDIS__HELP)
  @ResourceOperation(resource = ResourcePermission.Resource.DATA,
      operation = ResourcePermission.Operation.MANAGE)
  public ResultModel enableUnsupportedCommands(
      @CliOption(key = REDIS__ENABLE_UNSUPPORTED_COMMANDS,
          unspecifiedDefaultValue = "false", specifiedDefaultValue = "true",
          help = REDIS__ENABLE_UNSUPPORTED_COMMANDS__HELP) Boolean enableUnsupportedCommands) {

    Set<DistributedMember> members = getAllNormalMembers();

    List<CliFunctionResult> results =
        executeAndGetFunctionResult(new RedisCommandFunction(), enableUnsupportedCommands, members);

    return ResultModel.createMemberStatusResult(results);
  }
}
