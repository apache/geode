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

import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class StopGatewaySenderCommand extends GfshCommand {
  private Supplier<StopGatewaySenderCommandDelegate> commandDelegateSupplier;
  private BiFunction<String[], String[], Set<DistributedMember>> findMembers;

  @SuppressWarnings("unused") // invoked by spring shell
  public StopGatewaySenderCommand() {
    this(null, null);
    findMembers = this::findMembers;
    commandDelegateSupplier = this::getCommandDelegate;
  }

  StopGatewaySenderCommand(Supplier<StopGatewaySenderCommandDelegate> commandDelegateSupplier,
      BiFunction<String[], String[], Set<DistributedMember>> findMembers) {
    this.commandDelegateSupplier = commandDelegateSupplier;
    this.findMembers = findMembers;
  }

  @SuppressWarnings("unused") // invoked by spring shell
  @CliCommand(value = CliStrings.STOP_GATEWAYSENDER, help = CliStrings.STOP_GATEWAYSENDER__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_WAN)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.GATEWAY)
  public ResultModel stopGatewaySender(@CliOption(key = CliStrings.STOP_GATEWAYSENDER__ID,
      mandatory = true, optionContext = ConverterHint.GATEWAY_SENDER_ID,
      help = CliStrings.STOP_GATEWAYSENDER__ID__HELP) String senderId,

      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.STOP_GATEWAYSENDER__GROUP__HELP) String[] onGroup,

      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.STOP_GATEWAYSENDER__MEMBER__HELP) String[] onMember) {

    Set<DistributedMember> dsMembers = findMembers.apply(onGroup, onMember);

    if (dsMembers.isEmpty()) {
      return ResultModel.createError(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }

    return commandDelegateSupplier.get().executeStopGatewaySender(senderId.trim(), getCache(),
        dsMembers);
  }

  private StopGatewaySenderCommandDelegate getCommandDelegate() {
    return new StopGatewaySenderCommandDelegateParallelImpl(getManagementService());
  }

  @FunctionalInterface
  interface StopGatewaySenderCommandDelegate {
    ResultModel executeStopGatewaySender(String id, Cache cache, Set<DistributedMember> dsMembers);
  }

}
