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
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.DestroyGatewayReceiverFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class DestroyGatewayReceiverCommand extends SingleGfshCommand {
  public static final String DESTROY_GATEWAYRECEIVER = "destroy gateway-receiver";
  public static final String DESTROY_GATEWAYRECEIVER__HELP =
      "Destroy the Gateway Receiver on a member or members.";
  public static final String DESTROY_GATEWAYRECEIVER__GROUP__HELP =
      "Group(s) of members on which to destroy the Gateway Receiver.";
  public static final String DESTROY_GATEWAYRECEIVER__MEMBER__HELP =
      "Name/Id of the member on which to destroy the Gateway Receiver.";

  @CliCommand(value = DESTROY_GATEWAYRECEIVER, help = DESTROY_GATEWAYRECEIVER__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_WAN)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.GATEWAY)
  public ResultModel destroyGatewayReceiver(
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = DESTROY_GATEWAYRECEIVER__GROUP__HELP) String[] onGroups,
      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          optionContext = ConverterHint.MEMBERIDNAME,
          help = DESTROY_GATEWAYRECEIVER__MEMBER__HELP) String[] onMember,
      @CliOption(key = CliStrings.IFEXISTS, help = CliStrings.IFEXISTS_HELP,
          specifiedDefaultValue = "true", unspecifiedDefaultValue = "false") boolean ifExists) {

    boolean persisted = true;
    Set<DistributedMember> members = getMembers(onGroups, onMember);

    List<CliFunctionResult> functionResults =
        executeAndGetFunctionResult(DestroyGatewayReceiverFunction.INSTANCE, null, members);

    ResultModel result = ResultModel.createMemberStatusResult(functionResults, ifExists);
    result.setConfigObject(new CacheConfig.GatewayReceiver());


    return result;
  }

  @Override
  public boolean updateConfigForGroup(String group, CacheConfig config, Object element) {
    if (config.getGatewayReceiver() != null) {
      config.setGatewayReceiver(null);
      return true;
    }
    return false;
  }
}
