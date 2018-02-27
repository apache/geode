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

import javax.management.ObjectName;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.GatewaySenderMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class LoadBalanceGatewaySenderCommand implements GfshCommand {

  @CliCommand(value = CliStrings.LOAD_BALANCE_GATEWAYSENDER,
      help = CliStrings.LOAD_BALANCE_GATEWAYSENDER__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_WAN)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.GATEWAY)
  public Result loadBalanceGatewaySender(@CliOption(key = CliStrings.LOAD_BALANCE_GATEWAYSENDER__ID,
      mandatory = true, optionContext = ConverterHint.GATEWAY_SENDER_ID,
      help = CliStrings.LOAD_BALANCE_GATEWAYSENDER__ID__HELP) String senderId) {

    Result result;
    if (senderId != null) {
      senderId = senderId.trim();
    }

    InternalCache cache = getCache();
    SystemManagementService service =
        (SystemManagementService) ManagementService.getExistingManagementService(cache);
    TabularResultData resultData = ResultBuilder.createTabularResultData();
    Set<DistributedMember> dsMembers = getAllNormalMembers(cache);

    if (dsMembers.isEmpty()) {
      result = ResultBuilder.createInfoResult(CliStrings.GATEWAY_MSG_MEMBERS_NOT_FOUND);
    } else {
      boolean gatewaySenderExists = false;
      for (DistributedMember member : dsMembers) {
        GatewaySenderMXBean bean;
        if (cache.getDistributedSystem().getDistributedMember().getId().equals(member.getId())) {
          bean = service.getLocalGatewaySenderMXBean(senderId);
        } else {
          ObjectName objectName = service.getGatewaySenderMBeanName(member, senderId);
          bean = service.getMBeanProxy(objectName, GatewaySenderMXBean.class);
        }
        if (bean != null) {
          gatewaySenderExists = true;
          bean.rebalance();
          GatewayCommandsUtils.accumulateStartResult(resultData, member.getId(),
              CliStrings.GATEWAY_OK, CliStrings.format(
                  CliStrings.GATEWAY_SENDER_0_IS_REBALANCED_ON_MEMBER_1, senderId, member.getId()));
        } else {
          GatewayCommandsUtils.accumulateStartResult(resultData, member.getId(),
              CliStrings.GATEWAY_ERROR,
              CliStrings.format(CliStrings.GATEWAY_SENDER_0_IS_NOT_AVAILABLE_ON_MEMBER_1, senderId,
                  member.getId()));
        }
      }
      if (gatewaySenderExists) {
        result = ResultBuilder.buildResult(resultData);
      } else {
        result = ResultBuilder.createInfoResult(CliStrings.format(
            CliStrings.GATEWAY_SENDER_0_IS_NOT_FOUND_ON_ANY_MEMBER, new Object[] {senderId}));
      }
    }

    return result;
  }
}
