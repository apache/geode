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
import org.apache.geode.management.GatewayReceiverMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class StartGatewayReceiverCommand implements GfshCommand {

  @CliCommand(value = CliStrings.START_GATEWAYRECEIVER,
      help = CliStrings.START_GATEWAYRECEIVER__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_WAN)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.GATEWAY)
  public Result startGatewayReceiver(@CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
      optionContext = ConverterHint.MEMBERGROUP,
      help = CliStrings.START_GATEWAYRECEIVER__GROUP__HELP) String[] onGroup,

      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.START_GATEWAYRECEIVER__MEMBER__HELP) String[] onMember)
      throws Exception {
    Result result;

    InternalCache cache = getCache();
    SystemManagementService service =
        (SystemManagementService) ManagementService.getExistingManagementService(cache);

    GatewayReceiverMXBean receiverBean;

    TabularResultData resultData = ResultBuilder.createTabularResultData();

    Set<DistributedMember> dsMembers = CliUtil.findMembers(onGroup, onMember, getCache());

    if (dsMembers.isEmpty()) {
      return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }

    for (DistributedMember member : dsMembers) {
      ObjectName gatewayReceiverObjectName = MBeanJMXAdapter.getGatewayReceiverMBeanName(member);

      if (gatewayReceiverObjectName != null) {
        receiverBean =
            service.getMBeanProxy(gatewayReceiverObjectName, GatewayReceiverMXBean.class);
        if (receiverBean != null) {
          if (receiverBean.isRunning()) {
            GatewayCommandsUtils.accumulateStartResult(resultData, member.getId(),
                CliStrings.GATEWAY_ERROR,
                CliStrings.format(CliStrings.GATEWAY_RECEIVER_IS_ALREADY_STARTED_ON_MEMBER_0,
                    new Object[] {member.getId()}));
          } else {
            receiverBean.start();
            GatewayCommandsUtils.accumulateStartResult(resultData, member.getId(),
                CliStrings.GATEWAY_OK,
                CliStrings.format(CliStrings.GATEWAY_RECEIVER_IS_STARTED_ON_MEMBER_0,
                    new Object[] {member.getId()}));
          }
        } else {
          GatewayCommandsUtils.accumulateStartResult(resultData, member.getId(),
              CliStrings.GATEWAY_ERROR,
              CliStrings.format(CliStrings.GATEWAY_RECEIVER_IS_NOT_AVAILABLE_ON_MEMBER_0,
                  new Object[] {member.getId()}));
        }
      } else {
        GatewayCommandsUtils.accumulateStartResult(resultData, member.getId(),
            CliStrings.GATEWAY_ERROR,
            CliStrings.format(CliStrings.GATEWAY_RECEIVER_IS_NOT_AVAILABLE_ON_MEMBER_0,
                new Object[] {member.getId()}));
      }
    }
    result = ResultBuilder.buildResult(resultData);

    return result;
  }
}
