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
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class PauseGatewaySenderCommand implements GfshCommand {

  @CliCommand(value = CliStrings.PAUSE_GATEWAYSENDER, help = CliStrings.PAUSE_GATEWAYSENDER__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_WAN)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.GATEWAY)
  public Result pauseGatewaySender(@CliOption(key = CliStrings.PAUSE_GATEWAYSENDER__ID,
      mandatory = true, optionContext = ConverterHint.GATEWAY_SENDER_ID,
      help = CliStrings.PAUSE_GATEWAYSENDER__ID__HELP) String senderId,

      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.PAUSE_GATEWAYSENDER__GROUP__HELP) String[] onGroup,

      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.PAUSE_GATEWAYSENDER__MEMBER__HELP) String[] onMember) {

    Result result;
    if (senderId != null) {
      senderId = senderId.trim();
    }

    try {
      InternalCache cache = getCache();
      SystemManagementService service =
          (SystemManagementService) ManagementService.getExistingManagementService(cache);

      GatewaySenderMXBean bean;

      TabularResultData resultData = ResultBuilder.createTabularResultData();

      Set<DistributedMember> dsMembers = CliUtil.findMembers(onGroup, onMember);

      if (dsMembers.isEmpty()) {
        return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
      }

      for (DistributedMember member : dsMembers) {
        if (cache.getDistributedSystem().getDistributedMember().getId().equals(member.getId())) {
          bean = service.getLocalGatewaySenderMXBean(senderId);
        } else {
          ObjectName objectName = service.getGatewaySenderMBeanName(member, senderId);
          bean = service.getMBeanProxy(objectName, GatewaySenderMXBean.class);
        }
        if (bean != null) {
          if (bean.isRunning()) {
            if (bean.isPaused()) {
              GatewayCommandsUtils.accumulateStartResult(resultData, member.getId(),
                  CliStrings.GATEWAY_ERROR,
                  CliStrings.format(CliStrings.GATEWAY_SENDER_0_IS_ALREADY_PAUSED_ON_MEMBER_1,
                      senderId, member.getId()));
            } else {
              bean.pause();
              GatewayCommandsUtils.accumulateStartResult(resultData, member.getId(),
                  CliStrings.GATEWAY_OK, CliStrings.format(
                      CliStrings.GATEWAY_SENDER_0_IS_PAUSED_ON_MEMBER_1, senderId, member.getId()));
            }
          } else {
            GatewayCommandsUtils.accumulateStartResult(resultData, member.getId(),
                CliStrings.GATEWAY_ERROR,
                CliStrings.format(CliStrings.GATEWAY_SENDER_0_IS_NOT_RUNNING_ON_MEMBER_1, senderId,
                    member.getId()));
          }
        } else {
          GatewayCommandsUtils.accumulateStartResult(resultData, member.getId(),
              CliStrings.GATEWAY_ERROR,
              CliStrings.format(CliStrings.GATEWAY_SENDER_0_IS_NOT_AVAILABLE_ON_MEMBER_1, senderId,
                  member.getId()));
        }
      }
      result = ResultBuilder.buildResult(resultData);
    } catch (Exception e) {
      LogWrapper.getInstance().warning(CliStrings.GATEWAY_ERROR + CliUtil.stackTraceAsString(e));
      result = ResultBuilder.createGemFireErrorResult(CliStrings.GATEWAY_ERROR + e.getMessage());
    }
    return result;
  }
}
