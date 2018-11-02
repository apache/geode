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

import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.GatewaySenderMXBean;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class ResumeGatewaySenderCommand extends SingleGfshCommand {

  @CliCommand(value = CliStrings.RESUME_GATEWAYSENDER, help = CliStrings.RESUME_GATEWAYSENDER__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_WAN)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.GATEWAY)
  public ResultModel resumeGatewaySender(@CliOption(key = CliStrings.RESUME_GATEWAYSENDER__ID,
      mandatory = true, optionContext = ConverterHint.GATEWAY_SENDER_ID,
      help = CliStrings.RESUME_GATEWAYSENDER__ID__HELP) String senderId,

      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.RESUME_GATEWAYSENDER__GROUP__HELP) String[] onGroup,
      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.RESUME_GATEWAYSENDER__MEMBER__HELP) String[] onMember) {

    if (senderId != null) {
      senderId = senderId.trim();
    }

    final Cache cache = getCache();
    final SystemManagementService service = getManagementService();

    Set<DistributedMember> dsMembers = findMembers(onGroup, onMember);
    if (dsMembers.isEmpty()) {
      return ResultModel.createError(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }

    GatewaySenderMXBean bean;
    ResultModel resultModel = new ResultModel();
    TabularResultModel resultData = resultModel.addTable(CliStrings.RESUME_GATEWAYSENDER);
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
            bean.resume();
            resultData.addMemberStatusResultRow(member.getId(),
                CliStrings.GATEWAY_OK, CliStrings.format(
                    CliStrings.GATEWAY_SENDER_0_IS_RESUMED_ON_MEMBER_1, senderId, member.getId()));
          } else {
            resultData.addMemberStatusResultRow(member.getId(),
                CliStrings.GATEWAY_ERROR,
                CliStrings.format(CliStrings.GATEWAY_SENDER_0_IS_NOT_PAUSED_ON_MEMBER_1, senderId,
                    member.getId()));
          }
        } else {
          resultData.addMemberStatusResultRow(member.getId(),
              CliStrings.GATEWAY_ERROR,
              CliStrings.format(CliStrings.GATEWAY_SENDER_0_IS_NOT_RUNNING_ON_MEMBER_1, senderId,
                  member.getId()));
        }
      } else {
        resultData.addMemberStatusResultRow(member.getId(),
            CliStrings.GATEWAY_ERROR,
            CliStrings.format(CliStrings.GATEWAY_SENDER_0_IS_NOT_AVAILABLE_ON_MEMBER_1, senderId,
                member.getId()));
      }
    }

    return resultModel;
  }
}
