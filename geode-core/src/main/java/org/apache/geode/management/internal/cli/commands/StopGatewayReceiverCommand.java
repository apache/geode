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
import org.apache.geode.management.GatewayReceiverMXBean;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class StopGatewayReceiverCommand extends SingleGfshCommand {
  @CliCommand(value = CliStrings.STOP_GATEWAYRECEIVER, help = CliStrings.STOP_GATEWAYRECEIVER__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_WAN)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.GATEWAY)
  public ResultModel stopGatewayReceiver(@CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
      optionContext = ConverterHint.MEMBERGROUP,
      help = CliStrings.STOP_GATEWAYRECEIVER__GROUP__HELP) String[] onGroup,

      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.STOP_GATEWAYRECEIVER__MEMBER__HELP) String[] onMember)
      throws Exception {

    SystemManagementService service = getManagementService();

    GatewayReceiverMXBean receiverBean;

    Set<DistributedMember> dsMembers = findMembers(onGroup, onMember);
    if (dsMembers.isEmpty()) {
      return ResultModel.createError(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }

    ResultModel resultModel = new ResultModel();
    TabularResultModel resultData = resultModel.addTable(CliStrings.STOP_GATEWAYRECEIVER);
    for (DistributedMember member : dsMembers) {
      ObjectName gatewayReceiverObjectName = MBeanJMXAdapter.getGatewayReceiverMBeanName(member);

      if (gatewayReceiverObjectName != null) {
        receiverBean =
            service.getMBeanProxy(gatewayReceiverObjectName, GatewayReceiverMXBean.class);
        if (receiverBean != null) {
          if (receiverBean.isRunning()) {
            receiverBean.stop();
            resultData.addMemberStatusResultRow(member.getId(),
                CliStrings.GATEWAY_OK,
                CliStrings.format(CliStrings.GATEWAY_RECEIVER_IS_STOPPED_ON_MEMBER_0,
                    new Object[] {member.getId()}));
          } else {
            resultData.addMemberStatusResultRow(member.getId(),
                CliStrings.GATEWAY_ERROR,
                CliStrings.format(CliStrings.GATEWAY_RECEIVER_IS_NOT_RUNNING_ON_MEMBER_0,
                    new Object[] {member.getId()}));
          }
        } else {
          resultData.addMemberStatusResultRow(member.getId(),
              CliStrings.GATEWAY_ERROR,
              CliStrings.format(CliStrings.GATEWAY_RECEIVER_IS_NOT_AVAILABLE_ON_MEMBER_0,
                  new Object[] {member.getId()}));
        }
      } else {
        resultData.addMemberStatusResultRow(member.getId(),
            CliStrings.GATEWAY_ERROR,
            CliStrings.format(CliStrings.GATEWAY_RECEIVER_IS_NOT_AVAILABLE_ON_MEMBER_0,
                new Object[] {member.getId()}));
      }
    }

    return resultModel;
  }
}
