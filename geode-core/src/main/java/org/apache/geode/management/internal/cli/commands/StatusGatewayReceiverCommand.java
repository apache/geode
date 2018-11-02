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

public class StatusGatewayReceiverCommand extends SingleGfshCommand {
  @CliCommand(value = CliStrings.STATUS_GATEWAYRECEIVER,
      help = CliStrings.STATUS_GATEWAYRECEIVER__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_WAN)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public ResultModel statusGatewayReceiver(@CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
      optionContext = ConverterHint.MEMBERGROUP,
      help = CliStrings.STATUS_GATEWAYRECEIVER__GROUP__HELP) String[] onGroup,

      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.STATUS_GATEWAYRECEIVER__MEMBER__HELP) String[] onMember) {

    SystemManagementService service = getManagementService();

    Set<DistributedMember> dsMembers = findMembers(onGroup, onMember);
    if (dsMembers.isEmpty()) {
      return ResultModel.createError(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }

    ResultModel crd = new ResultModel();
    TabularResultModel availableReceiverData =
        crd.addTable(CliStrings.SECTION_GATEWAY_RECEIVER_AVAILABLE);

    TabularResultModel notAvailableReceiverData =
        crd.addTable(CliStrings.SECTION_GATEWAY_RECEIVER_NOT_AVAILABLE);

    for (DistributedMember member : dsMembers) {
      ObjectName gatewayReceiverObjectName = MBeanJMXAdapter.getGatewayReceiverMBeanName(member);
      if (gatewayReceiverObjectName != null) {
        GatewayReceiverMXBean receiverBean =
            service.getMBeanProxy(gatewayReceiverObjectName, GatewayReceiverMXBean.class);
        if (receiverBean != null) {
          buildReceiverStatus(member.getId(), receiverBean, availableReceiverData);
          continue;
        }
      }
      buildReceiverStatus(member.getId(), null, notAvailableReceiverData);
    }

    return crd;
  }

  private TabularResultModel buildReceiverStatus(String memberId, GatewayReceiverMXBean bean,
      TabularResultModel resultData) {
    resultData.accumulate(CliStrings.RESULT_HOST_MEMBER, memberId);
    if (bean != null) {
      resultData.accumulate(CliStrings.RESULT_PORT, Integer.toString(bean.getPort()));
      resultData.accumulate(CliStrings.RESULT_STATUS,
          bean.isRunning() ? CliStrings.GATEWAY_RUNNING : CliStrings.GATEWAY_NOT_RUNNING);
    } else {
      resultData.accumulate(CliStrings.GATEWAY_ERROR,
          CliStrings.GATEWAY_RECEIVER_IS_NOT_AVAILABLE_OR_STOPPED);
    }
    return resultData;
  }
}
