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

public class StatusGatewaySenderCommand extends SingleGfshCommand {
  @CliCommand(value = CliStrings.STATUS_GATEWAYSENDER, help = CliStrings.STATUS_GATEWAYSENDER__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_WAN)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public ResultModel statusGatewaySender(@CliOption(key = CliStrings.STATUS_GATEWAYSENDER__ID,
      mandatory = true, optionContext = ConverterHint.GATEWAY_SENDER_ID,
      help = CliStrings.STATUS_GATEWAYSENDER__ID__HELP) String senderId,

      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.STATUS_GATEWAYSENDER__GROUP__HELP) String[] onGroup,

      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.STATUS_GATEWAYSENDER__MEMBER__HELP) String[] onMember) {

    if (senderId != null) {
      senderId = senderId.trim();
    }

    Cache cache = getCache();
    SystemManagementService service = getManagementService();

    GatewaySenderMXBean bean;

    ResultModel crd = new ResultModel();
    Set<DistributedMember> dsMembers = findMembers(onGroup, onMember);

    if (dsMembers.isEmpty()) {
      return ResultModel.createError(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }

    TabularResultModel availableSenderData =
        crd.addTable(CliStrings.SECTION_GATEWAY_SENDER_AVAILABLE);

    TabularResultModel notAvailableSenderData =
        crd.addTable(CliStrings.SECTION_GATEWAY_SENDER_NOT_AVAILABLE);

    for (DistributedMember member : dsMembers) {
      if (cache.getDistributedSystem().getDistributedMember().getId().equals(member.getId())) {
        bean = service.getLocalGatewaySenderMXBean(senderId);
      } else {
        ObjectName objectName = service.getGatewaySenderMBeanName(member, senderId);
        bean = service.getMBeanProxy(objectName, GatewaySenderMXBean.class);
      }
      if (bean != null) {
        buildSenderStatus(member.getId(), bean, availableSenderData);
      } else {
        buildSenderStatus(member.getId(), bean, notAvailableSenderData);
      }
    }

    return crd;
  }

  private TabularResultModel buildSenderStatus(String memberId, GatewaySenderMXBean bean,
      TabularResultModel resultData) {
    resultData.accumulate(CliStrings.RESULT_HOST_MEMBER, memberId);
    if (bean != null) {
      resultData.accumulate(CliStrings.RESULT_TYPE,
          bean.isParallel() ? CliStrings.SENDER_PARALLEL : CliStrings.SENDER_SERIAL);
      if (!bean.isParallel()) {
        resultData.accumulate(CliStrings.RESULT_POLICY,
            bean.isPrimary() ? CliStrings.SENDER_PRIMARY : CliStrings.SENDER_SECONADRY);
      }
      if (bean.isRunning()) {
        if (bean.isPaused()) {
          resultData.accumulate(CliStrings.RESULT_STATUS, CliStrings.SENDER_PAUSED);
        } else {
          resultData.accumulate(CliStrings.RESULT_STATUS, CliStrings.GATEWAY_RUNNING);
        }
      } else {
        resultData.accumulate(CliStrings.RESULT_STATUS, CliStrings.GATEWAY_NOT_RUNNING);
      }
    } else {
      resultData.accumulate(CliStrings.GATEWAY_ERROR, CliStrings.GATEWAY_SENDER_IS_NOT_AVAILABLE);
    }

    return resultData;
  }
}
