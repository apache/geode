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

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import javax.management.ObjectName;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.GatewayReceiverMXBean;
import org.apache.geode.management.GatewaySenderMXBean;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class ListGatewayCommand extends InternalGfshCommand {
  @CliCommand(value = CliStrings.LIST_GATEWAY, help = CliStrings.LIST_GATEWAY__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_WAN)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public Result listGateway(
      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.LIST_GATEWAY__MEMBER__HELP) String[] onMember,
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.LIST_GATEWAY__GROUP__HELP) String[] onGroup)
      throws Exception {

    Result result;
    SystemManagementService service = (SystemManagementService) getManagementService();

    Set<DistributedMember> dsMembers = findMembers(onGroup, onMember);

    if (dsMembers.isEmpty()) {
      return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }

    Map<String, Map<String, GatewaySenderMXBean>> gatewaySenderBeans = new TreeMap<>();
    Map<String, GatewayReceiverMXBean> gatewayReceiverBeans = new TreeMap<>();

    DistributedSystemMXBean dsMXBean = service.getDistributedSystemMXBean();
    for (DistributedMember member : dsMembers) {
      String memberName = member.getName();
      String memberNameOrId =
          (memberName != null && !memberName.isEmpty()) ? memberName : member.getId();
      ObjectName gatewaySenderObjectNames[] = dsMXBean.listGatewaySenderObjectNames(memberNameOrId);
      // gateway senders : a member can have multiple gateway senders defined
      // on it
      if (gatewaySenderObjectNames != null) {
        for (ObjectName name : gatewaySenderObjectNames) {
          GatewaySenderMXBean senderBean = service.getMBeanProxy(name, GatewaySenderMXBean.class);
          if (senderBean != null) {
            if (gatewaySenderBeans.containsKey(senderBean.getSenderId())) {
              Map<String, GatewaySenderMXBean> memberToBeanMap =
                  gatewaySenderBeans.get(senderBean.getSenderId());
              memberToBeanMap.put(member.getId(), senderBean);
            } else {
              Map<String, GatewaySenderMXBean> memberToBeanMap = new TreeMap<>();
              memberToBeanMap.put(member.getId(), senderBean);
              gatewaySenderBeans.put(senderBean.getSenderId(), memberToBeanMap);
            }
          }
        }
      }
      // gateway receivers : a member can have only one gateway receiver
      ObjectName gatewayReceiverObjectName = MBeanJMXAdapter.getGatewayReceiverMBeanName(member);
      if (gatewayReceiverObjectName != null) {
        GatewayReceiverMXBean receiverBean;
        receiverBean =
            service.getMBeanProxy(gatewayReceiverObjectName, GatewayReceiverMXBean.class);
        if (receiverBean != null) {
          gatewayReceiverBeans.put(member.getId(), receiverBean);
        }
      }
    }
    if (gatewaySenderBeans.isEmpty() && gatewayReceiverBeans.isEmpty()) {
      return ResultBuilder.createUserErrorResult(CliStrings.GATEWAYS_ARE_NOT_AVAILABLE_IN_CLUSTER);
    }
    CompositeResultData crd = ResultBuilder.createCompositeResultData();
    crd.setHeader(CliStrings.HEADER_GATEWAYS);
    accumulateListGatewayResult(crd, gatewaySenderBeans, gatewayReceiverBeans);
    result = ResultBuilder.buildResult(crd);

    return result;
  }

  private void accumulateListGatewayResult(CompositeResultData crd,
      Map<String, Map<String, GatewaySenderMXBean>> gatewaySenderBeans,
      Map<String, GatewayReceiverMXBean> gatewayReceiverBeans) {
    if (!gatewaySenderBeans.isEmpty()) {
      TabularResultData gatewaySenderData = crd.addSection(CliStrings.SECTION_GATEWAY_SENDER)
          .addTable(CliStrings.TABLE_GATEWAY_SENDER).setHeader(CliStrings.HEADER_GATEWAY_SENDER);
      for (Map.Entry<String, Map<String, GatewaySenderMXBean>> entry : gatewaySenderBeans
          .entrySet()) {
        for (Map.Entry<String, GatewaySenderMXBean> memberToBean : entry.getValue().entrySet()) {
          gatewaySenderData.accumulate(CliStrings.RESULT_GATEWAY_SENDER_ID, entry.getKey());
          gatewaySenderData.accumulate(CliStrings.RESULT_HOST_MEMBER, memberToBean.getKey());
          gatewaySenderData.accumulate(CliStrings.RESULT_REMOTE_CLUSTER,
              memberToBean.getValue().getRemoteDSId());
          gatewaySenderData.accumulate(CliStrings.RESULT_TYPE, memberToBean.getValue().isParallel()
              ? CliStrings.SENDER_PARALLEL : CliStrings.SENDER_SERIAL);
          gatewaySenderData.accumulate(CliStrings.RESULT_STATUS, memberToBean.getValue().isRunning()
              ? CliStrings.GATEWAY_RUNNING : CliStrings.GATEWAY_NOT_RUNNING);
          gatewaySenderData.accumulate(CliStrings.RESULT_QUEUED_EVENTS,
              memberToBean.getValue().getEventQueueSize());
          gatewaySenderData.accumulate(CliStrings.RESULT_RECEIVER,
              memberToBean.getValue().getGatewayReceiver());
        }
      }
    }

    if (!gatewayReceiverBeans.isEmpty()) {
      TabularResultData gatewayReceiverData = crd.addSection(CliStrings.SECTION_GATEWAY_RECEIVER)
          .addTable(CliStrings.TABLE_GATEWAY_RECEIVER)
          .setHeader(CliStrings.HEADER_GATEWAY_RECEIVER);
      for (Map.Entry<String, GatewayReceiverMXBean> entry : gatewayReceiverBeans.entrySet()) {
        gatewayReceiverData.accumulate(CliStrings.RESULT_HOST_MEMBER, entry.getKey());
        gatewayReceiverData.accumulate(CliStrings.RESULT_PORT, entry.getValue().getPort());
        gatewayReceiverData.accumulate(CliStrings.RESULT_SENDERS_COUNT,
            entry.getValue().getClientConnectionCount());
        gatewayReceiverData.accumulate(CliStrings.RESULT_SENDER_CONNECTED,
            entry.getValue().getConnectedGatewaySenders());
      }
    }
  }
}
