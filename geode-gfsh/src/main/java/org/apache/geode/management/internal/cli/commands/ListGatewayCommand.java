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

import static java.util.stream.Collectors.joining;

import java.util.Arrays;
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
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class ListGatewayCommand extends GfshCommand {
  @CliCommand(value = CliStrings.LIST_GATEWAY, help = CliStrings.LIST_GATEWAY__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_WAN)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public ResultModel listGateway(
      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.LIST_GATEWAY__MEMBER__HELP) String[] onMember,
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.LIST_GATEWAY__GROUP__HELP) String[] onGroup,
      @CliOption(key = {CliStrings.LIST_GATEWAY__SHOW_RECEIVERS_ONLY},
          specifiedDefaultValue = "true", unspecifiedDefaultValue = "false",
          help = CliStrings.LIST_GATEWAY__SHOW_RECEIVERS_ONLY__HELP) boolean showReceiversOnly,
      @CliOption(key = {CliStrings.LIST_GATEWAY__SHOW_SENDERS_ONLY},
          specifiedDefaultValue = "true", unspecifiedDefaultValue = "false",
          help = CliStrings.LIST_GATEWAY__SHOW_SENDERS_ONLY__HELP) boolean showSendersOnly)

      throws Exception {

    if (showReceiversOnly && showSendersOnly) {
      return ResultModel.createError(CliStrings.LIST_GATEWAY__ERROR_ON_SHOW_PARAMETERS);
    }

    ResultModel result = new ResultModel();
    SystemManagementService service = getManagementService();

    Set<DistributedMember> dsMembers = findMembers(onGroup, onMember);

    if (dsMembers.isEmpty()) {
      return ResultModel.createInfo(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }

    Map<String, Map<String, GatewaySenderMXBean>> gatewaySenderBeans = new TreeMap<>();
    Map<String, GatewayReceiverMXBean> gatewayReceiverBeans = new TreeMap<>();

    DistributedSystemMXBean dsMXBean = service.getDistributedSystemMXBean();
    for (DistributedMember member : dsMembers) {
      String memberName = member.getName();
      String memberNameOrId =
          (memberName != null && !memberName.isEmpty()) ? memberName : member.getId();

      if (!showReceiversOnly) {
        ObjectName[] gatewaySenderObjectNames =
            dsMXBean.listGatewaySenderObjectNames(memberNameOrId);
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
      }
      // gateway receivers : a member can have only one gateway receiver
      if (!showSendersOnly) {
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
    }
    if (gatewaySenderBeans.isEmpty() && gatewayReceiverBeans.isEmpty()) {
      return ResultModel.createInfo(CliStrings.GATEWAYS_ARE_NOT_AVAILABLE_IN_CLUSTER);
    }

    accumulateListGatewayResult(result, gatewaySenderBeans, gatewayReceiverBeans);

    return result;
  }

  static String getStatus(GatewaySenderMXBean mbean) {
    if (!mbean.isRunning()) {
      return CliStrings.GATEWAY_NOT_RUNNING;
    }
    if (mbean.isPaused()) {
      return CliStrings.SENDER_PAUSED;
    }
    if (mbean.isConnected()) {
      return CliStrings.GATEWAY_RUNNING_CONNECTED;
    }
    // can only reach here when running, not paused and not connected
    return CliStrings.GATEWAY_RUNNING_NOT_CONNECTED;
  }

  protected void accumulateListGatewayResult(ResultModel result,
      Map<String, Map<String, GatewaySenderMXBean>> gatewaySenderBeans,
      Map<String, GatewayReceiverMXBean> gatewayReceiverBeans) {
    if (!gatewaySenderBeans.isEmpty()) {
      TabularResultModel gatewaySenders = result.addTable("gatewaySenders");
      gatewaySenders.setHeader(CliStrings.SECTION_GATEWAY_SENDER);
      for (Map.Entry<String, Map<String, GatewaySenderMXBean>> entry : gatewaySenderBeans
          .entrySet()) {
        for (Map.Entry<String, GatewaySenderMXBean> memberToBean : entry.getValue().entrySet()) {
          gatewaySenders.accumulate(CliStrings.RESULT_GATEWAY_SENDER_ID, entry.getKey());
          gatewaySenders.accumulate(CliStrings.RESULT_HOST_MEMBER, memberToBean.getKey());
          gatewaySenders.accumulate(CliStrings.RESULT_REMOTE_CLUSTER,
              memberToBean.getValue().getRemoteDSId() + "");
          gatewaySenders.accumulate(CliStrings.RESULT_TYPE, memberToBean.getValue().isParallel()
              ? CliStrings.SENDER_PARALLEL : CliStrings.SENDER_SERIAL);
          gatewaySenders.accumulate(CliStrings.RESULT_STATUS, getStatus(memberToBean.getValue()));
          gatewaySenders.accumulate(CliStrings.RESULT_QUEUED_EVENTS,
              memberToBean.getValue().getEventQueueSize() + "");
          gatewaySenders.accumulate(CliStrings.RESULT_RECEIVER,
              memberToBean.getValue().getGatewayReceiver());
        }
      }
    }

    if (!gatewayReceiverBeans.isEmpty()) {
      TabularResultModel gatewaySenders = result.addTable("gatewayReceivers");
      gatewaySenders.setHeader(CliStrings.SECTION_GATEWAY_RECEIVER);
      for (Map.Entry<String, GatewayReceiverMXBean> entry : gatewayReceiverBeans.entrySet()) {
        gatewaySenders.accumulate(CliStrings.RESULT_HOST_MEMBER, entry.getKey());
        gatewaySenders.accumulate(CliStrings.RESULT_PORT, entry.getValue().getPort() + "");
        gatewaySenders.accumulate(CliStrings.RESULT_SENDERS_COUNT,
            entry.getValue().getClientConnectionCount() + "");
        if (entry.getValue() == null || entry.getValue().getConnectedGatewaySenders() == null) {
          gatewaySenders.accumulate(CliStrings.RESULT_SENDER_CONNECTED, "");
        } else {
          gatewaySenders.accumulate(CliStrings.RESULT_SENDER_CONNECTED,
              Arrays.stream(entry.getValue().getConnectedGatewaySenders()).collect(joining(", ")));
        }
      }
    }
  }
}
