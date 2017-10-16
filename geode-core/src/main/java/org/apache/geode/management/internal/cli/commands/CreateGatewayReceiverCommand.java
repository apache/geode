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
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.GatewayReceiverCreateFunction;
import org.apache.geode.management.internal.cli.functions.GatewayReceiverFunctionArgs;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class CreateGatewayReceiverCommand implements GfshCommand {

  @CliCommand(value = CliStrings.CREATE_GATEWAYRECEIVER,
      help = CliStrings.CREATE_GATEWAYRECEIVER__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_WAN)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.GATEWAY)
  public Result createGatewayReceiver(@CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
      optionContext = ConverterHint.MEMBERGROUP,
      help = CliStrings.CREATE_GATEWAYRECEIVER__GROUP__HELP) String[] onGroups,

      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.CREATE_GATEWAYRECEIVER__MEMBER__HELP) String[] onMember,

      @CliOption(key = CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART,
          help = CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART__HELP) Boolean manualStart,

      @CliOption(key = CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT,
          help = CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT__HELP) Integer startPort,

      @CliOption(key = CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT,
          help = CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT__HELP) Integer endPort,

      @CliOption(key = CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS,
          help = CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS__HELP) String bindAddress,

      @CliOption(key = CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS,
          help = CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS__HELP) Integer maximumTimeBetweenPings,

      @CliOption(key = CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE,
          help = CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE__HELP) Integer socketBufferSize,

      @CliOption(key = CliStrings.CREATE_GATEWAYRECEIVER__GATEWAYTRANSPORTFILTER,
          help = CliStrings.CREATE_GATEWAYRECEIVER__GATEWAYTRANSPORTFILTER__HELP) String[] gatewayTransportFilters,

      @CliOption(key = CliStrings.CREATE_GATEWAYRECEIVER__HOSTNAMEFORSENDERS,
          help = CliStrings.CREATE_GATEWAYRECEIVER__HOSTNAMEFORSENDERS__HELP) String hostnameForSenders) {

    Result result;

    AtomicReference<XmlEntity> xmlEntity = new AtomicReference<>();
    try {
      GatewayReceiverFunctionArgs gatewayReceiverFunctionArgs = new GatewayReceiverFunctionArgs(
          manualStart, startPort, endPort, bindAddress, socketBufferSize, maximumTimeBetweenPings,
          gatewayTransportFilters, hostnameForSenders);

      Set<DistributedMember> membersToCreateGatewayReceiverOn =
          CliUtil.findMembers(onGroups, onMember);

      if (membersToCreateGatewayReceiverOn.isEmpty()) {
        return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
      }

      ResultCollector<?, ?> resultCollector =
          CliUtil.executeFunction(GatewayReceiverCreateFunction.INSTANCE,
              gatewayReceiverFunctionArgs, membersToCreateGatewayReceiverOn);
      @SuppressWarnings("unchecked")
      List<CliFunctionResult> gatewayReceiverCreateResults =
          (List<CliFunctionResult>) resultCollector.getResult();

      TabularResultData tabularResultData = ResultBuilder.createTabularResultData();
      final String errorPrefix = "ERROR: ";

      for (CliFunctionResult gatewayReceiverCreateResult : gatewayReceiverCreateResults) {
        boolean success = gatewayReceiverCreateResult.isSuccessful();
        tabularResultData.accumulate("Member", gatewayReceiverCreateResult.getMemberIdOrName());
        tabularResultData.accumulate("Status",
            (success ? "" : errorPrefix) + gatewayReceiverCreateResult.getMessage());

        if (success && xmlEntity.get() == null) {
          xmlEntity.set(gatewayReceiverCreateResult.getXmlEntity());
        }
      }
      result = ResultBuilder.buildResult(tabularResultData);
    } catch (IllegalArgumentException e) {
      LogWrapper.getInstance().info(e.getMessage());
      result = ResultBuilder.createUserErrorResult(e.getMessage());
    }

    if (xmlEntity.get() != null) {
      persistClusterConfiguration(result,
          () -> getSharedConfiguration().addXmlEntity(xmlEntity.get(), onGroups));
    }
    return result;
  }
}
