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

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.GatewayReceiverCreateFunction;
import org.apache.geode.management.internal.cli.functions.GatewayReceiverFunctionArgs;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class CreateGatewayReceiverCommand extends GfshCommand {

  @CliCommand(value = CliStrings.CREATE_GATEWAYRECEIVER,
      help = CliStrings.CREATE_GATEWAYRECEIVER__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_WAN,
      interceptor = "org.apache.geode.management.internal.cli.commands.CreateGatewayReceiverCommand$Interceptor")
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

    GatewayReceiverFunctionArgs gatewayReceiverFunctionArgs =
        new GatewayReceiverFunctionArgs(manualStart, startPort, endPort, bindAddress,
            socketBufferSize, maximumTimeBetweenPings, gatewayTransportFilters, hostnameForSenders);

    Set<DistributedMember> membersToCreateGatewayReceiverOn = getMembers(onGroups, onMember);

    List<CliFunctionResult> gatewayReceiverCreateResults =
        executeAndGetFunctionResult(GatewayReceiverCreateFunction.INSTANCE,
            gatewayReceiverFunctionArgs, membersToCreateGatewayReceiverOn);

    CommandResult result = ResultBuilder.buildResult(gatewayReceiverCreateResults);
    XmlEntity xmlEntity = findXmlEntity(gatewayReceiverCreateResults);
    // no xml needs to be updated, simply return
    if (xmlEntity == null) {
      return result;
    }

    // has xml but unable to persist to cluster config, need to print warning message and return
    if (onMember != null || getConfigurationService() == null) {
      result.setCommandPersisted(false);
      return result;
    }

    // update cluster config
    getConfigurationService().addXmlEntity(xmlEntity, onGroups);
    return result;
  }

  public static class Interceptor extends AbstractCliAroundInterceptor {
    @Override
    public Result preExecution(GfshParseResult parseResult) {
      Integer startPort = (Integer) parseResult.getParamValue("start-port");
      Integer endPort = (Integer) parseResult.getParamValue("end-port");

      if (startPort == null) {
        startPort = GatewayReceiver.DEFAULT_START_PORT;
      }

      if (endPort == null) {
        endPort = GatewayReceiver.DEFAULT_END_PORT;
      }

      if (startPort > endPort) {
        return ResultBuilder.createUserErrorResult("start-port must be smaller than end-port.");
      }

      return ResultBuilder.createInfoResult("");
    }
  }
}
