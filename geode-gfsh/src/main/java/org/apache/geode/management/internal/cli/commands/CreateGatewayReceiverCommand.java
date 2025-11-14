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

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.cache.configuration.GatewayReceiverConfig;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.functions.GatewayReceiverCreateFunction;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class CreateGatewayReceiverCommand extends SingleGfshCommand {

  @ShellMethod(value = CliStrings.CREATE_GATEWAYRECEIVER__HELP,
      key = CliStrings.CREATE_GATEWAYRECEIVER)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_WAN,
      interceptor = "org.apache.geode.management.internal.cli.commands.CreateGatewayReceiverCommand$Interceptor")
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.GATEWAY)
  public ResultModel createGatewayReceiver(
      @ShellOption(value = {CliStrings.GROUP, CliStrings.GROUPS},
          help = CliStrings.CREATE_GATEWAYRECEIVER__GROUP__HELP) String[] onGroups,

      @ShellOption(value = {CliStrings.MEMBER, CliStrings.MEMBERS},
          help = CliStrings.CREATE_GATEWAYRECEIVER__MEMBER__HELP) String[] onMember,

      @ShellOption(value = CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART,
          help = CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART__HELP,
          defaultValue = "false") Boolean manualStart,

      @ShellOption(value = CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT,
          help = CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT__HELP) Integer startPort,

      @ShellOption(value = CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT,
          help = CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT__HELP) Integer endPort,

      @ShellOption(value = CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS,
          help = CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS__HELP) String bindAddress,

      @ShellOption(value = CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS,
          help = CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS__HELP) Integer maximumTimeBetweenPings,

      @ShellOption(value = CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE,
          help = CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE__HELP) Integer socketBufferSize,

      @ShellOption(value = CliStrings.CREATE_GATEWAYRECEIVER__GATEWAYTRANSPORTFILTER,
          help = CliStrings.CREATE_GATEWAYRECEIVER__GATEWAYTRANSPORTFILTER__HELP) String[] gatewayTransportFilters,

      @ShellOption(value = CliStrings.CREATE_GATEWAYRECEIVER__HOSTNAMEFORSENDERS,
          help = CliStrings.CREATE_GATEWAYRECEIVER__HOSTNAMEFORSENDERS__HELP) String hostnameForSenders,

      @ShellOption(value = CliStrings.IFNOTEXISTS, help = CliStrings.IFNOTEXISTS_HELP,
          defaultValue = "false") Boolean ifNotExists) {

    GatewayReceiverConfig configuration =
        buildConfiguration(manualStart, startPort, endPort, bindAddress, maximumTimeBetweenPings,
            socketBufferSize, gatewayTransportFilters, hostnameForSenders);

    Set<DistributedMember> membersToCreateGatewayReceiverOn = getMembers(onGroups, onMember);

    List<CliFunctionResult> gatewayReceiverCreateResults =
        executeAndGetFunctionResult(GatewayReceiverCreateFunction.INSTANCE,
            new Object[] {configuration, ifNotExists}, membersToCreateGatewayReceiverOn);

    ResultModel result = ResultModel.createMemberStatusResult(gatewayReceiverCreateResults);
    result.setConfigObject(configuration);
    return result;
  }

  @Override
  public boolean updateConfigForGroup(String group, CacheConfig config, Object configObject) {
    config.setGatewayReceiver((GatewayReceiverConfig) configObject);
    return true;
  }

  private GatewayReceiverConfig buildConfiguration(Boolean manualStart, Integer startPort,
      Integer endPort, String bindAddress, Integer maximumTimeBetweenPings,
      Integer socketBufferSize, String[] gatewayTransportFilters, String hostnameForSenders) {
    GatewayReceiverConfig configuration = new GatewayReceiverConfig();

    if (gatewayTransportFilters != null) {
      List<DeclarableType> filters =
          Arrays.stream(gatewayTransportFilters).map(fullyQualifiedClassName -> {
            DeclarableType thisFilter = new DeclarableType();
            thisFilter.setClassName(fullyQualifiedClassName);
            return thisFilter;
          }).collect(Collectors.toList());
      configuration.getGatewayTransportFilters().addAll(filters);
    }
    if (startPort != null) {
      configuration.setStartPort(String.valueOf(startPort));
    }
    if (endPort != null) {
      configuration.setEndPort(String.valueOf(endPort));
    }
    configuration.setBindAddress(bindAddress);
    if (maximumTimeBetweenPings != null) {
      configuration.setMaximumTimeBetweenPings(String.valueOf(maximumTimeBetweenPings));
    }
    if (socketBufferSize != null) {
      configuration.setSocketBufferSize(String.valueOf(socketBufferSize));
    }
    configuration.setHostnameForSenders(hostnameForSenders);
    configuration.setManualStart(manualStart);
    return configuration;
  }

  public static class Interceptor extends AbstractCliAroundInterceptor {
    @Override
    public ResultModel preExecution(GfshParseResult parseResult) {
      Integer startPort = (Integer) parseResult.getParamValue("start-port");
      Integer endPort = (Integer) parseResult.getParamValue("end-port");

      if (startPort == null) {
        startPort = GatewayReceiver.DEFAULT_START_PORT;
      }

      if (endPort == null) {
        endPort = GatewayReceiver.DEFAULT_END_PORT;
      }

      if (startPort > endPort) {
        return ResultModel.createError("start-port must be smaller than end-port.");
      }

      return ResultModel.createInfo("");
    }
  }
}
