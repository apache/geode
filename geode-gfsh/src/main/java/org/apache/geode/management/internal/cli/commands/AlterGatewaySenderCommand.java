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

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.lang.Identifiable;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.internal.cli.functions.AlterGatewaySenderFunction;
import org.apache.geode.management.internal.cli.functions.GatewaySenderFunctionArgs;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.exceptions.EntityNotFoundException;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class AlterGatewaySenderCommand extends SingleGfshCommand {
  private final AlterGatewaySenderFunction alterGatewaySenderFunction =
      new AlterGatewaySenderFunction();
  private static final Logger logger = LogService.getLogger();

  @CliCommand(value = CliStrings.ALTER_GATEWAYSENDER,
      help = CliStrings.ALTER_GATEWAYSENDER__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_WAN)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.GATEWAY)

  public ResultModel alterGatewaySender(@CliOption(key = CliStrings.ALTER_GATEWAYSENDER__ID,
      mandatory = true, optionContext = ConverterHint.GATEWAY_SENDER_ID,
      help = CliStrings.ALTER_GATEWAYSENDER__ID__HELP) String senderId,
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.ALTER_GATEWAYSENDER__GROUP__HELP) String[] onGroup,
      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.ALTER_GATEWAYSENDER__MEMBER__HELP) String[] onMember,
      @CliOption(key = CliStrings.ALTER_GATEWAYSENDER__ALERTTHRESHOLD,
          help = CliStrings.ALTER_GATEWAYSENDER__ALERTTHRESHOLD__HELP) Integer alertThreshold,
      @CliOption(key = CliStrings.ALTER_GATEWAYSENDER__BATCHSIZE,
          help = CliStrings.ALTER_GATEWAYSENDER__BATCHSIZE__HELP) Integer batchSize,
      @CliOption(key = CliStrings.ALTER_GATEWAYSENDER__BATCHTIMEINTERVAL,
          help = CliStrings.ALTER_GATEWAYSENDER__BATCHTIMEINTERVAL__HELP) Integer batchTimeInterval,
      @CliOption(key = CliStrings.ALTER_GATEWAYSENDER__GATEWAYEVENTFILTER,
          help = CliStrings.ALTER_GATEWAYSENDER__GATEWAYEVENTFILTER__HELP) String[] gatewayEventFilters,
      @CliOption(key = CliStrings.ALTER_GATEWAYSENDER__GROUPTRANSACTIONEVENTS,
          specifiedDefaultValue = "true",
          help = CliStrings.ALTER_GATEWAYSENDER__GROUPTRANSACTIONEVENTS__HELP) Boolean groupTransactionEvents)
      throws EntityNotFoundException {

    // need not check if any running servers has this gateway-sender. A server with this
    // gateway-sender id
    // may be shutdown, but we still need to update Cluster Configuration.
    if (getConfigurationPersistenceService() == null) {
      return ResultModel.createError("Cluster Configuration Service is not available. "
          + "Please connect to a locator with running Cluster Configuration Service.");
    }

    final String id = senderId.trim();

    CacheConfig.GatewaySender oldConfiguration = findGW(id);

    if (oldConfiguration == null) {
      String message = String.format("Cannot find a gateway sender with id '%s'.", id);
      throw new EntityNotFoundException(message);
    }

    if (groupTransactionEvents != null && groupTransactionEvents
        && !oldConfiguration.mustGroupTransactionEvents()) {
      if (!oldConfiguration.isParallel() && (oldConfiguration.getDispatcherThreads() == null
          || Integer.parseInt(oldConfiguration.getDispatcherThreads()) > 1)) {
        return ResultModel.createError(
            "alter-gateway-sender cannot be performed for --group-transaction-events attribute if serial sender and dispatcher-threads is greater than 1.");
      }

      if (oldConfiguration.isEnableBatchConflation()) {
        return ResultModel.createError(
            "alter-gateway-sender cannot be performed for --group-transaction-events attribute if batch-conflation is enabled.");
      }
    }

    Set<DistributedMember> dsMembers = findMembers(onGroup, onMember);

    if (dsMembers.isEmpty()) {
      return ResultModel.createError(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }

    CacheConfig.GatewaySender gwConfiguration = new CacheConfig.GatewaySender();
    gwConfiguration.setId(id);

    boolean modify = false;

    if (alertThreshold != null) {
      modify = true;
      gwConfiguration.setAlertThreshold(alertThreshold.toString());
    }

    if (batchSize != null) {
      modify = true;
      gwConfiguration.setBatchSize(batchSize.toString());
    }

    if (batchTimeInterval != null) {
      modify = true;
      gwConfiguration.setBatchTimeInterval(batchTimeInterval.toString());
    }

    if (groupTransactionEvents != null) {
      modify = true;
      gwConfiguration.setGroupTransactionEvents(groupTransactionEvents);
    }

    if (gatewayEventFilters != null) {
      modify = true;
      gwConfiguration.getGatewayEventFilters()
          .addAll((stringsToDeclarableTypes(gatewayEventFilters)));
    }

    if (!modify) {
      return ResultModel.createError(CliStrings.ALTER_GATEWAYSENDER__RELEVANT__OPTION__MESSAGE);
    }

    GatewaySenderFunctionArgs gatewaySenderFunctionArgs =
        new GatewaySenderFunctionArgs(gwConfiguration);

    List<CliFunctionResult> gatewaySenderAlterResults =
        executeAndGetFunctionResult(alterGatewaySenderFunction, gatewaySenderFunctionArgs,
            dsMembers);

    ResultModel resultModel = ResultModel.createMemberStatusResult(gatewaySenderAlterResults);

    resultModel.setConfigObject(gwConfiguration);

    return resultModel;
  }

  @Override
  public boolean updateConfigForGroup(String group, CacheConfig config, Object configObject) {
    List<CacheConfig.GatewaySender> gwSenders = config.getGatewaySenders();
    if (gwSenders.isEmpty()) {
      return false;
    }

    boolean gwConfigsHaveBeenUpdated = false;
    CacheConfig.GatewaySender gwConfiguration =
        ((CacheConfig.GatewaySender) configObject);

    String gwId = gwConfiguration.getId();

    for (CacheConfig.GatewaySender sender : gwSenders) {
      if (gwId.equals(sender.getId())) {
        gwConfigsHaveBeenUpdated = true;
        if (StringUtils.isNotBlank(gwConfiguration.getBatchSize())) {
          sender.setBatchSize(gwConfiguration.getBatchSize());
        }

        if (StringUtils.isNotBlank(gwConfiguration.getBatchTimeInterval())) {
          sender.setBatchTimeInterval(gwConfiguration.getBatchTimeInterval());
        }

        if (StringUtils.isNotBlank(gwConfiguration.getAlertThreshold())) {
          sender.setAlertThreshold(gwConfiguration.getAlertThreshold());
        }
        if (gwConfiguration.mustGroupTransactionEvents() != null) {
          sender.setGroupTransactionEvents(gwConfiguration.mustGroupTransactionEvents());
        }

        if (!gwConfiguration.getGatewayEventFilters().isEmpty()) {
          if (!sender.getGatewayEventFilters().isEmpty()) {
            sender.getGatewayEventFilters().clear();
          }
          sender.getGatewayEventFilters().addAll(gwConfiguration.getGatewayEventFilters());
        }

      }
    }
    return gwConfigsHaveBeenUpdated;

  }

  private CacheConfig.GatewaySender findGW(String gwId) {
    CacheConfig.GatewaySender gwsender = null;
    InternalConfigurationPersistenceService ccService =
        (InternalConfigurationPersistenceService) this.getConfigurationPersistenceService();
    if (ccService == null) {
      return null;
    }

    Set<String> groups = ccService.getGroups();

    for (String group : groups) {
      gwsender =
          Identifiable.find(ccService.getCacheConfig(group).getGatewaySenders(), gwId);
      if (gwsender != null) {
        return gwsender;
      }
    }
    return gwsender;
  }

  private List<DeclarableType> stringsToDeclarableTypes(String[] objects) {
    return Arrays.stream(objects).map(fullyQualifiedClassName -> {
      DeclarableType thisFilter = new DeclarableType();
      thisFilter.setClassName(fullyQualifiedClassName);
      return thisFilter;
    }).collect(Collectors.toList());
  }
}
