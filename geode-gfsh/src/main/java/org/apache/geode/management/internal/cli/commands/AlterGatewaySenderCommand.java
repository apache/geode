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
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.configuration.ClassName;
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

  @ShellMethod(value = CliStrings.ALTER_GATEWAYSENDER__HELP, key = CliStrings.ALTER_GATEWAYSENDER)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_WAN)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.GATEWAY)

  public ResultModel alterGatewaySender(@ShellOption(value = CliStrings.ALTER_GATEWAYSENDER__ID,
      defaultValue = ShellOption.NULL,
      help = CliStrings.ALTER_GATEWAYSENDER__ID__HELP) String senderId,
      @ShellOption(value = {CliStrings.GROUP, CliStrings.GROUPS},
          defaultValue = ShellOption.NULL,
          help = CliStrings.ALTER_GATEWAYSENDER__GROUP__HELP) String[] onGroup,
      @ShellOption(value = {CliStrings.MEMBER, CliStrings.MEMBERS},
          defaultValue = ShellOption.NULL,
          help = CliStrings.ALTER_GATEWAYSENDER__MEMBER__HELP) String[] onMember,
      @ShellOption(value = CliStrings.ALTER_GATEWAYSENDER__ALERTTHRESHOLD,
          defaultValue = ShellOption.NULL,
          help = CliStrings.ALTER_GATEWAYSENDER__ALERTTHRESHOLD__HELP) Integer alertThreshold,
      @ShellOption(value = CliStrings.ALTER_GATEWAYSENDER__BATCHSIZE,
          defaultValue = ShellOption.NULL,
          help = CliStrings.ALTER_GATEWAYSENDER__BATCHSIZE__HELP) Integer batchSize,
      @ShellOption(value = CliStrings.ALTER_GATEWAYSENDER__BATCHTIMEINTERVAL,
          defaultValue = ShellOption.NULL,
          help = CliStrings.ALTER_GATEWAYSENDER__BATCHTIMEINTERVAL__HELP) Integer batchTimeInterval,
      // Spring Shell 2.x Migration: Changed from ClassName[] to String[] to handle CLEAR marker
      // In Spring Shell 1.x, we could use @CliOption(specifiedDefaultValue="") to detect when
      // --gateway-event-filter= was provided without a value, which signaled clearing filters.
      // Spring Shell 2.x removed 'specifiedDefaultValue' and strips trailing '=' from options,
      // making it impossible to distinguish "--option" from "--option=". To work around this,
      // we now accept String[] and check for the special marker "CLEAR" (case-insensitive) to
      // indicate that all existing filters should be removed. Regular filter class names are
      // converted to ClassName[] in the method body.
      @ShellOption(value = CliStrings.ALTER_GATEWAYSENDER__GATEWAYEVENTFILTER,
          defaultValue = ShellOption.NULL,
          help = CliStrings.ALTER_GATEWAYSENDER__GATEWAYEVENTFILTER__HELP
              + " Use 'CLEAR' to remove all existing filters.") String[] gatewayEventFiltersStrings,
      @ShellOption(value = CliStrings.ALTER_GATEWAYSENDER__GROUPTRANSACTIONEVENTS,
          defaultValue = ShellOption.NULL,
          help = CliStrings.ALTER_GATEWAYSENDER__GROUPTRANSACTIONEVENTS__HELP) Boolean groupTransactionEvents)
      throws EntityNotFoundException {

    // need not check if any running servers has this gateway-sender. A server with this
    // gateway-sender id
    // may be shutdown, but we still need to update Cluster Configuration.
    if (getConfigurationPersistenceService() == null) {
      return ResultModel.createError("Cluster Configuration Service is not available. "
          + "Please connect to a locator with running Cluster Configuration Service.");
    }

    if (senderId == null) {
      return ResultModel.createError("You must specify a gateway sender id.");
    }

    final String id = senderId.trim();

    CacheConfig.GatewaySender oldConfiguration = findGatewaySenderConfiguration(id);

    if (oldConfiguration == null) {
      String message = String.format("Cannot find a gateway sender with id '%s'.", id);
      throw new EntityNotFoundException(message);
    }

    if (alertThreshold != null && alertThreshold < 0) {
      return ResultModel.createError(
          "alter-gateway-sender cannot be performed for --alert-threshold values smaller then 0.");
    }

    if (batchSize != null && batchSize < 1) {
      return ResultModel.createError(
          "alter-gateway-sender cannot be performed for --batch-size values smaller then 1.");
    }

    if (batchTimeInterval != null && batchTimeInterval < -1) {
      return ResultModel.createError(
          "alter-gateway-sender cannot be performed for --batch-time-interval values smaller then -1.");
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

    // Don't allow alter gateway sender command to be performed if all members are not the current
    // version.
    if (!verifyAllCurrentVersion(dsMembers)) {
      return ResultModel.createError(
          CliStrings.ALTER_GATEWAYSENDER__MSG__CAN_NOT_CREATE_DIFFERENT_VERSIONS);
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

    // Spring Shell 2.x Migration: Handle gateway event filters with CLEAR marker support
    //
    // Background: In Spring Shell 1.x, we used @CliOption(specifiedDefaultValue="") to detect
    // when users provided --gateway-event-filter= (with trailing equals but no value). This
    // allowed us to distinguish between:
    // 1. Option not provided at all (null)
    // 2. Option provided with empty value (empty string array) -> clear filters
    // 3. Option provided with values (string array with filter class names) -> set filters
    //
    // Problem: Spring Shell 2.x removed the 'specifiedDefaultValue' annotation parameter and
    // changed the command line parser behavior. The parser now strips trailing '=' characters,
    // making --gateway-event-filter= identical to --gateway-event-filter. Both result in null
    // being passed to the command handler, so we can no longer distinguish case 2 from case 1.
    //
    // Solution: Introduce a special marker value "CLEAR" (case-insensitive) that users must
    // explicitly provide to remove all existing filters:
    // --gateway-event-filter=CLEAR -> clears all filters
    // --gateway-event-filter=com.example.Filter1,com.example.Filter2 -> sets filters
    // (option not provided) -> no change to filters
    //
    // This is a breaking change from Spring Shell 1.x behavior, but it's the only viable
    // workaround given Spring Shell 2.x's architectural limitations.
    ClassName[] gatewayEventFilters = null;
    if (gatewayEventFiltersStrings != null && gatewayEventFiltersStrings.length > 0) {
      // Check for special "CLEAR" marker (case-insensitive)
      if (gatewayEventFiltersStrings.length == 1
          && gatewayEventFiltersStrings[0].equalsIgnoreCase("CLEAR")) {
        // Use ClassName.EMPTY to signal that filters should be cleared
        gatewayEventFilters = new ClassName[] {ClassName.EMPTY};
      } else {
        // Convert String[] to ClassName[] for regular filter class names
        gatewayEventFilters = new ClassName[gatewayEventFiltersStrings.length];
        for (int i = 0; i < gatewayEventFiltersStrings.length; i++) {
          gatewayEventFilters[i] = new ClassName(gatewayEventFiltersStrings[i]);
        }
      }
    }

    if (gatewayEventFilters != null) {
      modify = true;
      if (gatewayEventFilters.length == 1
          && gatewayEventFilters[0].getClassName().isEmpty()) {
        // Clear all existing filters
        gwConfiguration.getGatewayEventFilters();
      } else {
        // Add/replace filters
        gwConfiguration.getGatewayEventFilters()
            .addAll(Arrays.stream(gatewayEventFilters)
                .map(l -> new DeclarableType(l.getClassName()))
                .collect(Collectors.toList()));
      }
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

        if (gwConfiguration.areGatewayEventFiltersUpdated()) {
          if (!sender.getGatewayEventFilters().isEmpty()) {
            sender.getGatewayEventFilters().clear();
          }
          if (!gwConfiguration.getGatewayEventFilters().isEmpty()) {
            sender.getGatewayEventFilters().addAll(gwConfiguration.getGatewayEventFilters());
          }
        }

      }
    }
    return gwConfigsHaveBeenUpdated;

  }

  private CacheConfig.GatewaySender findGatewaySenderConfiguration(String gwId) {
    CacheConfig.GatewaySender gwsender = null;
    InternalConfigurationPersistenceService ccService = getConfigurationPersistenceService();
    if (ccService == null) {
      return null;
    }

    Set<String> groups = ccService.getGroups();

    for (String group : groups) {
      List<CacheConfig.GatewaySender> gwSendersList =
          ccService.getCacheConfig(group).getGatewaySenders();
      if (gwSendersList.isEmpty()) {
        continue;
      }

      for (CacheConfig.GatewaySender sender : gwSendersList) {
        if (sender.getId().equals(gwId)) {
          return sender;
        }
      }

    }
    return gwsender;
  }

  boolean verifyAllCurrentVersion(Set<DistributedMember> members) {
    return members.stream().allMatch(
        member -> ((InternalDistributedMember) member).getVersion()
            .equals(KnownVersion.CURRENT));
  }

}
