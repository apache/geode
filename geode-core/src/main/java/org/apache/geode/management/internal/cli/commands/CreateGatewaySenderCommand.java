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
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Version;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.GatewaySenderCreateFunction;
import org.apache.geode.management.internal.cli.functions.GatewaySenderFunctionArgs;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class CreateGatewaySenderCommand extends SingleGfshCommand {
  @CliCommand(value = CliStrings.CREATE_GATEWAYSENDER, help = CliStrings.CREATE_GATEWAYSENDER__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_WAN,
      interceptor = "org.apache.geode.management.internal.cli.commands.CreateGatewaySenderCommand$Interceptor")
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.GATEWAY)
  public ResultModel createGatewaySender(@CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
      optionContext = ConverterHint.MEMBERGROUP,
      help = CliStrings.CREATE_GATEWAYSENDER__GROUP__HELP) String[] onGroups,

      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.CREATE_GATEWAYSENDER__MEMBER__HELP) String[] onMember,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__ID, mandatory = true,
          help = CliStrings.CREATE_GATEWAYSENDER__ID__HELP) String id,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID, mandatory = true,
          help = CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID__HELP) Integer remoteDistributedSystemId,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__PARALLEL, specifiedDefaultValue = "true",
          unspecifiedDefaultValue = "false",
          help = CliStrings.CREATE_GATEWAYSENDER__PARALLEL__HELP) boolean parallel,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__MANUALSTART,
          help = CliStrings.CREATE_GATEWAYSENDER__MANUALSTART__HELP) Boolean manualStart,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__SOCKETBUFFERSIZE,
          help = CliStrings.CREATE_GATEWAYSENDER__SOCKETBUFFERSIZE__HELP) Integer socketBufferSize,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__SOCKETREADTIMEOUT,
          help = CliStrings.CREATE_GATEWAYSENDER__SOCKETREADTIMEOUT__HELP) Integer socketReadTimeout,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__ENABLEBATCHCONFLATION,
          help = CliStrings.CREATE_GATEWAYSENDER__ENABLEBATCHCONFLATION__HELP) Boolean enableBatchConflation,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__BATCHSIZE,
          help = CliStrings.CREATE_GATEWAYSENDER__BATCHSIZE__HELP) Integer batchSize,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__BATCHTIMEINTERVAL,
          help = CliStrings.CREATE_GATEWAYSENDER__BATCHTIMEINTERVAL__HELP) Integer batchTimeInterval,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__ENABLEPERSISTENCE,
          help = CliStrings.CREATE_GATEWAYSENDER__ENABLEPERSISTENCE__HELP) Boolean enablePersistence,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__DISKSTORENAME,
          help = CliStrings.CREATE_GATEWAYSENDER__DISKSTORENAME__HELP) String diskStoreName,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__DISKSYNCHRONOUS,
          help = CliStrings.CREATE_GATEWAYSENDER__DISKSYNCHRONOUS__HELP) Boolean diskSynchronous,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__MAXQUEUEMEMORY,
          help = CliStrings.CREATE_GATEWAYSENDER__MAXQUEUEMEMORY__HELP) Integer maxQueueMemory,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__ALERTTHRESHOLD,
          help = CliStrings.CREATE_GATEWAYSENDER__ALERTTHRESHOLD__HELP) Integer alertThreshold,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__DISPATCHERTHREADS,
          help = CliStrings.CREATE_GATEWAYSENDER__DISPATCHERTHREADS__HELP) Integer dispatcherThreads,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__ORDERPOLICY,
          help = CliStrings.CREATE_GATEWAYSENDER__ORDERPOLICY__HELP) OrderPolicy orderPolicy,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__GATEWAYEVENTFILTER,
          help = CliStrings.CREATE_GATEWAYSENDER__GATEWAYEVENTFILTER__HELP) String[] gatewayEventFilters,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__GATEWAYTRANSPORTFILTER,
          help = CliStrings.CREATE_GATEWAYSENDER__GATEWAYTRANSPORTFILTER__HELP) String[] gatewayTransportFilter) {

    CacheConfig.GatewaySender configuration =
        buildConfiguration(id, remoteDistributedSystemId, parallel, manualStart,
            socketBufferSize, socketReadTimeout, enableBatchConflation, batchSize,
            batchTimeInterval, enablePersistence, diskStoreName, diskSynchronous, maxQueueMemory,
            alertThreshold, dispatcherThreads, orderPolicy == null ? null : orderPolicy.name(),
            gatewayEventFilters, gatewayTransportFilter);

    GatewaySenderFunctionArgs gatewaySenderFunctionArgs =
        new GatewaySenderFunctionArgs(configuration);
    Set<DistributedMember> membersToCreateGatewaySenderOn = getMembers(onGroups, onMember);

    // Don't allow sender to be created if all members are not the current version.
    if (!verifyAllCurrentVersion(membersToCreateGatewaySenderOn)) {
      return ResultModel.createError(
          CliStrings.CREATE_GATEWAYSENDER__MSG__CAN_NOT_CREATE_DIFFERENT_VERSIONS);
    }

    List<CliFunctionResult> gatewaySenderCreateResults =
        executeAndGetFunctionResult(GatewaySenderCreateFunction.INSTANCE, gatewaySenderFunctionArgs,
            membersToCreateGatewaySenderOn);

    ResultModel resultModel = ResultModel.createMemberStatusResult(gatewaySenderCreateResults);
    resultModel.setConfigObject(configuration);

    return resultModel;
  }

  @Override
  public void updateClusterConfig(String group, CacheConfig config, Object configObject) {
    config.getGatewaySenders().add((CacheConfig.GatewaySender) configObject);
  }

  private boolean verifyAllCurrentVersion(Set<DistributedMember> members) {
    return members.stream().allMatch(
        member -> ((InternalDistributedMember) member).getVersionObject().equals(Version.CURRENT));
  }

  private CacheConfig.GatewaySender buildConfiguration(String id, Integer remoteDSId,
      Boolean parallel,
      Boolean manualStart,
      Integer socketBufferSize,
      Integer socketReadTimeout,
      Boolean enableBatchConflation,
      Integer batchSize,
      Integer batchTimeInterval,
      Boolean enablePersistence,
      String diskStoreName,
      Boolean diskSynchronous,
      Integer maxQueueMemory,
      Integer alertThreshold,
      Integer dispatcherThreads,
      String orderPolicy,
      String[] gatewayEventFilters,
      String[] gatewayTransportFilters) {
    CacheConfig.GatewaySender sender = new CacheConfig.GatewaySender();
    sender.setId(id);
    sender.setRemoteDistributedSystemId(int2string(remoteDSId));
    sender.setParallel(parallel);
    sender.setManualStart(manualStart);
    sender.setSocketBufferSize(int2string(socketBufferSize));
    sender.setSocketReadTimeout(int2string(socketReadTimeout));
    sender.setEnableBatchConflation(enableBatchConflation);
    sender.setBatchSize(int2string(batchSize));
    sender.setBatchTimeInterval(int2string(batchTimeInterval));
    sender.setEnablePersistence(enablePersistence);
    sender.setDiskStoreName(diskStoreName);
    sender.setDiskSynchronous(diskSynchronous);
    sender.setMaximumQueueMemory(int2string(maxQueueMemory));
    sender.setAlertThreshold(int2string(alertThreshold));
    sender.setDispatcherThreads(int2string(dispatcherThreads));
    sender.setOrderPolicy(orderPolicy);
    if (gatewayEventFilters != null) {
      sender.getGatewayEventFilters().addAll((stringsToDeclarableTypes(gatewayEventFilters)));
    }
    if (gatewayTransportFilters != null) {
      sender.getGatewayTransportFilters().addAll(stringsToDeclarableTypes(gatewayTransportFilters));
    }

    return sender;
  }

  private List<DeclarableType> stringsToDeclarableTypes(String[] objects) {
    return Arrays.stream(objects).map(fullyQualifiedClassName -> {
      DeclarableType thisFilter = new DeclarableType();
      thisFilter.setClassName(fullyQualifiedClassName);
      return thisFilter;
    }).collect(Collectors.toList());
  }

  private String int2string(Integer i) {
    return Optional.ofNullable(i).map(in -> in.toString()).orElse(null);
  }

  public static class Interceptor extends AbstractCliAroundInterceptor {
    @Override
    public Result preExecution(GfshParseResult parseResult) {
      Integer dispatcherThreads =
          (Integer) parseResult.getParamValue(CliStrings.CREATE_GATEWAYSENDER__DISPATCHERTHREADS);
      OrderPolicy orderPolicy =
          (OrderPolicy) parseResult.getParamValue(CliStrings.CREATE_GATEWAYSENDER__ORDERPOLICY);
      Boolean parallel =
          (Boolean) parseResult.getParamValue(CliStrings.CREATE_GATEWAYSENDER__PARALLEL);
      if (dispatcherThreads != null && dispatcherThreads > 1 && orderPolicy == null) {
        return ResultBuilder.createUserErrorResult(
            "Must specify --order-policy when --dispatcher-threads is larger than 1.");
      }

      if (parallel && orderPolicy == OrderPolicy.THREAD) {
        return ResultBuilder.createUserErrorResult(
            "Parallel Gateway Sender can not be created with THREAD OrderPolicy");
      }

      return ResultBuilder.createInfoResult("");
    }
  }
}
