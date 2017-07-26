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
import org.apache.geode.management.internal.cli.functions.GatewaySenderCreateFunction;
import org.apache.geode.management.internal.cli.functions.GatewaySenderFunctionArgs;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class CreateGatewaySenderCommand implements GfshCommand {
  public CreateGatewaySenderCommand() {}

  @CliCommand(value = CliStrings.CREATE_GATEWAYSENDER, help = CliStrings.CREATE_GATEWAYSENDER__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_WAN)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.GATEWAY)
  public Result createGatewaySender(@CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
      optionContext = ConverterHint.MEMBERGROUP,
      help = CliStrings.CREATE_GATEWAYSENDER__GROUP__HELP) String[] onGroups,

      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.CREATE_GATEWAYSENDER__MEMBER__HELP) String[] onMember,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__ID, mandatory = true,
          help = CliStrings.CREATE_GATEWAYSENDER__ID__HELP) String id,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID, mandatory = true,
          help = CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID__HELP) Integer remoteDistributedSystemId,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__PARALLEL,
          help = CliStrings.CREATE_GATEWAYSENDER__PARALLEL__HELP) Boolean parallel,

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
          help = CliStrings.CREATE_GATEWAYSENDER__ORDERPOLICY__HELP) String orderPolicy,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__GATEWAYEVENTFILTER,
          help = CliStrings.CREATE_GATEWAYSENDER__GATEWAYEVENTFILTER__HELP) String[] gatewayEventFilters,

      @CliOption(key = CliStrings.CREATE_GATEWAYSENDER__GATEWAYTRANSPORTFILTER,
          help = CliStrings.CREATE_GATEWAYSENDER__GATEWAYTRANSPORTFILTER__HELP) String[] gatewayTransportFilter) {

    Result result;

    AtomicReference<XmlEntity> xmlEntity = new AtomicReference<XmlEntity>();
    try {
      GatewaySenderFunctionArgs gatewaySenderFunctionArgs = new GatewaySenderFunctionArgs(id,
          remoteDistributedSystemId, parallel, manualStart, socketBufferSize, socketReadTimeout,
          enableBatchConflation, batchSize, batchTimeInterval, enablePersistence, diskStoreName,
          diskSynchronous, maxQueueMemory, alertThreshold, dispatcherThreads, orderPolicy,
          gatewayEventFilters, gatewayTransportFilter);

      Set<DistributedMember> membersToCreateGatewaySenderOn =
          CliUtil.findMembers(onGroups, onMember);

      if (membersToCreateGatewaySenderOn.isEmpty()) {
        return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
      }

      ResultCollector<?, ?> resultCollector =
          CliUtil.executeFunction(GatewaySenderCreateFunction.INSTANCE, gatewaySenderFunctionArgs,
              membersToCreateGatewaySenderOn);
      @SuppressWarnings("unchecked")
      List<CliFunctionResult> gatewaySenderCreateResults =
          (List<CliFunctionResult>) resultCollector.getResult();

      TabularResultData tabularResultData = ResultBuilder.createTabularResultData();
      final String errorPrefix = "ERROR: ";
      for (CliFunctionResult gatewaySenderCreateResult : gatewaySenderCreateResults) {
        boolean success = gatewaySenderCreateResult.isSuccessful();
        tabularResultData.accumulate("Member", gatewaySenderCreateResult.getMemberIdOrName());
        tabularResultData.accumulate("Status",
            (success ? "" : errorPrefix) + gatewaySenderCreateResult.getMessage());

        if (success && xmlEntity.get() == null) {
          xmlEntity.set(gatewaySenderCreateResult.getXmlEntity());
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
