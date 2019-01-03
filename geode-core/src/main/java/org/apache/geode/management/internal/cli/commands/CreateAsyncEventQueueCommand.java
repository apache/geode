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
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueFactoryImpl;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.CreateAsyncEventQueueFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class CreateAsyncEventQueueCommand extends SingleGfshCommand {
  @CliCommand(value = CliStrings.CREATE_ASYNC_EVENT_QUEUE,
      help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__HELP)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.DEPLOY)
  public ResultModel createAsyncEventQueue(
      @CliOption(key = CliStrings.CREATE_ASYNC_EVENT_QUEUE__ID, mandatory = true,
          help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__ID__HELP) String id,
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__GROUP__HELP) String[] groups,
      @CliOption(key = CliStrings.CREATE_ASYNC_EVENT_QUEUE__PARALLEL,
          unspecifiedDefaultValue = "false", specifiedDefaultValue = "true",
          help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__PARALLEL__HELP) boolean parallel,
      @CliOption(key = CliStrings.CREATE_ASYNC_EVENT_QUEUE__ENABLEBATCHCONFLATION,
          unspecifiedDefaultValue = "false", specifiedDefaultValue = "true",
          help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__ENABLEBATCHCONFLATION__HELP) boolean enableBatchConflation,
      @CliOption(key = CliStrings.CREATE_ASYNC_EVENT_QUEUE__BATCH_SIZE,
          unspecifiedDefaultValue = "100",
          help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__BATCH_SIZE__HELP) int batchSize,
      @CliOption(key = CliStrings.CREATE_ASYNC_EVENT_QUEUE__BATCHTIMEINTERVAL,
          unspecifiedDefaultValue = AsyncEventQueueFactoryImpl.DEFAULT_BATCH_TIME_INTERVAL + "",
          help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__BATCHTIMEINTERVAL__HELP) int batchTimeInterval,
      @CliOption(key = CliStrings.CREATE_ASYNC_EVENT_QUEUE__PERSISTENT,
          unspecifiedDefaultValue = "false", specifiedDefaultValue = "true",
          help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__PERSISTENT__HELP) boolean persistent,
      @CliOption(key = CliStrings.CREATE_ASYNC_EVENT_QUEUE__DISK_STORE,
          help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__DISK_STORE__HELP) String diskStore,
      @CliOption(key = CliStrings.CREATE_ASYNC_EVENT_QUEUE__DISKSYNCHRONOUS,
          unspecifiedDefaultValue = "true", specifiedDefaultValue = "true",
          help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__DISKSYNCHRONOUS__HELP) boolean diskSynchronous,
      @CliOption(key = CliStrings.CREATE_ASYNC_EVENT_QUEUE__FORWARD_EXPIRATION_DESTROY,
          unspecifiedDefaultValue = "false", specifiedDefaultValue = "true",
          help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__FORWARD_EXPIRATION_DESTROY__HELP) boolean forwardExpirationDestroy,
      @CliOption(key = CliStrings.CREATE_ASYNC_EVENT_QUEUE__MAXIMUM_QUEUE_MEMORY,
          unspecifiedDefaultValue = "100",
          help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__MAXIMUM_QUEUE_MEMORY__HELP) int maxQueueMemory,
      @CliOption(key = CliStrings.CREATE_ASYNC_EVENT_QUEUE__DISPATCHERTHREADS,
          unspecifiedDefaultValue = "1",
          help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__DISPATCHERTHREADS__HELP) int dispatcherThreads,
      @CliOption(key = CliStrings.CREATE_ASYNC_EVENT_QUEUE__ORDERPOLICY,
          unspecifiedDefaultValue = "KEY",
          help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__ORDERPOLICY__HELP) String orderPolicy,
      @CliOption(key = CliStrings.CREATE_ASYNC_EVENT_QUEUE__GATEWAYEVENTFILTER,
          help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__GATEWAYEVENTFILTER__HELP) String[] gatewayEventFilters,
      @CliOption(key = CliStrings.CREATE_ASYNC_EVENT_QUEUE__SUBSTITUTION_FILTER,
          help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__SUBSTITUTION_FILTER__HELP) String gatewaySubstitutionListener,
      @CliOption(key = CliStrings.CREATE_ASYNC_EVENT_QUEUE__LISTENER, mandatory = true,
          help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__LISTENER__HELP) String listener,
      @CliOption(key = CliStrings.CREATE_ASYNC_EVENT_QUEUE__LISTENER_PARAM_AND_VALUE,
          help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__LISTENER_PARAM_AND_VALUE__HELP) String[] listenerParamsAndValues) {

    if (persistent) {
      authorize(ResourcePermission.Resource.CLUSTER, ResourcePermission.Operation.WRITE,
          ResourcePermission.Target.DISK);
    }
    Properties listenerProperties = new Properties();

    if (listenerParamsAndValues != null) {
      for (String listenerParamsAndValue : listenerParamsAndValues) {
        final int hashPosition = listenerParamsAndValue.indexOf('#');
        if (hashPosition == -1) {
          listenerProperties.put(listenerParamsAndValue, "");
        } else {
          listenerProperties.put(listenerParamsAndValue.substring(0, hashPosition),
              listenerParamsAndValue.substring(hashPosition + 1));
        }
      }
    }

    Set<DistributedMember> targetMembers = getMembers(groups, null);
    CacheConfig.AsyncEventQueue config = new CacheConfig.AsyncEventQueue();
    config.setAsyncEventListener(new DeclarableType(listener, listenerProperties));
    config.setBatchSize(String.valueOf(batchSize));
    config.setBatchTimeInterval(String.valueOf(batchTimeInterval));
    config.setDiskStoreName(diskStore);
    config.setDiskSynchronous(diskSynchronous);
    config.setDispatcherThreads(String.valueOf(dispatcherThreads));
    config.setEnableBatchConflation(enableBatchConflation);
    config.setForwardExpirationDestroy(forwardExpirationDestroy);
    if (gatewayEventFilters != null) {
      config.getGatewayEventFilters().addAll(Arrays.stream(gatewayEventFilters)
          .map(classname -> new DeclarableType((classname))).collect(Collectors.toList()));
    }
    if (gatewaySubstitutionListener != null) {
      config.setGatewayEventSubstitutionFilter(new DeclarableType(gatewaySubstitutionListener));
    }
    config.setId(id);
    config.setMaximumQueueMemory(String.valueOf(maxQueueMemory));
    config.setOrderPolicy(orderPolicy);
    config.setParallel(parallel);
    config.setPersistent(persistent);

    CreateAsyncEventQueueFunction function = new CreateAsyncEventQueueFunction();
    List<CliFunctionResult> results = executeAndGetFunctionResult(function, config, targetMembers);

    ResultModel commandResult = ResultModel.createMemberStatusResult(results);
    commandResult.setConfigObject(config);
    return commandResult;
  }

  @Override
  public boolean updateConfigForGroup(String group, CacheConfig config, Object configObject) {
    config.getAsyncEventQueues().add((CacheConfig.AsyncEventQueue) configObject);
    return true;
  }
}
