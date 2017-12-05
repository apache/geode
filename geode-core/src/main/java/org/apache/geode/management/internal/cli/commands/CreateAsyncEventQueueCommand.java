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
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueFactoryImpl;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.functions.AsyncEventQueueFunctionArgs;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.CreateAsyncEventQueueFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class CreateAsyncEventQueueCommand implements GfshCommand {
  @CliCommand(value = CliStrings.CREATE_ASYNC_EVENT_QUEUE,
      help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__HELP)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.DEPLOY)
  public Result createAsyncEventQueue(
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
      getSecurityService().authorize(ResourcePermission.Resource.CLUSTER,
          ResourcePermission.Operation.WRITE, ResourcePermission.Target.DISK);
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

    TabularResultData tabularData = ResultBuilder.createTabularResultData();
    AsyncEventQueueFunctionArgs aeqArgs = new AsyncEventQueueFunctionArgs(id, parallel,
        enableBatchConflation, batchSize, batchTimeInterval, persistent, diskStore, diskSynchronous,
        maxQueueMemory, dispatcherThreads, orderPolicy, gatewayEventFilters,
        gatewaySubstitutionListener, listener, listenerProperties, forwardExpirationDestroy);

    CreateAsyncEventQueueFunction function = new CreateAsyncEventQueueFunction();
    List<CliFunctionResult> results = executeAndGetFunctionResult(function, aeqArgs, targetMembers);

    if (results.size() == 0) {
      throw new RuntimeException("No results received.");
    }

    AtomicReference<XmlEntity> xmlEntity = new AtomicReference<>();
    for (CliFunctionResult result : results) {
      if (!result.isSuccessful()) {
        tabularData.accumulate("Member", result.getMemberIdOrName());
        tabularData.accumulate("Result", "ERROR: " + result.getErrorMessage());
      } else {
        tabularData.accumulate("Member", result.getMemberIdOrName());
        tabularData.accumulate("Result", result.getMessage());

        // if one member is successful in creating the AEQ and xmlEntity is not set yet,
        // save the xmlEntity that is to be persisted
        if (result.isSuccessful() && xmlEntity.get() == null) {
          xmlEntity.set(result.getXmlEntity());
        }
      }
    }
    CommandResult commandResult = ResultBuilder.buildResult(tabularData);
    if (xmlEntity.get() != null) {
      persistClusterConfiguration(commandResult,
          () -> getSharedConfiguration().addXmlEntity(xmlEntity.get(), groups));
    }
    return commandResult;
  }

}
