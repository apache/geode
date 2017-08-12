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
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.SystemFailure;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.cli.Result.Status;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.domain.AsyncEventQueueDetails;
import org.apache.geode.management.internal.cli.functions.AsyncEventQueueFunctionArgs;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.CreateAsyncEventQueueFunction;
import org.apache.geode.management.internal.cli.functions.ListAsyncEventQueuesFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;
import org.apache.geode.security.ResourcePermission.Target;

/**
 * The QueueCommands class encapsulates all GemFire Queue commands in Gfsh.
 * </p>
 * 
 * @since GemFire 8.0
 */
public class QueueCommands implements GfshCommand {

  @CliCommand(value = CliStrings.CREATE_ASYNC_EVENT_QUEUE,
      help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__HELP)
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE, target = Target.JAR)
  public Result createAsyncEventQueue(
      @CliOption(key = CliStrings.CREATE_ASYNC_EVENT_QUEUE__ID, mandatory = true,
          help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__ID__HELP) String id,
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__GROUP__HELP) String[] groups,
      @CliOption(key = CliStrings.CREATE_ASYNC_EVENT_QUEUE__PARALLEL,
          unspecifiedDefaultValue = "false", specifiedDefaultValue = "true",
          help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__PARALLEL__HELP) Boolean parallel,
      @CliOption(key = CliStrings.CREATE_ASYNC_EVENT_QUEUE__ENABLEBATCHCONFLATION,
          unspecifiedDefaultValue = "false", specifiedDefaultValue = "true",
          help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__ENABLEBATCHCONFLATION__HELP) Boolean enableBatchConflation,
      @CliOption(key = CliStrings.CREATE_ASYNC_EVENT_QUEUE__BATCH_SIZE,
          unspecifiedDefaultValue = "100",
          help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__BATCH_SIZE__HELP) int batchSize,
      @CliOption(key = CliStrings.CREATE_ASYNC_EVENT_QUEUE__BATCHTIMEINTERVAL,
          unspecifiedDefaultValue = "1000",
          help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__BATCHTIMEINTERVAL__HELP) int batchTimeInterval,
      @CliOption(key = CliStrings.CREATE_ASYNC_EVENT_QUEUE__PERSISTENT,
          unspecifiedDefaultValue = "false", specifiedDefaultValue = "true",
          help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__PERSISTENT__HELP) boolean persistent,
      @CliOption(key = CliStrings.CREATE_ASYNC_EVENT_QUEUE__DISK_STORE,
          help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__DISK_STORE__HELP) String diskStore,
      @CliOption(key = CliStrings.CREATE_ASYNC_EVENT_QUEUE__DISKSYNCHRONOUS,
          unspecifiedDefaultValue = "true", specifiedDefaultValue = "true",
          help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__DISKSYNCHRONOUS__HELP) Boolean diskSynchronous,
      @CliOption(key = CliStrings.CREATE_ASYNC_EVENT_QUEUE__FORWARD_EXPIRATION_DESTROY,
          unspecifiedDefaultValue = "false", specifiedDefaultValue = "false",
          help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__FORWARD_EXPIRATION_DESTROY__HELP) Boolean ignoreEvictionAndExpiration,
      @CliOption(key = CliStrings.CREATE_ASYNC_EVENT_QUEUE__MAXIMUM_QUEUE_MEMORY,
          unspecifiedDefaultValue = "100",
          help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__MAXIMUM_QUEUE_MEMORY__HELP) int maxQueueMemory,
      @CliOption(key = CliStrings.CREATE_ASYNC_EVENT_QUEUE__DISPATCHERTHREADS,
          unspecifiedDefaultValue = "1",
          help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__DISPATCHERTHREADS__HELP) Integer dispatcherThreads,
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
      getSecurityService().authorize(Resource.CLUSTER, Operation.WRITE, Target.DISK);
    }
    Properties listenerProperties = new Properties();

    try {
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

      TabularResultData tabularData = ResultBuilder.createTabularResultData();
      boolean accumulatedData = false;

      Set<DistributedMember> targetMembers = CliUtil.findMembers(groups, null);

      if (targetMembers.isEmpty()) {
        return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
      }

      AsyncEventQueueFunctionArgs aeqArgs = new AsyncEventQueueFunctionArgs(id, parallel,
          enableBatchConflation, batchSize, batchTimeInterval, persistent, diskStore,
          diskSynchronous, maxQueueMemory, dispatcherThreads, orderPolicy, gatewayEventFilters,
          gatewaySubstitutionListener, listener, listenerProperties, ignoreEvictionAndExpiration);

      ResultCollector<?, ?> rc =
          CliUtil.executeFunction(new CreateAsyncEventQueueFunction(), aeqArgs, targetMembers);

      List<CliFunctionResult> results = CliFunctionResult.cleanResults((List<?>) rc.getResult());

      AtomicReference<XmlEntity> xmlEntity = new AtomicReference<>();
      for (CliFunctionResult result : results) {
        if (result.getThrowable() != null) {
          tabularData.accumulate("Member", result.getMemberIdOrName());
          tabularData.accumulate("Result", "ERROR: " + result.getThrowable().getClass().getName()
              + ": " + result.getThrowable().getMessage());
          accumulatedData = true;
          tabularData.setStatus(Status.ERROR);
        } else if (result.isSuccessful()) {
          tabularData.accumulate("Member", result.getMemberIdOrName());
          tabularData.accumulate("Result", result.getMessage());
          accumulatedData = true;

          if (xmlEntity.get() == null) {
            xmlEntity.set(result.getXmlEntity());
          }
        }
      }

      if (!accumulatedData) {
        return ResultBuilder.createInfoResult("Unable to create async event queue(s).");
      }

      Result result = ResultBuilder.buildResult(tabularData);
      if (xmlEntity.get() != null) {
        persistClusterConfiguration(result,
            () -> getSharedConfiguration().addXmlEntity(xmlEntity.get(), groups));
      }
      return result;
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable th) {
      SystemFailure.checkFailure();
      return ResultBuilder.createGemFireErrorResult(
          CliStrings.format(CliStrings.CREATE_ASYNC_EVENT_QUEUE__ERROR_WHILE_CREATING_REASON_0,
              new Object[] {th.getMessage()}));
    }
  }

  @CliCommand(value = CliStrings.LIST_ASYNC_EVENT_QUEUES,
      help = CliStrings.LIST_ASYNC_EVENT_QUEUES__HELP)
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result listAsyncEventQueues() {
    try {
      TabularResultData tabularData = ResultBuilder.createTabularResultData();
      boolean accumulatedData = false;

      Set<DistributedMember> targetMembers = CliUtil.findMembers(null, null);

      if (targetMembers.isEmpty()) {
        return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
      }

      ResultCollector<?, ?> rc = CliUtil.executeFunction(new ListAsyncEventQueuesFunction(),
          new Object[] {}, targetMembers);
      List<CliFunctionResult> results = CliFunctionResult.cleanResults((List<?>) rc.getResult());

      for (CliFunctionResult result : results) {
        if (result.getThrowable() != null) {
          tabularData.accumulate("Member", result.getMemberIdOrName());
          tabularData.accumulate("Result", "ERROR: " + result.getThrowable().getClass().getName()
              + ": " + result.getThrowable().getMessage());
          accumulatedData = true;
          tabularData.setStatus(Status.ERROR);
        } else {
          AsyncEventQueueDetails[] details = (AsyncEventQueueDetails[]) result.getSerializables();
          for (AsyncEventQueueDetails detail : details) {
            tabularData.accumulate("Member", result.getMemberIdOrName());
            tabularData.accumulate("ID", detail.getId());
            tabularData.accumulate("Batch Size", detail.getBatchSize());
            tabularData.accumulate("Persistent", detail.isPersistent());
            tabularData.accumulate("Disk Store", detail.getDiskStoreName());
            tabularData.accumulate("Max Memory", detail.getMaxQueueMemory());

            Properties listenerProperties = detail.getListenerProperties();
            if (listenerProperties == null || listenerProperties.size() == 0) {
              tabularData.accumulate("Listener", detail.getListener());
            } else {
              StringBuilder propsStringBuilder = new StringBuilder();
              propsStringBuilder.append('(');
              boolean firstProperty = true;
              for (Map.Entry<Object, Object> property : listenerProperties.entrySet()) {
                if (!firstProperty) {
                  propsStringBuilder.append(',');
                } else {
                  firstProperty = false;
                }
                propsStringBuilder.append(property.getKey()).append('=')
                    .append(property.getValue());
              }
              propsStringBuilder.append(')');

              tabularData.accumulate("Listener",
                  detail.getListener() + propsStringBuilder.toString());
            }
            accumulatedData = true;
          }
        }
      }

      if (!accumulatedData) {
        return ResultBuilder
            .createInfoResult(CliStrings.LIST_ASYNC_EVENT_QUEUES__NO_QUEUES_FOUND_MESSAGE);
      }

      return ResultBuilder.buildResult(tabularData);
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable th) {
      SystemFailure.checkFailure();
      return ResultBuilder.createGemFireErrorResult(
          CliStrings.format(CliStrings.LIST_ASYNC_EVENT_QUEUES__ERROR_WHILE_LISTING_REASON_0,
              new Object[] {th.getMessage()}));
    }
  }
}
