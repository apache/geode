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

import org.springframework.shell.core.annotation.CliCommand;

import org.apache.geode.SystemFailure;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.domain.AsyncEventQueueDetails;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.ListAsyncEventQueuesFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class ListAsyncEventQueuesCommand extends GfshCommand {
  @CliCommand(value = CliStrings.LIST_ASYNC_EVENT_QUEUES,
      help = CliStrings.LIST_ASYNC_EVENT_QUEUES__HELP)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public Result listAsyncEventQueues() {
    try {
      TabularResultData tabularData = ResultBuilder.createTabularResultData();
      boolean accumulatedData = false;

      Set<DistributedMember> targetMembers = getAllNormalMembers();

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
          tabularData.setStatus(Result.Status.ERROR);
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
