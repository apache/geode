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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.shell.core.annotation.CliCommand;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.domain.AsyncEventQueueDetails;
import org.apache.geode.management.internal.cli.functions.ListAsyncEventQueuesFunction;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class ListAsyncEventQueuesCommand extends GfshCommand {
  private static final String[] DETAILS_OUTPUT_COLUMNS =
      {"Member", "ID", "Batch Size", "Persistent", "Disk Store", "Max Memory", "Listener",
          "Created with paused event processing", "Currently Paused"};
  private static final String ASYNC_EVENT_QUEUES_TABLE_SECTION = "Async Event Queues";
  private static final String MEMBER_ERRORS_TABLE_SECTION = "Member Errors";

  @CliCommand(value = CliStrings.LIST_ASYNC_EVENT_QUEUES,
      help = CliStrings.LIST_ASYNC_EVENT_QUEUES__HELP)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public ResultModel listAsyncEventQueues() {
    Set<DistributedMember> targetMembers = getAllNormalMembers();
    if (targetMembers.isEmpty()) {
      return ResultModel.createInfo(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }

    // Each (successful) member returns a list of AsyncEventQueueDetails.
    List<CliFunctionResult> results = executeAndGetFunctionResult(
        new ListAsyncEventQueuesFunction(), new Object[] {}, targetMembers);

    ResultModel result = buildAsyncEventQueueInfo(results);

    // Report any explicit errors as well.
    if (results.stream().anyMatch(r -> !r.isSuccessful())) {
      TabularResultModel errors = result.addTable(MEMBER_ERRORS_TABLE_SECTION);
      errors.setColumnHeader("Member", "Error");
      results.stream().filter(r -> !r.isSuccessful()).forEach(errorResult -> errors
          .addRow(errorResult.getMemberIdOrName(), errorResult.getStatusMessage()));
    }

    return result;
  }

  /**
   * @return An info result containing the table of AsyncEventQueueDetails.
   *         If no details are found, returns an info result message indicating so.
   */
  private ResultModel buildAsyncEventQueueInfo(List<CliFunctionResult> results) {
    if (results.stream().filter(CliFunctionResult::isSuccessful)
        .noneMatch(r -> {
          @SuppressWarnings("unchecked")
          final List<AsyncEventQueueDetails> resultObject =
              (List<AsyncEventQueueDetails>) r.getResultObject();
          return resultObject.size() > 0;
        })) {
      return ResultModel.createInfo(CliStrings.LIST_ASYNC_EVENT_QUEUES__NO_QUEUES_FOUND_MESSAGE);
    }

    ResultModel result = new ResultModel();
    TabularResultModel detailsTable = result.addTable(ASYNC_EVENT_QUEUES_TABLE_SECTION);
    detailsTable.setColumnHeader(DETAILS_OUTPUT_COLUMNS);

    results.stream().filter(CliFunctionResult::isSuccessful).forEach(successfulResult -> {
      String memberName = successfulResult.getMemberIdOrName();
      @SuppressWarnings("unchecked")
      final List<AsyncEventQueueDetails> resultObject =
          (List<AsyncEventQueueDetails>) successfulResult.getResultObject();
      resultObject
          .forEach(entry -> detailsTable.addRow(memberName, entry.getId(),
              String.valueOf(entry.getBatchSize()), String.valueOf(entry.isPersistent()),
              String.valueOf(entry.getDiskStoreName()), String.valueOf(entry.getMaxQueueMemory()),
              getListenerEntry(entry), String.valueOf(entry.isCreatedWithPausedEventProcessing()),
              String.valueOf(entry.isPausedEventProcessing())));
    });
    return result;
  }

  /**
   * @return The class of the entry's listener. If the listener is parameterized, these parameters
   *         are appended in a json format.
   */
  private String getListenerEntry(AsyncEventQueueDetails entry) {
    return entry.getListener() + propertiesToString(entry.getListenerProperties());
  }

  /**
   * @return A json format of the properties, or the empty string if the properties are empty.
   */
  static String propertiesToString(Properties props) {
    if (props == null || props.isEmpty()) {
      return "";
    }
    ObjectMapper mapper = new ObjectMapper();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      mapper.writeValue(baos, props);
    } catch (IOException e) {
      return e.getMessage();
    }
    return baos.toString();
  }
}
