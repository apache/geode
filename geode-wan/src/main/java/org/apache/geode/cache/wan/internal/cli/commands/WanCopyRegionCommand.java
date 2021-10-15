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

package org.apache.geode.cache.wan.internal.cli.commands;

import java.util.ArrayList;
import java.util.List;

import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.execute.FunctionInvocationTargetException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.functions.WanCopyRegionFunction;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

public class WanCopyRegionCommand extends GfshCommand {
  private final WanCopyRegionFunction wanCopyRegionFunction = new WanCopyRegionFunction();

  /* 'wan-copy region' command */
  public static final String WAN_COPY_REGION = "wan-copy region";
  public static final String WAN_COPY_REGION__HELP =
      "Copy a region with a senderId via WAN replication";
  public static final String WAN_COPY_REGION__REGION = "region";
  public static final String WAN_COPY_REGION__REGION__HELP =
      "Region from which data will be exported.";
  public static final String WAN_COPY_REGION__SENDERID = "sender-id";
  public static final String WAN_COPY_REGION__SENDERID__HELP =
      "Sender Id to use to copy the region.";
  public static final String WAN_COPY_REGION__MAXRATE = "max-rate";
  public static final String WAN_COPY_REGION__MAXRATE__HELP =
      "Maximum rate for copying in entries per second.";
  public static final String WAN_COPY_REGION__BATCHSIZE = "batch-size";
  public static final String WAN_COPY_REGION__BATCHSIZE__HELP =
      "Number of entries to be copied in each batch.";
  public static final String WAN_COPY_REGION__CANCEL = "cancel";
  public static final String WAN_COPY_REGION__CANCEL__HELP =
      "Cancel an ongoing wan-copy region command";
  public static final String WAN_COPY_REGION__MSG__REGION__NOT__FOUND = "Region {0} not found";
  public static final String WAN_COPY_REGION__MSG__REGION__NOT__USING_SENDER =
      "Region {0} is not configured to use sender {1}";
  public static final String WAN_COPY_REGION__MSG__SENDER__NOT__FOUND = "Sender {0} not found";
  public static final String WAN_COPY_REGION__MSG__SENDER__SERIAL__AND__NOT__PRIMARY =
      "Sender {0} is serial and not primary. 0 entries copied.";
  public static final String WAN_COPY_REGION__MSG__SENDER__NOT__RUNNING =
      "Sender {0} is not running";
  public static final String WAN_COPY_REGION__MSG__EXECUTION__CANCELED = "Execution canceled";
  public static final String WAN_COPY_REGION__MSG__EXECUTIONS__CANCELED =
      "Executions canceled: {0}";
  public static final String WAN_COPY_REGION__MSG__EXECUTION__FAILED =
      "Execution failed. Error: {0}";
  public static final String WAN_COPY_REGION__MSG__NO__CONNECTION__POOL =
      "No connection pool available to receiver";
  public static final String WAN_COPY_REGION__MSG__COMMAND__NOT__SUPPORTED__AT__REMOTE__SITE =
      "Command not supported at remote site.";
  public static final String WAN_COPY_REGION__MSG__NO__CONNECTION =
      "No connection available to receiver after having copied {0} entries";
  public static final String WAN_COPY_REGION__MSG__ERROR__AFTER__HAVING__COPIED =
      "Error ({0}) in operation after having copied {1} entries";
  public static final String WAN_COPY_REGION__MSG__CANCELED__BEFORE__HAVING__COPIED =
      "Operation canceled before having copied all entries";
  public static final String WAN_COPY_REGION__MSG__COPIED__ENTRIES = "Entries copied: {0}";
  public static final String WAN_COPY_REGION__MSG__NO__RUNNING__COMMAND =
      "No running command to be canceled for region {0} and sender {1}";
  public static final String WAN_COPY_REGION__MSG__ALREADY__RUNNING__COMMAND =
      "There is already a command running for region {0} and sender {1}";

  @CliAvailabilityIndicator({WAN_COPY_REGION})
  public boolean commandAvailable() {
    return isOnlineCommandAvailable();
  }

  @CliCommand(value = WAN_COPY_REGION, help = WAN_COPY_REGION__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DATA, CliStrings.TOPIC_GEODE_REGION})
  public ResultModel wanCopyRegion(
      @CliOption(key = WAN_COPY_REGION__REGION, mandatory = true,
          optionContext = ConverterHint.REGION_PATH,
          help = WAN_COPY_REGION__REGION__HELP) String regionName,
      @CliOption(key = WAN_COPY_REGION__SENDERID, mandatory = true,
          optionContext = ConverterHint.GATEWAY_SENDER_ID,
          help = WAN_COPY_REGION__SENDERID__HELP) String senderId,
      @CliOption(key = WAN_COPY_REGION__MAXRATE,
          unspecifiedDefaultValue = "0",
          help = WAN_COPY_REGION__MAXRATE__HELP) long maxRate,
      @CliOption(key = WAN_COPY_REGION__BATCHSIZE,
          unspecifiedDefaultValue = "1000",
          help = WAN_COPY_REGION__BATCHSIZE__HELP) int batchSize,
      @CliOption(key = WAN_COPY_REGION__CANCEL,
          unspecifiedDefaultValue = "false",
          specifiedDefaultValue = "true",
          help = WAN_COPY_REGION__CANCEL__HELP) boolean isCancel) {

    authorize(Resource.DATA, Operation.WRITE, regionName);
    final Object[] args = {regionName, senderId, isCancel, maxRate, batchSize};
    ResultCollector<?, ?> resultCollector =
        executeFunction(wanCopyRegionFunction, args, getAllNormalMembers());
    final List<CliFunctionResult> cliFunctionResults =
        getCliFunctionResults((List<CliFunctionResult>) resultCollector.getResult());
    return ResultModel.createMemberStatusResult(cliFunctionResults, false, false);
  }

  private List<CliFunctionResult> getCliFunctionResults(List<CliFunctionResult> resultsObjects) {
    final List<CliFunctionResult> cliFunctionResults = new ArrayList<>();
    for (Object result : resultsObjects) {
      if (result instanceof FunctionInvocationTargetException) {
        CliFunctionResult errorResult =
            new CliFunctionResult(
                ((FunctionInvocationTargetException) result).getMemberId().getName(),
                CliFunctionResult.StatusState.ERROR,
                ((FunctionInvocationTargetException) result).getMessage());
        cliFunctionResults.add(errorResult);
      } else {
        cliFunctionResults.add((CliFunctionResult) result);
      }
    }
    return cliFunctionResults;
  }
}
