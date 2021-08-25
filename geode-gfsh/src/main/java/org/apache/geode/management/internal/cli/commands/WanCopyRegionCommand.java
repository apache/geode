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

import java.util.ArrayList;
import java.util.List;

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

  @CliCommand(value = CliStrings.WAN_COPY_REGION, help = CliStrings.WAN_COPY_REGION__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DATA, CliStrings.TOPIC_GEODE_REGION})
  public ResultModel wanCopyRegion(
      @CliOption(key = CliStrings.WAN_COPY_REGION__REGION, mandatory = true,
          optionContext = ConverterHint.REGION_PATH,
          help = CliStrings.WAN_COPY_REGION__REGION__HELP) String regionName,
      @CliOption(key = CliStrings.WAN_COPY_REGION__SENDERID, mandatory = true,
          optionContext = ConverterHint.GATEWAY_SENDER_ID,
          help = CliStrings.WAN_COPY_REGION__SENDERID__HELP) String senderId,
      @CliOption(key = CliStrings.WAN_COPY_REGION__MAXRATE,
          unspecifiedDefaultValue = "0",
          help = CliStrings.WAN_COPY_REGION__MAXRATE__HELP) long maxRate,
      @CliOption(key = CliStrings.WAN_COPY_REGION__BATCHSIZE,
          unspecifiedDefaultValue = "1000",
          help = CliStrings.WAN_COPY_REGION__BATCHSIZE__HELP) int batchSize,
      @CliOption(key = CliStrings.WAN_COPY_REGION__CANCEL,
          unspecifiedDefaultValue = "false",
          specifiedDefaultValue = "true",
          help = CliStrings.WAN_COPY_REGION__CANCEL__HELP) boolean isCancel) {

    authorize(Resource.DATA, Operation.WRITE, regionName);
    final Object[] args = {regionName, senderId, isCancel, maxRate, batchSize};
    ResultCollector<?, ?> resultCollector =
        executeFunction(wanCopyRegionFunction, args, getAllNormalMembers());
    @SuppressWarnings("unchecked")
    final List<CliFunctionResult> cliFunctionResults =
        getCliFunctionResults((List<CliFunctionResult>) resultCollector.getResult());
    return ResultModel.createMemberStatusResult(cliFunctionResults, false, false);
  }

  private List<CliFunctionResult> getCliFunctionResults(List<CliFunctionResult> resultsObjects) {
    final List<CliFunctionResult> cliFunctionResults = new ArrayList<>();
    for (Object r : resultsObjects) {
      if (r instanceof FunctionInvocationTargetException) {
        CliFunctionResult errorResult =
            new CliFunctionResult(((FunctionInvocationTargetException) r).getMemberId().getName(),
                CliFunctionResult.StatusState.ERROR,
                ((FunctionInvocationTargetException) r).getMessage());
        cliFunctionResults.add(errorResult);
      } else {
        cliFunctionResults.add((CliFunctionResult) r);
      }
    }
    return cliFunctionResults;
  }
}
