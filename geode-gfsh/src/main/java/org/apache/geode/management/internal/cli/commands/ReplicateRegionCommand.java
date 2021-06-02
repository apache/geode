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

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.execute.FunctionInvocationTargetException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.functions.ReplicateRegionFunction;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

public class ReplicateRegionCommand extends GfshCommand {
  private final ReplicateRegionFunction replicateRegionFunction = new ReplicateRegionFunction();

  @CliCommand(value = CliStrings.REPLICATE_REGION, help = CliStrings.REPLICATE_REGION__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DATA, CliStrings.TOPIC_GEODE_REGION})
  public ResultModel replicateRegion(
      @CliOption(key = CliStrings.REPLICATE_REGION__REGION, mandatory = true,
          optionContext = ConverterHint.REGION_PATH,
          help = CliStrings.REPLICATE_REGION__REGION__HELP) String regionName,
      @CliOption(key = CliStrings.REPLICATE_REGION__SENDERID,
          optionContext = ConverterHint.GATEWAY_SENDER_ID,
          help = CliStrings.REPLICATE_REGION__SENDERID__HELP) String senderId,
      @CliOption(key = CliStrings.REPLICATE_REGION__MAXRATE,
          unspecifiedDefaultValue = "0",
          help = CliStrings.REPLICATE_REGION__MAXRATE__HELP) long maxRate,
      @CliOption(key = CliStrings.REPLICATE_REGION__BATCHSIZE,
          unspecifiedDefaultValue = "1000",
          help = CliStrings.REPLICATE_REGION__BATCHSIZE__HELP) int batchSize,
      @CliOption(key = CliStrings.REPLICATE_REGION__CANCEL,
          unspecifiedDefaultValue = "false",
          specifiedDefaultValue = "true",
          help = CliStrings.REPLICATE_REGION__CANCEL__HELP) boolean isCancel) {

    authorize(Resource.DATA, Operation.WRITE, regionName);
    ResultModel result;
    try {
      final Object[] args = {regionName, senderId, isCancel, maxRate, batchSize};
      ResultCollector<?, ?> rc =
          executeFunction(replicateRegionFunction, args, getAllNormalMembers());
      final List<CliFunctionResult> cliFunctionResults = new ArrayList<>();
      @SuppressWarnings("unchecked")
      final List resultsObjects = (List) rc.getResult();
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

      result = ResultModel.createMemberStatusResult(cliFunctionResults, null, null, false, false);

    } catch (CacheClosedException e) {
      result = ResultModel.createError(e.getMessage());
    } catch (FunctionInvocationTargetException e) {
      result = ResultModel.createError(
          CliStrings.format(CliStrings.COMMAND_FAILURE_MESSAGE, CliStrings.REPLICATE_REGION));
    }
    return result;
  }
}
