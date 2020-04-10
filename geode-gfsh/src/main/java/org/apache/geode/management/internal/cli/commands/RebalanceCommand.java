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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.StringUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.executors.LoggingExecutors;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.operation.RebalanceOperationPerformer;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.management.operation.RebalanceOperation;
import org.apache.geode.management.runtime.RebalanceRegionResult;
import org.apache.geode.management.runtime.RebalanceResult;
import org.apache.geode.security.ResourcePermission;

public class RebalanceCommand extends GfshCommand {
  @CliCommand(value = CliStrings.REBALANCE, help = CliStrings.REBALANCE__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DATA, CliStrings.TOPIC_GEODE_REGION})
  @ResourceOperation(resource = ResourcePermission.Resource.DATA,
      operation = ResourcePermission.Operation.MANAGE)
  public ResultModel rebalance(
      @CliOption(key = CliStrings.REBALANCE__INCLUDEREGION,
          help = CliStrings.REBALANCE__INCLUDEREGION__HELP) String[] includeRegions,
      @CliOption(key = CliStrings.REBALANCE__EXCLUDEREGION,
          help = CliStrings.REBALANCE__EXCLUDEREGION__HELP) String[] excludeRegions,
      @CliOption(key = CliStrings.REBALANCE__TIMEOUT, unspecifiedDefaultValue = "-1",
          help = CliStrings.REBALANCE__TIMEOUT__HELP) long timeout,
      @CliOption(key = CliStrings.REBALANCE__SIMULATE, specifiedDefaultValue = "true",
          unspecifiedDefaultValue = "false",
          help = CliStrings.REBALANCE__SIMULATE__HELP) boolean simulate) {

    ExecutorService commandExecutors =
        LoggingExecutors.newSingleThreadExecutor("RebalanceCommand", false);
    List<Future<ResultModel>> commandResult = new ArrayList<>();
    ResultModel result;
    try {
      commandResult.add(
          commandExecutors.submit(rebalanceCallable(includeRegions, excludeRegions, simulate)));

      Future<ResultModel> fs = commandResult.get(0);
      if (timeout > 0) {
        result = fs.get(timeout, TimeUnit.SECONDS);
      } else {
        result = fs.get();
      }
    } catch (TimeoutException timeoutException) {
      result = ResultModel.createInfo(CliStrings.REBALANCE__MSG__REBALANCE_WILL_CONTINUE);
    } catch (Exception ex) {
      result = ResultModel.createError(ex.getMessage());
    }

    return result;
  }

  public Callable<ResultModel> rebalanceCallable(String[] includeRegions, String[] excludeRegions,
      boolean simulate) {
    return new ExecuteRebalanceWithTimeout(includeRegions, excludeRegions, simulate,
        (InternalCache) getCache());
  }

  private void toCompositeResultData(ResultModel result,
      RebalanceRegionResult results, int index, boolean simulate,
      InternalCache cache) {
    List<String> rsltList = new ArrayList<>();
    rsltList.add(0, String.valueOf(results.getBucketCreateBytes()));
    rsltList.add(1, String.valueOf(results.getBucketCreateTimeInMilliseconds()));
    rsltList.add(2, String.valueOf(results.getBucketCreatesCompleted()));
    rsltList.add(3, String.valueOf(results.getBucketTransferBytes()));
    rsltList.add(4, String.valueOf(results.getBucketTransferTimeInMilliseconds()));
    rsltList.add(5, String.valueOf(results.getBucketTransfersCompleted()));
    rsltList.add(6, String.valueOf(results.getPrimaryTransferTimeInMilliseconds()));
    rsltList.add(7, String.valueOf(results.getPrimaryTransfersCompleted()));
    rsltList.add(8, String.valueOf(results.getTimeInMilliseconds()));
    rsltList.add(9, String.valueOf(results.getNumOfMembers()));
    String regionName = results.getRegionName();
    if (!regionName.startsWith("/")) {
      regionName = "/" + regionName;
    }
    rsltList.add(10, regionName);

    toCompositeResultData(result, rsltList, index, simulate, cache);
  }


  private void toCompositeResultData(ResultModel result,
      List<String> rstlist, int index, boolean simulate,
      InternalCache cache) {
    final int resultItemCount = 10;

    if (rstlist.size() <= resultItemCount || StringUtils.isEmpty(rstlist.get(resultItemCount))) {
      return;
    }

    TabularResultModel table1 = result.addTable("Table" + index);
    String newLine = System.getProperty("line.separator");
    StringBuilder resultStr = new StringBuilder();
    resultStr.append(newLine);
    table1.accumulate("Rebalanced Stats", CliStrings.REBALANCE__MSG__TOTALBUCKETCREATEBYTES);
    table1.accumulate("Value", rstlist.get(0));
    resultStr.append(CliStrings.REBALANCE__MSG__TOTALBUCKETCREATEBYTES).append(" = ")
        .append(rstlist.get(0)).append(newLine);
    table1.accumulate("Rebalanced Stats", CliStrings.REBALANCE__MSG__TOTALBUCKETCREATETIM);
    table1.accumulate("Value", rstlist.get(1));
    resultStr.append(CliStrings.REBALANCE__MSG__TOTALBUCKETCREATETIM).append(" = ")
        .append(rstlist.get(1)).append(newLine);

    table1.accumulate("Rebalanced Stats", CliStrings.REBALANCE__MSG__TOTALBUCKETCREATESCOMPLETED);
    table1.accumulate("Value", rstlist.get(2));
    resultStr.append(CliStrings.REBALANCE__MSG__TOTALBUCKETCREATESCOMPLETED).append(" = ")
        .append(rstlist.get(2)).append(newLine);

    table1.accumulate("Rebalanced Stats", CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERBYTES);
    table1.accumulate("Value", rstlist.get(3));
    resultStr.append(CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERBYTES).append(" = ")
        .append(rstlist.get(3)).append(newLine);

    table1.accumulate("Rebalanced Stats", CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERTIME);
    table1.accumulate("Value", rstlist.get(4));
    resultStr.append(CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERTIME).append(" = ")
        .append(rstlist.get(4)).append(newLine);

    table1.accumulate("Rebalanced Stats",
        CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERSCOMPLETED);
    table1.accumulate("Value", rstlist.get(5));
    resultStr.append(CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERSCOMPLETED).append(" = ")
        .append(rstlist.get(5)).append(newLine);

    table1.accumulate("Rebalanced Stats", CliStrings.REBALANCE__MSG__TOTALPRIMARYTRANSFERTIME);
    table1.accumulate("Value", rstlist.get(6));
    resultStr.append(CliStrings.REBALANCE__MSG__TOTALPRIMARYTRANSFERTIME).append(" = ")
        .append(rstlist.get(6)).append(newLine);

    table1.accumulate("Rebalanced Stats",
        CliStrings.REBALANCE__MSG__TOTALPRIMARYTRANSFERSCOMPLETED);
    table1.accumulate("Value", rstlist.get(7));
    resultStr.append(CliStrings.REBALANCE__MSG__TOTALPRIMARYTRANSFERSCOMPLETED).append(" = ")
        .append(rstlist.get(7)).append(newLine);

    table1.accumulate("Rebalanced Stats", CliStrings.REBALANCE__MSG__TOTALTIME);
    table1.accumulate("Value", rstlist.get(8));
    resultStr.append(CliStrings.REBALANCE__MSG__TOTALTIME).append(" = ").append(rstlist.get(8))
        .append(newLine);

    table1.accumulate("Rebalanced Stats", CliStrings.REBALANCE__MSG__MEMBER_COUNT);
    table1.accumulate("Value", rstlist.get(9));
    resultStr.append(CliStrings.REBALANCE__MSG__MEMBER_COUNT).append(" = ")
        .append(rstlist.get(9))
        .append(newLine);

    String headerText;
    if (simulate) {
      headerText = "Simulated partition regions";
    } else {
      headerText = "Rebalanced partition regions";
    }
    for (int i = resultItemCount; i < rstlist.size(); i++) {
      headerText = headerText + " " + rstlist.get(i);
    }
    table1.setHeader(headerText);
    cache.getLogger().info(headerText + resultStr);
  }


  // TODO EY Move this to its own class
  private class ExecuteRebalanceWithTimeout implements Callable<ResultModel> {

    String[] includeRegions = null;
    String[] excludeRegions = null;
    boolean simulate;
    InternalCache cache = null;

    @Override
    public ResultModel call() throws Exception {
      ResultModel result = executeRebalanceWithTimeout(includeRegions, excludeRegions, simulate);

      // if the result contains only error section, i.e. no rebalance operation is done, mark this
      // command result to be error. This would happy if user hasn't specified any valid region. If
      // only one region specified is valid and rebalance is done, the result would be marked as
      // success.
      if (result.getSectionSize() == 1 && result.getInfoSection("error") != null) {
        result.setStatus(Result.Status.ERROR);
      }

      return result;
    }

    ExecuteRebalanceWithTimeout(String[] includedRegions, String[] excludedRegions,
        boolean toSimulate, InternalCache cache) {
      includeRegions = includedRegions;
      excludeRegions = excludedRegions;
      simulate = toSimulate;
      this.cache = cache;
    }

    private ResultModel executeRebalanceWithTimeout(String[] includeRegions,
        String[] excludeRegions, boolean simulate) {

      ResultModel result = new ResultModel();
      RebalanceOperation operation = new RebalanceOperation();

      if (includeRegions != null) {
        operation.setIncludeRegions(Arrays.asList(includeRegions));
      }
      if (excludeRegions != null) {
        operation.setExcludeRegions(Arrays.asList(excludeRegions));
      }
      operation.setSimulate(simulate);

      // do rebalance
      RebalanceResult rebalanceResult = RebalanceOperationPerformer.perform(cache, operation);

      // check for error
      if (!rebalanceResult.getSuccess()) {
        result.addInfo("error");
      }

      // convert results to ResultModel
      int index = 0;
      for (RebalanceRegionResult rebalanceRegionResult : rebalanceResult
          .getRebalanceRegionResults()) {
        toCompositeResultData(result, rebalanceRegionResult, index, simulate, cache);
        index++;
      }

      return result;
    }
  }
}
