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
package org.apache.geode.management.internal.operation;

import java.util.ArrayList;
import java.util.List;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Cache;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.commands.RebalanceCommand;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.operation.RebalanceOperation;
import org.apache.geode.management.runtime.RebalanceRegionResult;
import org.apache.geode.management.runtime.RebalanceResult;

@Experimental
public class RebalanceOperationPerformer {
  public static RebalanceResult perform(Cache cache, RebalanceOperation parameters) {
    try {
      RebalanceCommand rebalanceCommand = new RebalanceCommand();
      rebalanceCommand.setCache(cache);

      ResultModel result =
          rebalanceCommand.rebalanceCallable(toArray(parameters.getIncludeRegions()),
              toArray(parameters.getExcludeRegions()), parameters.isSimulate()).call();

      if (result.getStatus().equals(Result.Status.ERROR)) {
        throw new RuntimeException("rebalance returned error: "
            + String.join("\n", result.getInfoSection("error").getContent()));
      }
      RebalanceResultImpl rebalanceResult = new RebalanceResultImpl();

      List<TabularResultModel> tableSections = result.getTableSections();

      List<RebalanceRegionResult> results = new ArrayList<>();
      for (TabularResultModel tableSection : tableSections) {
        results.add(toRegionStats(toRegion(tableSection.getHeader()), tableSection));
      }
      rebalanceResult.setRebalanceSummary(results);

      if (tableSections.size() == 0) {
        InfoResultModel info = result.getInfoSection("info");
        if (info != null)
          throw new RuntimeException(
              "rebalance returned info: " + String.join("\n", info.getContent()));
      }

      return rebalanceResult;
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static RebalanceRegionResult toRegionStats(String regionName, TabularResultModel table) {
    if (table == null) {
      return null;
    }
    if (table.getColumnSize() != 2) {
      throw new IllegalStateException();
    }
    RebalanceRegionResultImpl section = new RebalanceRegionResultImpl();
    section.setRegionName(regionName);
    for (int i = 0; i < table.getRowSize(); i++) {
      List<String> row = table.getValuesInRow(i);
      String key = row.get(0);
      String val = row.get(1);

      if (key.equals(CliStrings.REBALANCE__MSG__TOTALBUCKETCREATEBYTES)) {
        section.setBucketCreateBytes(Long.parseLong(val));
      } else if (key.equals(CliStrings.REBALANCE__MSG__TOTALBUCKETCREATETIM)) {
        section.setBucketCreateTimeInMilliseconds(Long.parseLong(val));
      } else if (key.equals(CliStrings.REBALANCE__MSG__TOTALBUCKETCREATESCOMPLETED)) {
        section.setBucketCreatesCompleted(Integer.parseInt(val));
      } else if (key.equals(CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERBYTES)) {
        section.setBucketTransferBytes(Long.parseLong(val));
      } else if (key.equals(CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERTIME)) {
        section.setBucketTransferTimeInMilliseconds(Long.parseLong(val));
      } else if (key.equals(CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERSCOMPLETED)) {
        section.setBucketTransfersCompleted(Integer.parseInt(val));
      } else if (key.equals(CliStrings.REBALANCE__MSG__TOTALPRIMARYTRANSFERTIME)) {
        section.setPrimaryTransferTimeInMilliseconds(Long.parseLong(val));
      } else if (key.equals(CliStrings.REBALANCE__MSG__TOTALPRIMARYTRANSFERSCOMPLETED)) {
        section.setPrimaryTransfersCompleted(Integer.parseInt(val));
      } else if (key.equals(CliStrings.REBALANCE__MSG__TOTALTIME)) {
        section.setTimeInMilliseconds(Long.parseLong(val));
      }
    }
    return section;
  }

  private static String toRegion(String header) {
    return header.replace("Rebalanced partition regions /", "");
  }

  private static final String[] STRING_ARRAY_TYPE = new String[] {};

  private static String[] toArray(List<String> list) {
    if (list == null) {
      return null;
    } else {
      return list.toArray(STRING_ARRAY_TYPE);
    }
  }
}
