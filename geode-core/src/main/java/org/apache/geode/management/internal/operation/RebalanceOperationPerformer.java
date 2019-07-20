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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Cache;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.commands.RebalanceCommand;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.operation.RebalanceOperation;
import org.apache.geode.management.runtime.RebalanceResult;

@Experimental
public class RebalanceOperationPerformer {
  public static RebalanceResult perform(Cache cache, RebalanceOperation parameters) {
    try {
      RebalanceCommand rebalanceCommand = new RebalanceCommand();
      rebalanceCommand.setCache(cache);

      ResultModel result = rebalanceCommand
          .rebalance(arr(parameters.getIncludeRegions()), arr(parameters.getExcludeRegions()), -1,
              parameters.isSimulate());

      if (result.getStatus().equals(Result.Status.ERROR)) {
        throw new RuntimeException(
            "rebalance returned error: " + String
                .join("\n", result.getInfoSection("error").getContent()));
      }
      RebalanceResult rebalanceResult = new RebalanceResult();

      List<TabularResultModel> tableSections = result.getTableSections();
      TabularResultModel lastSection =
          (tableSections.size() > 0) ? tableSections.get(tableSections.size() - 1) : null;
      for (TabularResultModel table : tableSections) {
        rebalanceResult.getPerRegionResults().add(toMap(table));
      }
      rebalanceResult.setRebalanceSummary(toMap(lastSection));

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

  private static Map<String, String> toMap(TabularResultModel table) {
    if (table == null)
      return null;
    Map<String, String> section = new LinkedHashMap<>();
    if (table.getColumnSize() != 2)
      throw new IllegalStateException();
    for (int i = 0; i < table.getRowSize(); i++) {
      List<String> row = table.getValuesInRow(i);
      String key = row.get(0);
      String val = row.get(1);
      key = key.replace("(", "").replace(")", "").replace(' ', '-').toLowerCase();
      section.put(key, val);
    }
    return section;
  }

  private static final String[] STRING_ARRAY_TYPE = new String[] {};

  private static String[] arr(List<String> list) {
    if (list == null) {
      return null;
    } else {
      return list.toArray(STRING_ARRAY_TYPE);
    }
  }
}
