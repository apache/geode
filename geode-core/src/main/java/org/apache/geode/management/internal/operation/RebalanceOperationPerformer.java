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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.partition.PartitionRebalanceInfo;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.commands.RebalanceCommand;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.operation.RebalanceOperation;
import org.apache.geode.management.runtime.RebalanceResult;
import org.apache.geode.util.internal.GeodeJsonMapper;

@Experimental
public class RebalanceOperationPerformer {
  /**
   * the {@link ResultModel} contains human-readable strings, however we want property names that
   * more closely resemble {@link PartitionRebalanceInfo} (but with time units)
   */
  private static final Map<String, String> translations =
      Collections.unmodifiableMap(new HashMap<String, String>() {
        {
          put(CliStrings.REBALANCE__MSG__TOTALBUCKETCREATEBYTES, "bucketCreateBytes");
          put(CliStrings.REBALANCE__MSG__TOTALBUCKETCREATETIM, "bucketCreateTimeInMilliseconds");
          put(CliStrings.REBALANCE__MSG__TOTALBUCKETCREATESCOMPLETED, "bucketCreatesCompleted");
          put(CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERBYTES, "bucketTransferBytes");
          put(CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERTIME,
              "bucketTransferTimeInMilliseconds");
          put(CliStrings.REBALANCE__MSG__TOTALBUCKETTRANSFERSCOMPLETED, "bucketTransfersCompleted");
          put(CliStrings.REBALANCE__MSG__TOTALPRIMARYTRANSFERTIME,
              "primaryTransferTimeInMilliseconds");
          put(CliStrings.REBALANCE__MSG__TOTALPRIMARYTRANSFERSCOMPLETED,
              "primaryTransfersCompleted");
          put(CliStrings.REBALANCE__MSG__TOTALTIME, "timeInMilliseconds");
        }
      });

  public static RebalanceResult perform(Cache cache, RebalanceOperation parameters) {
    try {
      RebalanceCommand rebalanceCommand = new RebalanceCommand();
      rebalanceCommand.setCache(cache);

      ResultModel result = rebalanceCommand.rebalanceCallable(arr(parameters.getIncludeRegions()),
          arr(parameters.getExcludeRegions()), parameters.isSimulate()).call();

      if (result.getStatus().equals(Result.Status.ERROR)) {
        throw new RuntimeException(
            "rebalance returned error: " + String
                .join("\n", result.getInfoSection("error").getContent()));
      }
      RebalanceResultImpl rebalanceResult = new RebalanceResultImpl();

      List<TabularResultModel> tableSections = result.getTableSections();

      Map<String, RebalanceResult.PerRegionStats> results = new LinkedHashMap<>();
      for (TabularResultModel tableSection : tableSections) {
        results.put(toRegion(tableSection.getHeader()), toRegionStats(toMap(tableSection)));
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

  private static RebalanceResult.PerRegionStats toRegionStats(Map<String, Long> props)
      throws IOException {
    ObjectMapper mapper = GeodeJsonMapper.getMapper();
    String json = mapper.writeValueAsString(props);
    return mapper.readValue(json, RebalanceResultImpl.PerRegionStatsImpl.class);
  }

  private static String toRegion(String header) {
    return header.replace("Rebalanced partition regions /", "");
  }

  private static Map<String, Long> toMap(TabularResultModel table) {
    if (table == null)
      return null;
    Map<String, Long> section = new LinkedHashMap<>();
    if (table.getColumnSize() != 2)
      throw new IllegalStateException();
    for (int i = 0; i < table.getRowSize(); i++) {
      List<String> row = table.getValuesInRow(i);
      String key = row.get(0);
      String val = row.get(1);
      String betterKey = translations.get(key);
      if (betterKey != null) {
        key = betterKey;
      }
      section.put(key, toLong(val));
    }
    return section;
  }

  /**
   * try to convert a result model value to a first-class numeric type so it can be serialized
   * without string quotes
   */
  private static Long toLong(String val) {
    try {
      return Long.parseLong(val);
    } catch (Exception e) {
      return null;
    }
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
