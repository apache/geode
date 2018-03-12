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
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.internal.cache.execute.AbstractExecution;
import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.domain.IndexDetails;
import org.apache.geode.management.internal.cli.domain.IndexDetails.IndexStatisticsDetails;
import org.apache.geode.management.internal.cli.functions.ListIndexFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class ListIndexCommand extends GfshCommand {
  @CliCommand(value = CliStrings.LIST_INDEX, help = CliStrings.LIST_INDEX__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_DATA})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ, target = ResourcePermission.Target.QUERY)
  public Result listIndex(@CliOption(key = CliStrings.LIST_INDEX__STATS,
      specifiedDefaultValue = "true", unspecifiedDefaultValue = "false",
      help = CliStrings.LIST_INDEX__STATS__HELP) final boolean showStats) {

    return toTabularResult(getIndexListing(), showStats);
  }

  private Result toTabularResult(final List<IndexDetails> indexDetailsList,
      final boolean showStats) {
    if (!indexDetailsList.isEmpty()) {
      final TabularResultData indexData = ResultBuilder.createTabularResultData();

      for (final IndexDetails indexDetails : indexDetailsList) {
        indexData.accumulate("Member Name",
            StringUtils.defaultString(indexDetails.getMemberName()));
        indexData.accumulate("Member ID", indexDetails.getMemberId());
        indexData.accumulate("Region Path", indexDetails.getRegionPath());
        indexData.accumulate("Name", indexDetails.getIndexName());
        if (indexDetails.getIndexType() == null) {
          indexData.accumulate("Type", "");
        } else {
          indexData.accumulate("Type", indexDetails.getIndexType().getName());
        }
        indexData.accumulate("Indexed Expression", indexDetails.getIndexedExpression());
        indexData.accumulate("From Clause", indexDetails.getFromClause());
        indexData.accumulate("Valid Index", indexDetails.getIsValid());

        if (showStats) {
          final IndexStatisticsDetailsAdapter adapter =
              new IndexStatisticsDetailsAdapter(indexDetails.getIndexStatisticsDetails());

          indexData.accumulate("Uses", adapter.getTotalUses());
          indexData.accumulate("Updates", adapter.getNumberOfUpdates());
          indexData.accumulate("Update Time", adapter.getTotalUpdateTime());
          indexData.accumulate("Keys", adapter.getNumberOfKeys());
          indexData.accumulate("Values", adapter.getNumberOfValues());
        }
      }

      return ResultBuilder.buildResult(indexData);
    } else {
      return ResultBuilder.createInfoResult(CliStrings.LIST_INDEX__INDEXES_NOT_FOUND_MESSAGE);
    }
  }

  List<IndexDetails> getIndexListing() {
    final Execution functionExecutor = getMembersFunctionExecutor(getAllMembers());

    if (functionExecutor instanceof AbstractExecution) {
      ((AbstractExecution) functionExecutor).setIgnoreDepartedMembers(true);
    }

    final ResultCollector<?, ?> resultsCollector =
        functionExecutor.execute(new ListIndexFunction());
    final List<?> results = (List<?>) resultsCollector.getResult();
    final List<IndexDetails> indexDetailsList = new ArrayList<>(results.size());

    for (Object result : results) {
      if (result instanceof Set) { // ignore FunctionInvocationTargetExceptions and other Exceptions
        indexDetailsList.addAll((Set<IndexDetails>) result);
      }
    }
    Collections.sort(indexDetailsList);
    return indexDetailsList;
  }

  protected static class IndexStatisticsDetailsAdapter {

    private final IndexStatisticsDetails indexStatisticsDetails;

    protected IndexStatisticsDetailsAdapter(final IndexStatisticsDetails indexStatisticsDetails) {
      this.indexStatisticsDetails = indexStatisticsDetails;
    }

    public IndexStatisticsDetails getIndexStatisticsDetails() {
      return indexStatisticsDetails;
    }

    public String getNumberOfKeys() {
      return getIndexStatisticsDetails() != null
          ? StringUtils.defaultString(getIndexStatisticsDetails().getNumberOfKeys()) : "";
    }

    public String getNumberOfUpdates() {
      return getIndexStatisticsDetails() != null
          ? StringUtils.defaultString(getIndexStatisticsDetails().getNumberOfUpdates()) : "";
    }

    public String getNumberOfValues() {
      return getIndexStatisticsDetails() != null
          ? StringUtils.defaultString(getIndexStatisticsDetails().getNumberOfValues()) : "";
    }

    public String getTotalUpdateTime() {
      return getIndexStatisticsDetails() != null
          ? StringUtils.defaultString(getIndexStatisticsDetails().getTotalUpdateTime()) : "";
    }

    public String getTotalUses() {
      return getIndexStatisticsDetails() != null
          ? StringUtils.defaultString(getIndexStatisticsDetails().getTotalUses()) : "";
    }
  }
}
