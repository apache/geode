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

package org.apache.geode.cache.lucene.internal.cli.commands;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.lucene.internal.cli.LuceneCliStrings;
import org.apache.geode.cache.lucene.internal.cli.LuceneFunctionSerializable;
import org.apache.geode.cache.lucene.internal.cli.LuceneIndexDetails;
import org.apache.geode.cache.lucene.internal.cli.LuceneIndexInfo;
import org.apache.geode.cache.lucene.internal.cli.LuceneIndexStatus;
import org.apache.geode.cache.lucene.internal.cli.functions.LuceneDescribeIndexFunction;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.exceptions.UserErrorException;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.util.ManagementUtils;

public abstract class LuceneCommandBase extends GfshCommand {

  private static final LuceneDescribeIndexFunction describeIndexFunction =
      new LuceneDescribeIndexFunction();

  protected ResultModel toTabularResult(final List<LuceneIndexDetails> indexDetailsList,
      boolean stats) {
    if (!indexDetailsList.isEmpty()) {
      ResultModel result = new ResultModel();
      TabularResultModel indexData = result.addTable("lucene-indexes");

      for (final LuceneIndexDetails indexDetails : indexDetailsList) {
        indexData.accumulate("Index Name", indexDetails.getIndexName());
        indexData.accumulate("Region Path", indexDetails.getRegionPath());
        indexData.accumulate("Server Name", indexDetails.getServerName());
        indexData.accumulate("Indexed Fields", indexDetails.getSearchableFieldNamesString());
        indexData.accumulate("Field Analyzer", indexDetails.getFieldAnalyzersString());
        indexData.accumulate("Serializer", indexDetails.getSerializerString());
        indexData.accumulate("Status", indexDetails.getStatus().toString());

        if (stats) {
          LuceneIndexStatus luceneIndexStatus = indexDetails.getStatus();
          if (luceneIndexStatus == LuceneIndexStatus.NOT_INITIALIZED
              || luceneIndexStatus == LuceneIndexStatus.INDEXING_IN_PROGRESS) {
            indexData.accumulate("Query Executions", "NA");
            indexData.accumulate("Updates", "NA");
            indexData.accumulate("Commits", "NA");
            indexData.accumulate("Documents", "NA");
          } else {
            indexData.accumulate("Query Executions",
                indexDetails.getIndexStats().get("queryExecutions").toString());
            indexData.accumulate("Updates",
                indexDetails.getIndexStats().get("updates").toString());
            indexData.accumulate("Commits",
                indexDetails.getIndexStats().get("commits").toString());
            indexData.accumulate("Documents",
                indexDetails.getIndexStats().get("documents").toString());
          }
        }
      }
      return result;
    } else {
      return ResultModel
          .createInfo(LuceneCliStrings.LUCENE_LIST_INDEX__INDEXES_NOT_FOUND_MESSAGE);
    }
  }

  @SuppressWarnings("unchecked")
  protected List<LuceneIndexDetails> getIndexDetails(LuceneIndexInfo indexInfo) throws Exception {
    final ResultCollector<?, ?> rc =
        executeFunctionOnRegion(describeIndexFunction, indexInfo, true);
    final List<LuceneIndexDetails> funcResults = (List<LuceneIndexDetails>) rc.getResult();
    return funcResults.stream().filter(Objects::nonNull).collect(Collectors.toList());
  }

  protected ResultCollector<?, ?> executeFunctionOnRegion(Function function,
      LuceneFunctionSerializable functionArguments, boolean returnAllMembers) {
    Set<DistributedMember> targetMembers = ManagementUtils.getRegionAssociatedMembers(
        functionArguments.getRegionPath(), (InternalCache) getCache(), returnAllMembers);
    if (targetMembers.isEmpty()) {
      throw new UserErrorException(CliStrings.format(
          LuceneCliStrings.LUCENE_DESTROY_INDEX__MSG__COULDNOT_FIND_MEMBERS_FOR_REGION_0,
          new Object[] {functionArguments.getRegionPath()}));
    }
    return executeFunction(function, functionArguments, targetMembers);
  }

  protected boolean indexCommandsAvailable() {
    Gfsh gfsh = Gfsh.getCurrentInstance();

    // command should always be available on the server
    if (gfsh == null) {
      return true;
    }

    // if in gfshVM, only when gfsh is connected and ready
    return gfsh.isConnectedAndReady();
  }
}
