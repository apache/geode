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

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.lucene.internal.cli.LuceneCliStrings;
import org.apache.geode.cache.lucene.internal.cli.LuceneIndexDetails;
import org.apache.geode.cache.lucene.internal.cli.functions.LuceneListIndexFunction;
import org.apache.geode.cache.lucene.internal.security.LucenePermission;
import org.apache.geode.internal.cache.execute.AbstractExecution;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.security.ResourcePermission;

public class LuceneListIndexCommand extends LuceneCommandBase {

  @CliCommand(value = LuceneCliStrings.LUCENE_LIST_INDEX,
      help = LuceneCliStrings.LUCENE_LIST_INDEX__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_DATA})
  public ResultModel listIndex(@CliOption(key = LuceneCliStrings.LUCENE_LIST_INDEX__STATS,
      specifiedDefaultValue = "true", unspecifiedDefaultValue = "false",
      help = LuceneCliStrings.LUCENE_LIST_INDEX__STATS__HELP) final boolean stats) {

    authorize(ResourcePermission.Resource.CLUSTER, ResourcePermission.Operation.READ,
        LucenePermission.TARGET);
    return toTabularResult(getIndexListing(), stats);
  }

  @SuppressWarnings("unchecked")
  protected List<LuceneIndexDetails> getIndexListing() {
    final Execution functionExecutor = getMembersFunctionExecutor(getAllMembers());

    if (functionExecutor instanceof AbstractExecution) {
      ((AbstractExecution) functionExecutor).setIgnoreDepartedMembers(true);
    }

    final ResultCollector resultsCollector =
        functionExecutor.execute(new LuceneListIndexFunction());
    final List<Set<LuceneIndexDetails>> results =
        (List<Set<LuceneIndexDetails>>) resultsCollector.getResult();

    List<LuceneIndexDetails> sortedResults =
        results.stream().flatMap(Collection::stream).sorted().collect(Collectors.toList());
    LinkedHashSet<LuceneIndexDetails> uniqResults = new LinkedHashSet<>(sortedResults);
    sortedResults.clear();
    sortedResults.addAll(uniqResults);
    return sortedResults;
  }

  @CliAvailabilityIndicator({
      LuceneCliStrings.LUCENE_LIST_INDEX,
  })
  public boolean indexCommandsAvailable() {
    return super.indexCommandsAvailable();
  }
}
