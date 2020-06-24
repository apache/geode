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
package org.apache.geode.cache.lucene.internal.cli;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.lucene.internal.cli.functions.LuceneCreateIndexFunction;
import org.apache.geode.cache.lucene.internal.security.LucenePermission;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.internal.cli.remote.CommandExecutor;
import org.apache.geode.management.internal.cli.result.CommandResultException;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

@SuppressWarnings("unused")
public class LuceneCreateIndexCommand extends LuceneCommandBase {

  /**
   * On the server, we also verify the resource operation permissions CLUSTER:WRITE:DISK
   */
  @CliCommand(value = LuceneCliStrings.LUCENE_CREATE_INDEX,
      help = LuceneCliStrings.LUCENE_CREATE_INDEX__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_DATA})
  // TODO : Add optionContext for indexName
  public ResultModel createIndex(
      @CliOption(key = LuceneCliStrings.LUCENE__INDEX_NAME, mandatory = true,
          help = LuceneCliStrings.LUCENE_CREATE_INDEX__NAME__HELP) final String indexName,

      @CliOption(key = LuceneCliStrings.LUCENE__REGION_PATH, mandatory = true,
          optionContext = ConverterHint.REGION_PATH,
          help = LuceneCliStrings.LUCENE_CREATE_INDEX__REGION_HELP) final String regionPath,

      @CliOption(key = LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, mandatory = true,
          help = LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD_HELP) final String[] fields,

      @CliOption(key = LuceneCliStrings.LUCENE_CREATE_INDEX__ANALYZER,
          help = LuceneCliStrings.LUCENE_CREATE_INDEX__ANALYZER_HELP) final String[] analyzers,
      @CliOption(key = LuceneCliStrings.LUCENE_CREATE_INDEX__SERIALIZER,
          help = LuceneCliStrings.LUCENE_CREATE_INDEX__SERIALIZER_HELP) final String serializer)
      throws CommandResultException {

    // Every lucene index potentially writes to disk.
    authorize(Resource.CLUSTER, Operation.MANAGE, LucenePermission.TARGET);

    final InternalCache cache = (InternalCache) getCache();

    // trim fields for any leading trailing spaces.
    String[] trimmedFields = Arrays.stream(fields).map(String::trim).toArray(String[]::new);
    LuceneIndexInfo indexInfo =
        new LuceneIndexInfo(indexName, regionPath, trimmedFields, analyzers, serializer);

    final ResultCollector<?, ?> rc =
        this.executeFunctionOnAllMembers(new LuceneCreateIndexFunction(getModuleService()),
            indexInfo);
    final List<CliFunctionResult> funcResults = (List<CliFunctionResult>) rc.getResult();
    final XmlEntity xmlEntity = funcResults.stream().filter(CliFunctionResult::isSuccessful)
        .map(CliFunctionResult::getXmlEntity).filter(Objects::nonNull).findFirst().orElse(null);
    final ResultModel result = new ResultModel();
    final TabularResultModel tabularResult = result.addTable("lucene-indexes");
    for (final CliFunctionResult cliFunctionResult : funcResults) {
      tabularResult.accumulate("Member", cliFunctionResult.getMemberIdOrName());

      if (cliFunctionResult.isSuccessful()) {
        tabularResult.accumulate("Status", "Successfully created lucene index");
      } else {
        tabularResult.accumulate("Status", "Failed: " + cliFunctionResult.getMessage());
      }
    }

    // if at least one member returns with successful deletion, we will need to update cc
    InternalConfigurationPersistenceService configurationPersistenceService =
        getConfigurationPersistenceService();
    if (xmlEntity != null) {
      if (configurationPersistenceService == null) {
        result.addInfo().addLine(CommandExecutor.SERVICE_NOT_RUNNING_CHANGE_NOT_PERSISTED);
      } else {
        configurationPersistenceService.addXmlEntity(xmlEntity, null);
      }
    }

    return result;
  }

  protected ResultCollector<?, ?> executeFunctionOnAllMembers(Function function,
      final LuceneFunctionSerializable functionArguments)
      throws IllegalArgumentException {
    Set<DistributedMember> targetMembers = getAllNormalMembers();
    return executeFunction(function, functionArguments, targetMembers);
  }

  @CliAvailabilityIndicator(LuceneCliStrings.LUCENE_CREATE_INDEX)
  public boolean indexCommandsAvailable() {
    return super.indexCommandsAvailable();
  }
}
