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

import org.apache.commons.lang3.StringUtils;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.lucene.internal.cli.LuceneCliStrings;
import org.apache.geode.cache.lucene.internal.cli.LuceneDestroyIndexInfo;
import org.apache.geode.cache.lucene.internal.cli.functions.LuceneDestroyIndexFunction;
import org.apache.geode.cache.lucene.internal.security.LucenePermission;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.internal.cli.remote.CommandExecutor;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.security.ResourcePermission;

public class LuceneDestroyIndexCommand extends LuceneCommandBase {

  private static final LuceneDestroyIndexFunction destroyIndexFunction =
      new LuceneDestroyIndexFunction();

  @CliCommand(value = LuceneCliStrings.LUCENE_DESTROY_INDEX,
      help = LuceneCliStrings.LUCENE_DESTROY_INDEX__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_DATA})
  public ResultModel destroyIndex(@CliOption(key = LuceneCliStrings.LUCENE__INDEX_NAME,
      help = LuceneCliStrings.LUCENE_DESTROY_INDEX__NAME__HELP) final String indexName,

      @CliOption(key = LuceneCliStrings.LUCENE__REGION_PATH, mandatory = true,
          optionContext = ConverterHint.REGION_PATH,
          help = LuceneCliStrings.LUCENE_DESTROY_INDEX__REGION_HELP) final String regionPath) {

    if (indexName != null && StringUtils.isEmpty(indexName)) {
      return ResultModel.createInfo(
          CliStrings.format(LuceneCliStrings.LUCENE_DESTROY_INDEX__MSG__INDEX_CANNOT_BE_EMPTY));
    }

    authorize(ResourcePermission.Resource.CLUSTER, ResourcePermission.Operation.MANAGE,
        LucenePermission.TARGET);

    // Get members >= Geode 1.8 (when the new destroy code path went into the product)
    Set<DistributedMember> validVersionMembers =
        getNormalMembersWithSameOrNewerVersion(KnownVersion.GEODE_1_7_0);
    if (validVersionMembers.isEmpty()) {
      return ResultModel.createInfo(CliStrings.format(
          LuceneCliStrings.LUCENE_DESTROY_INDEX__MSG__COULD_NOT_FIND__MEMBERS_GREATER_THAN_VERSION_0,
          KnownVersion.GEODE_1_7_0));
    }

    // Execute the destroy index function
    LuceneDestroyIndexInfo indexInfo = new LuceneDestroyIndexInfo(indexName, regionPath);
    ResultCollector<?, ?> rc =
        executeFunction(destroyIndexFunction, indexInfo, validVersionMembers);

    // Get the result
    List<CliFunctionResult> cliFunctionResults = (List<CliFunctionResult>) rc.getResult();
    ResultModel result = getDestroyIndexResult(cliFunctionResults, indexName, regionPath);

    // Get and process the xml entity
    XmlEntity xmlEntity = findXmlEntity(cliFunctionResults);

    // if at least one member returns with successful deletion, we will need to update cc
    InternalConfigurationPersistenceService configurationPersistenceService =
        getConfigurationPersistenceService();
    if (xmlEntity != null) {
      if (configurationPersistenceService == null) {
        result.addInfo().addLine(CommandExecutor.SERVICE_NOT_RUNNING_CHANGE_NOT_PERSISTED);
      } else {
        configurationPersistenceService.deleteXmlEntity(xmlEntity, null);
      }
    }

    return result;
  }

  private ResultModel getDestroyIndexResult(List<CliFunctionResult> cliFunctionResults,
      String indexName, String regionPath) {

    ResultModel result = new ResultModel();
    TabularResultModel tabularResult = result.addTable("lucene-indexes");
    for (CliFunctionResult cliFunctionResult : cliFunctionResults) {
      tabularResult.accumulate("Member", cliFunctionResult.getMemberIdOrName());
      if (cliFunctionResult.isSuccessful()) {
        tabularResult.accumulate("Status",
            indexName == null
                ? CliStrings.format(
                    LuceneCliStrings.LUCENE_DESTROY_INDEX__MSG__SUCCESSFULLY_DESTROYED_INDEXES_FROM_REGION_0,
                    new Object[] {regionPath})
                : CliStrings.format(
                    LuceneCliStrings.LUCENE_DESTROY_INDEX__MSG__SUCCESSFULLY_DESTROYED_INDEX_0_FROM_REGION_1,
                    indexName, regionPath));
      } else {
        tabularResult.accumulate("Status", cliFunctionResult.getMessage());
      }
    }
    return result;
  }

  private XmlEntity findXmlEntity(List<CliFunctionResult> functionResults) {
    return functionResults.stream().filter(CliFunctionResult::isSuccessful)
        .map(CliFunctionResult::getXmlEntity).filter(Objects::nonNull).findFirst().orElse(null);
  }

  @CliAvailabilityIndicator(LuceneCliStrings.LUCENE_DESTROY_INDEX)
  public boolean indexCommandsAvailable() {
    return super.indexCommandsAvailable();
  }
}
