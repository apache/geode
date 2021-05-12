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

import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.lucene.internal.cli.LuceneCliStrings;
import org.apache.geode.cache.lucene.internal.cli.LuceneIndexInfo;
import org.apache.geode.cache.lucene.internal.security.LucenePermission;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.security.ResourcePermission;

public class LuceneDescribeIndexCommand extends LuceneCommandBase {

  @CliCommand(value = LuceneCliStrings.LUCENE_DESCRIBE_INDEX,
      help = LuceneCliStrings.LUCENE_DESCRIBE_INDEX__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_REGION, CliStrings.TOPIC_GEODE_DATA})
  public ResultModel describeIndex(
      @CliOption(key = LuceneCliStrings.LUCENE__INDEX_NAME, mandatory = true,
          help = LuceneCliStrings.LUCENE_DESCRIBE_INDEX__NAME__HELP) final String indexName,

      @CliOption(key = LuceneCliStrings.LUCENE__REGION_PATH, mandatory = true,
          optionContext = ConverterHint.REGION_PATH,
          help = LuceneCliStrings.LUCENE_DESCRIBE_INDEX__REGION_HELP) final String regionPath)
      throws Exception {

    authorize(ResourcePermission.Resource.CLUSTER, ResourcePermission.Operation.READ,
        LucenePermission.TARGET);
    LuceneIndexInfo indexInfo = new LuceneIndexInfo(indexName, regionPath);
    return toTabularResult(getIndexDetails(indexInfo), true);
  }

  @CliAvailabilityIndicator(LuceneCliStrings.LUCENE_DESCRIBE_INDEX)
  public boolean indexCommandsAvailable() {
    return super.indexCommandsAvailable();
  }
}
