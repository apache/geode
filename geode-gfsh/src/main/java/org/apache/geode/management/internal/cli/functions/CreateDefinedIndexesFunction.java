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
package org.apache.geode.management.internal.cli.functions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.MultiIndexCreationException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;

public class CreateDefinedIndexesFunction implements InternalFunction<Set<RegionConfig.Index>> {

  private static final long serialVersionUID = 6756381106602823693L;

  @Override
  public String getId() {
    return CreateDefinedIndexesFunction.class.getName();
  }

  @Override
  @SuppressWarnings("deprecation")
  public void execute(FunctionContext<Set<RegionConfig.Index>> context) {
    Cache cache = context.getCache();
    QueryService queryService = cache.getQueryService();
    ResultSender<CliFunctionResult> sender = context.getResultSender();
    String memberId = cache.getDistributedSystem().getDistributedMember().getId();
    Set<RegionConfig.Index> indexDefinitions = context.getArguments();

    try {
      for (RegionConfig.Index indexDefinition : indexDefinitions) {
        String indexName = indexDefinition.getName();
        String regionPath = indexDefinition.getFromClause();
        String indexedExpression = indexDefinition.getExpression();
        org.apache.geode.cache.query.IndexType indexType =
            org.apache.geode.cache.query.IndexType.valueOfSynonym(indexDefinition.getType());

        if (indexType == org.apache.geode.cache.query.IndexType.PRIMARY_KEY) {
          queryService.defineKeyIndex(indexName, indexedExpression, regionPath);
        } else if (indexType == org.apache.geode.cache.query.IndexType.HASH) {
          queryService.defineHashIndex(indexName, indexedExpression, regionPath);
        } else {
          queryService.defineIndex(indexName, indexedExpression, regionPath);
        }
      }

      List<Index> indexes = queryService.createDefinedIndexes();

      if (!indexes.isEmpty()) {
        for (Index index : indexes) {
          sender.sendResult(
              new CliFunctionResult(memberId, true, "Created index " + index.getName()));
        }
      } else {
        sender.sendResult(
            new CliFunctionResult(memberId, true, CliStrings.DEFINE_INDEX__FAILURE__MSG));
      }

    } catch (MultiIndexCreationException multiIndexCreationException) {
      // Some indexes may have been created, so let's get those
      List<String> failedIndexes =
          new ArrayList<>(multiIndexCreationException.getFailedIndexNames());
      List<String> createdIndexes =
          indexDefinitions.stream().filter(i -> !failedIndexes.contains(i.getName()))
              .map(RegionConfig.Index::getName).collect(Collectors.toList());

      for (String index : createdIndexes) {
        sender.sendResult(new CliFunctionResult(memberId, true, "Created index " + index));
      }

      for (Map.Entry<String, Exception> ex : multiIndexCreationException.getExceptionsMap()
          .entrySet()) {
        sender.sendResult(new CliFunctionResult(memberId, false, String
            .format("Failed to create index %s: %s", ex.getKey(), ex.getValue().getMessage())));
      }
    } catch (Exception ex) {
      sender.sendResult(new CliFunctionResult(memberId, false, ex.getMessage()));
    } finally {
      queryService.clearDefinedIndexes();
    }

    sender.lastResult(null);
  }
}
