/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.management.internal.cli.functions;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.MultiIndexCreationException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.management.internal.cli.domain.IndexInfo;
import org.apache.geode.management.internal.cli.i18n.CliStrings;

public class CreateDefinedIndexesFunction extends FunctionAdapter implements
    InternalEntity {

  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext context) {
    String memberId = null;
    List<Index> indexes = null;
    Cache cache = null;
    try {
      cache = CacheFactory.getAnyInstance();
      memberId = cache.getDistributedSystem().getDistributedMember().getId();
      QueryService queryService = cache.getQueryService();
      Set<IndexInfo> indexDefinitions = (Set<IndexInfo>) context.getArguments();
      for(IndexInfo indexDefinition : indexDefinitions) {
        String indexName = indexDefinition.getIndexName();
        String indexedExpression = indexDefinition.getIndexedExpression();
        String regionPath = indexDefinition.getRegionPath();        
        if(indexDefinition.getIndexType() == IndexInfo.KEY_INDEX) {
          queryService.defineKeyIndex(indexName, indexedExpression, regionPath);
        } else if(indexDefinition.getIndexType() == IndexInfo.HASH_INDEX) {
          queryService.defineHashIndex(indexName, indexedExpression, regionPath);
        } else {
          queryService.defineIndex(indexName, indexedExpression, regionPath);
        }
      }
      indexes = queryService.createDefinedIndexes();
      context.getResultSender().lastResult(new CliFunctionResult(memberId));
    } catch (MultiIndexCreationException e) {
      StringBuffer sb = new StringBuffer();
      sb.append("Index creation failed for indexes: ").append("\n");
      for(Map.Entry<String, Exception> failedIndex : e.getExceptionsMap().entrySet()) {
         sb.append(failedIndex.getKey()).append(" : ").append(failedIndex.getValue().getMessage()).append("\n");
      }     
      context.getResultSender().lastResult(
          new CliFunctionResult(memberId, e, sb.toString()));
    } catch (Exception e) {
      String exceptionMessage = CliStrings.format(
          CliStrings.EXCEPTION_CLASS_AND_MESSAGE, e.getClass().getName(),
          e.getMessage());
      context.getResultSender().lastResult(
          new CliFunctionResult(memberId, e, exceptionMessage));
    }
  }

  public void createCommandObject(IndexInfo info) {
    Cache cache = CacheFactory.getAnyInstance();
    QueryService queryService = cache.getQueryService();
  }

  @Override
  public String getId() {
    return CreateDefinedIndexesFunction.class.getName();
  }

}
