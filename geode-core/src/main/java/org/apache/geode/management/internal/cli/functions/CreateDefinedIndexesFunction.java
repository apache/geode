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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.MultiIndexCreationException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.cli.converters.IndexTypeConverter;
import org.apache.geode.management.internal.cli.domain.IndexInfo;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;

public class CreateDefinedIndexesFunction extends InternalFunction {

  private static final long serialVersionUID = 6756381106602823693L;

  @Override
  public String getId() {
    return CreateDefinedIndexesFunction.class.getName();
  }

//  XmlEntity createXmlEntity(final String regionName) {
//    return new XmlEntity(CacheXml.REGION, "name", regionName);
//  }

  @Override
  public void execute(FunctionContext context) {
    Cache cache;
    String memberId = null;
    boolean lastResultSent = Boolean.FALSE;

    try {
      cache = context.getCache();
      ResultSender sender = context.getResultSender();
      QueryService queryService = cache.getQueryService();
      memberId = cache.getDistributedSystem().getDistributedMember().getId();
      Set<RegionConfig.Index> indexDefinitions = (Set<RegionConfig.Index>) context.getArguments();

      for (RegionConfig.Index indexDefinition : indexDefinitions) {
        String indexName = indexDefinition.getName();
        String regionPath = indexDefinition.getFromClause();
        String indexedExpression = indexDefinition.getExpression();
        IndexType indexType = IndexType.valueOf(indexDefinition.getType());

        if (indexType == IndexType.PRIMARY_KEY) {
          queryService.defineKeyIndex(indexName, indexedExpression, regionPath);
        } else if (indexType == IndexType.HASH) {
          queryService.defineHashIndex(indexName, indexedExpression, regionPath);
        } else {
          queryService.defineIndex(indexName, indexedExpression, regionPath);
        }
      }

      List<Index> indexes = queryService.createDefinedIndexes();
      // Build the results. We pass back a list of regions on which indexes were created
      List<String> processedRegions = new ArrayList<>();
//      List<CliFunctionResult> functionResults = new ArrayList<>();

      for (Index index : indexes) {
        String regionName = index.getRegion().getName();

        if (!processedRegions.contains(regionName)) {
//          XmlEntity xmlEntity = createXmlEntity(regionName);
//          functionResults.add(new CliFunctionResult(memberId, xmlEntity));
          processedRegions.add(regionName);
        }
      }

//      for (Iterator<CliFunctionResult> iterator = functionResults.iterator(); iterator.hasNext();) {
//        CliFunctionResult cliFunctionResult = iterator.next();
//
//        if (iterator.hasNext()) {
//          sender.sendResult(cliFunctionResult);
//        } else {
//          sender.lastResult(cliFunctionResult);
//          lastResultSent = Boolean.TRUE;
//        }
//      }
//
//      if (!lastResultSent) {
//        // No indexes were created and no exceptions were thrown during the process.
//        // We still need to make sure the function returns to the caller.
//        sender.lastResult(
//            new CliFunctionResult(memberId, true, CliStrings.DEFINE_INDEX__FAILURE__MSG));
//      }
      sender.lastResult(new CliFunctionResult(memberId, processedRegions));
    } catch (MultiIndexCreationException multiIndexCreationException) {
//      StringBuffer sb = new StringBuffer();
//      sb.append("Index creation failed for indexes: ").append("\n");
//      for (Map.Entry<String, Exception> failedIndex : multiIndexCreationException.getExceptionsMap()
//          .entrySet()) {
//        sb.append(failedIndex.getKey()).append(" : ").append(failedIndex.getValue().getMessage())
//            .append("\n");
//      }
      context.getResultSender()
          .lastResult(new CliFunctionResult(memberId, multiIndexCreationException));
    } catch (Exception exception) {
      String exceptionMessage = CliStrings.format(CliStrings.EXCEPTION_CLASS_AND_MESSAGE,
          exception.getClass().getName(), exception.getMessage());
      context.getResultSender()
          .lastResult(new CliFunctionResult(memberId, exception, exceptionMessage));
    }


  }

}
