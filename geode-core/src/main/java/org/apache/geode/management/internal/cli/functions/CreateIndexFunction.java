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

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.query.IndexExistsException;
import org.apache.geode.cache.query.IndexInvalidException;
import org.apache.geode.cache.query.IndexNameConflictException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.domain.IndexInfo;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;

/***
 * Function to create index in a member, based on different arguments passed to it
 *
 */
public class CreateIndexFunction extends FunctionAdapter implements
InternalEntity {


  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext context) {
    final IndexInfo indexInfo = (IndexInfo)context.getArguments();
    String memberId = null;
    try {
      Cache cache = CacheFactory.getAnyInstance();
      memberId = cache.getDistributedSystem().getDistributedMember().getId();
      QueryService queryService = cache.getQueryService();
      String indexName = indexInfo.getIndexName();
      String indexedExpression = indexInfo.getIndexedExpression();
      String fromClause = indexInfo.getRegionPath();
      //Check to see if the region path contains an alias e.g "/region1 r1"
      //Then the first string will be the regionPath
      String []regionPathTokens = fromClause.trim().split(" ");
      String regionPath = regionPathTokens[0];

      switch (indexInfo.getIndexType()) {
        case IndexInfo.RANGE_INDEX:
          queryService.createIndex(indexName, indexedExpression, fromClause);
          break;
        case IndexInfo.KEY_INDEX:
          queryService.createKeyIndex(indexName, indexedExpression, fromClause);
          break;
        case IndexInfo.HASH_INDEX:
          queryService.createHashIndex(indexName, indexedExpression, fromClause);
          break;
        default :
          queryService.createIndex(indexName, indexedExpression, fromClause);
      }
      
      XmlEntity xmlEntity = new XmlEntity(CacheXml.REGION, "name", cache.getRegion(regionPath).getName());
      context.getResultSender().lastResult(new CliFunctionResult(memberId, xmlEntity));
    } catch (IndexExistsException e) {
      String message = CliStrings.format(CliStrings.CREATE_INDEX__INDEX__EXISTS, indexInfo.getIndexName());
      context.getResultSender().lastResult(new CliFunctionResult(memberId, false, message));
    } catch (IndexNameConflictException e) {
      String message = CliStrings.format(CliStrings.CREATE_INDEX__NAME__CONFLICT, indexInfo.getIndexName());
      context.getResultSender().lastResult(new CliFunctionResult(memberId, false, message));
    } catch (RegionNotFoundException e) {
      String message = CliStrings.format(CliStrings.CREATE_INDEX__INVALID__REGIONPATH, indexInfo.getRegionPath());
      context.getResultSender().lastResult(new CliFunctionResult(memberId, false, message));
    } catch (IndexInvalidException e) {
      context.getResultSender().lastResult(new CliFunctionResult(memberId, e, e.getMessage()));
    }
    catch (Exception e) {
      String exceptionMessage = CliStrings.format(CliStrings.EXCEPTION_CLASS_AND_MESSAGE, e.getClass().getName(), e.getMessage());
      context.getResultSender().lastResult(new CliFunctionResult(memberId, e, e.getMessage()));
    }
  }
 
  @Override
  public String getId() {
    return CreateIndexFunction.class.getName();
  }
}
