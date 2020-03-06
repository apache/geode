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

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.query.IndexExistsException;
import org.apache.geode.cache.query.IndexInvalidException;
import org.apache.geode.cache.query.IndexNameConflictException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;

/***
 * Function to create index in a member, based on different arguments passed to it
 *
 */
public class CreateIndexFunction implements InternalFunction<RegionConfig.Index> {


  private static final long serialVersionUID = 1L;

  @Override
  @SuppressWarnings("deprecation")
  public void execute(FunctionContext<RegionConfig.Index> context) {
    final RegionConfig.Index indexInfo = context.getArguments();
    String memberId = null;
    try {
      Cache cache = context.getCache();
      memberId = cache.getDistributedSystem().getDistributedMember().getId();
      QueryService queryService = cache.getQueryService();
      String indexName = indexInfo.getName();
      String indexedExpression = indexInfo.getExpression();
      String fromClause = indexInfo.getFromClause();
      if (indexInfo.isKeyIndex()) {
        queryService.createKeyIndex(indexName, indexedExpression, fromClause);
      } else if ("hash".equals(indexInfo.getType())) {
        queryService.createHashIndex(indexName, indexedExpression, fromClause);
      } else {
        queryService.createIndex(indexName, indexedExpression, fromClause);
      }

      context.getResultSender()
          .lastResult(new CliFunctionResult(memberId, true, "Index successfully created"));

    } catch (IndexExistsException e) {
      String message =
          CliStrings.format(CliStrings.CREATE_INDEX__INDEX__EXISTS, indexInfo.getName());
      context.getResultSender().lastResult(new CliFunctionResult(memberId, false, message));
    } catch (IndexNameConflictException e) {
      String message =
          CliStrings.format(CliStrings.CREATE_INDEX__NAME__CONFLICT, indexInfo.getName());
      context.getResultSender().lastResult(new CliFunctionResult(memberId, false, message));
    } catch (RegionNotFoundException e) {
      String message = CliStrings.format(CliStrings.CREATE_INDEX__INVALID__REGIONPATH,
          indexInfo.getFromClause());
      context.getResultSender().lastResult(new CliFunctionResult(memberId, false, message));
    } catch (IndexInvalidException e) {
      context.getResultSender().lastResult(new CliFunctionResult(memberId, e, e.getMessage()));
    } catch (Exception e) {
      String exceptionMessage = CliStrings.format(CliStrings.EXCEPTION_CLASS_AND_MESSAGE,
          e.getClass().getName(), e.getMessage());
      context.getResultSender().lastResult(new CliFunctionResult(memberId, e, exceptionMessage));
    }
  }

  @Override
  public String getId() {
    return CreateIndexFunction.class.getName();
  }
}
