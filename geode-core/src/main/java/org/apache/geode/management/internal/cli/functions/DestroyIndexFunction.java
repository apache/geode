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

import java.util.Collection;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;

public class DestroyIndexFunction extends CliFunction {
  private static final long serialVersionUID = -868082551095130315L;

  @Override
  public CliFunctionResult executeFunction(FunctionContext context) {
    RegionConfig.Index indexInfo = (RegionConfig.Index) context.getArguments();
    String memberId = null;

    CliFunctionResult result;
    try {
      Cache cache = context.getCache();
      memberId = cache.getDistributedSystem().getDistributedMember().getId();
      QueryService queryService = cache.getQueryService();
      String indexName = indexInfo.getName();
      String regionPath = indexInfo.getFromClause();

      if (regionPath != null && !regionPath.isEmpty()) {
        Region<?, ?> region = cache.getRegion(regionPath);

        if (region != null) {
          if (indexName == null || indexName.isEmpty()) {
            queryService.removeIndexes(region);
            result = new CliFunctionResult(memberId, CliFunctionResult.StatusState.OK,
                "Destroyed all indexes on region " + regionPath);
          } else {
            Index index = queryService.getIndex(region, indexName);

            if (index != null) {
              queryService.removeIndex(index);
              result = new CliFunctionResult(memberId, CliFunctionResult.StatusState.OK,
                  "Destroyed index " + indexName + " on region " + regionPath);
            } else {
              result = new CliFunctionResult(memberId, CliFunctionResult.StatusState.IGNORABLE,
                  CliStrings.format(CliStrings.DESTROY_INDEX__INDEX__NOT__FOUND, indexName));
            }
          }
        } else {
          result = new CliFunctionResult(memberId, CliFunctionResult.StatusState.ERROR,
              CliStrings.format(CliStrings.DESTROY_INDEX__REGION__NOT__FOUND, regionPath));
        }
      } else {
        if (indexName == null || indexName.isEmpty()) {
          queryService.removeIndexes();
          result = new CliFunctionResult(memberId, CliFunctionResult.StatusState.OK,
              "Destroyed all indexes");
        } else {
          boolean indexRemoved = removeIndexByName(indexName, queryService);
          if (indexRemoved) {
            result = new CliFunctionResult(memberId, CliFunctionResult.StatusState.OK,
                "Destroyed index " + indexName);
          } else {
            result = new CliFunctionResult(memberId, CliFunctionResult.StatusState.IGNORABLE,
                CliStrings.format(CliStrings.DESTROY_INDEX__INDEX__NOT__FOUND, indexName));
          }
        }
      }
    } catch (CacheClosedException e) {
      result = new CliFunctionResult(memberId, e, e.getMessage());
    } catch (Exception e) {
      result = new CliFunctionResult(memberId, e, e.getMessage());
    }

    return result;
  }

  /***
   *
   * @return true if the index was found and removed/false if the index was not found.
   */
  private boolean removeIndexByName(String name, QueryService queryService) {
    boolean removed = false;
    Collection<Index> indexes = queryService.getIndexes();

    for (Index index : indexes) {
      if (index.getName().equals(name)) {
        queryService.removeIndex(index);
        removed = true;
      }
    }

    return removed;
  }

  @Override
  public String getId() {
    return DestroyIndexFunction.class.getName();
  }

}
