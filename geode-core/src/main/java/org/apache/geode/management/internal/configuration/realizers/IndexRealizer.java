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

package org.apache.geode.management.internal.configuration.realizers;


import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.api.RealizationResult;
import org.apache.geode.management.configuration.Index;
import org.apache.geode.management.configuration.IndexType;
import org.apache.geode.management.runtime.IndexInfo;

public class IndexRealizer implements ConfigurationRealizer<Index, IndexInfo> {
  @Immutable
  private static Logger logger = LogService.getLogger();

  @Override
  public RealizationResult create(Index config, InternalCache cache) {
    QueryService queryService = cache.getQueryService();
    String indexName = config.getName();
    String indexedExpression = config.getExpression();
    String fromClause = config.getRegionPath();
    RealizationResult realizationResult = new RealizationResult();
    try {
      if (config.getIndexType() == IndexType.KEY) {
        queryService.createKeyIndex(indexName, indexedExpression, fromClause);
      } else {
        queryService.createIndex(indexName, indexedExpression, fromClause);
      }
      realizationResult.setSuccess(true);
      realizationResult.setMessage("Index " + indexName + " successfully created");
      return realizationResult;
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      realizationResult.setSuccess(false);
      realizationResult.setMessage(e.getMessage());
    }

    return realizationResult;
  }

  @Override
  public IndexInfo get(Index config, InternalCache cache) {
    String regionName = config.getRegionName();
    String indexName = config.getName();
    if (regionName == null || indexName == null) {
      return null;
    }
    Region<Object, Object> region = cache.getRegion("/" + regionName);
    if (region == null) {
      return null;
    }
    QueryService queryService = cache.getQueryService();
    org.apache.geode.cache.query.Index index = queryService.getIndex(region, indexName);
    if (index == null) {
      return null;
    }

    return new IndexInfo();
  }

  @Override
  public RealizationResult update(Index config, InternalCache cache) {
    return null;
  }

  @Override
  public RealizationResult delete(Index config, InternalCache cache) {
    return null;
  }
}
