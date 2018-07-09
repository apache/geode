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
package org.apache.geode.security.query;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.security.query.data.PdxTrade;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;

/**
 * This test verifies client/server distinct order-by queries with integrated security
 */
@Category({DistributedTest.class, SecurityTest.class})
public class QuerySecurityDistinctQueryDUnitTest extends QuerySecurityBase {

  public RegionShortcut getRegionType() {
    return RegionShortcut.PARTITION;
  }

  @Test
  public void executingDistinctOrderByQuery() {
    // Do puts from the client
    int numObjects = 1000;
    keys = new Object[numObjects];
    values = new Object[numObjects];
    for (int i = 0; i < numObjects; i++) {
      String key = String.valueOf(i);
      Object value = new PdxTrade(key, "PVTL", 100, 30);
      keys[i] = key;
      values[i] = value;
    }
    putIntoRegion(superUserClient, keys, values, regionName);

    // Execute query from the client and validate the size of the results
    superUserClient.invoke(() -> {
      int limit = 500;
      String query = "<trace> select distinct * from /" + regionName
          + " where cusip = 'PVTL' order by id asc limit " + limit;
      QueryService queryService = getClientCache().getQueryService();
      Object results = queryService.newQuery(query).execute();
      assertThat(results).isInstanceOf(SelectResults.class);
      assertThat(((SelectResults) results).size()).isEqualTo(limit);
    });

  }
}
