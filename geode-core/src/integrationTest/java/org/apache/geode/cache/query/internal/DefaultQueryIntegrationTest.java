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
package org.apache.geode.cache.query.internal;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.QueryExecutionTimeoutException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.junit.categories.OQLQueryTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({OQLQueryTest.class})
public class DefaultQueryIntegrationTest {
  @Rule
  public ServerStarterRule serverStarterRule = new ServerStarterRule().withAutoStart();

  @Test
  public void testDefaultQueryObjectReusableAfterFirstExecutionTimesOut() throws Exception {
    GemFireCacheImpl.MAX_QUERY_EXECUTION_TIME = 10000;

    final InternalCache cache = serverStarterRule.getCache();
    final String regionName = "exampleRegion";
    final RegionFactory<Integer, Integer> regionFactory =
        cache.createRegionFactory(RegionShortcut.LOCAL);
    final Region<Integer, Integer> exampleRegion = regionFactory.create(regionName);
    final int numRegionEntries = 10;
    for (int i = 0; i < numRegionEntries; ++i) {
      exampleRegion.put(i, i);
    }

    final String queryString = "select * from /" + regionName;
    final DefaultQuery defaultQuery = new DefaultQuery(queryString, cache, false);

    // Install a test hook which causes the query to timeout
    DefaultQuery.testHook =
        (spot, query, executionContext) -> {
          if (spot != DefaultQuery.TestHook.SPOTS.BEFORE_QUERY_DEPENDENCY_COMPUTATION) {
            return;
          }

          await("stall the query execution so that it gets cancelled")
              .until(executionContext::isCanceled);
        };

    assertThatThrownBy(defaultQuery::execute).isInstanceOf(QueryExecutionTimeoutException.class);

    // Uninstall test hook so that query object is reused to execute again, this time successfully
    DefaultQuery.testHook = null;

    final SelectResults results = (SelectResults) defaultQuery.execute();

    for (int i = 0; i < numRegionEntries; ++i) {
      assertThat(results.contains(i)).isTrue();
    }
  }
}
