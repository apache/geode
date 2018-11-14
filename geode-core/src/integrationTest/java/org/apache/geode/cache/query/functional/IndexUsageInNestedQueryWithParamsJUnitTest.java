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
package org.apache.geode.cache.query.functional;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Limit;
import org.apache.geode.cache.query.data.PortfolioMetric;
import org.apache.geode.cache.query.data.PortfolioWithMetrics;
import org.apache.geode.cache.query.data.QuerySupportService;
import org.apache.geode.cache.query.data.TempLimit;
import org.apache.geode.test.junit.categories.OQLIndexTest;

@Category({OQLIndexTest.class})
public class IndexUsageInNestedQueryWithParamsJUnitTest {
  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
    Region portfolioRegion = CacheUtils.createRegion("portfolio", PortfolioWithMetrics.class);
    Region tempLimitRegion = CacheUtils.createRegion("TempLimit", TempLimit.class);
    QueryService queryService = CacheUtils.getQueryService();
    queryService.defineIndex("portfolioIdIndex", "p.id", "/portfolio p");
    queryService.defineIndex("temporaryOriginalLimitIdIdx", "tl.originalId", "/TempLimit tl");
    queryService.createDefinedIndexes();
    PortfolioWithMetrics p = new PortfolioWithMetrics(1L);
    p.setMetrics(Arrays
        .asList(new PortfolioMetric(Arrays.asList(new Limit(11L), new Limit(12L))),
            new PortfolioMetric(Arrays.asList(new Limit(21L), new Limit(22L)))));
    portfolioRegion.put(p.getId(), p);
    tempLimitRegion.put(91L, new TempLimit(91L, 12L, new Limit(91L)));
    tempLimitRegion.put(92L, new TempLimit(92L, 21L, new Limit(92L)));
  }

  @After
  public void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }

  @Test
  public void testingJPMCNestedQueryFailure()
      throws Exception {

    QueryService queryService = CacheUtils.getQueryService();
    Query query = queryService.newQuery(
        "<TRACE>select l.id from /portfolio p, p.metrics m, ($1).getLimitsAndTempLimits(m) l WHERE p.id IN SET(1L,2L)");
    QuerySupportService querySupportService = new QuerySupportService(CacheUtils.getCache());
    Object[] params = new Object[] {querySupportService.createQueryContext()};
    SelectResults result = (SelectResults) query.execute(params);
    assertEquals("The query returned a wrong result set = " + result.asList(), true,
        result.containsAll(new ArrayList<>(Arrays.asList(21L, 22L, 92L, 12L, 11L, 91L))));
  }

}
