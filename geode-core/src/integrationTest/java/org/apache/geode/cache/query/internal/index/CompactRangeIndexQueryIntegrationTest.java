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
package org.apache.geode.cache.query.internal.index;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.data.PortfolioPdx;
import org.apache.geode.test.junit.categories.OQLIndexTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({OQLIndexTest.class})
public class CompactRangeIndexQueryIntegrationTest {

  @Rule
  public ServerStarterRule serverStarterRule = new ServerStarterRule().withAutoStart();

  @Test
  public void multipleNotEqualsClausesOnAPartitionedRegionShouldReturnCorrectResults()
      throws Exception {
    Cache cache = serverStarterRule.getCache();
    Region region = cache.createRegionFactory(RegionShortcut.PARTITION).create("portfolios");
    int numMatching = 10;
    QueryService qs = cache.getQueryService();
    qs.createIndex("statusIndex", "p.status", SEPARATOR + "portfolios p");
    for (int i = 0; i < numMatching * 2; i++) {
      PortfolioPdx p = new PortfolioPdx(i);
      if (i < numMatching) {
        p.status = "1";
      }
      region.put("KEY-" + i, p);
    }

    Query q = qs.newQuery(
        "select * from " + SEPARATOR
            + "portfolios p where p.pk <> '0' and p.status <> '0' and p.status <> '1' and p.status <> '2'");
    SelectResults rs = (SelectResults) q.execute();
    assertEquals(numMatching, rs.size());
  }

  @Test
  public void whenAuxFilterWithAnIterableFilterShouldNotCombineFiltersIntoAndJunction()
      throws Exception {
    Cache cache = serverStarterRule.getCache();
    Region region = cache.createRegionFactory(RegionShortcut.PARTITION).create("ExampleRegion");
    QueryService qs = cache.getQueryService();
    qs.createIndex("ExampleRegionIndex", "er['codeNumber','origin']",
        SEPARATOR + "ExampleRegion er");

    for (int i = 0; i < 10; i++) {
      Map<String, Object> data = new HashMap<>();
      data.put("codeNumber", 1);
      if ((i % 3) == 0) {
        data.put("origin", "src_common");
      } else {
        data.put("origin", "src_" + i);
      }
      data.put("attr", "attrValue");
      data.put("country", "JPY");

      region.put(String.valueOf(i), data);
    }

    Query q = qs.newQuery(
        "select * from " + SEPARATOR
            + "ExampleRegion E where E['codeNumber']=1 and E['origin']='src_common' and (E['country']='JPY' or E['ccountrycy']='USD')");
    SelectResults rs = (SelectResults) q.execute();
    assertEquals(4, rs.size());
  }

  @Test
  // getSizeEstimate will be called only when having a choice between two indexes
  public void getSizeEstimateShouldNotThrowClassCastException() throws Exception {
    String regionName = "portfolio";

    Cache cache = serverStarterRule.getCache();
    assertNotNull(cache);
    Region region =
        cache.createRegionFactory().setDataPolicy(DataPolicy.REPLICATE).create(regionName);

    Portfolio p = new Portfolio(1);
    region.put(1, p);
    Portfolio p2 = new Portfolio(3);
    region.put(2, p2);

    QueryService queryService = cache.getQueryService();
    queryService.createIndex("statusIndex", "status", SEPARATOR + "portfolio");
    queryService.createIndex("idIndex", "ID", SEPARATOR + "portfolio");

    SelectResults results = (SelectResults) queryService
        .newQuery("select * from " + SEPARATOR + "portfolio where status = 4 AND ID = 'StringID'")
        .execute();

    assertNotNull(results);
  }
}
