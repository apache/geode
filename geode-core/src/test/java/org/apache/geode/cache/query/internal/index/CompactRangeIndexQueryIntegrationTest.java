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
package org.apache.geode.cache.query.internal.index;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.internal.Op;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.PortfolioPdx;

import org.apache.geode.cache.query.internal.index.CompactRangeIndex;
import org.apache.geode.cache.query.internal.index.IndexProtocol;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class CompactRangeIndexQueryIntegrationTest {

  @Test
  public void multipleNotEqualsClausesOnAPartitionedRegionShouldReturnCorrectResults() throws Exception {
    Cache cache = CacheUtils.getCache();
    Region region = cache.createRegionFactory(RegionShortcut.PARTITION).create("portfolios");
    int numMatching = 10;
    QueryService qs = cache.getQueryService();
    qs.createIndex("statusIndex", "p.status", "/portfolios p");
    for (int i = 0; i < numMatching * 2; i++) {
      PortfolioPdx p = new PortfolioPdx(i);
      if (i < numMatching) {
        p.status = "1";
      }
      region.put("KEY-"+ i, p);
    }

    Query q = qs.newQuery("select * from /portfolios p where p.pk <> '0' and p.status <> '0' and p.status <> '1' and p.status <> '2'");
    SelectResults rs = (SelectResults) q.execute();
    assertEquals( numMatching, rs.size());
  }

  @Test
  public void whenAuxFilterWithAnIterableFilterShouldNotCombineFiltersIntoAndJunction() throws Exception {
    CacheUtils.startCache();
    Cache cache = CacheUtils.getCache();
    Region region = cache.createRegionFactory(RegionShortcut.PARTITION).create("ExampleRegion");
    QueryService qs = cache.getQueryService();
    qs.createIndex("ExampleRegionIndex", "er['codeNumber','origin']", "/ExampleRegion er");

    for (int i = 0; i < 10; i++) {
      Map<String, Object> data = new HashMap<String, Object>();
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

    Query q = qs.newQuery("select * from /ExampleRegion E where E['codeNumber']=1 and E['origin']='src_common' and (E['country']='JPY' or E['ccountrycy']='USD')");
    SelectResults rs = (SelectResults) q.execute();
    assertEquals( 4, rs.size());
  }
}
