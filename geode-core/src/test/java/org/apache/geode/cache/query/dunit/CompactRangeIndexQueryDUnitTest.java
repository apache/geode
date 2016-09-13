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
package com.gemstone.gemfire.cache.query.dunit;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.data.PortfolioPdx;

import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.cache.internal.JUnit4CacheTestCase;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class CompactRangeIndexQueryDUnitTest extends JUnit4CacheTestCase {

  VM vm0;

  @Override
  public final void postSetUp() throws Exception {
    getSystem();
    Invoke.invokeInEveryVM(new SerializableRunnable("getSystem") {
      public void run() {
        getSystem();
      }
    });
    Host host = Host.getHost(0);
    vm0 = host.getVM(0);
  }

  @Test
  public void multipleNotEqualsClausesOnAPartitionedRegionShouldReturnCorrectResults() throws Exception {
    Cache cache = getCache();
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
    Cache cache = getCache();
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
