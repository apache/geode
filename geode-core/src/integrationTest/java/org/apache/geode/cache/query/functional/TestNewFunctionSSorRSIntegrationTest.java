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
/*
 * TestNewFunction.java
 *
 * Created on June 16, 2005, 3:55 PM
 */
package org.apache.geode.cache.query.functional;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.QueryObserverAdapter;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.test.junit.categories.OQLQueryTest;

/**
 * TODO: does this test provide any valuable coverage?
 */
@Category({OQLQueryTest.class})
public class TestNewFunctionSSorRSIntegrationTest {

  @Before
  public void setUp() throws Exception {
    CacheUtils.startCache();
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
  }

  @Test
  public void testNewFunc() throws Exception {
    Region region = CacheUtils.createRegion("portfolios", Portfolio.class);
    for (int i = 0; i < 4; i++) {
      region.put("" + i, new Portfolio(i));
      // CacheUtils.log(new Portfolio(i));
    }

    Object[][] r = new Object[2][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();

    String[] queries =
        {"SELECT DISTINCT * from " + SEPARATOR
            + "portfolios pf , pf.positions.values pos where status = 'inactive'",
            "select distinct * from " + SEPARATOR + "portfolios where ID > 1 ",

        };

    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      q = CacheUtils.getQueryService().newQuery(queries[i]);
      QueryObserverImpl observer1 = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer1);
      r[i][0] = q.execute();
    }

    qs.createIndex("sIndex", IndexType.FUNCTIONAL, "status", SEPARATOR + "portfolios");
    qs.createIndex("iIndex", IndexType.FUNCTIONAL, "ID", SEPARATOR + "portfolios");
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      q = CacheUtils.getQueryService().newQuery(queries[i]);
      QueryObserverImpl observer2 = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer2);
      r[i][1] = q.execute();
      if (!observer2.isIndexesUsed) {
        fail("Index NOT Used");
      }
    }

    StructSetOrResultsSet ssORrs = new StructSetOrResultsSet();
    ssORrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length, queries);
  }

  private static class QueryObserverImpl extends QueryObserverAdapter {

    boolean isIndexesUsed = false;
    ArrayList indexesUsed = new ArrayList();

    @Override
    public void beforeIndexLookup(Index index, int oper, Object key) {
      indexesUsed.add(index.getName());
    }

    @Override
    public void afterIndexLookup(Collection results) {
      if (results != null) {
        isIndexesUsed = true;
      }
    }
  }

}
