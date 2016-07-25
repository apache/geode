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
/*
 * TestNewFunction.java
 *
 * Created on June 16, 2005, 3:55 PM
 */
package com.gemstone.gemfire.cache.query.functional;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.Utils;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.internal.QueryObserverAdapter;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * TODO: does this test provide any valuable coverage?
 */
@Category(IntegrationTest.class)
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

    Object r[][] = new Object[2][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();

    String queries[] = {
      "SELECT DISTINCT * from /portfolios pf , pf.positions.values pos where status = 'inactive'",
      "select distinct * from /portfolios where ID > 1 ",

    };

    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      q = CacheUtils.getQueryService().newQuery(queries[i]);
      QueryObserverImpl observer1 = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer1);
      r[i][0] = q.execute();
      if (!observer1.isIndexesUsed) {
        CacheUtils.log("NO INDEX IS USED!");
      }
      CacheUtils.log(Utils.printResult(r[i][0]));
    }

    qs.createIndex("sIndex", IndexType.FUNCTIONAL, "status", "/portfolios");
    qs.createIndex("iIndex", IndexType.FUNCTIONAL, "ID", "/portfolios");
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      q = CacheUtils.getQueryService().newQuery(queries[i]);
      QueryObserverImpl observer2 = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer2);
      r[i][1] = q.execute();
      if (observer2.isIndexesUsed) {
        CacheUtils.log("YES INDEX IS USED!");
      } else {
        fail("Index NOT Used");
      }
      CacheUtils.log(Utils.printResult(r[i][1]));
    }

    StructSetOrResultsSet ssORrs = new StructSetOrResultsSet();
    ssORrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length, queries);
  }

  private static class QueryObserverImpl extends QueryObserverAdapter {

    boolean isIndexesUsed = false;
    ArrayList indexesUsed = new ArrayList();

    public void beforeIndexLookup(Index index, int oper, Object key) {
      indexesUsed.add(index.getName());
    }

    public void afterIndexLookup(Collection results) {
      if (results != null) {
        isIndexesUsed = true;
      }
    }
  }

}