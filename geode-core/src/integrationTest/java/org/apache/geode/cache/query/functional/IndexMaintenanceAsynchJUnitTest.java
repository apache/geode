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
 * IndexMaintenanceAsynchJUnitTest.java
 *
 * Created on May 10, 2005, 5:26 PM
 */
package org.apache.geode.cache.query.functional;

import static org.apache.geode.cache.query.CacheUtils.getQueryService;
import static org.apache.geode.cache.query.internal.QueryObserverHolder.setInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexStatistics;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.QueryObserverAdapter;
import org.apache.geode.cache.query.internal.index.IndexProtocol;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.junit.categories.OQLIndexTest;

@Category({OQLIndexTest.class})
public class IndexMaintenanceAsynchJUnitTest {

  @Before
  public void setUp() throws Exception {
    if (!isInitDone) {
      init();
    }
  }

  @After
  public void tearDown() throws Exception {}

  static QueryService qs;
  static boolean isInitDone = false;
  static Region region;
  static IndexProtocol index;

  private static void init() {
    try {
      String queryString;
      Query query;
      Object result;
      Cache cache = CacheUtils.getCache();
      region = CacheUtils.createRegion("portfolios", Portfolio.class, false);
      for (int i = 0; i < 4; i++) {
        region.put("" + i, new Portfolio(i));
      }
      qs = cache.getQueryService();
      index = (IndexProtocol) qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "status",
          "/portfolios");
      IndexStatistics stats = index.getStatistics();
      assertEquals(4, stats.getNumUpdates());

      // queryString= "SELECT DISTINCT * FROM /portfolios p, p.positions.values pos where
      // pos.secId='IBM'";
      queryString = "SELECT DISTINCT * FROM /portfolios";
      query = CacheUtils.getQueryService().newQuery(queryString);

      result = query.execute();

    } catch (Exception e) {
      e.printStackTrace();
    }
    isInitDone = true;
  }

  @Test
  public void testAddEntry() throws Exception {
    String queryString;
    Object result;
    Query query;
    try {
      IndexStatistics stats = index.getStatistics();
      for (int i = 5; i < 9; i++) {
        region.put("" + i, new Portfolio(i));
      }
      final IndexStatistics st = stats;
      WaitCriterion ev = new WaitCriterion() {
        public boolean done() {
          return st.getNumUpdates() == 8;
        }

        public String description() {
          return "index updates never became 8";
        }
      };
      GeodeAwaitility.await().untilAsserted(ev);

      // queryString= "SELECT DISTINCT * FROM /portfolios p, p.positions.values pos where
      // pos.secId='IBM'";
      queryString = "SELECT DISTINCT * FROM /portfolios where status = 'active'";
      query = getQueryService().newQuery(queryString);
      QueryObserverImpl observer = new QueryObserverImpl();
      setInstance(observer);

      result = query.execute();
      if (!observer.isIndexesUsed) {
        fail("NO INDEX USED");
      }

      if (((Collection) result).size() != 4) {
        fail("Did not obtain expected size of result for the query");
      }
      // Task ID: IMA 1

    } catch (Exception e) {
      e.printStackTrace();

    }
  }

  class QueryObserverImpl extends QueryObserverAdapter {
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
