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
 * IndexUsageInNestedQuery.java
 *
 * Created on June 6, 2005, 11:39 AM
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
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.QueryObserverAdapter;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.test.junit.categories.OQLIndexTest;

@Category({OQLIndexTest.class})
public class IndexUsageInNestedQueryJUnitTest {

  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
    Region r = CacheUtils.createRegion("portfolios", Portfolio.class);
    for (int i = 0; i < 4; i++) {
      r.put(i + "", new Portfolio(i));
    }
  }

  @After
  public void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }

  @Test
  public void testNestedQueriesResultsasStructSet() throws Exception {

    QueryService qs;
    qs = CacheUtils.getQueryService();
    String queries[] = {
        "select distinct * from " + SEPARATOR + "portfolios p, (select distinct pos  as poos from "
            + SEPARATOR + "portfolios x, x.positions.values pos"
            + " where pos.secId = 'YHOO') as k",
        "select distinct * from " + SEPARATOR + "portfolios p, (select distinct pos as poos from "
            + SEPARATOR + "portfolios p, p.positions.values pos"
            + " where pos.secId = 'YHOO') as k",
        "select distinct * from " + SEPARATOR + "portfolios p, (select distinct x.ID as ID  from "
            + SEPARATOR + "portfolios x"
            + " where x.ID = p.ID) as k ", // Currently Index Not Getting Used
        "select distinct * from " + SEPARATOR + "portfolios p, (select distinct pos as poos from "
            + SEPARATOR + "portfolios x, p.positions.values pos"
            + " where x.ID = p.ID) as k", // Currently Index Not Getting Used
        "select distinct * from " + SEPARATOR
            + "portfolios p, (select distinct x as pf , myPos as poos from " + SEPARATOR
            + "portfolios x, x.positions.values as myPos) as k "
            + "  where k.poos.secId = 'YHOO'",
        "select distinct * from " + SEPARATOR
            + "portfolios p, (select distinct x as pf , myPos as poos from " + SEPARATOR
            + "portfolios x, x.positions.values as myPos) as K"
            + "  where K.poos.secId = 'YHOO'",
        "select distinct * from " + SEPARATOR
            + "portfolios p, (select distinct val from positions.values as val where val.secId = 'YHOO') as k ",
        "select distinct * from " + SEPARATOR + "portfolios x, x.positions.values where "
            + "secId = element(select distinct vals.secId from " + SEPARATOR
            + "portfolios p, p.positions.values vals where vals.secId = 'YHOO')",

    };
    SelectResults r[][] = new SelectResults[queries.length][2];

    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = (SelectResults) q.execute();
        if (!observer.isIndexesUsed) {
          CacheUtils.log("NO INDEX USED");
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    // Create an Index on status and execute the same query again.

    qs = CacheUtils.getQueryService();
    qs.createIndex("idIndex", IndexType.FUNCTIONAL, "p.ID", SEPARATOR + "portfolios p");
    qs.createIndex("secIdIndex", IndexType.FUNCTIONAL, "b.secId",
        SEPARATOR + "portfolios pf, pf.positions.values b");
    qs.createIndex("cIndex", IndexType.FUNCTIONAL,
        "pf.collectionHolderMap[(pf.ID).toString()].arr[pf.ID]", SEPARATOR + "portfolios pf");
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        QueryObserverImpl observer2 = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer2);
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        r[i][1] = (SelectResults) q.execute();

      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    CacheUtils.compareResultsOfWithAndWithoutIndex(r, this);
  }



  class QueryObserverImpl extends QueryObserverAdapter {
    boolean isIndexesUsed = false;
    ArrayList<String> indexesUsed = new ArrayList<>();

    @Override
    public void beforeIndexLookup(Index index, int oper, Object key) {
      indexesUsed.add(index.getName());
    }

    @Override
    public void afterIndexLookup(Collection results) {
      if (results != null) {
        isIndexesUsed = true;
        // CacheUtils.log(Utils.printResult(results));
      }
    }
  }

}
