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
 * CompareIndexUsageTest.java
 *
 * Created on April 20, 2005, 5:33 PM
 */
package org.apache.geode.cache.query.functional;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

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
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.data.Position;
import org.apache.geode.cache.query.internal.QueryObserverAdapter;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.cache.query.types.StructType;
import org.apache.geode.test.junit.categories.OQLIndexTest;

@Category({OQLIndexTest.class})
public class IndexUseMultFrmSnglCondJUnitTest {

  @Before
  public void setUp() throws Exception {
    CacheUtils.startCache();
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
  }

  @Test
  public void testIndexUsageComaprison() throws Exception {
    Region region = CacheUtils.createRegion("portfolios", Portfolio.class);
    StructType resArType1 = null;
    StructType resArType2 = null;
    String[] strAr1 = null;
    String[] strAr2 = null;
    int resArSize1 = 0;
    int resArSize2 = 0;
    Object valPf1 = null;
    Object valPos1 = null;
    Object valPf2 = null;
    Object valPos2 = null;
    String SECID1 = null;
    String SECID2 = null;
    Iterator iter1 = null;
    Iterator iter2 = null;
    Set set1 = null;
    Set set2 = null;
    for (int i = 0; i < 4; i++) {
      region.put("" + i, new Portfolio(i));
    }
    QueryService qs = CacheUtils.getQueryService();
    String[] queries =
        {"SELECT DISTINCT * from " + SEPARATOR
            + "portfolios pf, pf.positions.values pos where pos.secId = 'IBM'"};
    SelectResults[][] r = new SelectResults[queries.length][2];

    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][0] = (SelectResults) q.execute();

        if (observer.isIndexesUsed) {
          fail("If index were not there how did they get used ???? ");
        }
        resArType1 = (StructType) (r[i][0]).getCollectionType().getElementType();
        resArSize1 = ((r[i][0]).size());
        CacheUtils.log(resArType1);
        strAr1 = resArType1.getFieldNames();

        set1 = ((r[i][0]).asSet());
        for (final Object o : set1) {
          Struct stc1 = (Struct) o;
          valPf1 = stc1.get(strAr1[0]);
          valPos1 = stc1.get(strAr1[1]);
          SECID1 = (((Position) valPos1).getSecId());

        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    // Create an Index and Run the Same Query as above.
    qs.createIndex("secIdIndex", IndexType.FUNCTIONAL, "b.secId",
        SEPARATOR + "portfolios pf, pf.positions.values b");

    for (int j = 0; j < queries.length; j++) {
      Query q2 = null;
      try {
        q2 = CacheUtils.getQueryService().newQuery(queries[j]);
        QueryObserverImpl observer2 = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer2);
        r[j][1] = (SelectResults) q2.execute();
        if (observer2.isIndexesUsed != true) {
          fail("FAILED: Index NOT Used");
        }
        resArType2 = (StructType) (r[j][1]).getCollectionType().getElementType();
        CacheUtils.log(resArType2);
        resArSize2 = (r[j][1]).size();
        strAr2 = resArType2.getFieldNames();
        set2 = ((r[j][1]).asSet());
        for (final Object o : set2) {
          Struct stc2 = (Struct) o;
          valPf2 = stc2.get(strAr2[0]);
          valPos2 = stc2.get(strAr2[1]);
          SECID2 = (((Position) valPos2).getSecId());

        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(q2.getQueryString());
      }
    }

    if ((resArType1).equals(resArType2)) {
      CacheUtils.log("Both Search Results are of the same Type i.e.--> " + resArType2);
    } else {
      fail("FAILED:Search result Type is different in both the cases");
    }

    if ((resArSize1 == resArSize2) || resArSize1 != 0) {
      CacheUtils
          .log("Search Results Size is Non Zero and is of Same Size i.e.  Size= " + resArSize1);
    } else {
      fail("FAILED:Search result size is different in both the cases");
    }
    iter2 = set2.iterator();
    iter1 = set1.iterator();
    while (iter1.hasNext()) {
      Struct stc2 = (Struct) iter2.next();
      Struct stc1 = (Struct) iter1.next();
      if (stc2.get(strAr2[0]) != stc1.get(strAr1[0])) {
        fail(
            "FAILED: In both the Cases the first member of StructSet i.e. Portfolio are different. ");
      }
      if (stc2.get(strAr2[1]) != stc1.get(strAr1[1])
          || !((Position) stc1.get(strAr1[1])).secId.equals("IBM")) {
        fail("FAILED: In both the cases either Positions Or secIds obtained are different");
      }
    }

    CacheUtils.compareResultsOfWithAndWithoutIndex(r, this);
  }


  @Test
  public void testMultiFromWithSingleConditionUsingIndex() throws Exception {
    // create region 1 and 2
    Region region1 = CacheUtils.createRegion("portfolios1", Portfolio.class);
    Region region2 = CacheUtils.createRegion("portfolios2", Portfolio.class);
    for (int i = 0; i < 100; i++) {
      Portfolio p = null;
      if (i != 0 && i < 5) {
        p = new Portfolio(5);
      } else {
        p = new Portfolio(i);
      }
      region1.put(i, p);
      region2.put(i, p);
    }

    QueryService qs = CacheUtils.getQueryService();
    // create and execute query
    String queryString = "SELECT * from " + SEPARATOR + "portfolios1 P1, " + SEPARATOR
        + "portfolios2 P2 WHERE P1.ID = 5";
    Query query = qs.newQuery(queryString);
    SelectResults sr1 = (SelectResults) query.execute();

    // create index
    Index index =
        qs.createIndex("P1IDIndex", IndexType.FUNCTIONAL, "P1.ID", SEPARATOR + "portfolios1 P1");

    // execute query
    SelectResults sr2 = (SelectResults) query.execute();
    assertEquals("Index result set does not match unindexed result set size", sr1.size(),
        sr2.size());
    // size will be number of matching in region 1 x region 2 size
    assertEquals("Query result set size does not match expected size", 5 * region2.size(),
        sr2.size());
  }

  @Test
  public void testMultiFromWithSingleConditionUsingRangeIndex() throws Exception {
    // create region 1 and 2
    Region region1 = CacheUtils.createRegion("portfolios1", Portfolio.class);
    Region region2 = CacheUtils.createRegion("portfolios2", Portfolio.class);
    for (int i = 0; i < 100; i++) {
      Portfolio p = null;
      if (i != 0 && i < 5) {
        p = new Portfolio(5);
      } else {
        p = new Portfolio(i);
      }
      region1.put(i, p);
      region2.put(i, p);
    }

    QueryService qs = CacheUtils.getQueryService();
    // create and execute query
    String queryString =
        "SELECT * from " + SEPARATOR + "portfolios1 P1, P1.positions.values WHERE P1.ID = 5";
    Query query = qs.newQuery(queryString);
    SelectResults sr1 = (SelectResults) query.execute();

    // create index
    Index index = qs.createIndex("P1IDIndex", IndexType.FUNCTIONAL, "P1.ID",
        SEPARATOR + "portfolios1 P1, P1.positions.values");

    // execute query
    SelectResults sr2 = (SelectResults) query.execute();
    assertEquals("Index result set does not match unindexed result set size", sr1.size(),
        sr2.size());
    // size will be number of matching in region 1 x region 2 size
    assertEquals("Query result set size does not match expected size", 10, sr2.size());
  }

  class QueryObserverImpl extends QueryObserverAdapter {

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
