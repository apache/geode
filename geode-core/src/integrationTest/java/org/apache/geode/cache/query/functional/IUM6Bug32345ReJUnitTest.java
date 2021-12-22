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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
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

/**
 * IUM6Bug32345ReJUnitTest.java
 *
 * Created on May 5, 2005, 10:32 AM
 */
@Category({OQLIndexTest.class})
public class IUM6Bug32345ReJUnitTest {
  StructType resType1 = null;
  StructType resType2 = null;

  String[] strg1 = null;
  String[] strg2 = null;

  int resSize1 = 0;
  int resSize2 = 0;

  Object valPf1 = null;
  Object valPos1 = null;

  Object valPf2 = null;
  Object valPos2 = null;

  Iterator itert1 = null;
  Iterator itert2 = null;

  Set set1 = null;
  Set set2 = null;

  boolean isActive1 = false;
  boolean isActive2 = false;

  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
  }

  @After
  public void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }

  @Test
  public void testComparisonBetnWithAndWithoutIndexCreation() throws Exception {

    Region region = CacheUtils.createRegion("pos", Portfolio.class);
    for (int i = 0; i < 4; i++) {
      region.put("" + i, new Portfolio(i));
    }
    QueryService qs;
    qs = CacheUtils.getQueryService();
    String[] queries = {
        "SELECT DISTINCT * FROM " + SEPARATOR
            + "pos pf,  positions.values pos where pf.status='active' and pos.secId= 'IBM' and ID = 0"};
    SelectResults[][] sr = new SelectResults[queries.length][2];
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        sr[i][0] = (SelectResults) q.execute();
        resType1 = (StructType) (sr[i][0]).getCollectionType().getElementType();
        resSize1 = ((sr[i][0]).size());

        strg1 = resType1.getFieldNames();

        set1 = ((sr[i][0]).asSet());
        for (final Object o : set1) {
          Struct stc1 = (Struct) o;
          valPf1 = stc1.get(strg1[0]);
          valPos1 = stc1.get(strg1[1]);
          isActive1 = ((Portfolio) stc1.get(strg1[0])).isActive();

        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }

    // Create an Index on status and execute the same query again.

    qs = CacheUtils.getQueryService();
    qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "pf.status",
        SEPARATOR + "pos pf, pf.positions.values pos");
    // Retesting BUG # 32345
    // Index index2 = (Index)qs.createIndex("secIdIndex", IndexType.FUNCTIONAL,"pos.secId","/pos pf,
    // pf.positions.values pos");
    qs.createIndex("IDIndex", IndexType.FUNCTIONAL, "pf.ID",
        SEPARATOR + "pos pf, pf.positions.values pos");
    String[] queries2 = {
        "SELECT DISTINCT * FROM " + SEPARATOR
            + "pos pf,  positions.values pos where pf.status='active' and pos.secId= 'IBM' and ID = 0"};
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        QueryObserverImpl observer2 = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer2);
        sr[i][1] = (SelectResults) q.execute();
        if (!observer2.isIndexesUsed) {
          fail("Index NOT Used");
        }
        resType2 = (StructType) (sr[i][1]).getCollectionType().getElementType();
        resSize2 = ((sr[i][1]).size());
        // CacheUtils.log(resType2);
        strg2 = resType2.getFieldNames();
        // CacheUtils.log(strg2[0]);
        // CacheUtils.log(strg2[1]);

        set2 = ((sr[i][1]).asSet());
        for (final Object o : set2) {
          Struct stc2 = (Struct) o;
          valPf2 = stc2.get(strg2[0]);
          valPos2 = stc2.get(strg2[1]);
          isActive2 = ((Portfolio) stc2.get(strg2[0])).isActive();
          // CacheUtils.log(valPf2);
          // CacheUtils.log(valPos2);
        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }

    if ((resType1).equals(resType2)) {
      CacheUtils.log("Both Search Results are of the same Type i.e.--> " + resType1);
    } else {
      fail("FAILED:Search result Type is different in both the cases");
    }
    if (resSize1 == resSize2 || resSize1 != 0) {
      CacheUtils
          .log("Both Search Results are Non-zero and are of Same Size i.e.  Size= " + resSize1);
    } else {
      fail("FAILED:Search result Type is different in both the cases");
    }
    itert2 = set2.iterator();
    itert1 = set1.iterator();
    while (itert1.hasNext()) {
      Struct stc2 = (Struct) itert2.next();
      Struct stc1 = (Struct) itert1.next();
      if (stc2.get(strg2[0]) != stc1.get(strg1[0])) {
        fail(
            "FAILED: In both the Cases the first member of StructSet i.e. Portfolio are different. ");
      }
      if (stc2.get(strg2[1]) != stc1.get(strg1[1])) {
        fail("FAILED: In both the cases Positions are different");
      }
      if (!StringUtils.equals(((Position) stc2.get(strg2[1])).secId,
          ((Position) stc1.get(strg1[1])).secId)) {
        fail("FAILED: In both the cases Positions secIds are different");
      }
      if (((Portfolio) stc2.get(strg2[0])).isActive() != ((Portfolio) stc1.get(strg1[0]))
          .isActive()) {
        fail("FAILED: Status of the Portfolios found are different");
      }
      if (((Portfolio) stc2.get(strg2[0])).getID() != ((Portfolio) stc1.get(strg1[0])).getID()) {
        fail("FAILED: IDs of the Portfolios found are different");
      }
    }
    CacheUtils.compareResultsOfWithAndWithoutIndex(sr, this);
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
