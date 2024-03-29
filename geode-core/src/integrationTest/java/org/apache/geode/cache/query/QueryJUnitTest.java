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
/******
 * THIS FILE IS ENCODED IN UTF-8 IN ORDER TO TEST UNICODE IN FIELD NAMES. THE ENCODING MUST BE
 * SPECIFIED AS UTF-8 WHEN COMPILED
 *******/
/*
 * QueryJUnitTest.java JUnit based test
 *
 * Created on March 8, 2005, 4:54 PM
 */
package org.apache.geode.cache.query;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.runners.MethodSorters.NAME_ASCENDING;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.data.Position;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.ExecutionContext;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@FixMethodOrder(NAME_ASCENDING)
@Category({OQLQueryTest.class})
public class QueryJUnitTest {

  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
  }

  @After
  public void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }

  @Test
  public void test000GetQueryString() {
    CacheUtils.log("testGetQueryString");
    String queryStr = "SELECT DISTINCT * FROM " + SEPARATOR + "root";
    Query q = CacheUtils.getQueryService().newQuery(queryStr);
    if (!queryStr.equals(q.getQueryString())) {
      fail("Query.getQueryString() returns different query string");
    }
  }

  @Test
  public void test001Execute() {
    CacheUtils.log("testExecute");
    try {
      Region region = CacheUtils.createRegion("Portfolios", Portfolio.class);
      region.put("1", new Portfolio(1));
      region.put("2", new Portfolio(0));
      String queryStr = "SELECT DISTINCT * FROM " + SEPARATOR + "Portfolios";
      Query q = CacheUtils.getQueryService().newQuery(queryStr);
      SelectResults results = (SelectResults) q.execute();
      assertEquals(results.size(), 2);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception during Query.execute");
    }
  }

  @Test
  public void test002UnicodeInQuery() {
    CacheUtils.log("testUnicodeInQuery");
    try {
      Region region = CacheUtils.createRegion("Portfolios", Portfolio.class);
      region.put("1", new Portfolio(1));
      region.put("2", new Portfolio(0));
      String queryStr =
          "SELECT DISTINCT * FROM " + SEPARATOR + "Portfolios WHERE unicodeṤtring = 'ṤẐṶ'";
      Query q = CacheUtils.getQueryService().newQuery(queryStr);
      SelectResults results = (SelectResults) q.execute();
      assertEquals(results.size(), 1);
      Portfolio p = (Portfolio) results.iterator().next();
      assertEquals(p.unicodeṤtring, "ṤẐṶ");
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception during Query.execute");
    }
  }

  @Test
  public void test003Compile() {
    CacheUtils.log("testCompile");
    // fail("The test case is empty.");
  }

  @Test
  public void test004IsCompiled() {
    CacheUtils.log("testIsCompiled");
    String queryStr = "SELECT DISTINCT * FROM " + SEPARATOR + "root";
    Query q = CacheUtils.getQueryService().newQuery(queryStr);
    if (q.isCompiled()) {
      fail("Query.isCompiled() returns true for non-compiled query");
    }
  }

  @Test
  public void test005GetStatistics() {
    CacheUtils.log("testGetStatistics");
    String queryStr = "SELECT DISTINCT * FROM " + SEPARATOR + "Portfolios where status='active'";
    Query q = CacheUtils.getQueryService().newQuery(queryStr);
    QueryStatistics qst = q.getStatistics();
    if (qst.getNumExecutions() != 0 && qst.getTotalExecutionTime() != 0) {
      fail("QueryStatistics not initialized properly");
    }
    try {
      Region region = CacheUtils.createRegion("Portfolios", Portfolio.class);
      CacheUtils.getQueryService().createIndex("testIndex", IndexType.FUNCTIONAL, "status",
          SEPARATOR + "Portfolios");
      for (int i = 0; i < 10000; i++) {
        region.put(i + "", new Portfolio(i));
      }
      q.execute();
      qst = q.getStatistics();
      if (qst.getNumExecutions() != 1) { // || qst.getTotalExecutionTime()==0){ // bruce - time
                                         // based CachePerfStats are disabled by default
        fail("QueryStatistics not updated.");
      }

      for (int i = 0; i < 10; i++) {
        q.execute();
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception during Query.execute");
    }
  }

  @Test
  public void test006GetRegionsInQuery() {

    String[] queryStrs =
        new String[] {"SELECT DISTINCT * FROM " + SEPARATOR + "Portfolios where status='active'",
            SEPARATOR + "Portfolios", SEPARATOR + "Portfolios.values",
            SEPARATOR + "Portfolios.keys()", SEPARATOR + "Portfolios.entries(false)",
            "null = null",
            "select distinct * from " + SEPARATOR
                + "Employees where not (select distinct * from collect).isEmpty",
            "select distinct * from $2 where salary > $1",
            "SELECT DISTINCT key: key, iD: entry.value.iD, secId: posnVal.secId  FROM " + SEPARATOR
                + "pos.entries entry, entry.value.positions.values posnVal  WHERE entry.value.\"type\" = 'type0' AND posnVal.secId = 'YHOO'",
            "SELECT DISTINCT * FROM (SELECT DISTINCT * FROM " + SEPARATOR
                + "Portfolios ptf, positions pos) WHERE pos.value.secId = 'IBM'",
            "SELECT DISTINCT * FROM " + SEPARATOR
                + "Portfolios WHERE NOT(SELECT DISTINCT * FROM positions.values p WHERE p.secId = 'IBM').isEmpty",
            "SELECT DISTINCT * FROM " + SEPARATOR
                + "Portfolios where status = ELEMENT(SELECT DISTINCT * FROM " + SEPARATOR
                + "Portfolios p where p.ID = 0).status",
            "Select distinct * from " + SEPARATOR + "Portfolios pf, " + SEPARATOR + "Portfolios2, "
                + SEPARATOR + "Portfolios3, " + SEPARATOR + "Data where pf.status='active'",
            "select distinct * from " + SEPARATOR
                + "portfolios p, p.positions.values myPos, (select distinct * from " + SEPARATOR
                + "Employees x)  where myPos.secId = 'YHOO'",
            "select distinct * from " + SEPARATOR
                + "portfolios p, p.positions.values myPos, (select distinct * from " + SEPARATOR
                + "Employees x, " + SEPARATOR + "portfolios)  where myPos.secId = 'YHOO'",
            "select distinct * from " + SEPARATOR
                + "portfolios p, p.positions.values myPos, (select distinct * from " + SEPARATOR
                + "Employees x, " + SEPARATOR + "Portfolios)  where myPos.secId = 'YHOO'",
            "select distinct " + SEPARATOR + "Portfolios.size, key FROM " + SEPARATOR
                + "pos.entries",
            "select distinct " + SEPARATOR + "Portfolios2.size, key FROM " + SEPARATOR
                + "pos.entries WHERE (Select distinct * from " + SEPARATOR
                + "portfolios4, entries).size = 3",

        };
    String[][] regions = new String[][] {{SEPARATOR + "Portfolios"}, {SEPARATOR + "Portfolios"},
        {SEPARATOR + "Portfolios"},
        {SEPARATOR + "Portfolios"}, {SEPARATOR + "Portfolios"}, {}, {SEPARATOR + "Employees"},
        {SEPARATOR + "Portfolios"}, {SEPARATOR + "pos"},
        {SEPARATOR + "Portfolios"}, {SEPARATOR + "Portfolios"}, {SEPARATOR + "Portfolios"},
        {SEPARATOR + "Portfolios", SEPARATOR + "Portfolios2", SEPARATOR + "Portfolios3",
            SEPARATOR + "Data"},
        {SEPARATOR + "portfolios", SEPARATOR + "Employees"},
        {SEPARATOR + "portfolios", SEPARATOR + "Employees"},
        {SEPARATOR + "portfolios", SEPARATOR + "Employees", SEPARATOR + "Portfolios"},
        {SEPARATOR + "Portfolios", SEPARATOR + "pos"},
        {SEPARATOR + "Portfolios2", SEPARATOR + "pos", SEPARATOR + "portfolios4"}

    };

    Object[] params = new Object[] {"", CacheUtils.createRegion("Portfolios", Portfolio.class)};
    for (int i = 0; i < queryStrs.length; ++i) {
      Query q = CacheUtils.getQueryService().newQuery(queryStrs[i]);

      Set set = ((DefaultQuery) q).getRegionsInQuery(params);
      String[] qRegions = regions[i];
      assertEquals("region names don't match in query #" + i + "(\"" + queryStrs[i] + "\"",
          new HashSet(Arrays.asList(qRegions)), set);
    }
    DefaultQuery q = (DefaultQuery) CacheUtils.getQueryService().newQuery(queryStrs[0]);

    Set set = q.getRegionsInQuery(params);
    try {
      set.add("test");
      fail("The set returned should not be modifiable");
    } catch (Exception e) {
      // Expected
    }
  }

  @Test
  public void test007UndefinedResults() {
    CacheUtils.log("testQueryExceptionLogMessage");
    Region region = CacheUtils.createRegion("Portfolios", Portfolio.class);
    region.put("1", new Portfolio(1));
    region.put("2", new Portfolio(0));
    String queryStr = "SELECT DISTINCT * FROM " + SEPARATOR + "Portfolios.ketset";
    Query q = CacheUtils.getQueryService().newQuery(queryStr);
    Object results = null;

    try {
      results = q.execute();
    } catch (Exception e) {
      fail("Query execution failed " + e);
    }
    assertEquals(0, ((SelectResults) results).size());

    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    AttributesFactory af = new AttributesFactory();
    af.setPartitionAttributes(paf.create());

    region = CacheUtils.createRegion("PortfoliosPR", af.create(), false);
    region.put("1", new Portfolio(1));
    region.put("2", new Portfolio(0));
    queryStr = "SELECT DISTINCT * FROM " + SEPARATOR + "PortfoliosPR.ketset";
    q = CacheUtils.getQueryService().newQuery(queryStr);
    try {
      results = q.execute();
    } catch (Exception e) {
      fail("Query execution failed " + e);
    }
    assertEquals(0, ((SelectResults) results).size());
  }

  @Test
  public void test008NullCollectionField() {

    /*
     * This test relies on the static Position counter starting at the same value each time.
     * By starting the counter at a well-defined value, this test can be run more than once
     * in the same JVM.
     * Because of its reliance on that static counter, this test cannot be run in parallel.
     */
    Position.resetCounter();

    Region region = CacheUtils.createRegion("Portfolios", Portfolio.class);
    for (int i = 0; i < 10; i++) {
      Portfolio p = new Portfolio(i);
      if (i % 2 == 0) {
        p.positions = null;
      }
      region.put("key-" + i, p);
    }

    String queryStr = "select * from " + region.getFullPath() + " p where p.positions = NULL ";

    Query q = CacheUtils.getQueryService().newQuery(queryStr);
    SelectResults sr = null;
    try {
      sr = (SelectResults) q.execute();
    } catch (Exception e) {
      fail("Query execution failed " + e);
    }
    assertEquals("Incorrect result size ", 5, sr.size());

    queryStr = "select * from " + region.getFullPath()
        + " p, p.positions.values pos  where pos.secId = 'APPL' ";

    q = CacheUtils.getQueryService().newQuery(queryStr);
    try {
      sr = (SelectResults) q.execute();
    } catch (Exception e) {
      fail("Query execution failed " + e);
    }
    assertEquals("Incorrect result size ", 2, sr.size());
  }

  @Test
  public void testThreadSafetyOfCompiledSelectScopeId() throws Exception {
    try {
      Cache cache = CacheUtils.getCache();
      RegionFactory<Integer, Portfolio> rf = cache.createRegionFactory(RegionShortcut.PARTITION);
      Region r = rf.create("keyzset");
      for (int i = 0; i < 100; i++) {
        r.put(i, new Portfolio(i));
      }
      ScopeThreadingTestHook scopeIDTestHook = new ScopeThreadingTestHook(3);
      DefaultQuery.testHook = scopeIDTestHook;
      QueryService qs = cache.getQueryService();
      Query q = qs.newQuery(
          "SELECT DISTINCT * FROM " + SEPARATOR
              + "keyzset.keySet key WHERE key.id > 0 AND key.id <= 0 ORDER BY key asc LIMIT $3");
      Thread q1 = new Thread(new QueryRunnable(q, new Object[] {10, 20, 10}));
      Thread q2 = new Thread(new QueryRunnable(q, new Object[] {5, 10, 5}));
      Thread q3 = new Thread(new QueryRunnable(q, new Object[] {2, 10, 8}));
      q1.start();
      q2.start();
      q3.start();
      q1.join();
      q2.join();
      q3.join();
      assertEquals("Exceptions were thrown due to DefaultQuery not being thread-safe", true,
          scopeIDTestHook.isOk());
    } finally {
      DefaultQuery.testHook = null;
    }
  }

  @Test
  public void creatingACompiledJunctionWithACompiledInClauseDoesNotThrowException()
      throws Exception {
    Cache cache = CacheUtils.getCache();
    RegionFactory<Integer, Portfolio> rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
    Region regionA = rf.create("regionA");
    Region regionB = rf.create("regionB");

    for (int i = 1; i <= 100; i++) {
      regionA.put(Integer.toString(i), new TestUserObject("" + i, "" + i, "" + i, "" + i));
      regionB.put(Integer.toString(i), new TestUserObject("" + i, "" + i, "" + i, "" + i));
    }
    QueryService qs = CacheUtils.getQueryService();

    Index regionAUserCodeIndex = qs.createIndex("regionAUserCodeIndex",
        IndexType.FUNCTIONAL, "userId", SEPARATOR + "regionA ");
    Index regionBUserCodeIndex = qs.createIndex("regionAUserCodeIndex",
        IndexType.FUNCTIONAL, "userId", SEPARATOR + "regionB ");

    Index regionAUserNameIndex = qs.createIndex("regionAUserNameIndex",
        IndexType.FUNCTIONAL, "userName", SEPARATOR + "regionA ");
    Index regionBUserNameIndex = qs.createIndex("regionBUserNameIndex",
        IndexType.FUNCTIONAL, "userName", SEPARATOR + "regionB ");

    Query query = qs.newQuery(
        "select regionB.userId,regionA.professionCode,regionB.postCode,regionB.userName from "
            + SEPARATOR + "regionA regionA," + SEPARATOR
            + "regionB regionB where regionA.userId = regionB.userId and regionA.professionCode in Set('1','2','3') and regionB.postCode = '1' and regionB.userId='1'");
    SelectResults results = (SelectResults) query.execute();
    assertTrue(results.size() > 0);
  }

  public static class TestUserObject implements Serializable {
    public String professionCode;
    public String userId;
    public String postCode;
    public String userName;

    public TestUserObject() {

    }

    public TestUserObject(final String professionCode, final String userId, final String postCode,
        final String userName) {
      this.professionCode = professionCode;
      this.userId = userId;
      this.postCode = postCode;
      this.userName = userName;
    }
  }

  private class QueryRunnable implements Runnable {
    private final Query q;
    private final Object[] params;

    public QueryRunnable(Query q, Object[] params) {
      this.q = q;
      this.params = params;
    }

    @Override
    public void run() {
      try {
        q.execute(params);
      } catch (Exception e) {
        throw new AssertionError("exception occurred while executing query", e);
      }
    }
  }

  private static class ScopeThreadingTestHook implements DefaultQuery.TestHook {
    private final CyclicBarrier barrier;
    private final List<Exception> exceptionsThrown = new LinkedList<>();

    public ScopeThreadingTestHook(int numThreads) {
      barrier = new CyclicBarrier(numThreads);
    }

    @Override
    public void doTestHook(final SPOTS spot, final DefaultQuery _ignored,
        final ExecutionContext executionContext) {
      if (spot == SPOTS.BEFORE_QUERY_EXECUTION) {
        try {
          barrier.await(8, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          exceptionsThrown.add(e);
          Thread.currentThread().interrupt();
        } catch (Exception e) {
          exceptionsThrown.add(e);
        }
      }
    }

    public boolean isOk() {
      return exceptionsThrown.size() == 0;
    }
  }
}
