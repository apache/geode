/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/******
* THIS FILE IS ENCODED IN UTF-8 IN ORDER TO TEST UNICODE IN FIELD NAMES.
* THE ENCODING MUST BE SPECIFIED AS UTF-8 WHEN COMPILED
*******/
/*
 * QueryJUnitTest.java
 * JUnit based test
 *
 * Created on March 8, 2005, 4:54 PM
 */
package com.gemstone.gemfire.cache.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.runners.MethodSorters.NAME_ASCENDING;

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

import util.TestException;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.internal.DefaultQuery;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 *
 * @author vaibhav
 */
@FixMethodOrder(NAME_ASCENDING)
@Category(IntegrationTest.class)
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
    String queryStr = "SELECT DISTINCT * FROM /root";
    Query q = CacheUtils.getQueryService().newQuery(queryStr);
    if(!queryStr.equals(q.getQueryString())){
      fail("Query.getQueryString() returns different query string");
    }
  }
  
  @Test
  public void test001Execute() {
    CacheUtils.log("testExecute");
    try{
      Region region = CacheUtils.createRegion("Portfolios", Portfolio.class);
      region.put("1",new Portfolio(1));
      region.put("2",new Portfolio(0));
      String queryStr = "SELECT DISTINCT * FROM /Portfolios";
      Query q = CacheUtils.getQueryService().newQuery(queryStr);
      SelectResults results = (SelectResults)q.execute();
      assertEquals(results.size(), 2);
    }catch(Exception e){
      e.printStackTrace();
      fail("Exception during Query.execute");
    }
  }
  
  @Test
  public void test002UnicodeInQuery() {
    CacheUtils.log("testUnicodeInQuery");
    try{
      Region region = CacheUtils.createRegion("Portfolios", Portfolio.class);
      region.put("1",new Portfolio(1));
      region.put("2",new Portfolio(0));
      String queryStr = "SELECT DISTINCT * FROM /Portfolios WHERE unicodeṤtring = 'ṤẐṶ'";
      Query q = CacheUtils.getQueryService().newQuery(queryStr);
      SelectResults results = (SelectResults)q.execute();
      assertEquals(results.size(), 1);
      Portfolio p = (Portfolio)results.iterator().next();
      assertEquals(p.unicodeṤtring, "ṤẐṶ");
    }catch(Exception e){
      e.printStackTrace();
      fail("Exception during Query.execute");
    }
  }

  @Test
  public void test003UnicodeInRegionNameAndQuery() {
    CacheUtils.log("testUnicodeInQuery");
    try{
      Region region = CacheUtils.createRegion("中æå«", Portfolio.class);
      Portfolio p = new Portfolio(0);
      p.unicodeṤtring = "中æå«";
      region.put("1",p);
      region.put("2",new Portfolio(1));
      String queryStr = "SELECT DISTINCT * FROM " + region.getFullPath() +" WHERE unicodeṤtring = '中æå«'";
      Query q = CacheUtils.getQueryService().newQuery(queryStr);
      SelectResults results = (SelectResults)q.execute();
      assertEquals(results.size(), 1);
      p = (Portfolio)results.iterator().next();
      assertEquals(p.unicodeṤtring, "中æå«");
    }catch(Exception e){
      e.printStackTrace();
      fail("Exception during Query.execute");
    }
  }

  @Test
  public void test004UnicodeInRegionNameAndQueryWithIndex() {
    try {
      String unicode = "‰∏≠ÊñáÂ±Á´";
      Region region = CacheUtils.createRegion(unicode, Portfolio.class);
      CacheUtils.getQueryService().createIndex("unicodeIndex", "unicodeṤtring",
          "/'" + unicode + "'");
      Portfolio p = new Portfolio(0);
      p.unicodeṤtring = unicode;
      region.put("1", p);
      region.put("2", new Portfolio(1));
      String queryStr = "SELECT DISTINCT * FROM /'" + unicode
          + "' WHERE unicodeṤtring = '" + unicode + "'";
      Query q = CacheUtils.getQueryService().newQuery(queryStr);
      SelectResults results = (SelectResults) q.execute();
      assertEquals(results.size(), 1);
      p = (Portfolio) results.iterator().next();
      assertEquals(p.unicodeṤtring, unicode);

      String unicode2 = "‰áÂ±Á∏≠Êñ´";
      CacheUtils.createRegion(unicode2, Portfolio.class);
      queryStr = "SELECT DISTINCT * FROM /'" + unicode + "' u1, /'" + unicode2
          + "' u2 WHERE u1.unicodeṤtring = '" + unicode
          + "' order by u1.unicodeṤtring  limit 1";
      CacheUtils.log(queryStr);
      CacheUtils.getQueryService().newQuery(queryStr).execute();

    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception during Query.execute");
    }
  }


  @Test
  public void test005UnicodeInRegionNameAndQueryWithIndexUsingQuotesAsDelim() {
    try {
      String unicode = "‰∏≠ÊñáÂ±*+|<?>=. !@#$%^&*()_+,;:Á´[]{}?";
      Region region = CacheUtils.createRegion(unicode, Portfolio.class);
      CacheUtils.getQueryService().createIndex("unicodeIndex", "unicodeṤtring",
          "/'" + unicode + "'");
      Portfolio p = new Portfolio(0);
      p.unicodeṤtring = unicode;
      region.put("1", p);
      region.put("2", new Portfolio(1));
      String queryStr = "SELECT DISTINCT * FROM /'" + unicode
          + "' WHERE unicodeṤtring = '" + unicode + "'";
      Query q = CacheUtils.getQueryService().newQuery(queryStr);
      SelectResults results = (SelectResults) q.execute();
      assertEquals(results.size(), 1);
      p = (Portfolio) results.iterator().next();
      assertEquals(p.unicodeṤtring, unicode);

      String unicode2 = "!@#$%^&*(|)_?+,;: Á´‰∏≠ÊñáÂ±*+<>=.[]{}?";
      CacheUtils.createRegion(unicode2, Portfolio.class);
      queryStr = "SELECT DISTINCT * FROM /'" + unicode + "' u1, /'" + unicode2
          + "' u2 WHERE u1.unicodeṤtring = '" + unicode
          + "' order by u1.unicodeṤtring  limit 1";
      CacheUtils.log(queryStr);
      CacheUtils.getQueryService().newQuery(queryStr).execute();

    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception during Query.execute");
    }
  }

  @Test
  public void test006Compile() {
    CacheUtils.log("testCompile");
    //fail("The test case is empty.");
  }
  
  @Test
  public void test007IsCompiled() {
    CacheUtils.log("testIsCompiled");
    String queryStr = "SELECT DISTINCT * FROM /root";
    Query q = CacheUtils.getQueryService().newQuery(queryStr);
    if(q.isCompiled())
      fail("Query.isCompiled() returns true for non-compiled query");
  }
    
  @Test
  public void test008GetStatistics() {
    CacheUtils.log("testGetStatistics");
    String queryStr = "SELECT DISTINCT * FROM /Portfolios where status='active'";
    Query q = CacheUtils.getQueryService().newQuery(queryStr);
    QueryStatistics qst = q.getStatistics();
    if(qst.getNumExecutions()!=0 && qst.getTotalExecutionTime()!=0){
      fail("QueryStatistics not initialized properly");
    }
    try{
      Region region = CacheUtils.createRegion("Portfolios", Portfolio.class);
      CacheUtils.getQueryService().createIndex("testIndex", IndexType.FUNCTIONAL,
                                         "status", "/Portfolios");
      for(int i=0;i<10000;i++){
        region.put(i+"",new Portfolio(i));
      }
      q.execute();
      qst = q.getStatistics();
      if(qst.getNumExecutions() != 1) { // || qst.getTotalExecutionTime()==0){  // bruce - time based CachePerfStats are disabled by default
        fail("QueryStatistics not updated.");
      }
      
      for (int i = 0; i < 10; i++) {
        q.execute();
      }
      
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Exception during Query.execute");
    }
  }
  
  @Test
  public void test009GetRegionsInQuery() {
    
    String queryStrs[] = new String[] {
        "SELECT DISTINCT * FROM /Portfolios where status='active'",
        "/Portfolios", "/Portfolios.values","/Portfolios.keys()","/Portfolios.entries(false)",
        "null = null", "select distinct * from /Employees where not (select distinct * from collect).isEmpty",
        "select distinct * from $2 where salary > $1","SELECT DISTINCT key: key, iD: entry.value.iD, secId: posnVal.secId  FROM /pos.entries entry, entry.value.positions.values posnVal  WHERE entry.value.\"type\" = 'type0' AND posnVal.secId = 'YHOO'",
        "SELECT DISTINCT * FROM (SELECT DISTINCT * FROM /Portfolios ptf, positions pos) WHERE pos.value.secId = 'IBM'",
        "SELECT DISTINCT * FROM /Portfolios WHERE NOT(SELECT DISTINCT * FROM positions.values p WHERE p.secId = 'IBM').isEmpty",
        "SELECT DISTINCT * FROM /Portfolios where status = ELEMENT(SELECT DISTINCT * FROM /Portfolios p where p.ID = 0).status",
        "Select distinct * from /Portfolios pf, /Portfolios2, /Portfolios3, /Data where pf.status='active'",
        "select distinct * from /portfolios p, p.positions.values myPos, (select distinct * from /Employees x)  where myPos.secId = 'YHOO'",  
        "select distinct * from /portfolios p, p.positions.values myPos, (select distinct * from /Employees x, /portfolios)  where myPos.secId = 'YHOO'",
        "select distinct * from /portfolios p, p.positions.values myPos, (select distinct * from /Employees x, /Portfolios)  where myPos.secId = 'YHOO'",
        "select distinct /Portfolios.size, key FROM /pos.entries",
        "select distinct /Portfolios2.size, key FROM /pos.entries WHERE (Select distinct * from /portfolios4, entries).size = 3",
        
    };
    String regions[][]= new String[][] {
        {"/Portfolios"}, {"/Portfolios"},{"/Portfolios"},{"/Portfolios"},{"/Portfolios"},{}, {"/Employees"},
        {"/Portfolios"}, {"/pos"}, {"/Portfolios"}, {"/Portfolios"}, {"/Portfolios"}, {"/Portfolios","/Portfolios2","/Portfolios3","/Data"},
        {"/portfolios","/Employees"}, {"/portfolios","/Employees"}, {"/portfolios","/Employees","/Portfolios"},
        {"/Portfolios", "/pos"}, {"/Portfolios2", "/pos", "/portfolios4"}
        
    };
    
    Object[] params = new Object[] {"", CacheUtils.createRegion("Portfolios", Portfolio.class) };
    for(int i=0; i<queryStrs.length;++i) {
       Query q = CacheUtils.getQueryService().newQuery(queryStrs[i]);
      
       Set set = ((DefaultQuery)q).getRegionsInQuery(params);
       String qRegions[] = regions[i];
       assertEquals("region names don't match in query #" + i + "(\"" + queryStrs[i] + "\"",
                    new HashSet(Arrays.asList(qRegions)), set);
    }
    DefaultQuery q = (DefaultQuery)CacheUtils.getQueryService().newQuery(queryStrs[0]);
    
    Set set = q.getRegionsInQuery(params);
    try{
      set.add("test");
      fail("The set returned should not be modifiable");
    }catch(Exception e) {     
      //Expected
    }
  }

  @Test
  public void test010UndefinedResults() {
    CacheUtils.log("testQueryExceptionLogMessage");
    Region region = CacheUtils.createRegion("Portfolios", Portfolio.class);
    region.put("1",new Portfolio(1));
    region.put("2",new Portfolio(0));
    String queryStr = "SELECT DISTINCT * FROM /Portfolios.ketset";
    Query q = CacheUtils.getQueryService().newQuery(queryStr);
    Object results = null;
    
    try {
      results = q.execute();
    } catch (Exception e) {
      fail("Query execution failed " + e);
    }
    assertEquals(0, ((SelectResults)results).size());

    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    AttributesFactory af = new AttributesFactory();
    af.setPartitionAttributes(paf.create());

    region = CacheUtils.createRegion("PortfoliosPR", af.create(), false);
    region.put("1",new Portfolio(1));
    region.put("2",new Portfolio(0));
    queryStr = "SELECT DISTINCT * FROM /PortfoliosPR.ketset";
    q = CacheUtils.getQueryService().newQuery(queryStr);
    try {
      results = q.execute();
    } catch (Exception e) {
      fail("Query execution failed " + e);
    }
    assertEquals(0, ((SelectResults)results).size());
  }  

  @Test
  public void test011NullCollectionField() {
    Region region = CacheUtils.createRegion("Portfolios", Portfolio.class);
    for (int i = 0; i < 10; i++) {
      Portfolio p = new Portfolio(i);
      if (i % 2 == 0) {
        p.positions = null;
      }
      region.put("key-" + i, p);
    }

    String queryStr = "select * from " + region.getFullPath()
        + " p where p.positions = NULL ";
 
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
    assertEquals("Incorrect result size ", 1, sr.size());
  }

  @Test
  public void testThreadSafetyOfCompiledSelectScopeId() throws Exception {
    try {
      Cache cache = CacheUtils.getCache();
      RegionFactory<Integer, Portfolio> rf = cache
          .createRegionFactory(RegionShortcut.PARTITION);
      Region r = rf.create("keyzset");
      for (int i = 0; i < 100; i++) {
        r.put(i, new Portfolio(i));
      }
      ScopeThreadingTestHook scopeIDTestHook = new ScopeThreadingTestHook(3);
      DefaultQuery.testHook = scopeIDTestHook;
      QueryService qs = cache.getQueryService();
      Query q = qs
          .newQuery("SELECT DISTINCT * FROM /keyzset.keySet key WHERE key.id > 0 AND key.id <= 0 ORDER BY key asc LIMIT $3");
      Thread q1 = new Thread(new QueryRunnable(q, new Object[] { 10, 20, 10 }));
      Thread q2 = new Thread(new QueryRunnable(q, new Object[] { 5, 10, 5 }));
      Thread q3 = new Thread(new QueryRunnable(q, new Object[] { 2, 10, 8 }));
      q1.start();
      q2.start();
      q3.start();
      q1.join();
      q2.join();
      q3.join();
      assertEquals("Exceptions were thrown due to DefaultQuery not being thread-safe", true, scopeIDTestHook.isOk());
    }
    finally {
      DefaultQuery.testHook = null;
    }
  }

  private class QueryRunnable implements Runnable {
    private Query q;
    private Object[] params;

    public QueryRunnable(Query q, Object[] params) {
      this.q = q;
      this.params = params;
    }

    public void run() {
      try {
        q.execute(params);
      } catch (Exception e) {
        throw new TestException("exception occured while executing query", e);
      }
    }
  }

  public class ScopeThreadingTestHook implements DefaultQuery.TestHook {
    private CyclicBarrier barrier;
    private List<Exception> exceptionsThrown = new LinkedList<Exception>();

    public ScopeThreadingTestHook(int numThreads) {
      barrier = new CyclicBarrier(numThreads);
    }

    @Override
    public void doTestHook(int spot) {
      this.doTestHook(spot + "");
    }

    @Override
    public void doTestHook(String spot) {
      if (spot.equals("1")) {
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
