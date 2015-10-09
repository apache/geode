/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * IndexTest.java
 *
 * Created on February 23, 2005, 3:17 PM
 */
package com.gemstone.gemfire.cache.query.internal.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryInvalidException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.functional.StructSetOrResultsSet;
import com.gemstone.gemfire.cache.query.internal.QueryObserverAdapter;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.cache.query.internal.index.IndexManager.TestHook;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * 
 * @author vaibhav
 */
@Category(IntegrationTest.class)
public class IndexUseJUnitTest
{

  Region region;

  QueryService qs;

  @Before
  public void setUp() throws Exception {
    CacheUtils.startCache();
    region = CacheUtils.createRegion("pos", Portfolio.class);
    region.put("0", new Portfolio(0));
    region.put("1", new Portfolio(1));
    region.put("2", new Portfolio(2));
    region.put("3", new Portfolio(3));

    qs = CacheUtils.getQueryService();
    qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "status", "/pos");
    qs.createIndex("idIndex", IndexType.FUNCTIONAL, "ID", "/pos");
    qs.createIndex("secIdIndex", IndexType.FUNCTIONAL, "P1.secId", "/pos");
    qs.createIndex("secIdIndex2", IndexType.FUNCTIONAL, "P2.secId", "/pos");
    qs.createIndex("p1secindex", "p.position1.secId", " /pos p ");
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
    IndexManager indexManager = ((LocalRegion)region).getIndexManager();
    if (indexManager != null)
      indexManager.destroy();
    IndexManager.TEST_RANGEINDEX_ONLY = false;
    
  }

  @Test
  public void testIndexUseSingleCondition() throws Exception {
    String testData[][] = { { "status", "'active'" }, { "ID", "2" },
        { "P1.secId", "'IBM'" }, };
    String operators[] = { "=", "<>", "!=", "<", "<=", ">", ">=" };
    for (int i = 0; i < operators.length; i++) {
      String operator = operators[i];
      for (int j = 0; j < testData.length; j++) {
        Query q = qs.newQuery("SELECT DISTINCT * FROM /pos where "
            + testData[j][0] + " " + operator + " " + testData[j][1]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        q.execute();
        if (!observer.isIndexesUsed) {
          fail("Index not uesd for operator '" + operator + "'");
        }
      }
    }
  }

  @Test
  public void testIndexUseMultipleConditions() throws Exception {
    String testData[][] = { { "P1.secType = 'a'", "0" },
        { "status = 'active' AND ID = 2", "1" },
        { "status = 'active' AND ID = 2 AND P1.secId  = 'IBM'", "1" },
        { "status = 'active' OR ID = 2", "2" },
        { "status = 'active' OR ID = 2 OR P1.secId  = 'IBM'", "3" },
        { "status = 'active' AND ID = 2 OR P1.secId  = 'IBM'", "2" },
        { "status = 'active' AND ( ID = 2 OR P1.secId  = 'IBM')", "1" },
        { "status = 'active' OR ID = 2 AND P1.secId  = 'IBM'", "2" },
        { "(status = 'active' OR ID = 2) AND P1.secId  = 'IBM'", "1" },
        { "NOT (status = 'active') AND ID = 2", "1" },
        { "status = 'active' AND NOT( ID = 2 )", "1" },
        { "NOT (status = 'active') OR ID = 2", "2" },
        { "status = 'active' OR NOT( ID = 2 )", "2" },
        { "status = 'active' AND P1.secType = 'a'", "1" },
        { "status = 'active' OR P1.secType = 'a'", "0" },
        { "status = 'active' AND ID =1 AND P1.secType = 'a'", "1" },
        { "status = 'active' AND ID = 1 OR P1.secType = 'a'", "0" },
        { "status = 'active' OR ID = 1 AND P1.secType = 'a'", "2" },
        { "P2.secId = null", "1" }, { "IS_UNDEFINED(P2.secId)", "1" },
        { "IS_DEFINED(P2.secId)", "1" }, { "P2.secId = UNDEFINED", "0" }, };
    for (int j = 0; j < testData.length; j++) {
      Query q = qs.newQuery("SELECT DISTINCT * FROM /pos where "
          + testData[j][0]);
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      q.execute();
      if (observer.indexesUsed.size() != Integer.parseInt(testData[j][1])) {
        fail("Wrong Index use for " + testData[j][0] + "\n Indexes used "
            + observer.indexesUsed);
      }
    }
  }

  /**
   * Test to check if Region object is passed as bind argument, the index
   * utilization occurs or not
   * @author ashahid
   */
  @Test
  public void testBug36421_part1() {
    try {
      String testData[][] = { { "status", "'active'" }, };
      String operators[] = { "=" };
      for (int i = 0; i < operators.length; i++) {
        String operator = operators[i];
        for (int j = 0; j < testData.length; j++) {
          Query q = qs.newQuery("SELECT DISTINCT * FROM $1 where "
              + testData[j][0] + " " + operator + " " + testData[j][1]);
          QueryObserverImpl observer = new QueryObserverImpl();
          QueryObserverHolder.setInstance(observer);
          q.execute(new Object[] { CacheUtils.getRegion("/pos") });
          if (!observer.isIndexesUsed) {
            fail("Index not uesd for operator '" + operator + "'");
          }
        }
      }
    }
    catch (Exception e) {
      CacheUtils.getLogger().error(e);
      fail("Test faield due to exception =" + e);
    }
  }

  /**
   * Test to check if Region short cut method is used for querying, the index
   * utilization occurs or not
   * @author ashahid
   */
  @Test
  public void testBug36421_part2() {
    try {
      String testData[][] = { { "status", "'active'" }, };
      String operators[] = { "=" };
      for (int i = 0; i < operators.length; i++) {
        String operator = operators[i];
        for (int j = 0; j < testData.length; j++) {

          QueryObserverImpl observer = new QueryObserverImpl();
          QueryObserverHolder.setInstance(observer);
          CacheUtils.getRegion("/pos").query(
              testData[j][0] + " " + operator + " " + testData[j][1]);
          if (!observer.isIndexesUsed) {
            fail("Index not uesd for operator '" + operator + "'");
          }
        }
      }
    }
    catch (Exception e) {
      CacheUtils.getLogger().error(e);
      fail("Test failed due to exception =" + e);
    }
  }

  /**
   * Test to check if a parametrized query when using different bind arguments
   * of Region uses the index correctly
   * @author ashahid
   */
  @Test
  public void testBug36421_part3() {

    Query q = null;
    try {
      q = qs.newQuery("SELECT DISTINCT * FROM $1 z where z.status = 'active'");
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      q.execute(new Object[] { CacheUtils.getRegion("/pos") });
      if (!observer.isIndexesUsed) {
        fail("Index not uesd for operator '='");
      }
      assertTrue(observer.indexesUsed.get(0).equals("statusIndex"));
      region = CacheUtils.createRegion("pos1", Portfolio.class);
      region.put("0", new Portfolio(0));
      region.put("1", new Portfolio(1));
      region.put("2", new Portfolio(2));
      region.put("3", new Portfolio(3));
      qs.createIndex("statusIndex1", IndexType.FUNCTIONAL, "pf1.status",
          "/pos1 pf1");
      region.put("4", new Portfolio(4));
      observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      q.execute(new Object[] { CacheUtils.getRegion("/pos1") });
      if (!observer.isIndexesUsed) {
        fail("Index not uesd for operator'='");
      }
      assertTrue(observer.indexesUsed.get(0).equals("statusIndex1"));

    }
    catch (Exception e) {
      CacheUtils.getLogger().error(e);
      fail("Test failed due to exception =" + e);
    }
  }

  /**
   * Test to check if Region short cut method is used for querying, the Primary
   * key index utilization occurs or not 
   * @author ashahid
   */
  @Test
  public void testBug36421_part4() {
//    Query q = null;
    try {
      qs.createIndex("pkIndex", IndexType.PRIMARY_KEY, "pk", "/pos");
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      SelectResults rs = CacheUtils.getRegion("/pos").query("pk = '2'");
      if (!observer.isIndexesUsed) {
        fail("Index not uesd for operator '='");
      }
      assertTrue(rs.size() == 1);
      assertTrue(((Portfolio)rs.iterator().next()).pkid.equals("2"));
      assertTrue(observer.indexesUsed.get(0).equals("pkIndex"));
      CacheUtils.getRegion("/pos").put("7", new Portfolio(7));
      observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      rs = CacheUtils.getRegion("/pos").query("pk = '7'");
      if (!observer.isIndexesUsed) {
        fail("Index not uesd for operator '='");
      }
      assertTrue(rs.size() == 1);
      assertTrue(((Portfolio)rs.iterator().next()).pkid.equals("7"));
      assertTrue(observer.indexesUsed.get(0).equals("pkIndex"));
    }
    catch (Exception e) {
      CacheUtils.getLogger().error(e);
      fail("Test failed due to exception =" + e);
    }
  }
  
  @Test
  public void testMapIndexUsageAllKeys() throws Exception {
    QueryService qs;
    qs = CacheUtils.getQueryService();
    LocalRegion testRgn = (LocalRegion)CacheUtils.createRegion("testRgn", null);
    int ID =1;
    //Add some test data now
    //Add 5 main objects. 1 will contain key1, 2 will contain key1 & key2
    // and so on
    for(; ID <=30; ++ID) {
      MapKeyIndexData mkid = new MapKeyIndexData(ID);
      for(int j =1; j<= ID;++j) {
        mkid.maap.put("key1", j*1);
        mkid.maap.put("key2", j*2);
        mkid.maap.put("key3", j*3);
      }
      testRgn.put(ID, mkid);
    }
    String queries[] = {
        "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.maap['key2'] >= 16"
       /* "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.maap.get('key2') >= 16" ,*/        
    };
    String queriesIndexNotUsed[] = {        
         "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.maap.get('key2') >= 16"         
     };
    Object r[][]= new Object[queries.length][2];
    
    qs = CacheUtils.getQueryService();
    
  //Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
        Query q = null;
        try {
            q = CacheUtils.getQueryService().newQuery(queries[i]);
            CacheUtils.getLogger().info("Executing query: " + queries[i]);              
            r[i][0] = q.execute();         
          CacheUtils.log("Executed query: " + queries[i] );
        } catch (Exception e) {
            e.printStackTrace();
            fail(q.getQueryString());
        }
    }  
    
    Index i1 = qs.createIndex("Index1", IndexType.FUNCTIONAL, "objs.maap[*]",
        "/testRgn objs");
    
          //Execute Queries with Indexes
      for (int i = 0; i < queries.length; i++) {
          Query q = null;
          try {
              q = CacheUtils.getQueryService().newQuery(queries[i]);
              CacheUtils.getLogger().info("Executing query: " + queries[i]);
              QueryObserverImpl observer = new QueryObserverImpl();
              QueryObserverHolder.setInstance(observer);
              r[i][1] = q.execute();
              CacheUtils.log("Executing query: " + queries[i] + " with index created");
              if(!observer.isIndexesUsed){
                  fail("Index is NOT uesd");
              }              
              Iterator itr = observer.indexesUsed.iterator();
              assertTrue(itr.hasNext());
              String temp = itr.next().toString();
              assertEquals(temp,"Index1");       
              
          } catch (Exception e) {
              e.printStackTrace();
              fail(q.getQueryString());
          }
      }
      StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
      ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r,queries.length,queries);
      
      //Test queries index not used
      for (int i = 0; i < queriesIndexNotUsed.length; i++) {
        Query q = null;
        try {
            q = CacheUtils.getQueryService().newQuery(queriesIndexNotUsed[i]);
            CacheUtils.getLogger().info("Executing query: " + queriesIndexNotUsed[i]);
            QueryObserverImpl observer = new QueryObserverImpl();
            QueryObserverHolder.setInstance(observer);            
            CacheUtils.log("Executing query: " + queriesIndexNotUsed[i] + " with index created");
            q.execute();
            assertFalse(observer.isIndexesUsed);                          
            Iterator itr = observer.indexesUsed.iterator();
            assertFalse(itr.hasNext());                   
            
        } catch (Exception e) {
            e.printStackTrace();
            fail(q.getQueryString());
        }
    }
      
  }
  
  @Test
  public void testCompactMapIndexUsageWithIndexOnSingleKey() throws Exception
  {
    QueryService qs;
    qs = CacheUtils.getQueryService();
    LocalRegion testRgn = (LocalRegion)CacheUtils.createRegion("testRgn", null);
    int ID = 1;
    // Add some test data now
    // Add 5 main objects. 1 will contain key1, 2 will contain key1 & key2
    // and so on
    for (; ID <= 30; ++ID) {
      MapKeyIndexData mkid = new MapKeyIndexData(ID);
      for (int j = 1; j <= ID; ++j) {
        mkid.addKeyValue("key1", j * 1);
        mkid.addKeyValue("key2", j * 2);
        mkid.addKeyValue("key3", j * 3);
      }
      testRgn.put(ID, mkid);
    }

    qs = CacheUtils.getQueryService();
    String queries[] = { "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.maap['key2'] >= 3" };

    String queriesIndexNotUsed[] = {
        "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.maap.get('key2') >= 3",
        "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.maap['key3'] >= 3", };

    Object r[][] = new Object[queries.length][2];

    // Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute();
        CacheUtils.log("Executed query: " + queries[i]);
      }
      catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }

    Index i1 = qs.createIndex("Index1", IndexType.FUNCTIONAL,
        "objs.maap['key2']", "/testRgn objs");
    assertTrue(i1 instanceof CompactRangeIndex);
    // Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        CacheUtils.log("Executing query: " + queries[i]
            + " with index created");
        if (!observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }
        Iterator itr = observer.indexesUsed.iterator();
        assertTrue(itr.hasNext());
        String temp = itr.next().toString();
        assertEquals(temp, "Index1");

      }
      catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length,queries);

    // Test queries index not used
    for (int i = 0; i < queriesIndexNotUsed.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queriesIndexNotUsed[i]);
        CacheUtils.getLogger().info(
            "Executing query: " + queriesIndexNotUsed[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        CacheUtils.log("Executing query: " + queriesIndexNotUsed[i]
            + " with index created");
        q.execute();
        assertFalse(observer.isIndexesUsed);
        Iterator itr = observer.indexesUsed.iterator();
        assertFalse(itr.hasNext());

      }
      catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }

    String query = "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.liist[0] >= 2";
    SelectResults withoutIndex, withIndex;
    Query q = CacheUtils.getQueryService().newQuery(query);
    CacheUtils.getLogger().info("Executing query: " + query);
    withoutIndex = (SelectResults)q.execute();
    CacheUtils.log("Executed query: " + query);

    Index i2 = qs.createIndex("Index2", IndexType.FUNCTIONAL, "objs.liist[0]",
        "/testRgn objs");
    assertTrue(i2 instanceof CompactRangeIndex);
    CacheUtils.getLogger().info("Executing query: " + query);
    QueryObserverImpl observer = new QueryObserverImpl();
    QueryObserverHolder.setInstance(observer);
    withIndex = (SelectResults)q.execute();
    CacheUtils.log("Executing query: " + query + " with index created");
    if (!observer.isIndexesUsed) {
      fail("Index is NOT uesd");
    }
    Iterator itr = observer.indexesUsed.iterator();
    assertTrue(itr.hasNext());
    String temp = itr.next().toString();
    assertEquals(temp, "Index2");

    ssOrrs = new StructSetOrResultsSet();
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(new Object[][] { {
        withoutIndex, withIndex } }, 1,queries);

  }
  @Test
  public void testCompactMapIndexUsageWithIndexOnMultipleKeys() throws Exception
  {
    QueryService qs;
    qs = CacheUtils.getQueryService();
    LocalRegion testRgn = (LocalRegion)CacheUtils.createRegion("testRgn", null);
    int ID = 1;
    // Add some test data now
    // Add 5 main objects. 1 will contain key1, 2 will contain key1 & key2
    // and so on
    for (; ID <= 30; ++ID) {
      MapKeyIndexData mkid = new MapKeyIndexData(ID);
      for (int j = 1; j <= ID; ++j) {
        mkid.addKeyValue("key1", j * 1);
        mkid.addKeyValue("key2", j * 2);
        mkid.addKeyValue("key3", j * 3);
      }
      testRgn.put(ID, mkid);
    }

    qs = CacheUtils.getQueryService();
    String queries[] = {
        "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.maap['key2'] >= 3",
        "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.maap['key3'] >= 3" };

    String queriesIndexNotUsed[] = {
        "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.maap.get('key2') >= 16",
        "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.maap['key4'] >= 16", };

    Object r[][] = new Object[queries.length][2];

    // Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute();
        CacheUtils.log("Executed query: " + queries[i]);
      }
      catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }

    Index i1 = qs.createIndex("Index1", IndexType.FUNCTIONAL,
        "objs.maap['key2','key3']", "/testRgn objs");
    assertTrue(i1 instanceof CompactMapRangeIndex);
    // Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        CacheUtils.log("Executing query: " + queries[i]
            + " with index created");
        if (!observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }
        Iterator itr = observer.indexesUsed.iterator();
        assertTrue(itr.hasNext());
        String temp = itr.next().toString();
        assertEquals(temp, "Index1");

      }
      catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length,queries);

    // Test queries index not used
    for (int i = 0; i < queriesIndexNotUsed.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queriesIndexNotUsed[i]);
        CacheUtils.getLogger().info(
            "Executing query: " + queriesIndexNotUsed[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        CacheUtils.log("Executing query: " + queriesIndexNotUsed[i]
            + " with index created");
        q.execute();
        assertFalse(observer.isIndexesUsed);
        Iterator itr = observer.indexesUsed.iterator();
        assertFalse(itr.hasNext());

      }
      catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }

  }
      
  @Test
  public void testMapIndexUsageWithIndexOnMultipleKeys() throws Exception
  {
    IndexManager.TEST_RANGEINDEX_ONLY = true;
    QueryService qs;
    qs = CacheUtils.getQueryService();
    LocalRegion testRgn = (LocalRegion)CacheUtils.createRegion("testRgn", null);
    int ID = 1;
    // Add some test data now
    // Add 5 main objects. 1 will contain key1, 2 will contain key1 & key2
    // and so on
    for (; ID <= 30; ++ID) {
      MapKeyIndexData mkid = new MapKeyIndexData(ID);
      for (int j = 1; j <= ID; ++j) {
        mkid.addKeyValue("key1", j * 1);
        mkid.addKeyValue("key2", j * 2);
        mkid.addKeyValue("key3", j * 3);
      }
      testRgn.put(ID, mkid);
    }

    qs = CacheUtils.getQueryService();
    String queries[] = {
        "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.maap['key2'] >= 3",
        "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.maap['key3'] >= 3" };

    String queriesIndexNotUsed[] = {
        "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.maap.get('key2') >= 16",
        "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.maap['key4'] >= 16", };

    Object r[][] = new Object[queries.length][2];

    // Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute();
        CacheUtils.log("Executed query: " + queries[i]);
      }
      catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }

    Index i1 = qs.createIndex("Index1", IndexType.FUNCTIONAL,
        "objs.maap['key2','key3']", "/testRgn objs");
    assertTrue(i1 instanceof MapRangeIndex);
    // Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        CacheUtils.log("Executing query: " + queries[i]
            + " with index created");
        if (!observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }
        Iterator itr = observer.indexesUsed.iterator();
        assertTrue(itr.hasNext());
        String temp = itr.next().toString();
        assertEquals(temp, "Index1");

      }
      catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length,queries);

    // Test queries index not used
    for (int i = 0; i < queriesIndexNotUsed.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queriesIndexNotUsed[i]);
        CacheUtils.getLogger().info(
            "Executing query: " + queriesIndexNotUsed[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        CacheUtils.log("Executing query: " + queriesIndexNotUsed[i]
            + " with index created");
        q.execute();
        assertFalse(observer.isIndexesUsed);
        Iterator itr = observer.indexesUsed.iterator();
        assertFalse(itr.hasNext());

      }
      catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }

  }

  @Test
  public void testIndexUsageWithOrderBy() throws Exception
  {
    QueryService qs;
    qs = CacheUtils.getQueryService();
    LocalRegion testRgn = (LocalRegion)CacheUtils.createRegion("testRgn", null);
    
    int numObjects = 30;
    // Add some test data now
    // Add 5 main objects. 1 will contain key1, 2 will contain key1 & key2
    // and so on
    for (int i=0; i < numObjects; i++) {
      Portfolio p = new Portfolio(i);
      p.pkid = ("" +(numObjects - i));
      testRgn.put("" + i, p);
    }

    qs = CacheUtils.getQueryService();
    String queries[] = {
        "SELECT DISTINCT * FROM /testRgn p  WHERE p.ID <= 10 order by p.pkid asc limit 1",
        "SELECT DISTINCT * FROM /testRgn p  WHERE p.ID <= 10 order by p.pkid desc limit 1",
    };

    Object r[][] = new Object[queries.length][2];

    // Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        // verify individual result
        SelectResults sr = (SelectResults)q.execute();
        List results = sr.asList();
        for (int rows=0; rows < results.size(); rows++) {
          Portfolio p = (Portfolio)results.get(0);
          CacheUtils.getLogger().info("p: " + p);
          if (i == 0) {
            assertEquals(p.getID(), 10);
            assertEquals(p.pkid, "" + (numObjects - 10));
          } else if (i == 1) {
            assertEquals(p.getID(), 0);
            assertEquals(p.pkid, "" + numObjects);
          }
        }
        r[i][0] = sr;
        CacheUtils.log("Executed query: " + queries[i]);
      }
      catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }

    Index i1 = qs.createIndex("Index1", IndexType.FUNCTIONAL, "p.ID", "/testRgn p");
 
    // Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        SelectResults sr = (SelectResults)q.execute();
        List results = sr.asList();
        for (int rows=0; rows < results.size(); rows++) {
          Portfolio p = (Portfolio)results.get(0);
          CacheUtils.getLogger().info("index p: " + p);
          if (i == 0) {
            assertEquals(p.getID(), 10);
            assertEquals(p.pkid, "" + (numObjects - 10));
          } else if (i == 1) {
            assertEquals(p.getID(), 0);
            assertEquals(p.pkid, "" + numObjects);
          }
        }
        r[i][1] = sr;
        //r[i][1] = q.execute();
        CacheUtils.log("Executing query: " + queries[i]
            + " with index created");
        if (!observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }
        Iterator itr = observer.indexesUsed.iterator();
        assertTrue(itr.hasNext());
        String temp = itr.next().toString();
        assertEquals(temp, "Index1");
      }
      
      catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length,queries);
  }
  
  @Test
  public void testIndexUsageWithOrderBy3() throws Exception
  {
    QueryService qs;
    qs = CacheUtils.getQueryService();
    LocalRegion testRgn = (LocalRegion)CacheUtils.createRegion("testRgn", null);
    
    int numObjects = 30;
    // Add some test data now
    // Add 5 main objects. 1 will contain key1, 2 will contain key1 & key2
    // and so on
    for (int i=0; i < numObjects; i++) {
      Portfolio p = new Portfolio(i);
      p.pkid = ("" +(numObjects - i));
      testRgn.put("" + i, p);
    }

    qs = CacheUtils.getQueryService();
    String queries[] = {
        "SELECT DISTINCT * FROM /testRgn p  WHERE p.ID <= 10 order by ID asc limit 1",
        "SELECT DISTINCT * FROM /testRgn p  WHERE p.ID <= 10 order by p.ID desc limit 1",
    };

    Object r[][] = new Object[queries.length][2];

    
    Index i1 = qs.createIndex("Index1", IndexType.FUNCTIONAL, "p.ID", "/testRgn p");
 
    // Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        SelectResults sr = (SelectResults)q.execute();
        List results = sr.asList();
        for (int rows=0; rows < results.size(); rows++) {
          Portfolio p = (Portfolio)results.get(0);
          CacheUtils.getLogger().info("index p: " + p);
          if (i == 0) {
            assertEquals(p.getID(), 0);
            
          } else if (i == 1) {
            assertEquals(p.getID(), 10);
            
          }
        }
        r[i][1] = sr;
        //r[i][1] = q.execute();
        CacheUtils.log("Executing query: " + queries[i]
            + " with index created");
        if (!observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }
        Iterator itr = observer.indexesUsed.iterator();
        assertTrue(itr.hasNext());
        String temp = itr.next().toString();
        assertEquals(temp, "Index1");
      }
      
      catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
   
  }

  @Test
  public void testIndexUsageWithOrderBy2() throws Exception
  {
    QueryService qs;
    qs = CacheUtils.getQueryService();
    LocalRegion testRgn = (LocalRegion)CacheUtils.createRegion("testRgn", null);
    
    int numObjects = 30;
    // Add some test data now
    // Add 5 main objects. 1 will contain key1, 2 will contain key1 & key2
    // and so on
    for (int i=0; i < numObjects; i++) {
      Portfolio p = new Portfolio(i % 2);
      p.createTime = (numObjects - i);
      testRgn.put("" + i, p);
    }

    qs = CacheUtils.getQueryService();
    String queries[] = {
        "SELECT DISTINCT p.key, p.value FROM /testRgn.entrySet p  WHERE p.value.ID <= 10 order by p.value.createTime asc limit 1",
        "SELECT DISTINCT p.key, p.value FROM /testRgn.entrySet p  WHERE p.value.ID <= 10 order by p.value.createTime desc limit 1",
    };

    Object r[][] = new Object[queries.length][2];

    // Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        // verify individual result
        SelectResults sr = (SelectResults)q.execute();
        List results = sr.asList();
        for (int rows=0; rows < results.size(); rows++) {
          Struct s = (Struct)results.get(0);
          Portfolio p = (Portfolio)s.get("value");
          CacheUtils.getLogger().info("p: " + p);
          if (i == 0) {
            assertEquals(p.createTime, 1);
          } else if (i == 1) {
            assertEquals(p.createTime, numObjects);
          }
        }
        r[i][0] = sr;
        CacheUtils.log("Executed query: " + queries[i]);
      }
      catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }

    Index i1 = qs.createIndex("Index1", IndexType.FUNCTIONAL, "p.value.ID", "/testRgn.entrySet p");
 
    // Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        SelectResults sr = (SelectResults)q.execute();
        List results = sr.asList();
        for (int rows=0; rows < results.size(); rows++) {
          Struct s = (Struct)results.get(0);
          Portfolio p = (Portfolio)s.get("value");
          CacheUtils.getLogger().info("index p: " + p);
          if (i == 0) {
            assertEquals(p.createTime, 1);
          } else if (i == 1) {
            assertEquals(p.createTime, numObjects);
          }
        }
        r[i][1] = sr;
        //r[i][1] = q.execute();
        CacheUtils.log("Executing query: " + queries[i]
            + " with index created");
        if (!observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }
        Iterator itr = observer.indexesUsed.iterator();
        assertTrue(itr.hasNext());
        String temp = itr.next().toString();
        assertEquals(temp, "Index1");
      }
      
      catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length,queries);
  }

  @Test
  public void testIncorrectIndexOperatorSyntax() {
    QueryService qs;
    qs = CacheUtils.getQueryService();
    LocalRegion testRgn = (LocalRegion)CacheUtils.createRegion("testRgn", null);
    int ID =1;
    //Add some test data now
    //Add 5 main objects. 1 will contain key1, 2 will contain key1 & key2
    // and so on
    for(; ID <=30; ++ID) {
      MapKeyIndexData mkid = new MapKeyIndexData(ID);
      for(int j =1; j<= ID;++j) {
        mkid.maap.put("key1", j*1);
        mkid.maap.put("key2", j*2);
        mkid.maap.put("key3", j*3);
      }
      testRgn.put(ID, mkid);
    }
    
    
    qs = CacheUtils.getQueryService();
    try { 
    Query q = CacheUtils.getQueryService().newQuery(
        "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.maap[*] >= 3");
    fail("Should have thrown exception");
    }catch(QueryInvalidException qe) {
      //ok      
    }           
    
    try { 
    Query q = CacheUtils.getQueryService().newQuery(
        "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.maap['key1','key2'] >= 3");
    fail("Should have thrown exception");
    }catch(QueryInvalidException qe) {
      //ok      
    }   
      
  }
  
  @Test
  public void testRangeGroupingBehaviourOfCompactMapIndex() throws Exception {
    QueryService qs;
    qs = CacheUtils.getQueryService();
    LocalRegion testRgn = (LocalRegion)CacheUtils.createRegion("testRgn", null);
    int ID = 1;
    // Add some test data now
    // Add 5 main objects. 1 will contain key1, 2 will contain key1 & key2
    // and so on
    for (; ID <= 30; ++ID) {
      MapKeyIndexData mkid = new MapKeyIndexData(ID);
      for (int j = 1; j <= ID; ++j) {
        mkid.addKeyValue("key1", j * 1);
        mkid.addKeyValue("key2", j * 2);
        mkid.addKeyValue("key3", j * 3);
      }
      testRgn.put(ID, mkid);
    }

    qs = CacheUtils.getQueryService();
    String queries[] = {
        "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.maap['key2'] >= 3 and itr1.maap['key2'] <=18",
        "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.maap['key3'] >= 3  and  itr1.maap['key3'] >= 13 ",
        "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.maap['key2'] >= 3  and  itr1.maap['key3'] >= 13 ",
        "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.maap['key2'] >= 3  and  itr1.maap['key3'] < 18 "
        
    };


    Object r[][] = new Object[queries.length][2];

    // Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute();
        CacheUtils.log("Executed query: " + queries[i]);
      }
      catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }

    Index i1 = qs.createIndex("Index1", IndexType.FUNCTIONAL,
        "objs.maap['key2','key3']", "/testRgn objs");
    assertTrue(i1 instanceof CompactMapRangeIndex);
    // Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        CacheUtils.log("Executing query: " + queries[i]
            + " with index created");
        if (!observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }
        Iterator itr = observer.indexesUsed.iterator();
        assertTrue(itr.hasNext());
        String temp = itr.next().toString();
        assertEquals(temp, "Index1");

      }
      catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length,queries);
    
  }
  
  @Test
  public void testRangeGroupingBehaviourOfMapIndex() throws Exception {
    IndexManager.TEST_RANGEINDEX_ONLY = true;
    QueryService qs;
    qs = CacheUtils.getQueryService();
    LocalRegion testRgn = (LocalRegion)CacheUtils.createRegion("testRgn", null);
    int ID = 1;
    // Add some test data now
    // Add 5 main objects. 1 will contain key1, 2 will contain key1 & key2
    // and so on
    for (; ID <= 30; ++ID) {
      MapKeyIndexData mkid = new MapKeyIndexData(ID);
      for (int j = 1; j <= ID; ++j) {
        mkid.addKeyValue("key1", j * 1);
        mkid.addKeyValue("key2", j * 2);
        mkid.addKeyValue("key3", j * 3);
      }
      testRgn.put(ID, mkid);
    }

    qs = CacheUtils.getQueryService();
    String queries[] = {
        "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.maap['key2'] >= 3 and itr1.maap['key2'] <=18",
        "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.maap['key3'] >= 3  and  itr1.maap['key3'] >= 13 ",
        "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.maap['key2'] >= 3  and  itr1.maap['key3'] >= 13 ",
        "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.maap['key2'] >= 3  and  itr1.maap['key3'] < 18 "
        
    };


    Object r[][] = new Object[queries.length][2];

    // Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute();
        CacheUtils.log("Executed query: " + queries[i]);
      }
      catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }

    Index i1 = qs.createIndex("Index1", IndexType.FUNCTIONAL,
        "objs.maap['key2','key3']", "/testRgn objs");
    assertTrue(i1 instanceof MapRangeIndex);
    // Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        CacheUtils.log("Executing query: " + queries[i]
            + " with index created");
        if (!observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }
        Iterator itr = observer.indexesUsed.iterator();
        assertTrue(itr.hasNext());
        String temp = itr.next().toString();
        assertEquals(temp, "Index1");

      }
      catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length,queries);
    
  }
  
  @Test
  public void testMapIndexUsableQueryOnEmptyRegion() throws Exception
  {

    QueryService qs;
    qs = CacheUtils.getQueryService();
    LocalRegion testRgn = (LocalRegion)CacheUtils.createRegion("testRgn", null);
    Index i1 = qs.createIndex("Index1", IndexType.FUNCTIONAL,
        "objs.maap['key2','key3']", "/testRgn objs");
    qs = CacheUtils.getQueryService();
    // Execute Queries without Indexes
    Query q = CacheUtils.getQueryService().newQuery(
        "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.maap['key2'] >= 3 ");
    SelectResults sr = (SelectResults)q.execute();
    assertTrue(sr.isEmpty());

  }

  @Test
  public void testSizeEstimateLTInRangeIndexForNullMap() throws Exception {
    QueryService qs = CacheUtils.getQueryService();
    LocalRegion testRgn = (LocalRegion)CacheUtils.createRegion("testRgn", null);
    //Create indexes
    Index i1 = qs.createIndex("Index1", IndexType.FUNCTIONAL,
        "p.status", "/testRgn p, p.positions");
    Index i2 = qs.createIndex("Index2", IndexType.FUNCTIONAL,
        "p.ID", "/testRgn p, p.positions");
    
    //put values
    testRgn.put(0, new Portfolio(0));
    testRgn.put(1, new Portfolio(1));

    //Set TestHook in RangeIndex
    TestHook hook = new RangeIndexTestHook();
    RangeIndex.setTestHook(hook);
    // Execute Queries without Indexes
    Query q = CacheUtils.getQueryService().newQuery(
        "<trace> SELECT * FROM /testRgn p, p.positions where p.status = 'active' AND p.ID > 0 ");
    
    //Following should throw NullPointerException.
    SelectResults sr = (SelectResults)q.execute();
    
    assertTrue("RangeIndexTestHook was not hooked for spot 2", ((RangeIndexTestHook)hook).isHooked(2));
    RangeIndex.setTestHook(null);
  }

  @Test
  public void testSizeEstimateGTInRangeIndexForNullMap() throws Exception {
    QueryService qs = CacheUtils.getQueryService();
    LocalRegion testRgn = (LocalRegion)CacheUtils.createRegion("testRgn", null);
    //Create indexes
    Index i1 = qs.createIndex("Index1", IndexType.FUNCTIONAL,
        "p.status", "/testRgn p, p.positions");
    Index i2 = qs.createIndex("Index2", IndexType.FUNCTIONAL,
        "p.ID", "/testRgn p, p.positions");
    
    //put values
    testRgn.put(0, new Portfolio(0));
    testRgn.put(1, new Portfolio(1));

    //Set TestHook in RangeIndex
    TestHook hook = new RangeIndexTestHook();
    RangeIndex.setTestHook(hook);
    // Execute Queries without Indexes
    Query q = CacheUtils.getQueryService().newQuery(
        "<trace> SELECT * FROM /testRgn p, p.positions where p.status = 'active' AND p.ID < 0 ");
    
    //Following should throw NullPointerException.
    SelectResults sr = (SelectResults)q.execute();
    
    assertTrue("RangeIndexTestHook was not hooked for spot 1", ((RangeIndexTestHook)hook).isHooked(1));
    RangeIndex.setTestHook(null);
  }

  @Test
  public void testSizeEstimateLTInCompactRangeIndexForNullMap() throws Exception {
    QueryService qs = CacheUtils.getQueryService();
    LocalRegion testRgn = (LocalRegion)CacheUtils.createRegion("testRgn", null);
    //Create indexes
    Index i1 = qs.createIndex("Index1", IndexType.FUNCTIONAL,
        "p.status", "/testRgn p");
    Index i2 = qs.createIndex("Index2", IndexType.FUNCTIONAL,
        "p.ID", "/testRgn p");
    
    //put values
    testRgn.put(0, new Portfolio(0));
    testRgn.put(1, new Portfolio(1));

    //Set TestHook in RangeIndex
    TestHook hook = new RangeIndexTestHook();
    CompactRangeIndex.setTestHook(hook);
    // Execute Queries without Indexes
    Query q = CacheUtils.getQueryService().newQuery(
        "<trace> SELECT * FROM /testRgn p where p.status = 'active' AND p.ID > 0 ");
    
    //Following should throw NullPointerException.
    SelectResults sr = (SelectResults)q.execute();
    
    assertTrue("RangeIndexTestHook was not hooked for spot 2", ((RangeIndexTestHook)hook).isHooked(2));
    CompactRangeIndex.setTestHook(null);
  }

  @Test
  public void testSizeEstimateGTInCompactRangeIndexForNullMap() throws Exception {
    QueryService qs = CacheUtils.getQueryService();
    LocalRegion testRgn = (LocalRegion)CacheUtils.createRegion("testRgn", null);
    //Create indexes
    Index i1 = qs.createIndex("Index1", IndexType.FUNCTIONAL,
        "p.status", "/testRgn p");
    Index i2 = qs.createIndex("Index2", IndexType.FUNCTIONAL,
        "p.ID", "/testRgn p");
    
    //put values
    testRgn.put(0, new Portfolio(0));
    testRgn.put(1, new Portfolio(1));

    //Set TestHook in RangeIndex
    TestHook hook = new RangeIndexTestHook();
    CompactRangeIndex.setTestHook(hook);
    // Execute Queries without Indexes
    Query q = CacheUtils.getQueryService().newQuery(
        "<trace> SELECT * FROM /testRgn p where p.status = 'active' AND p.ID < 0 ");
    
    //Following should throw NullPointerException.
    SelectResults sr = (SelectResults)q.execute();
    
    assertTrue("RangeIndexTestHook was not hooked for spot 1", ((RangeIndexTestHook)hook).isHooked(1));
    CompactRangeIndex.setTestHook(null);
  }
  
  @Test
  public void testCompactMapIndexUsageAllKeysWithVariousValueTypes() throws Exception {
      QueryService qs;
      qs = CacheUtils.getQueryService();
      LocalRegion testRgn = (LocalRegion)CacheUtils.createRegion("testRgn", null);
      int ID =1;
      for(; ID <=30; ++ID) {
        TestObject object = new TestObject(ID);
        testRgn.put(ID, object);
      }
      String queries[] = {
          "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.testFields['string'] = '1'",
          "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.testFields['double'] > 1D",
          "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.testFields['integer'] > 1",
          "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.testFields['long'] > 1L"
      };
      String queriesIndexNotUsed[] = {        
          "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.testFields.get('string') = '1'",       
          "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.testFields.get('double') > 1D",         
          "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.testFields.get('integer') > 1",         
          "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.testFields.get('long') > 1L"         
       };
      Object r[][]= new Object[queries.length][2];
      
      qs = CacheUtils.getQueryService();
      
    //Execute Queries without Indexes
      for (int i = 0; i < queries.length; i++) {
          Query q = null;
          try {
              q = CacheUtils.getQueryService().newQuery(queries[i]);
              CacheUtils.getLogger().info("Executing query: " + queries[i]);              
              r[i][0] = q.execute();         
            CacheUtils.log("Executed query: " + queries[i] );
          } catch (Exception e) {
              e.printStackTrace();
              fail(q.getQueryString());
          }
      }  
      
      Index i1 = qs.createIndex("Index1", IndexType.FUNCTIONAL, "itr1.testFields[*]",
          "/testRgn itr1");
      
      assertTrue(i1 instanceof CompactMapRangeIndex);
      
            //Execute Queries with Indexes
        for (int i = 0; i < queries.length; i++) {
            Query q = null;
            try {
                q = CacheUtils.getQueryService().newQuery(queries[i]);
                CacheUtils.getLogger().info("Executing query: " + queries[i]);
                QueryObserverImpl observer = new QueryObserverImpl();
                QueryObserverHolder.setInstance(observer);
                r[i][1] = q.execute();
                CacheUtils.log("Executing query: " + queries[i] + " with index created");
                if(!observer.isIndexesUsed){
                    fail("Index is NOT uesd");
                }              
                Iterator itr = observer.indexesUsed.iterator();
                assertTrue(itr.hasNext());
                String temp = itr.next().toString();
                assertEquals(temp,"Index1");       
                
            } catch (Exception e) {
                e.printStackTrace();
                fail(q.getQueryString());
            }
        }
        StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
        ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r,queries.length,queries);
        
        //Test queries index not used
        for (int i = 0; i < queriesIndexNotUsed.length; i++) {
          Query q = null;
          try {
              q = CacheUtils.getQueryService().newQuery(queriesIndexNotUsed[i]);
              CacheUtils.getLogger().info("Executing query: " + queriesIndexNotUsed[i]);
              QueryObserverImpl observer = new QueryObserverImpl();
              QueryObserverHolder.setInstance(observer);            
              CacheUtils.log("Executing query: " + queriesIndexNotUsed[i] + " with index created");
              q.execute();
              assertFalse(observer.isIndexesUsed);                          
              Iterator itr = observer.indexesUsed.iterator();
              assertFalse(itr.hasNext());                   
              
          } catch (Exception e) {
              e.printStackTrace();
              fail(q.getQueryString());
          }
      }
    }
    
    @Test
  public void testCompactMapIndexUsageAllKeysOneIndex() throws Exception {
      QueryService qs;
      qs = CacheUtils.getQueryService();
      LocalRegion testRgn = (LocalRegion)CacheUtils.createRegion("testRgn", null);
      int ID =1;
      for(; ID <=30; ++ID) {
        TestObject object = new TestObject(ID);
        testRgn.put(ID, object);
      }
      String queries[] = {
          "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.testFields['string'] = '1'",
          "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.testFields['double'] > 1D",
          "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.testFields['integer'] > 1",
          "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.testFields['long'] > 1L"
      };
      String queriesIndexNotUsed[] = {        
          "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.testFields.get('string') = '1'",       
          "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.testFields.get('double') > 1D",         
          "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.testFields.get('integer') > 1",         
          "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.testFields.get('long') > 1L"         
       };
      Object r[][]= new Object[queries.length][2];
      
      qs = CacheUtils.getQueryService();
      
    //Execute Queries without Indexes
      for (int i = 0; i < queries.length; i++) {
          Query q = null;
          try {
              q = CacheUtils.getQueryService().newQuery(queries[i]);
              CacheUtils.getLogger().info("Executing query: " + queries[i]);              
              r[i][0] = q.execute();         
            CacheUtils.log("Executed query: " + queries[i] );
          } catch (Exception e) {
              e.printStackTrace();
              fail(q.getQueryString());
          }
      }  
      
      Index i1 = qs.createIndex("Index1", IndexType.FUNCTIONAL, "itr1.testFields['string','double','integer','long']",
          "/testRgn itr1");
      
      assertTrue(i1 instanceof CompactMapRangeIndex);
      
            //Execute Queries with Indexes
        for (int i = 0; i < queries.length; i++) {
            Query q = null;
            try {
                q = CacheUtils.getQueryService().newQuery(queries[i]);
                CacheUtils.getLogger().info("Executing query: " + queries[i]);
                QueryObserverImpl observer = new QueryObserverImpl();
                QueryObserverHolder.setInstance(observer);
                r[i][1] = q.execute();
                CacheUtils.log("Executing query: " + queries[i] + " with index created");
                if(!observer.isIndexesUsed){
                    fail("Index is NOT uesd");
                }              
                Iterator itr = observer.indexesUsed.iterator();
                assertTrue(itr.hasNext());
                String temp = itr.next().toString();
                assertEquals(temp,"Index1");       
                
            } catch (Exception e) {
                e.printStackTrace();
                fail(q.getQueryString());
            }
        }
        StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
        ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r,queries.length,queries);
        
        //Test queries index not used
        for (int i = 0; i < queriesIndexNotUsed.length; i++) {
          Query q = null;
          try {
              q = CacheUtils.getQueryService().newQuery(queriesIndexNotUsed[i]);
              CacheUtils.getLogger().info("Executing query: " + queriesIndexNotUsed[i]);
              QueryObserverImpl observer = new QueryObserverImpl();
              QueryObserverHolder.setInstance(observer);            
              CacheUtils.log("Executing query: " + queriesIndexNotUsed[i] + " with index created");
              q.execute();
              assertFalse(observer.isIndexesUsed);                          
              Iterator itr = observer.indexesUsed.iterator();
              assertFalse(itr.hasNext());                   
              
          } catch (Exception e) {
              e.printStackTrace();
              fail(q.getQueryString());
          }
      }
    }
    
    @Test
  public void testCompactMapIndexUsageManyGetKeysWithVariousValueTypes() throws Exception {
      QueryService qs;
      qs = CacheUtils.getQueryService();
      LocalRegion testRgn = (LocalRegion)CacheUtils.createRegion("testRgn", null);
      int ID =1;
      for(; ID <=30; ++ID) {
        TestObject object = new TestObject(ID);
        testRgn.put(ID, object);
      }
      String queries[] = {
          "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.testFields.get('string') = '1'",       
          "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.testFields.get('double') > 1D",         
          "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.testFields.get('integer') > 1",         
          "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.testFields.get('long') > 1L" 
      };
      String queriesIndexNotUsed[] = {        
          "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.testFields['string'] = '1'",
          "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.testFields['double'] > 1D",
          "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.testFields['integer'] > 1",
          "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.testFields['long'] > 1L"
       };
      Object r[][]= new Object[queries.length][2];
      
      qs = CacheUtils.getQueryService();
      
      //Execute Queries without Indexes
      for (int i = 0; i < queries.length; i++) {
          Query q = null;
          try {
              q = CacheUtils.getQueryService().newQuery(queries[i]);
              CacheUtils.getLogger().info("Executing query: " + queries[i]);              
              r[i][0] = q.execute();         
            CacheUtils.log("Executed query: " + queries[i] );
          } catch (Exception e) {
              e.printStackTrace();
              fail(q.getQueryString());
          }
      }  
      
      Index i1 = qs.createIndex("Index1", IndexType.FUNCTIONAL, "itr1.testFields.get('string')",
          "/testRgn itr1");
      Index i2 = qs.createIndex("Index2", IndexType.FUNCTIONAL, "itr1.testFields.get('double')",
          "/testRgn itr1");
      Index i3 = qs.createIndex("Index3", IndexType.FUNCTIONAL, "itr1.testFields.get('integer')",
          "/testRgn itr1");
      Index i4 = qs.createIndex("Index4", IndexType.FUNCTIONAL, "itr1.testFields.get('long')",
          "/testRgn itr1");  
      Index i5 = qs.createIndex("Index5", IndexType.FUNCTIONAL, "itr1.testFields.get('complex')",
          "/testRgn itr1");
      
      assertTrue(i1 instanceof CompactRangeIndex);
      
            //Execute Queries with Indexes
        for (int i = 0; i < queries.length; i++) {
            Query q = null;
            try {
                q = CacheUtils.getQueryService().newQuery(queries[i]);
                CacheUtils.getLogger().info("Executing query: " + queries[i]);
                QueryObserverImpl observer = new QueryObserverImpl();
                QueryObserverHolder.setInstance(observer);
                r[i][1] = q.execute();
                CacheUtils.log("Executing query: " + queries[i] + " with index created");
                if(!observer.isIndexesUsed){
                    fail("Index is NOT uesd");
                }              
                Iterator itr = observer.indexesUsed.iterator();
                assertTrue(itr.hasNext());
                String temp = itr.next().toString();
                assertEquals(temp,"Index" + (i + 1));       
                
            } catch (Exception e) {
                e.printStackTrace();
                fail(q.getQueryString());
            }
        }
        StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
        ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r,queries.length,queries);
        
        //Test queries index not used
        for (int i = 0; i < queriesIndexNotUsed.length; i++) {
          Query q = null;
          try {
              q = CacheUtils.getQueryService().newQuery(queriesIndexNotUsed[i]);
              CacheUtils.getLogger().info("Executing query: " + queriesIndexNotUsed[i]);
              QueryObserverImpl observer = new QueryObserverImpl();
              QueryObserverHolder.setInstance(observer);            
              CacheUtils.log("Executing query: " + queriesIndexNotUsed[i] + " with index created");
              q.execute();
              assertFalse(observer.isIndexesUsed);                          
              Iterator itr = observer.indexesUsed.iterator();
              assertFalse(itr.hasNext());                   
              
          } catch (Exception e) {
              e.printStackTrace();
              fail(q.getQueryString());
          }
      }
    }
    
    @Test
  public void testCompactMapIndexUsageManyKeysWithVariousValueTypes() throws Exception {
      QueryService qs;
      qs = CacheUtils.getQueryService();
      LocalRegion testRgn = (LocalRegion)CacheUtils.createRegion("testRgn", null);
      int ID =1;
      for(; ID <=30; ++ID) {
        TestObject object = new TestObject(ID);
        testRgn.put(ID, object);
      }
      String queries[] = {
          "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.testFields['string'] = '1'",
          "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.testFields['double'] > 1D",
          "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.testFields['integer'] > 1",
          "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.testFields['long'] > 1L"
      };
      String queriesIndexNotUsed[] = {        
          "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.testFields.get('string') = '1'",       
          "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.testFields.get('double') > 1D",         
          "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.testFields.get('integer') > 1",         
          "SELECT DISTINCT * FROM /testRgn itr1  WHERE itr1.testFields.get('long') > 1L"         
       };
      Object r[][]= new Object[queries.length][2];
      
      qs = CacheUtils.getQueryService();
      
    //Execute Queries without Indexes
      for (int i = 0; i < queries.length; i++) {
          Query q = null;
          try {
              q = CacheUtils.getQueryService().newQuery(queries[i]);
              CacheUtils.getLogger().info("Executing query: " + queries[i]);              
              r[i][0] = q.execute();         
            CacheUtils.log("Executed query: " + queries[i] );
          } catch (Exception e) {
              e.printStackTrace();
              fail(q.getQueryString());
          }
      }  
      
      Index i1 = qs.createIndex("Index1", IndexType.FUNCTIONAL, "itr1.testFields['string']",
          "/testRgn itr1");
      Index i2 = qs.createIndex("Index2", IndexType.FUNCTIONAL, "itr1.testFields['double']",
          "/testRgn itr1");
      Index i3 = qs.createIndex("Index3", IndexType.FUNCTIONAL, "itr1.testFields['integer']",
          "/testRgn itr1");
      Index i4 = qs.createIndex("Index4", IndexType.FUNCTIONAL, "itr1.testFields['long']",
          "/testRgn itr1");
      Index i5 = qs.createIndex("Index5", IndexType.FUNCTIONAL, "itr1.testFields['complex']",
          "/testRgn itr1");
      
      assertTrue(i1 instanceof CompactRangeIndex);
      
            //Execute Queries with Indexes
        for (int i = 0; i < queries.length; i++) {
            Query q = null;
            try {
                q = CacheUtils.getQueryService().newQuery(queries[i]);
                CacheUtils.getLogger().info("Executing query: " + queries[i]);
                QueryObserverImpl observer = new QueryObserverImpl();
                QueryObserverHolder.setInstance(observer);
                r[i][1] = q.execute();
                CacheUtils.log("Executing query: " + queries[i] + " with index created");
                if(!observer.isIndexesUsed){
                    fail("Index is NOT uesd");
                }              
                Iterator itr = observer.indexesUsed.iterator();
                assertTrue(itr.hasNext());
                String temp = itr.next().toString();
                assertEquals(temp,"Index" + (i+1));       
                
            } catch (Exception e) {
                e.printStackTrace();
                fail(q.getQueryString());
            }
        }
        StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
        ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r,queries.length,queries);
        
        //Test queries index not used
        for (int i = 0; i < queriesIndexNotUsed.length; i++) {
          Query q = null;
          try {
              q = CacheUtils.getQueryService().newQuery(queriesIndexNotUsed[i]);
              CacheUtils.getLogger().info("Executing query: " + queriesIndexNotUsed[i]);
              QueryObserverImpl observer = new QueryObserverImpl();
              QueryObserverHolder.setInstance(observer);            
              CacheUtils.log("Executing query: " + queriesIndexNotUsed[i] + " with index created");
              q.execute();
              assertFalse(observer.isIndexesUsed);                          
              Iterator itr = observer.indexesUsed.iterator();
              assertFalse(itr.hasNext());                   
              
          } catch (Exception e) {
              e.printStackTrace();
              fail(q.getQueryString());
          }
      }
    }

    @Test
  public void testIndexUseSelfJoin() throws Exception {
      String[] queries = {"SELECT DISTINCT * FROM /pos p1, /pos p2 where p1.status = p2.status",
          "SELECT DISTINCT * FROM /pos p1, /pos p2 where p1.ID = p2.ID",
          "SELECT DISTINCT * FROM /pos p1, /pos p2 where p1.P1.secId = p2.P1.secId",
          "SELECT DISTINCT * FROM /pos p1, /pos p2 where p1.status = p2.status and p1.status = 'active'",
          "SELECT DISTINCT * FROM /pos p1, /pos p2 where p1.ID = p2.ID and p1.ID < 2",
          "SELECT * FROM /pos p1, /pos p2 where p1.ID = p2.ID",
          "SELECT * FROM /pos p1, /pos p2 where p1.P1.secId = p2.P1.secId",
          "SELECT * FROM /pos p1, /pos p2 where p1.status = p2.status and p1.status = 'active'",
          "SELECT * FROM /pos p1, /pos p2 where p1.ID = p2.ID and p1.ID < 2"};

      SelectResults[][] sr = new SelectResults[queries.length][2];
        for (int j = 0; j < queries.length; j++) {
          Query q = qs.newQuery(queries[j]);
          QueryObserverImpl observer = new QueryObserverImpl();
          QueryObserverHolder.setInstance(observer);
          sr[j][0] = (SelectResults) q.execute();
          if(sr[j][0].size() == 0) {
            fail("Query " + q.getQueryString() + " should have returned results");
          }
          if (!observer.isIndexesUsed) {
            fail("Index should have been used for query '" + q.getQueryString() + "'");
          }
        }
        qs.removeIndexes();
        for (int j = 0; j < queries.length; j++) {
          Query q = qs.newQuery(queries[j]);
          QueryObserverImpl observer = new QueryObserverImpl();
          QueryObserverHolder.setInstance(observer);
          sr[j][1] = (SelectResults) q.execute();
          if(sr[j][1].size() == 0) {
            fail("Query " + q.getQueryString() + " should have returned results");
          }
          if (observer.isIndexesUsed) {
            fail("Index should not be used for query '" + q.getQueryString() + "'");
          }
        }
        CacheUtils.compareResultsOfWithAndWithoutIndex(sr);
    }

    @Test
  public void testUndefinedInResults() throws Exception {
      Portfolio p1 = new Portfolio(0);
      p1.position1 = null;
      region.put("0", p1);
      Portfolio p2 = new Portfolio(2);
      p2.position1 = null;
      region.put("2", p2);

      String query = "SELECT p.position1.secId FROM /pos p where p.position1.secId != 'MSFT' ";
      SelectResults[][] sr = new SelectResults[1][2];
      sr[0][0] = (SelectResults) qs.newQuery(query).execute();
      qs.removeIndexes();
      sr[0][1] = (SelectResults) qs.newQuery(query).execute();
      if(!CacheUtils.compareResultsOfWithAndWithoutIndex(sr)) {
        fail("Query results not same with and without index");
      }
    }

    @Test
    public void testBug52444() throws Exception {
        // Create partitioned region
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        AttributesFactory af = new AttributesFactory();
        af.setPartitionAttributes(paf.create());
        Region region = CacheUtils.createRegion("testBug52444", af.create(), false);

        // Add index
        PartitionedIndex index = (PartitionedIndex) qs.createIndex("statusIndex", "status", region.getFullPath());

        // Do puts
        for (int i=0; i<200; i++) {
          region.put(i, new Portfolio(i));
        }
        
        // Initialize query observer
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        
        // Create and run query
        Query query = qs.newQuery("SELECT * FROM " + region.getFullPath() + " where status = 'active'");
        query.execute();

        // Verify index was used
        assertTrue(observer.isIndexesUsed);

        // Get the first index entry in the PartitionedIndex bucketIndexes and delete the index from it (to simulate what happens when a bucket is moved)
        Map.Entry<Region,List<Index>> firstIndexEntry = index.getFirstBucketIndex();
        assertTrue(!firstIndexEntry.getValue().isEmpty());
        index.removeFromBucketIndexes(firstIndexEntry.getKey(), firstIndexEntry.getValue().iterator().next());
        
        // Verify the index was removed from the entry and the entry was removed from the bucket indexes
        assertTrue(firstIndexEntry.getValue().isEmpty());      
        Map.Entry<Region,List<Index>> nextFirstIndexEntry = index.getFirstBucketIndex();
        assertTrue(!nextFirstIndexEntry.getValue().isEmpty());
        
        // Run query again
        observer.reset();
        query.execute();
        
        // Verify index was still used
        assertTrue(observer.isIndexesUsed);
      }

  class QueryObserverImpl extends QueryObserverAdapter
  {
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
    
    public void reset() {
      this.isIndexesUsed = false;
      this.indexesUsed.clear();
    }
  }
  
  public class RangeIndexTestHook implements TestHook {

    int lastHook = -1;
    @Override
    public void hook(int spot) throws RuntimeException {
      lastHook = spot;
      CacheUtils.getCache().getLogger().fine("Inside RangeIndexTestHook for spot "+ spot);
      if (spot == 1) { //LT size estimate
        CacheUtils.getCache().getRegion("testRgn").clear();
      } else if (spot == 2) { //GT size estimate
        CacheUtils.getCache().getRegion("testRgn").clear();
      }
    }

    public boolean isHooked(int spot) {
      return spot==lastHook;
    }
  }
  static  class MapKeyIndexData {
    int id;
    public Map maap = new HashMap();
    public List liist = new ArrayList();
    public MapKeyIndexData(int id) {
      this.id = id;
    }
    public void addKeyValue(Object key, Object value) {
      this.maap.put(key,value);
      this.liist.add(value);
    }
  }
  private class TestObject {
      public Map testFields = new HashMap();
      TestObject(int i) {
        testFields.put("string", "String Value" + i);
        testFields.put("double", (double) i);
        testFields.put("integer", i);
        testFields.put("long", (long) i);
        testFields.put("complex", new CompObject(i));
      }
      public Map getTestFields() {
        return testFields;
      }
    }
    
    private class CompObject implements Comparable {
      int value;
      CompObject(int i) {
        value = i;
      }
      
      
      public int compareTo(Object o) {
        if (o instanceof CompObject) {
          CompObject other = (CompObject) o;
          if (value > other.value) {
            return 1;
          }
          else if (value == other.value){
            return 0;
          }
          else {
            return -1;
          }
        }
        throw new ClassCastException("Could not cast " + o.getClass().getName() + " to compObject");
      }
      
    }

}
