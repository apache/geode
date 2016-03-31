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
 * BugJUnitTest.java
 * JUnit based test
 *
 * Created on April 13, 2005, 2:40 PM
 */
package com.gemstone.gemfire.cache.query;

import static com.gemstone.gemfire.cache.query.data.TestData.createAndPopulateSet;
import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.*;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.data.Position;
import com.gemstone.gemfire.cache.query.internal.DefaultQuery;
import com.gemstone.gemfire.cache.query.internal.QueryObserverAdapter;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.cache.query.internal.QueryUtils;
import com.gemstone.gemfire.internal.cache.LocalDataSet;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import com.gemstone.gemfire.cache.query.data.TestData.MyValue;

/**
 * Tests reported bugs
 */
@Category(IntegrationTest.class)
public class BugJUnitTest {
  Region region;
  Region region1;
  QueryService qs;
  Cache cache;
  
  public BugJUnitTest() {
  }
  
  @Before
  public void setUp() throws Exception {
    CacheUtils.startCache();
    cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    // leave the region untyped
//    attributesFactory.setValueConstraint(Portfolio.class);
    RegionAttributes regionAttributes = attributesFactory.create();
    
    region = cache.createRegion("pos",regionAttributes);
    region1 = cache.createRegion("pos1",regionAttributes);
    for (int i = 0; i < 4; i++) {
      Portfolio p = new Portfolio(i);
      region.put("" + i, p);
      region1.put("" + i, p);
    }    
    qs = cache.getQueryService();
  }
  
  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
  }
  
  @Test
  public void testBug41509() throws Exception {
    // Create Index.
    try {
      this.qs.createIndex("pos1_secIdIndex", IndexType.FUNCTIONAL,
        "p1.position1.secId","/pos1 p1");
      this.qs.createIndex("pos1_IdIndex", IndexType.FUNCTIONAL,
        "p1.position1.Id","/pos1 p1");
      this.qs.createIndex("pos_IdIndex", IndexType.FUNCTIONAL,
        "p.position1.Id", "/pos p");
    } catch (Exception ex) {
      fail("Failed to create Index. " + ex);
    }
    // Execute Query.
    try {
       String queryStr = "select distinct * from /pos p, /pos1 p1 where " +
       		"p.position1.Id = p1.position1.Id and p1.position1.secId in set('MSFT')";
      Query q = qs.newQuery(queryStr);
      CacheUtils.getLogger().fine("Executing:" + queryStr);
      q.execute();
    } catch (Exception ex) {
      fail("Query should have executed successfully. " + ex);
    }
  }
  
  /* Bug: 32429 An issue with nested queries :Iterator for the region in inner
   * query is not getting resolved.
   */
  @Test
  public void testBug32429() throws Exception {
    String[] queries = new String[] {
      "SELECT DISTINCT * FROM /pos where NOT(SELECT DISTINCT * FROM /pos p where p.ID = 0).isEmpty",
      "-- AMBIGUOUS\n" +
          "import com.gemstone.gemfire.cache.\"query\".data.Portfolio; " +
          "SELECT DISTINCT * FROM /pos TYPE Portfolio where status = ELEMENT(SELECT DISTINCT * FROM /pos p TYPE Portfolio where ID = 0).status",
      "SELECT DISTINCT * FROM /pos where status = ELEMENT(SELECT DISTINCT * FROM /pos p where p.ID = 0).status",
      "SELECT DISTINCT * FROM /pos x where status = ELEMENT(SELECT DISTINCT * FROM /pos p where p.ID = x.ID).status",
      "SELECT DISTINCT * FROM /pos x where status = ELEMENT(SELECT DISTINCT * FROM /pos p where p.ID = 0).status",
    };
    
    for (int i = 0; i < queries.length; i++) {
      Object r = null;
      String queryStr = queries[i];
      Query q = qs.newQuery(queryStr);
      CacheUtils.getLogger().fine("Executing:" + queryStr);
      try {
        r = q.execute();
        if (i == 1) fail("should have thrown an AmbiguousNameException");
      }
      catch (AmbiguousNameException e) {
        if (i != 1) throw e; // if it's 1 then pass 
      }
      if (r != null) CacheUtils.getLogger().fine(Utils.printResult(r));
    }
  }
  
  /* Bug 32375 Issue with Nested Query
   */
  @Test
  public void testBug32375() throws Exception {
    String queryStr;
    Query q;
    Object r;
    
    queryStr = "import com.gemstone.gemfire.cache.\"query\".data.Portfolio; " +
            "select distinct * from /pos, (select distinct * from /pos p TYPE Portfolio, p.positions where value!=null)";
    q = qs.newQuery(queryStr);
//    DebuggerSupport.waitForJavaDebugger(cache.getLogger());
    CacheUtils.getLogger().fine(queryStr);
    r = q.execute();
    CacheUtils.getLogger().fine(Utils.printResult(r));
  }

	 
  
  @Test
  public void testBug32251() throws QueryException {
    String queryStr;
    Query q;
    Object r;
    
    // partial fix for Bug 32251 was checked in so that if there is only
    // one untyped iterator, we assume names will associate with it.
    
    // the following used to fail due to inability to determine type of a dependent
    // iterator def, but now succeeds because there is only one untyped iterator
    queryStr = "Select distinct ID from /pos";
    q = qs.newQuery(queryStr);
//    DebuggerSupport.waitForJavaDebugger(cache.getLogger());
    CacheUtils.getLogger().fine(queryStr);
    r = q.execute();
    CacheUtils.getLogger().fine(Utils.printResult(r));
    Set expectedSet = createAndPopulateSet(4);
    assertEquals(expectedSet, ((SelectResults)r).asSet());
    
    // the following queries still fail because there is more than one
    // untyped iterator:    
    queryStr = "Select distinct value.secId from /pos , positions";
    q = qs.newQuery(queryStr);
    try {
      r = q.execute();
      fail("Expected a TypeMismatchException due to bug 32251");
      CacheUtils.getLogger().fine(queryStr);
      CacheUtils.getLogger().fine(Utils.printResult(r));
    }
    catch (TypeMismatchException e) {
      // expected due to bug 32251
    }
    
    queryStr = "Select distinct value.secId from /pos , getPositions(23)";
    q = qs.newQuery(queryStr);
    try {
      r = q.execute();
      fail("Expected a TypeMismatchException due to bug 32251");
      CacheUtils.getLogger().fine(queryStr);
      CacheUtils.getLogger().fine(Utils.printResult(r));
    }
    catch (TypeMismatchException e) {
      // expected due to bug 32251
    }
    
    queryStr = "Select distinct value.secId from /pos , getPositions($1)";
    q = qs.newQuery(queryStr);
    try {
      r = q.execute(new Object[] { new Integer(23) });
      fail("Expected a TypeMismatchException due to bug 32251");
      CacheUtils.getLogger().fine(queryStr);
      CacheUtils.getLogger().fine(Utils.printResult(r));
    }
    catch (TypeMismatchException e) {
      // expected due to bug 32251
    }

    // the following queries, however, should work:
//    DebuggerSupport.waitForJavaDebugger(cache.getLogger());
    queryStr = "Select distinct e.value.secId from /pos, getPositions(23) e";
    q = qs.newQuery(queryStr);
    r = q.execute();
    CacheUtils.getLogger().fine(queryStr);
    CacheUtils.getLogger().fine(Utils.printResult(r));
     
    queryStr = "import com.gemstone.gemfire.cache.\"query\".data.Position;" +
      "select distinct value.secId from /pos, (map<string, Position>)getPositions(23)";
    q = qs.newQuery(queryStr);
//    DebuggerSupport.waitForJavaDebugger(cache.getLogger());
    r = q.execute();
    CacheUtils.getLogger().fine(queryStr);
    CacheUtils.getLogger().fine(Utils.printResult(r));
    
    queryStr = "import java.util.Map$Entry as Entry;" +
      "select distinct value.secId from /pos, getPositions(23) type Entry";
//    DebuggerSupport.waitForJavaDebugger(cache.getLogger());
    q = qs.newQuery(queryStr);
    r = q.execute();
    CacheUtils.getLogger().fine(queryStr);
    CacheUtils.getLogger().fine(Utils.printResult(r));

  }
  
  @Test
  public void testBug32624() throws Exception {
    this.qs.createIndex("iIndex", IndexType.FUNCTIONAL, "e.value.status","/pos.entries e");
//    DebuggerSupport.waitForJavaDebugger(CacheUtils.getLogger());
    this.region.put("0",new Portfolio(0));
  }
  
  /**
   * This bug was occuring in simulation of Outer Join query for Schwab
   */
  @Test
  public void testBugResultMismatch() {
    try{
    Region region = CacheUtils.createRegion("portfolios", Portfolio.class);
    QueryService qs = CacheUtils.getQueryService();
    region.put("0", new Portfolio(0));
    region.put("1", new Portfolio(1));
    region.put("2", new Portfolio(2));
    region.put("3", new Portfolio(3));
    qs.createIndex("index1", IndexType.FUNCTIONAL, "status", "/portfolios pf");
    String query1 = "SELECT   DISTINCT iD as portfolio_id, pos.secId as sec_id from /portfolios p , p.positions.values pos  where p.status= 'active'";
    String query2= "select  DISTINCT * from  "
      +"( SELECT   DISTINCT iD as portfolio_id, pos.secId as sec_id from /portfolios p , p.positions.values pos where p.status= 'active')";
  
    Query q1 = CacheUtils.getQueryService().newQuery(query1 );
    Query q2 =CacheUtils.getQueryService().newQuery(query2 );
    SelectResults rs1 = (SelectResults) q1.execute();
    SelectResults rs2 = (SelectResults) q2.execute();
   
    SelectResults results = QueryUtils.union(rs1, rs2, null);
    }catch(Exception e) {
      e.printStackTrace();
      fail("Test failed due to exception= "+e);
    }   

  }
  @Test
  public void testBug36659() throws Exception
  {
    // Task ID: NQIU 9
    CacheUtils.getQueryService();
    String queries = "select distinct p.x from (select distinct x, pos from /pos x, x.positions.values pos) p, (select distinct * from /pos/positions rtPos where rtPos.secId = p.pos.secId)";
    Region r = CacheUtils.getRegion("/pos");
    Region r1 = r.createSubregion("positions", new AttributesFactory()
        .createRegionAttributes());

   
    r1.put("1", new Position("SUN", 2.272));
    r1.put("2", new Position("IBM", 2.272));
    r1.put("3", new Position("YHOO", 2.272));
    r1.put("4", new Position("GOOG", 2.272));
    r1.put("5", new Position("MSFT", 2.272));
    Query q = null;
    try {
      q = CacheUtils.getQueryService().newQuery(queries);
      CacheUtils.getLogger().info("Executing query: " + queries);
      //                DebuggerSupport.waitForJavaDebugger(CacheUtils.getLogger());
      SelectResults rs = (SelectResults)q.execute();
      CacheUtils.log(Utils.printResult(rs));
      assertTrue("Resultset size should be > 0",rs.size()>0 );
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }
  
 /**
   * Tests the Bug 38422 where the index results intersection results in
   * incorrect size
   */
  @Test
  public void testBug38422() {
    try {
      QueryService qs;
      qs = CacheUtils.getQueryService();
      Region rgn = CacheUtils.getRegion("/pos");
      //The region already contains 3 Portfolio object. The 4th Portfolio object
      // has its status field explictly made null.  Thus the query below will result
      // in intersection of two non empty sets. One which contains 4th Portfolio 
      //object containing null status & the second condition does not contain the
      // 4th portfolio object
      Portfolio pf = new Portfolio(4);
      pf.status = null;
      rgn.put(new Integer(4), pf);
      String queryStr = "select  * from /pos pf where pf.status != 'active' and pf.status != null";

      SelectResults r[][] = new SelectResults[1][2];
      Query qry = qs.newQuery(queryStr);
      SelectResults sr = null;
      sr = (SelectResults)qry.execute();
      r[0][0] = sr;
      qs.createIndex("statusIndx", IndexType.FUNCTIONAL, "pf.status",
              "/pos pf");
      sr = null;
      sr = (SelectResults)qry.execute();
      r[0][1] = sr;
      CacheUtils.compareResultsOfWithAndWithoutIndex(r,  this);
      assertEquals(2, (r[0][0]).size());
      assertEquals(2, (r[0][1]).size());

    }
    catch (Exception e) {
      e.printStackTrace();
      fail("Test failed due to exception =" + e.toString());
    }
  }
  
  @Test
  public void testEquijoinPRColocatedQuery_1() throws Exception
  {

    AttributesFactory factory = new AttributesFactory();
    factory.setPartitionAttributes(new PartitionAttributesFactory()
        .setRedundantCopies(1).setTotalNumBuckets(40).setPartitionResolver(
            new PartitionResolver() {

              public String getName()
              {
                return "blah";
              }

              public Serializable getRoutingObject(EntryOperation opDetails)
              {
                return (Serializable)opDetails.getKey();
              }

              public void close()
              {

              }

            }).create());
    PartitionedRegion pr1 = (PartitionedRegion)CacheUtils.getCache()
        .createRegion("pr1", factory.create());
    factory = new AttributesFactory();
    factory.setPartitionAttributes(new PartitionAttributesFactory()

    .setRedundantCopies(1).setTotalNumBuckets(40).setPartitionResolver(
        new PartitionResolver() {

          public String getName()
          {
            return "blah";
          }

          public Serializable getRoutingObject(EntryOperation opDetails)
          {
            return (Serializable)opDetails.getKey();
          }

          public void close()
          {

          }

        }).setColocatedWith(pr1.getName()).create());

    final PartitionedRegion pr2 = (PartitionedRegion)CacheUtils.getCache()
        .createRegion("pr2", factory.create());

    createAllNumPRAndEvenNumPR(pr1, pr2, 80);
    Set<Integer> set = createAndPopulateSet(15);
    LocalDataSet lds = new LocalDataSet(pr1, set);

    QueryObserverImpl observer = new QueryObserverImpl();
    QueryObserverHolder.setInstance(observer);
    QueryService qs = pr1.getCache().getQueryService();

    qs.createIndex("valueIndex", IndexType.FUNCTIONAL, "e.value", "/pr1 e");
    qs.createIndex("valueIndex", IndexType.FUNCTIONAL, "e.value", "/pr2 e");
    String query = "select distinct e1.value from /pr1 e1, " + "/pr2  e2"
        + " where e1.value=e2.value";
    DefaultQuery cury = (DefaultQuery)CacheUtils.getQueryService().newQuery(
        query);
    SelectResults r = (SelectResults)lds.executeQuery(cury, null, set);

    if (!observer.isIndexesUsed) {
      fail("Indexes should have been used");
    }

  }
  @Test
  public void testEquijoinPRColocatedQuery_2() throws Exception
  {

    AttributesFactory factory = new AttributesFactory();
    factory.setPartitionAttributes(new PartitionAttributesFactory()
        .setRedundantCopies(1).setTotalNumBuckets(40).setPartitionResolver(
            new PartitionResolver() {

              public String getName()
              {
                return "blah";
              }

              public Serializable getRoutingObject(EntryOperation opDetails)
              {
                return (Serializable)opDetails.getKey();
              }

              public void close()
              {

              }

            }).create());
    PartitionedRegion pr1 = (PartitionedRegion)CacheUtils.getCache()
        .createRegion("pr1", factory.create());
    factory = new AttributesFactory();
    factory.setPartitionAttributes(new PartitionAttributesFactory()

    .setRedundantCopies(1).setTotalNumBuckets(40).setPartitionResolver(
        new PartitionResolver() {

          public String getName()
          {
            return "blah";
          }

          public Serializable getRoutingObject(EntryOperation opDetails)
          {
            return (Serializable)opDetails.getKey();
          }

          public void close()
          {

          }

        }).setColocatedWith(pr1.getName()).create());

    final PartitionedRegion pr2 = (PartitionedRegion)CacheUtils.getCache()
        .createRegion("pr2", factory.create());

    createAllNumPRAndEvenNumPR(pr1, pr2, 80);
    Set<Integer> set = createAndPopulateSet(15);
    LocalDataSet lds = new LocalDataSet(pr1, set);

    QueryObserverImpl observer = new QueryObserverImpl();
    QueryObserverHolder.setInstance(observer);
    QueryService qs = pr1.getCache().getQueryService();

    qs.createIndex("valueIndex", IndexType.FUNCTIONAL, "e.value", "/pr1.entries e");
    qs.createIndex("valueIndex", IndexType.FUNCTIONAL, "e.value", "/pr2.entries e");
    String query = "select distinct e1.key from /pr1.entries e1,/pr2.entries  e2"
        + " where e1.value=e2.value";
    DefaultQuery cury = (DefaultQuery)CacheUtils.getQueryService().newQuery(
        query);
    SelectResults r = (SelectResults)lds.executeQuery(cury, null, set);

    if (!observer.isIndexesUsed) {
      fail("Indexes should have been used");
    }

  }

  private void createAllNumPRAndEvenNumPR(final PartitionedRegion pr1, final PartitionedRegion pr2, final int range) {
    IntStream.rangeClosed(1,range).forEach(i -> {
      pr1.put(i,new MyValue(i));
      if(i % 2 == 0){
        pr2.put(i, new MyValue(i));
      }
    });
  }


  class QueryObserverImpl extends QueryObserverAdapter
  {
    boolean isIndexesUsed = false;

    ArrayList indexesUsed = new ArrayList();

    String IndexName;

    public void beforeIndexLookup(Index index, int oper, Object key)
    {
      
      IndexName = index.getName();
      indexesUsed.add(index.getName());
    }

    public void afterIndexLookup(Collection results)
    {     
      if (results != null) {
        isIndexesUsed = true;
      }
    }
  }


}
