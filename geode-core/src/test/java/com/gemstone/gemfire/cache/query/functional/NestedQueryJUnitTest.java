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
 * NestedQueryJUnitTest.java
 * JUnit based test
 *
 * Created on March 28, 2005, 11:35 AM
 */
package com.gemstone.gemfire.cache.query.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import junit.framework.AssertionFailedError;

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
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Utils;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.internal.QueryObserverAdapter;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.cache.query.internal.StructImpl;
import com.gemstone.gemfire.cache.query.types.ObjectType;
//import com.gemstone.gemfire.internal.util.DebuggerSupport;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 *
 */
@Category(IntegrationTest.class)
public class NestedQueryJUnitTest {
  ObjectType resType1=null;
  ObjectType resType2= null;

  int resSize1=0;
  int resSize2=0;

  Iterator itert1=null;
  Iterator itert2=null;

  Set set1=null;
  Set set2=null;

  String s1;
  String s2;


  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
    Region r = CacheUtils.createRegion("Portfolios", Portfolio.class);
    for(int i=0;i<4;i++)
      r.put(i+"", new Portfolio(i));
  }

  @After
  public void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }

  public void atestQueries() throws Exception{
    String queryString;
    Query query;
    Object result;
    //Executes successfully
    queryString= "SELECT DISTINCT * FROM /Portfolios WHERE NOT(SELECT DISTINCT * FROM positions.values p WHERE p.secId = 'IBM').isEmpty";
    query = CacheUtils.getQueryService().newQuery(queryString);
    result = query.execute();
    CacheUtils.log(Utils.printResult(result));
    //Fails
    queryString= "SELECT DISTINCT * FROM /Portfolios where status = ELEMENT(SELECT DISTINCT * FROM /Portfolios p where p.ID = 0).status";
    query = CacheUtils.getQueryService().newQuery(queryString);
    result = query.execute();
    CacheUtils.log(Utils.printResult(result));
    //Executes successfully
    queryString= "SELECT DISTINCT * FROM /Portfolios x where status = ELEMENT(SELECT DISTINCT * FROM /Portfolios p where x.ID = p.ID).status";
    query = CacheUtils.getQueryService().newQuery(queryString);
    result = query.execute();
    CacheUtils.log(Utils.printResult(result));
    //Fails
    queryString= "SELECT DISTINCT * FROM /Portfolios x where status = ELEMENT(SELECT DISTINCT * FROM /Portfolios p where p.ID = x.ID).status";
    query = CacheUtils.getQueryService().newQuery(queryString);
    result = query.execute();
    CacheUtils.log(Utils.printResult(result));

    queryString= "SELECT DISTINCT * FROM /Portfolios x where status = ELEMENT(SELECT DISTINCT * FROM /Portfolios p where p.ID = 0).status";
    query = CacheUtils.getQueryService().newQuery(queryString);
    result = query.execute();
    CacheUtils.log(Utils.printResult(result));

  }
  @Test
  public void testQueries() throws Exception {
    String queries[]={
        "SELECT DISTINCT * FROM /Portfolios WHERE NOT(SELECT DISTINCT * FROM positions.values p WHERE p.secId = 'IBM').isEmpty",
        "SELECT DISTINCT * FROM /Portfolios where NOT(SELECT DISTINCT * FROM /Portfolios p where p.ID = 0).isEmpty",
        "SELECT DISTINCT * FROM /Portfolios where status = ELEMENT(SELECT DISTINCT * FROM /Portfolios p where ID = 0).status",
        "SELECT DISTINCT * FROM /Portfolios where status = ELEMENT(SELECT DISTINCT * FROM /Portfolios p where p.ID = 0).status",
        "SELECT DISTINCT * FROM /Portfolios x where status = ELEMENT(SELECT DISTINCT * FROM /Portfolios p where x.ID = p.ID).status",
        "SELECT DISTINCT * FROM /Portfolios x where status = ELEMENT(SELECT DISTINCT * FROM /Portfolios p where p.ID = x.ID).status",
        "SELECT DISTINCT * FROM /Portfolios x where status = ELEMENT(SELECT DISTINCT * FROM /Portfolios p where p.ID = 0).status"
    };
    for(int i=0;i<queries.length;i++){
      try{
        Query query = CacheUtils.getQueryService().newQuery(queries[i]);
        query.execute();
        //CacheUtils.log(Utils.printResult(result));
        CacheUtils.log("OK "+queries[i]);
      }catch(Exception e){
        CacheUtils.log("FAILED "+queries[i]);
        CacheUtils.log(e.getMessage());
        //e.printStackTrace();
      }
    }
  }
  @Test
  public void testNestedQueriesEvaluation() throws Exception {


    QueryService qs;
    qs = CacheUtils.getQueryService();
    String queries[] = {
        "SELECT DISTINCT * FROM /Portfolios where NOT(SELECT DISTINCT * FROM /Portfolios p where p.ID = 0).isEmpty",
        // NQIU 1: PASS       
        // "SELECT DISTINCT * FROM /Portfolios where status = ELEMENT(SELECT DISTINCT * FROM /Portfolios p where ID = 0).status",
        // NQIU 2 : Failed: 16 May'05
        "SELECT DISTINCT * FROM /Portfolios x where status = ELEMENT(SELECT DISTINCT * FROM /Portfolios p where p.ID = x.ID).status",
        //NQIU 3:PASS
        "SELECT DISTINCT * FROM /Portfolios x where status = ELEMENT(SELECT DISTINCT * FROM /Portfolios p where p.ID = 0).status",
        //NQIU 4:PASS
        "SELECT DISTINCT * FROM /Portfolios WHERE NOT(SELECT DISTINCT * FROM positions.values p WHERE p.secId = 'IBM').isEmpty",
        // NQIU 5: PASS
        "SELECT DISTINCT * FROM /Portfolios x where status = ELEMENT(SELECT DISTINCT * FROM /Portfolios p where x.ID = p.ID).status",
        // NQIU 6: PASS                
    };
    SelectResults r[][]= new SelectResults[queries.length][2];

    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);

        r[i][0] =(SelectResults) q.execute();
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        if(!observer.isIndexesUsed){
          CacheUtils.log("NO INDEX USED");
        }
        CacheUtils.log(Utils.printResult(r));
        resType1 =(r[i][0]).getCollectionType().getElementType();
        resSize1 =((r[i][0]).size());
        CacheUtils.log("Result Type= "+resType1);
        CacheUtils.log("Result Size= "+resSize1);
        set1=((r[i][0]).asSet());
        // Iterator iter=set1.iterator();

      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }

    //  Create an Index on status and execute the same query again.

    qs = CacheUtils.getQueryService();
    qs.createIndex("IDIndex", IndexType.FUNCTIONAL,"ID","/Portfolios pf");
    qs.createIndex("secIdIndex", IndexType.FUNCTIONAL,"b.secId","/Portfolios pf, pf.positions.values b");
    qs.createIndex("r1Index", IndexType.FUNCTIONAL, "secId","/Portfolios.values['0'].positions.values");

    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        QueryObserverImpl observer2 = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer2);
        r[i][1] = (SelectResults) q.execute();

        if(observer2.isIndexesUsed ==true){
          CacheUtils.log("YES INDEX IS USED!");
        }else if(i != 3){
          fail ("Index not used");
        }
        CacheUtils.log(Utils.printResult(r[i][1]));
        resType2 =(r[i][1]).getCollectionType().getElementType();
        resSize2 =((r[i][1]).size());
        //                CacheUtils.log("Result Type= "+resType2);
        //                CacheUtils.log("Result Size= "+resSize2);
        set2=((r[i][1]).asSet());

      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    for(int j=0;j<=1;j++){
      // Increase the value of j as you go on adding the Queries in queries[]
      if (((r[j][0]).getCollectionType().getElementType()).equals((r[j][1]).getCollectionType().getElementType())){
        CacheUtils.log("Both Search Results are of the same Type i.e.--> "+(r[j][0]).getCollectionType().getElementType());
      }else {
        fail("FAILED:Search result Type is different in both the cases");
      }
      if ((r[j][0]).size()==(r[j][1]).size()){
        CacheUtils.log("Both Search Results are of Same Size i.e.  Size= "+(r[j][1]).size());
      }else {
        fail("FAILED:Search result Type is different in both the cases");
      }
    }
    itert2 = set2.iterator();
    itert1 = set1.iterator();
    while (itert1.hasNext()){
      Portfolio p1= (Portfolio)itert1.next();
      Portfolio p2 = (Portfolio)itert2.next();
      if(!set1.contains(p2) || !set2.contains(p1))
        fail("FAILED: In both the Cases the members of ResultsSet are different.");
    }
    CacheUtils.compareResultsOfWithAndWithoutIndex(r, this);

  }

  @Test
  public void testNestedQueriesResultsasStructSet() throws Exception {

    QueryService qs;
    qs = CacheUtils.getQueryService();
    String queries[] = {

        "SELECT DISTINCT * FROM"
            + " (SELECT DISTINCT * FROM /Portfolios ptf, positions pos)"
            + " WHERE pos.value.secId = 'IBM'",
            "SELECT DISTINCT * FROM"
                + " (SELECT DISTINCT * FROM /Portfolios AS ptf, positions AS pos)"
                + " WHERE pos.value.secId = 'IBM'",
                "SELECT DISTINCT * FROM"
                    + " (SELECT DISTINCT * FROM ptf IN /Portfolios, pos IN positions)"
                    + " WHERE pos.value.secId = 'IBM'",
                    "SELECT DISTINCT * FROM"
                        + " (SELECT DISTINCT pos AS myPos FROM /Portfolios ptf, positions pos)"
                        + " WHERE myPos.value.secId = 'IBM'",
                        "SELECT DISTINCT * FROM"
                            + " (SELECT DISTINCT * FROM /Portfolios ptf, positions pos) p"
                            + " WHERE p.pos.value.secId = 'IBM'",
                            "SELECT DISTINCT * FROM"
                                + " (SELECT DISTINCT * FROM /Portfolios ptf, positions pos) p"
                                + " WHERE pos.value.secId = 'IBM'",
                                "SELECT DISTINCT * FROM"
                                    + " (SELECT DISTINCT * FROM /Portfolios, positions) p"
                                    + " WHERE p.positions.value.secId = 'IBM'",
                                    "SELECT DISTINCT * FROM"
                                        + " (SELECT DISTINCT * FROM /Portfolios, positions)"
                                        + " WHERE positions.value.secId = 'IBM'",
                                        "SELECT DISTINCT * FROM"
                                            + " (SELECT DISTINCT * FROM /Portfolios ptf, positions pos) p"
                                            + " WHERE p.get('pos').value.secId = 'IBM'",
                                            // NQIU 7: PASS
    };
    SelectResults r[][]= new SelectResults[queries.length][2];


    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        //                DebuggerSupport.waitForJavaDebugger(CacheUtils.getLogger());
        r[i][0] = (SelectResults)q.execute();
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        if(!observer.isIndexesUsed){
          CacheUtils.log("NO INDEX USED");
        }
        CacheUtils.log(Utils.printResult(r[i][0]));
        resType1 =(r[i][0]).getCollectionType().getElementType();
        resSize1 =((r[i][0]).size());
        CacheUtils.log("Result Type= "+resType1);
        CacheUtils.log("Result Size= "+resSize1);
        set1=((r[i][0]).asSet());
        // Iterator iter=set1.iterator();

      } catch (Exception e) {
        AssertionFailedError afe = new AssertionFailedError(q.getQueryString());
        afe.initCause(e);
        throw afe;
      }
    }

    //  Create an Index on status and execute the same query again.

    qs = CacheUtils.getQueryService();
    qs.createIndex("secIdIndex", IndexType.FUNCTIONAL,"b.secId","/Portfolios pf, pf.positions.values b");
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        r[i][1] = (SelectResults)q.execute();

        QueryObserverImpl observer2 = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer2);
        if(observer2.isIndexesUsed == true){
          CacheUtils.log("YES INDEX IS USED!");
        }
        CacheUtils.log(Utils.printResult(r[i][1]));
        resType2 =(r[i][1]).getCollectionType().getElementType();
        resSize2 =((r[i][1]).size());
        set2=((r[i][1]).asSet());

      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    for(int j=0;j<queries.length;j++){

      if (((r[j][0]).getCollectionType().getElementType()).equals((r[j][1]).getCollectionType().getElementType())){
        CacheUtils.log("Both Search Results are of the same Type i.e.--> "+(r[j][0]).getCollectionType().getElementType());
      }else {
        fail("FAILED:Search result Type is different in both the cases");
      }
      if ((r[j][0]).size()==(r[j][1]).size()){
        CacheUtils.log("Both Search Results are of Same Size i.e.  Size= "+(r[j][1]).size());
      }else {
        fail("FAILED:Search result Type is different in both the cases");
      }
    }
    boolean pass = true;
    itert1 = set1.iterator();
    while (itert1.hasNext()){
      StructImpl p1= (StructImpl)itert1.next();
      itert2 = set2.iterator();
      boolean found = false;
      while(itert2.hasNext()){
        StructImpl p2= (StructImpl)itert2.next();
        if((p1).equals(p2)) {
          found = true;
        }
      }
      if(!found)
        pass = false;
    }
    if(!pass)
      fail("Test failed");

    CacheUtils.compareResultsOfWithAndWithoutIndex(r, this);
  }

  @Test
  public void testNestedQueryWithProjectionDoesNotReturnUndefinedForBug45131() throws Exception {
    QueryService qs = CacheUtils.getQueryService();
    Region region1 = CacheUtils.createRegion("portfolios1", Portfolio.class);
    Region region2 = CacheUtils.createRegion("portfolios2", Portfolio.class);
    for (int i = 0; i <= 1000; i++) {
      Portfolio p = new Portfolio(i);
      p.createTime = (long)(i);
      region1.put(i, p);
      region2.put(i, p);
    }

    Index p1IdIndex = qs.createIndex("P1IDIndex", IndexType.FUNCTIONAL, "P.ID", "/portfolios1 P");
    Index p2IdIndex = qs.createIndex("P2IDIndex", IndexType.FUNCTIONAL, "P2.ID", "/portfolios2 P2");
    Index createTimeIndex = qs.createIndex("createTimeIndex", IndexType.FUNCTIONAL, "P.createTime", "/portfolios1 P");

    SelectResults results = (SelectResults) qs.newQuery("SELECT P2.ID FROM /portfolios2 P2 where P2.ID in (SELECT P.ID from /portfolios1 P where P.createTime >= 500L and P.createTime < 1000L)").execute();
    for (Object o:results) {
      assertNotSame(o, QueryService.UNDEFINED);
    }
  }

  @Test
  public void testExecCacheForNestedQueries() throws Exception {
    QueryService qs = CacheUtils.getQueryService();
    Region region1 = CacheUtils.createRegion("portfolios1", Portfolio.class);
    Region region2 = CacheUtils.createRegion("portfolios2", Portfolio.class);
    for (int i = 0; i <= 1000; i++) {
      Portfolio p = new Portfolio(i);
      p.createTime = (long) (i);
      region1.put(i, p);
      region2.put(i, p);
    }

    Index p1IdIndex = qs.createIndex("P1IDIndex", IndexType.FUNCTIONAL, "P.ID",
        "/portfolios1 P");
    Index p2IdIndex = qs.createIndex("P2IDIndex", IndexType.FUNCTIONAL,
        "P2.ID", "/portfolios2 P2");
    Index createTimeIndex = qs.createIndex("createTimeIndex",
        IndexType.FUNCTIONAL, "P.createTime", "/portfolios1 P");

    String rangeQueryString = "SELECT P.ID FROM /portfolios1 P WHERE P.createTime >= 500L AND P.createTime < 1000L";
    // Retrieve location ids that are within range
    String multiInnerQueryString = "SELECT P FROM /portfolios1 P WHERE P.ID IN(SELECT P2.ID FROM /portfolios2 P2 where P2.ID in ($1)) and P.createTime >=500L and P.createTime < 1000L";

    Query rangeQuery = qs.newQuery(rangeQueryString);
    SelectResults rangeResults = (SelectResults) rangeQuery.execute();

    Query multiInnerQuery = qs.newQuery(multiInnerQueryString);
    Object[] params = new Object[1];
    params[0] = rangeResults;

    SelectResults results = (SelectResults) multiInnerQuery.execute(params);
    // By now we would have hit the ClassCastException, instead we just check
    // size and make sure we got results.
    assertEquals(500, results.size());
  }

  /**
   * Tests a nested query with shorts converted to integer types in the result 
   * set of the inner query.  The short field in the outer query should be 
   * evaluated against the integer types and match.
   * @throws Exception
   */
  @Test
  public void testNestedQueryWithShortTypesFromInnerQuery() throws Exception {
    QueryService qs = CacheUtils.getQueryService();
    Region region1 = CacheUtils.createRegion("portfolios1", Portfolio.class);
    int numEntries = 1000;
    int numIds = 100;
    for (int i = 0; i < numEntries; i++) {
      Portfolio p = new Portfolio(i % (numIds));
      p.shortID = (short)p.ID;
      region1.put("" + i, p);
    }

    helpTestIndexForQuery("<trace>SELECT * FROM /portfolios1 p where p.shortID in (SELECT p.shortID FROM /portfolios1 p WHERE p.shortID = 1)", "p.shortID", "/portfolios1 p");
  }

  /**
   * Tests a nested query that has duplicate results in the inner query
   * Results should not be duplicated in the final result set
   * 
   * @throws Exception
   */
  @Test
  public void testNestedQueryWithMultipleMatchingResultsWithIn() throws Exception {
    QueryService qs = CacheUtils.getQueryService();
    Region region1 = CacheUtils.createRegion("portfolios1", Portfolio.class);
    int numEntries = 1000;
    int numIds = 100;
    for (int i = 0; i < numEntries; i++) {
      Portfolio p = new Portfolio(i % (numIds));
      p.shortID = (short)p.ID;
      region1.put("" + i, p);
    }

    helpTestIndexForQuery("<trace>SELECT * FROM /portfolios1 p where p.ID in (SELECT p.ID FROM /portfolios1 p WHERE p.ID = 1)", "p.ID", "/portfolios1 p");
  }

  /*
   * helper method to test against a compact range index
   * @param query
   * @throws Exception
   */
  private void helpTestIndexForQuery(String query, String indexedExpression, String regionPath) throws Exception {
    QueryService qs = CacheUtils.getQueryService();
    QueryObserverImpl observer = new QueryObserverImpl();
    QueryObserverHolder.setInstance(observer);
    SelectResults nonIndexedResults = (SelectResults)qs.newQuery(query).execute();
    assertFalse(observer.isIndexesUsed);

    qs.createIndex("newIndex", indexedExpression, regionPath);
    SelectResults indexedResults = (SelectResults)qs.newQuery(query).execute();
    assertEquals(nonIndexedResults.size(), indexedResults.size());
    assertTrue(observer.isIndexesUsed);
  }

  class QueryObserverImpl extends QueryObserverAdapter{
    boolean isIndexesUsed = false;
    ArrayList indexesUsed = new ArrayList();

    public void beforeIndexLookup(Index index, int oper, Object key) {
      indexesUsed.add(index.getName());
    }

    public void afterIndexLookup(Collection results) {
      if(results != null){
        isIndexesUsed = true;
      }
    }
  }
}

