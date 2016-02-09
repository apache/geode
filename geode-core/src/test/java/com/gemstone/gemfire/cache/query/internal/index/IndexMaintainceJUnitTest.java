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
 * IndexMaintainceJUnitTest.java
 *
 * Created on February 24, 2005, 5:47 PM
 */
package com.gemstone.gemfire.cache.query.internal.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexExistsException;
import com.gemstone.gemfire.cache.query.IndexNameConflictException;
import com.gemstone.gemfire.cache.query.IndexStatistics;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.RegionNotFoundException;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Utils;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.functional.StructSetOrResultsSet;
import com.gemstone.gemfire.cache.query.internal.QueryObserverAdapter;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.test.dunit.ThreadUtils;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * 
 * @author vaibhav
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Category(IntegrationTest.class)
public class IndexMaintainceJUnitTest {

  @Before
  public void setUp() throws Exception {
    if (!isInitDone) {
      init();
    }
  }

  @After
  public void tearDown() throws Exception {
  }

  static QueryService qs;
  static boolean isInitDone = false;
  static Region region;
  static IndexProtocol index;
  protected boolean indexUsed = false;

  private static void init() {
    try {
      Cache cache = CacheUtils.getCache();
      region = CacheUtils.createRegion("portfolio", Portfolio.class);
      region.put("0", new Portfolio(0));
      region.put("1", new Portfolio(1));
      region.put("2", new Portfolio(2));
      region.put("3", new Portfolio(3));
      qs = cache.getQueryService();
      index = (IndexProtocol) qs.createIndex("statusIndex",
          IndexType.FUNCTIONAL, "status", "/portfolio");
      assertTrue(index instanceof CompactRangeIndex);
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    isInitDone = true;
  }

  @Test
  public void test000BUG32452() throws IndexNameConflictException,
      IndexExistsException, RegionNotFoundException {
    Index i1 = qs.createIndex("tIndex", IndexType.FUNCTIONAL, "vals.secId",
        "/portfolio pf, pf.positions.values vals");
    Index i2 = qs.createIndex("dIndex", IndexType.FUNCTIONAL,
        "pf.getCW(pf.ID)", "/portfolio pf");
    Index i3 = qs.createIndex("fIndex", IndexType.FUNCTIONAL, "sIter",
        "/portfolio pf, pf.collectionHolderMap[(pf.ID).toString()].arr sIter");
    Index i4 = qs.createIndex("cIndex", IndexType.FUNCTIONAL,
        "pf.collectionHolderMap[(pf.ID).toString()].arr[pf.ID]",
        "/portfolio pf");
    Index i5 = qs.createIndex("inIndex", IndexType.FUNCTIONAL, "kIter.secId",
        "/portfolio['0'].positions.values kIter");
    Index i6 = qs.createIndex("sIndex", IndexType.FUNCTIONAL, "pos.secId",
        "/portfolio.values val, val.positions.values pos");
    Index i7 = qs.createIndex("p1Index", IndexType.PRIMARY_KEY, "pkid",
        "/portfolio pf");
    Index i8 = qs.createIndex("p2Index", IndexType.PRIMARY_KEY, "pk",
        "/portfolio pf");
    if (!i1.getCanonicalizedFromClause().equals(
        "/portfolio index_iter1, index_iter1.positions.values index_iter2")
        || !i1.getCanonicalizedIndexedExpression().equals("index_iter2.secId")
        || !i1.getFromClause()
            .equals("/portfolio pf, pf.positions.values vals")
        || !i1.getIndexedExpression().equals("vals.secId")) {
      fail("Mismatch found among fromClauses or IndexedExpressions");
    }
    if (!i2.getCanonicalizedFromClause().equals("/portfolio index_iter1")
        || !i2.getCanonicalizedIndexedExpression().equals(
            "index_iter1.getCW(index_iter1.ID)")
        || !i2.getFromClause().equals("/portfolio pf")
        || !i2.getIndexedExpression().equals("pf.getCW(pf.ID)")) {
      fail("Mismatch found among fromClauses or IndexedExpressions");
    }
    if (!i3
        .getCanonicalizedFromClause()
        .equals(
            "/portfolio index_iter1, index_iter1.collectionHolderMap[index_iter1.ID.toString()].arr index_iter3")
        || !i3.getCanonicalizedIndexedExpression().equals("index_iter3")
        || !i3
            .getFromClause()
            .equals(
                "/portfolio pf, pf.collectionHolderMap[(pf.ID).toString()].arr sIter")
        || !i3.getIndexedExpression().equals("sIter")) {
      fail("Mismatch found among fromClauses or IndexedExpressions");
    }
    if (!i4.getCanonicalizedFromClause().equals("/portfolio index_iter1")
        || !i4.getCanonicalizedIndexedExpression().equals(
            "index_iter1.collectionHolderMap[index_iter1.ID.toString()].arr[index_iter1.ID]")
        || !i4.getFromClause().equals("/portfolio pf")
        || !i4.getIndexedExpression().equals(
            "pf.collectionHolderMap[(pf.ID).toString()].arr[pf.ID]")) {
      fail("Mismatch found among fromClauses or IndexedExpressions");
    }
    if (!i5.getCanonicalizedFromClause().equals(
        "/portfolio['0'].positions.values index_iter4")
        || !i5.getCanonicalizedIndexedExpression().equals("index_iter4.secId")
        || !i5.getFromClause().equals("/portfolio['0'].positions.values kIter")
        || !i5.getIndexedExpression().equals("kIter.secId")) {
      fail("Mismatch found among fromClauses or IndexedExpressions");
    }
    if (!i6.getCanonicalizedFromClause().equals(
        "/portfolio.values index_iter5, index_iter5.positions.values index_iter6")
        || !i6.getCanonicalizedIndexedExpression().equals("index_iter6.secId")
        || !i6.getFromClause().equals(
            "/portfolio.values val, val.positions.values pos")
        || !i6.getIndexedExpression().equals("pos.secId")) {
      fail("Mismatch found among fromClauses or IndexedExpressions");
    }
    if (!i7.getCanonicalizedFromClause().equals("/portfolio index_iter1")
        || !i7.getCanonicalizedIndexedExpression().equals("index_iter1.pkid")
        || !i7.getFromClause().equals("/portfolio pf")
        || !i7.getIndexedExpression().equals("pkid")) {
      fail("Mismatch found among fromClauses or IndexedExpressions");
    }
    if (!i8.getCanonicalizedFromClause().equals("/portfolio index_iter1")
        || !i8.getCanonicalizedIndexedExpression().equals("index_iter1.pk")
        || !i8.getFromClause().equals("/portfolio pf")
        || !i8.getIndexedExpression().equals("pk")) {
      fail("Mismatch found among fromClauses or IndexedExpressions");
    }
    qs.removeIndex(i1);
    qs.removeIndex(i2);
    qs.removeIndex(i3);
    qs.removeIndex(i4);
    qs.removeIndex(i5);
    qs.removeIndex(i6);
    qs.removeIndex(i7);
    qs.removeIndex(i8);
    Index i9 = qs.createIndex("p3Index", IndexType.PRIMARY_KEY, "getPk",
        "/portfolio pf");
    if (!i9.getCanonicalizedFromClause().equals("/portfolio index_iter1")
        || !i9.getCanonicalizedIndexedExpression().equals("index_iter1.pk")
        || !i9.getFromClause().equals("/portfolio pf")
        || !i9.getIndexedExpression().equals("getPk")) {
      fail("Mismatch found among fromClauses or IndexedExpressions");
    }
    qs.removeIndex(i9);
  }

  @Test
  public void test001AddEntry() throws Exception {
    CacheUtils.log(((CompactRangeIndex) index).dump());
    IndexStatistics stats = index.getStatistics();
    assertEquals(4, stats.getNumberOfValues());
    //    com.gemstone.gemfire.internal.util.
    //    DebuggerSupport.waitForJavaDebugger(region.getCache().getLogger());
    region.put("4", new Portfolio(4));
    CacheUtils.log(((CompactRangeIndex) index).dump());
    stats = index.getStatistics();
    assertEquals(5, stats.getNumberOfValues());
    //Set results = new HashSet();
    //index.query("active", OQLLexerTokenTypes.TOK_EQ, results, new ExecutionContext(null, CacheUtils.getCache()));
    SelectResults results = region.query("status = 'active'");
    CacheUtils.log(Utils.printResult(results));
    assertEquals(3, results.size());
  }

  // !!!:ezoerner:20081030 disabled because modifying an object in place
  // and then putting it back into the cache breaks a CompactRangeIndex.
  // @todo file a ticket on this issue
  public void _test002UpdateEntry() throws Exception {
    IndexStatistics stats = index.getStatistics();
    CacheUtils.log(((CompactRangeIndex) index).dump());
    Portfolio p = (Portfolio) region.get("4");
    p.status = "inactive";
    region.put("4", p);
    assertEquals(5, stats.getNumberOfValues());
    //Set results = new HashSet();
    //index.query("active", OQLLexerTokenTypes.TOK_EQ, results,new ExecutionContext(null, CacheUtils.getCache()));
    SelectResults results = region.query("status = 'active'");
    assertEquals(2, results.size());
  }

  @Test
  public void test003InvalidateEntry() throws Exception {
    IndexStatistics stats = index.getStatistics();
    region.invalidate("4");
    assertEquals(4, stats.getNumberOfValues());
    //Set results = new HashSet();
    //index.query("active", OQLLexerTokenTypes.TOK_EQ, results,new ExecutionContext(null, CacheUtils.getCache()));
    SelectResults results = region.query("status = 'active'");
    assertEquals(2, results.size());
  }

  @Test
  public void test004DestroyEntry() throws Exception {
    IndexStatistics stats = index.getStatistics();
    region.put("4", new Portfolio(4));
    region.destroy("4");
    assertEquals(4, stats.getNumberOfValues());
    //Set results = new HashSet();
    //index.query("active", OQLLexerTokenTypes.TOK_EQ, results,new ExecutionContext(null, CacheUtils.getCache()));
    SelectResults results = region.query("status = 'active'");
    assertEquals(2, results.size());
  }

  //This test has a meaning only for Trunk code as it checks for Map implementation
  //Asif : Tests for Region clear operations on Index in a Local VM
  @Test
  public void test005IndexClearanceOnMapClear() {
    try {
      CacheUtils.restartCache();
      IndexMaintainceJUnitTest.isInitDone = false;
      init();
      Query q = qs
          .newQuery("SELECT DISTINCT * FROM /portfolio where status = 'active'");
      QueryObserverHolder.setInstance(new QueryObserverAdapter() {

        public void afterIndexLookup(Collection coll) {
          IndexMaintainceJUnitTest.this.indexUsed = true;
        }
      });
      SelectResults set = (SelectResults) q.execute();
      if (set.size() == 0 || !this.indexUsed) {
        fail("Either Size of the result set is zero or Index is not used ");
      }
      this.indexUsed = false;
      
      region.clear();
      set = (SelectResults) q.execute();
      if (set.size() != 0 || !this.indexUsed) {
        fail("Either Size of the result set is not zero or Index is not used ");
      }
    }
    catch (Exception e) {
      e.printStackTrace();
      fail(e.toString());
    }
    finally {
      IndexMaintainceJUnitTest.isInitDone = false;
      CacheUtils.restartCache();
    }
  }

  //Asif : Tests for Region clear operations on Index in a Local VM for cases
  // when a clear
  //operation & region put operation occur concurrentlty
  @Test
  public void test006ConcurrentMapClearAndRegionPutOperation() {
    try {
      CacheUtils.restartCache();
      IndexMaintainceJUnitTest.isInitDone = false;
      init();
      Query q = qs
          .newQuery("SELECT DISTINCT * FROM /portfolio where status = 'active'");
      QueryObserverHolder.setInstance(new QueryObserverAdapter() {

        public void afterIndexLookup(Collection coll) {
          IndexMaintainceJUnitTest.this.indexUsed = true;
        }

        public void beforeRerunningIndexCreationQuery() {
          //Spawn a separate thread here which does a put opertion on region
          Thread th = new Thread(new Runnable() {

            public void run() {
              //Assert that the size of region is now 0
              assertTrue(IndexMaintainceJUnitTest.region.size() == 0);
              IndexMaintainceJUnitTest.region.put("" + 8, new Portfolio(8));
            }
          });
          th.start();
          ThreadUtils.join(th, 30 * 1000);
          assertTrue(IndexMaintainceJUnitTest.region.size() == 1);
        }
      });
      SelectResults set = (SelectResults) q.execute();
      if (set.size() == 0 || !this.indexUsed) {
        fail("Either Size of the result set is zero or Index is not used ");
      }
      this.indexUsed = false;
      region.clear();
      set = (SelectResults) q.execute();
      if (set.size() != 1 || !this.indexUsed) {
        fail("Either Size of the result set is not one or Index is not used ");
      }
    }
    catch (Exception e) {
      e.printStackTrace();
      fail(e.toString());
    }
    finally {
      IndexMaintainceJUnitTest.isInitDone = false;
      CacheUtils.restartCache();
    }
  }
  
  @Test
  public void test007IndexUpdate() {
    try{
    CacheUtils.restartCache();
    IndexMaintainceJUnitTest.isInitDone = false;
    init();
    qs.removeIndexes();
  
    index = (IndexProtocol) qs.createIndex("statusIndex",
        IndexType.FUNCTIONAL, "pos.secId", "/portfolio p , p.positions.values pos");
    String queryStr = "Select distinct pf from /portfolio pf , pf.positions.values ps where ps.secId='SUN'";
    
    Query query =  qs.newQuery(queryStr);
    SelectResults rs =(SelectResults) query.execute();
    int size1 = rs.size();
    for(int i=4;i<50;++i) {
	    region.put(""+i, new Portfolio(i));
    }
    rs =(SelectResults) query.execute();
    int size2 = rs.size();
    assertTrue(size2>size1);
   }catch(Exception e) {
     e.printStackTrace();
     fail("Test failed due to exception="+e);
   }finally {
     IndexMaintainceJUnitTest.isInitDone = false;
     CacheUtils.restartCache();
   }
    
  }

  
  /**
   * Test to compare range and compact index.
   * They should return the same results.
   */
  @Test
  public void test008RangeAndCompactRangeIndex() {
    try{
      //CacheUtils.restartCache();
      if (!IndexMaintainceJUnitTest.isInitDone){
        init();
      }
      qs.removeIndexes();

      String[] queryStr = new String[] {
          "Select status from /portfolio pf where status='active'",
          "Select pf.ID from /portfolio pf where pf.ID > 2 and pf.ID < 100",       
          "Select * from /portfolio pf where pf.position1.secId > '2'",       
      };

      String[] queryFields = new String[] {
          "status",
          "ID",
          "position1.secId",
      };

      for (int i=0; i < queryStr.length; i++){
        // Clear indexes if any.
        qs.removeIndexes();

        // initialize region.
        region.clear();
        for (int k=0; k < 10; k++) {
          region.put(""+k, new Portfolio(k));
        }

        for (int j=0; j < 1; j++) { // With different region size.
          // Update Region.
          for (int k=0; k < (j * 100); k++) {
            region.put(""+k, new Portfolio(k));
          }

          // Create compact index.
          IndexManager.TEST_RANGEINDEX_ONLY = false;
          index = (IndexProtocol) qs.createIndex(queryFields[i] + "Index",
              IndexType.FUNCTIONAL, queryFields[i], "/portfolio");

          // Execute Query.    
          SelectResults[][] rs = new SelectResults[1][2];
          Query query =  qs.newQuery(queryStr[i]);
          rs[0][0] =(SelectResults) query.execute();

          // remove compact index.
          qs.removeIndexes();

          // Create Range Index.
          IndexManager.TEST_RANGEINDEX_ONLY = true;
          index = (IndexProtocol) qs.createIndex(queryFields[i] + "rIndex",
              IndexType.FUNCTIONAL, queryFields[i], "/portfolio");

          query =  qs.newQuery(queryStr[i]);
          rs[0][1] =(SelectResults) query.execute();

          CacheUtils.log("#### rs1 size is : " + (rs[0][0]).size() + " rs2 size is : " + (rs[0][1]).size());
          StructSetOrResultsSet ssORrs = new  StructSetOrResultsSet();
          ssORrs.CompareQueryResultsWithoutAndWithIndexes(rs, 1,queryStr);
        }
      }
    }catch(Exception e) {
      e.printStackTrace();
      fail("Test failed due to exception="+e);
    }finally {
      IndexManager.TEST_RANGEINDEX_ONLY = false;
      IndexMaintainceJUnitTest.isInitDone = false;
      CacheUtils.restartCache();
    }
  }

  /**
   * Test to compare range and compact index.
   * They should return the same results.
   */
  @Test
  public void test009AcquringCompactRangeIndexEarly() {
    try{
      //CacheUtils.restartCache();
      if (!IndexMaintainceJUnitTest.isInitDone){
        init();
      }
      qs.removeIndexes();

      String[] queryStr = new String[] {
          "Select status from /portfolio pf where status='active'",
          "Select * from /portfolio pf, pf.positions.values pos where pf.ID > 10 and pf.status='active'",
          "Select pf.ID from /portfolio pf where pf.ID > 2 and pf.ID < 100",       
          "Select * from /portfolio pf where pf.position1.secId > '2'",       
          "Select * from /portfolio pf, pf.positions.values pos where pos.secId > '2'",
      };

      // initialize region.
      region.clear();
      for (int k=0; k < 10; k++) {
        region.put(""+k, new Portfolio(k));
      }

      // Create range and compact-range indexes.
      qs.createIndex("id2Index ", IndexType.FUNCTIONAL, "pf.ID", "/portfolio pf");
      qs.createIndex("id2PosIndex ", IndexType.FUNCTIONAL, "pf.ID", "/portfolio pf, pf.positions.values");
      qs.createIndex("status2PosIndex ", IndexType.FUNCTIONAL, "pos.secId", "/portfolio pf, pf.positions.values pos");
      
      // Set the acquire compact range index flag to true
      //IndexManager.TEST_ACQUIRE_COMPACTINDEX_LOCKS_EARLY = true;
      // Update Region.
      for (int k=0; k <  100; k++) {
        region.put(""+k, new Portfolio(k));
      }
      for (int i=0; i < queryStr.length; i++){
        // Execute Query.    
        SelectResults[][] rs = new SelectResults[1][2];
        Query query =  qs.newQuery(queryStr[i]);
        rs[0][0] =(SelectResults) query.execute();
      }

    }catch(Exception e) {
      e.printStackTrace();
      fail("Test failed due to exception="+e);
    }finally {
      //IndexManager.TEST_ACQUIRE_COMPACTINDEX_LOCKS_EARLY = false;
      IndexMaintainceJUnitTest.isInitDone = false;
      CacheUtils.restartCache();
    }
  }
}
