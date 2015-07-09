/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * IteratorTypeDefJUnitTest.java
 *
 * Created on April 7, 2005, 12:40 PM
 */
/*
 * 
 * @author vikramj
 */
package com.gemstone.gemfire.cache.query.functional;

import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Utils;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class IteratorTypeDefJUnitTest {

  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
    Region region = CacheUtils.createRegion("portfolios", Portfolio.class);
    for (int i = 0; i < 4; i++) {
      region.put("" + i, new Portfolio(i));
    }
    CacheUtils.log(region);
  }

  @After
  public void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }

  @Test
  public void testIteratorDefSyntax() throws Exception {
    String queries[] = {
        "IMPORT com.gemstone.gemfire.cache.\"query\".data.Position;"
            + "SELECT DISTINCT secId FROM /portfolios,  positions.values pos TYPE Position WHERE iD > 0",
        "IMPORT com.gemstone.gemfire.cache.\"query\".data.Position;"
            + "SELECT DISTINCT secId FROM /portfolios, positions.values AS pos TYPE Position WHERE iD > 0",
        "IMPORT com.gemstone.gemfire.cache.\"query\".data.Position;"
            + "SELECT DISTINCT pos.secId FROM /portfolios, pos IN positions.values TYPE Position WHERE iD > 0",
        "SELECT DISTINCT pos.secId FROM /portfolios,  positions.values AS pos  WHERE iD > 0",
        "SELECT DISTINCT pos.secId FROM /portfolios, pos IN positions.values  WHERE iD > 0",};
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        Object r = q.execute();
        CacheUtils.log(Utils.printResult(r));
      }
      catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    CacheUtils.log("TestCase:testIteratorDefSyntax PASS");
  }
  
 @Test
  public void testIteratorDefSyntaxForObtainingResultBag() throws Exception {
    String queries[] = {
     "IMPORT com.gemstone.gemfire.cache.\"query\".data.Position;"+ 
"SELECT DISTINCT secId FROM /portfolios, (set<Position>)positions.values WHERE iD > 0",
    };
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        Object r = q.execute();
        CacheUtils.log(Utils.printResult(r));
        if (!(r instanceof SelectResults))
            fail("testIteratorDefSyntaxForObtainingResultBag: Test failed as obtained Result Data not an instance of SelectResults. Query= "+ q.getQueryString());
        if (((SelectResults)r).getCollectionType().allowsDuplicates()) 
            fail("testIteratorDefSyntaxForObtainingResultBag: results of query should not allow duplicates, but says it does");
      }
      catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    CacheUtils.log("TestCase:testIteratorDefSyntaxForObtainingResultSet PASS");
  }
  
  
  @Test
  public void testNOValueconstraintInCreatRegion() throws Exception {
      CacheUtils.createRegion("pos", null);  
      String queries[] = {
        "IMPORT com.gemstone.gemfire.cache.\"query\".data.Portfolio;"+ 
"SELECT DISTINCT * FROM (set<Portfolio>)/pos where iD > 0"
    };
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        Object r = q.execute();
        CacheUtils.log(Utils.printResult(r));
     }
      catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    CacheUtils.log("TestCase: testNOValueconstraintInCreatRegion PASS");
  } 
  
  @Test
  public void testNOConstraintOnRegion() throws Exception {
      Region region = CacheUtils.createRegion("portfl",null); 
      for (int i = 0; i < 4; i++) {
      region.put("" + i, new Portfolio(i));
    }
    CacheUtils.log(region);
      String queries[] = {
"IMPORT com.gemstone.gemfire.cache.\"query\".data.Position;"+ 
"IMPORT com.gemstone.gemfire.cache.\"query\".data.Portfolio;"+
"SELECT DISTINCT secId FROM (set<Portfolio>)/portfl, (set<Position>)positions.values WHERE iD > 0",
    };
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        Object r = q.execute();
        CacheUtils.log(Utils.printResult(r));
     }
      catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    CacheUtils.log("TestCase: testNOConstraintOnRegion PASS");
  } 
  
}
 
