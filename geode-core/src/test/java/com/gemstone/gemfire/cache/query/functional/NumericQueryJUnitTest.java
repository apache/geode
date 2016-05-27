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
package com.gemstone.gemfire.cache.query.functional;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.data.Numbers;
import com.gemstone.gemfire.cache.query.internal.QueryObserverAdapter;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * 
 *
 */
@Category(IntegrationTest.class)
public class NumericQueryJUnitTest {

  private static String testRegionName = "testRegion";
  private static Region testRegion;
  private static int numElem = 100;

  
  private static String EQ = "=";
  private static String LT = "<";
  private static String GT = ">";
  private static String GTE = ">=";
  private static String LTE = "<=";
  

  @Before
  public void setUp() throws Exception {
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "Query.VERBOSE", "true");
    CacheUtils.startCache();
  }

  @After
  public void tearDown() throws Exception {
    // Destroy current Region for other tests
    if (testRegion != null) {
      testRegion.destroyRegion();
    }
    CacheUtils.closeCache();
  }

  private String[] getQueriesOnRegion(String regionName, String field, String op) {
    return new String[] {
        "select * from /" + regionName + " r where " + field + op + " 50",
        "select * from /" + regionName + " r where " + field + op + " 50.0",
        "select * from /" + regionName + " r where " + field + op + " 50.0f",
        "select * from /" + regionName + " r where " + field + op + " 50.0d",
        "select * from /" + regionName + " r where " + field + op + " 50L",
      };
  }
 
  //This test is to determine if using a map with an in clause will correctly
  //compare mismatching types
  @Test
  public void testNumericsWithInClauseWithMap() throws Exception {
    Cache cache = CacheUtils.getCache();
    testRegion = createLocalRegion(testRegionName);
    
    Map<String, Object> map = new HashMap<String, Object>(); 
    map.put("bigdecimal", BigDecimal.valueOf(1234.5678D)); 
    map.put("string", "stringValue"); 
    map.put("integer", 777); 
    map.put("long", Long.valueOf(1000));
    map.put("biginteger", BigInteger.valueOf(1000));
    map.put("double", Double.valueOf(1000.0));
    map.put("short", Short.valueOf((short)1000));
    map.put("float", Float.valueOf(1000.0f));
    
    testRegion.put("1", map);

    QueryService qs = CacheUtils.getQueryService();
    //big decimal test
    SelectResults selectResults = helpTestFunctionalIndexForQuery("select * from /testRegion tr where tr['bigdecimal'] in set (1234.5678)", "tr['bigdecimal']", "/testRegion tr");
    assertEquals(1, selectResults.size());
    
    //integer test
    selectResults = helpTestFunctionalIndexForQuery("select * from /testRegion tr where tr['integer'] in set (777.0)", "tr['integer']", "/testRegion tr");
    assertEquals(1, selectResults.size());
    
    //long test
    selectResults = helpTestFunctionalIndexForQuery("select * from /testRegion tr where tr['long'] in set (1000.0)", "tr['long']", "/testRegion tr");
    assertEquals(1, selectResults.size());
    
    //big integer test
    selectResults = helpTestFunctionalIndexForQuery("select * from /testRegion tr where tr['biginteger'] in set (1000.0)", "tr['biginteger']", "/testRegion tr");
    assertEquals(1, selectResults.size());
    
    //double test
    selectResults = helpTestFunctionalIndexForQuery("select * from /testRegion tr where tr['double'] in set (1000)", "tr['double']", "/testRegion tr");
    assertEquals(1, selectResults.size());
    
    //short test
    selectResults = helpTestFunctionalIndexForQuery("select * from /testRegion tr where tr['short'] in set (1000.0)", "tr['short']", "/testRegion tr");
    assertEquals(1, selectResults.size());
    
    //float test
    selectResults = helpTestFunctionalIndexForQuery("select * from /testRegion tr where tr['float'] in set (1000)", "tr['float']", "/testRegion tr");
    assertEquals(1, selectResults.size());
    
  }
  
  
  //All queries are compared against themselves as well as the non indexed results

  /**Queries compared against a stored Float**/
  /**
   * Test on Local Region data against an indexed Float object and non indexed Float Object
   */
  @Test
  public void testQueriesOnLocalRegionWithIndexOnFloat() throws Exception {
    Cache cache = CacheUtils.getCache();
    testRegion = createLocalRegion(testRegionName);
    populateRegion(testRegion);
    assertNotNull(cache.getRegion(testRegionName));
    assertEquals(numElem * 2, cache.getRegion(testRegionName).size());
    String regionPath =  "/" + testRegionName  + " r";
    executeQueryTest(getQueriesOnRegion(testRegionName, "r.max1", EQ), "r.max1", regionPath);
    executeQueryTest(getQueriesOnRegion(testRegionName, "r.max1", LT), "r.max1", regionPath);
    executeQueryTest(getQueriesOnRegion(testRegionName, "r.max1", GT), "r.max1", regionPath);
    executeQueryTest(getQueriesOnRegion(testRegionName, "r.max1", GTE), "r.max1", regionPath);
    executeQueryTest(getQueriesOnRegion(testRegionName, "r.max1", LTE), "r.max1", regionPath);
  }
  
  /**
   * Test on Replicated Region data against an indexed Float object and non indexed Float Object
   */
  @Test
  public void testQueriesOnReplicatedRegion() throws Exception {
    Cache cache = CacheUtils.getCache();
    testRegion = createReplicatedRegion(testRegionName);
    populateRegion(testRegion);
    assertNotNull(cache.getRegion(testRegionName));
    assertEquals(numElem * 2, cache.getRegion(testRegionName).size());
    String regionPath =  "/" + testRegionName  + " r";
    executeQueryTest(getQueriesOnRegion(testRegionName, "r.max1", EQ), "r.max1", regionPath);
    executeQueryTest(getQueriesOnRegion(testRegionName, "r.max1", LT), "r.max1", regionPath);
    executeQueryTest(getQueriesOnRegion(testRegionName, "r.max1", GT), "r.max1", regionPath);
    executeQueryTest(getQueriesOnRegion(testRegionName, "r.max1", GTE), "r.max1", regionPath);
    executeQueryTest(getQueriesOnRegion(testRegionName, "r.max1", LTE), "r.max1", regionPath);
 }

  /**
   * Test on Partitioned Region data against an indexed Float object and non indexed Float Object
   */
  @Test
  public void testQueriesOnPartitionedRegion() throws Exception {
    Cache cache = CacheUtils.getCache();
    testRegion = createPartitionedRegion(testRegionName);
    populateRegion(testRegion);
    assertNotNull(cache.getRegion(testRegionName));
    assertEquals(numElem * 2, cache.getRegion(testRegionName).size());
    String regionPath =  "/" + testRegionName  + " r";
    executeQueryTest(getQueriesOnRegion(testRegionName, "r.max1", EQ), "r.max1", regionPath);
    executeQueryTest(getQueriesOnRegion(testRegionName, "r.max1", LT), "r.max1", regionPath);
    executeQueryTest(getQueriesOnRegion(testRegionName, "r.max1", GT), "r.max1", regionPath);
    executeQueryTest(getQueriesOnRegion(testRegionName, "r.max1", GTE), "r.max1", regionPath);
    executeQueryTest(getQueriesOnRegion(testRegionName, "r.max1", LTE), "r.max1", regionPath);
 }
  
  /**Queries compared against a stored Int**/
  /** 
   * Test on Local Region data against an indexed Int object and non indexed Int Object
   */
  @Test
  public void testQueriesOnLocalRegionWithIndexOnInt() throws Exception {
    Cache cache = CacheUtils.getCache();
    testRegion = createLocalRegion(testRegionName);
    populateRegion(testRegion);
    assertNotNull(cache.getRegion(testRegionName));
    assertEquals(numElem * 2, cache.getRegion(testRegionName).size());
    String regionPath =  "/" + testRegionName  + " r";
    executeQueryTest(getQueriesOnRegion(testRegionName, "r.id", EQ), "r.id", regionPath);
    executeQueryTest(getQueriesOnRegion(testRegionName, "r.id", LT), "r.id", regionPath);
    executeQueryTest(getQueriesOnRegion(testRegionName, "r.id", GT), "r.id", regionPath);
    executeQueryTest(getQueriesOnRegion(testRegionName, "r.id", GTE), "r.id", regionPath);
    executeQueryTest(getQueriesOnRegion(testRegionName, "r.id", LTE), "r.id", regionPath);
 }
  
  /**
   * Test on Replicated Region data against an indexed Int object and non indexed Int Object
   */
  @Test
  public void testQueriesOnReplicatedRegionWithIndexOnInt() throws Exception {
    Cache cache = CacheUtils.getCache();
    testRegion = createReplicatedRegion(testRegionName);
    populateRegion(testRegion);
    assertNotNull(cache.getRegion(testRegionName));
    assertEquals(numElem * 2, cache.getRegion(testRegionName).size());
    String regionPath =  "/" + testRegionName  + " r";
    executeQueryTest(getQueriesOnRegion(testRegionName, "r.id", EQ), "r.id", regionPath);
    executeQueryTest(getQueriesOnRegion(testRegionName, "r.id", LT), "r.id", regionPath);
    executeQueryTest(getQueriesOnRegion(testRegionName, "r.id", GT), "r.id", regionPath);
    executeQueryTest(getQueriesOnRegion(testRegionName, "r.id", GTE), "r.id", regionPath);
    executeQueryTest(getQueriesOnRegion(testRegionName, "r.id", LTE), "r.id", regionPath);
 }

  /**
   * Test on Partitioned Region data against an indexed Int object and non indexed Int Object
   */
  @Test
  public void testQueriesOnPartitionedRegionWithIndexOnInt() throws Exception {
    Cache cache = CacheUtils.getCache();
    testRegion = createPartitionedRegion(testRegionName);
    populateRegion(testRegion);
    assertNotNull(cache.getRegion(testRegionName));
    assertEquals(numElem * 2, cache.getRegion(testRegionName).size());
    String regionPath =  "/" + testRegionName  + " r";
    executeQueryTest(getQueriesOnRegion(testRegionName, "r.id", EQ), "r.id", regionPath);
    executeQueryTest(getQueriesOnRegion(testRegionName, "r.id", LT), "r.id", regionPath);
    executeQueryTest(getQueriesOnRegion(testRegionName, "r.id", GT), "r.id", regionPath);
    executeQueryTest(getQueriesOnRegion(testRegionName, "r.id", GTE), "r.id", regionPath);
    executeQueryTest(getQueriesOnRegion(testRegionName, "r.id", LTE), "r.id", regionPath);
 }

  /******** Region Creation Helper Methods *********/
  private Region createLocalRegion(String regionName) throws ParseException {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setDataPolicy(DataPolicy.NORMAL);
    RegionAttributes regionAttributes = attributesFactory.create();
    return cache.createRegion(regionName, regionAttributes);
  }

  private Region createReplicatedRegion(String regionName) throws ParseException {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes regionAttributes = attributesFactory.create();
    return cache.createRegion(regionName, regionAttributes);
  }

  private Region createPartitionedRegion(String regionName) throws ParseException {
    Cache cache = CacheUtils.getCache();
    PartitionAttributesFactory prAttFactory = new PartitionAttributesFactory();
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setPartitionAttributes(prAttFactory.create());
    RegionAttributes regionAttributes = attributesFactory.create();
    return cache.createRegion(regionName, regionAttributes);
  }

  private void populateRegion(Region region) throws Exception {
    for (int i = 1; i <= numElem; i++) {
      putData(i, region);
    }
  }
  
  //creates a Numbers object and puts it into the specified region
  private void putData(int id, Region region) throws ParseException {
    Numbers obj = new Numbers(id);
    region.put(id, obj);
    region.put(id + numElem, obj);
  }
  
  /**** Query Execution Helpers ****/
  
  private void executeQueryTest(String[] queries, String indexedExpression, String regionPath) throws Exception {
    ArrayList list = new ArrayList(queries.length);
    for (String query:queries) {
      helpTestFunctionalIndexForQuery(query, indexedExpression, regionPath);
    }
  }
  
  /*
   * helper method to test against a functional index
   * @param query
   * @throws Exception
   */
  private SelectResults helpTestFunctionalIndexForQuery(String query, String indexedExpression, String regionPath) throws Exception {
     MyQueryObserverAdapter observer = new MyQueryObserverAdapter();
    QueryObserverHolder.setInstance(observer);
    
    QueryService qs = CacheUtils.getQueryService();
    SelectResults nonIndexedResults = (SelectResults)qs.newQuery(query).execute();
    assertFalse(observer.indexUsed);

    Index index = qs.createIndex("testIndex", indexedExpression, regionPath);
    SelectResults indexedResults = (SelectResults)qs.newQuery(query).execute();
    assertEquals(nonIndexedResults.size(), indexedResults.size());
    assertTrue(observer.indexUsed);
    qs.removeIndex(index);
    return indexedResults;
  }
  
  
  class MyQueryObserverAdapter extends QueryObserverAdapter {
    public boolean indexUsed = false;
    
    public void afterIndexLookup(Collection results){
      super.afterIndexLookup(results);
      indexUsed = true;
    }
  }
}
