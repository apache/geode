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
/**
 * 
 */
package com.gemstone.gemfire.cache.query.functional;

import java.util.ArrayList;
import java.util.HashMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

import junit.framework.TestCase;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * This test runs {Select COUNT(*) from /regionName [where clause]} queries
 * on different types of regions with and without multiple indexes.
 * 
 * @author shobhit
 *
 */
@Category(IntegrationTest.class)
public class CountStarJUnitTest {

  private static String regionName = "test";
  private static String exampleRegionName = "employee";
  private int numElem = 100;
  
  public CountStarJUnitTest() {
  }

  @Before
  public void setUp() throws Exception {
    System.setProperty("gemfire.Query.VERBOSE", "true");
    CacheUtils.startCache();
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
  }

  private static HashMap<String, Integer> countStarQueries = new HashMap<String, Integer>();


  //EquiJoin Queries
  private static String[] countStarQueriesWithEquiJoins = { "select COUNT(*) from /" + regionName + " p, /"+ exampleRegionName +" e where p.ID = e.ID AND p.ID > 0",
    "select COUNT(*) from /" + regionName + " p, /"+ exampleRegionName +" e where p.ID = e.ID AND p.ID > 20 AND e.ID > 40",
    "select COUNT(*) from /" + regionName + " p, /"+ exampleRegionName +" e where p.ID = e.ID AND p.ID > 0 AND p.status = 'active'",
    "select COUNT(*) from /" + regionName + " p, /"+ exampleRegionName +" e where p.ID = e.ID OR e.status = 'active' "};

  
  //Queries with COUNT(*) to be executed with corresponding result count.
  static {
    countStarQueries.put("select COUNT(*) from /" + regionName , 100);
    countStarQueries.put("select COUNT(*) from /" + regionName + " where ID > 0",100);
    countStarQueries.put("select COUNT(*) from /" + regionName + " where ID < 0", 0);
    countStarQueries.put("select COUNT(*) from /" + regionName + " where ID > 0 LIMIT 50",50);
    countStarQueries.put("select COUNT(*) from /" + regionName + " where ID > 0 AND status='active'", 50);
    countStarQueries.put("select COUNT(*) from /" + regionName + " where ID > 0 OR status='active'", 100);
    countStarQueries.put("select COUNT(*) from /" + regionName + " where ID > 0 AND status LIKE 'act%'", 50);
    countStarQueries.put("select COUNT(*) from /" + regionName + " where ID > 0 OR status LIKE 'ina%'", 100);
    countStarQueries.put("select COUNT(*) from /" + regionName + " where ID IN SET(1, 2, 3, 4, 5)", 5);
    countStarQueries.put("select COUNT(*) from /" + regionName + " where NOT (ID > 5)", 5);
    
    //StructSet queries.
    countStarQueries.put("select COUNT(*) from /" + regionName + " p, p.positions.values pos where p.ID > 0 AND pos.secId = 'IBM'", 15);
    countStarQueries.put("select COUNT(*) from /" + regionName + " p, p.positions.values pos where p.ID > 0 AND pos.secId = 'IBM' LIMIT 5", 5);
    countStarQueries.put("select DISTINCT COUNT(*) from /" + regionName + " p, p.positions.values pos where p.ID > 0 AND pos.secId = 'IBM' ORDER BY p.ID", 15);
    countStarQueries.put("select COUNT(*) from /" + regionName + " p, p.positions.values pos where p.ID > 0 AND p.status = 'active' AND pos.secId = 'IBM'", 8);

    countStarQueries.put("select COUNT(*) from /" + regionName + " p, p.positions.values pos where p.ID > 0 AND p.status = 'active' OR pos.secId = 'IBM'", 107);

    countStarQueries.put("select COUNT(*) from /" + regionName + " p, p.positions.values pos where p.ID > 0 OR p.status = 'active' OR pos.secId = 'IBM'", 200);
    countStarQueries.put("select COUNT(*) from /" + regionName + " p, p.positions.values pos where p.ID > 0 OR p.status = 'active' OR pos.secId = 'IBM' LIMIT 150", 150);
    countStarQueries.put("select DISTINCT COUNT(*) from /" + regionName + " p, p.positions.values pos where p.ID > 0 OR p.status = 'active' OR pos.secId = 'IBM' ORDER BY p.ID", 200);
  }
  
  //Queries without indexes.
  /**
   * Test on Local Region data
   */
  @Test
  public void testCountStartQueriesOnLocalRegion(){
    Cache cache = CacheUtils.getCache();

    createLocalRegion();
    assertNotNull(cache.getRegion(regionName));
    assertEquals(numElem, cache.getRegion(regionName).size());
    
    QueryService queryService = cache.getQueryService();
    Query query1 = null;
    Query query2 = null;
    try {
      for(String queryStr: countStarQueries.keySet()){
        query1 = queryService.newQuery(queryStr);
        query2 = queryService.newQuery(queryStr.replace("COUNT(*)", "*"));
        
        SelectResults result1 = (SelectResults)query1.execute();
        SelectResults result2 = (SelectResults)query2.execute();
        assertEquals(queryStr, 1, result1.size());
        assertTrue(result1.asList().get(0) instanceof Integer);
        
        int count = ((Integer)result1.asList().get(0)).intValue();
        
        //Also verify with size of result2 to count
        assertEquals("COUNT(*) query result is wrong for query: " + queryStr , result2.size(), count);
        
        assertEquals("Query: "+ queryStr, countStarQueries.get(queryStr).intValue(), count);
        
      }
    } catch (Exception e){
      e.printStackTrace();
      fail("Query "+ query1+" Execution Failed!");
    }
  }

  /**
   * Test on Replicated Region data
   */
  @Test
  public void testCountStarQueriesOnReplicatedRegion(){
    Cache cache = CacheUtils.getCache();

    createReplicatedRegion();
    assertNotNull(cache.getRegion(regionName));
    assertEquals(numElem, cache.getRegion(regionName).size());
    
    QueryService queryService = cache.getQueryService();
    Query query1 = null;
    Query query2 = null;
    try {
      for(String queryStr: countStarQueries.keySet()){
        query1 = queryService.newQuery(queryStr);
        query2 = queryService.newQuery(queryStr.replace("COUNT(*)", "*"));
        
        SelectResults result1 = (SelectResults)query1.execute();
        SelectResults result2 = (SelectResults)query2.execute();
        assertEquals(queryStr, 1, result1.size());
        assertTrue(result1.asList().get(0) instanceof Integer);
        
        int count = ((Integer)result1.asList().get(0)).intValue();
        
        //Also verify with size of result2 to count
        assertEquals("COUNT(*) query result is wrong for query: " + queryStr , result2.size(), count);
        
        assertEquals("Query: "+ queryStr, countStarQueries.get(queryStr).intValue(), count);
      }
    } catch (Exception e){
      e.printStackTrace();
      fail("Query "+ query1+" Execution Failed!");
    }
    
    //Destroy current Region for other tests
    cache.getRegion(regionName).destroyRegion();
  }

  /**
   * Test on Partitioned Region data
   */
  @Test
  public void testCountStarQueriesOnPartitionedRegion(){
    Cache cache = CacheUtils.getCache();

    createPartitionedRegion();
    assertNotNull(cache.getRegion(regionName));
    assertEquals(numElem, cache.getRegion(regionName).size());
    
    QueryService queryService = cache.getQueryService();
    Query query1 = null;
    Query query2 = null;
    try {
      for(String queryStr: countStarQueries.keySet()){
        query1 = queryService.newQuery(queryStr);
        query2 = queryService.newQuery(queryStr.replace("COUNT(*)", "*"));
        
        SelectResults result1 = (SelectResults)query1.execute();
        SelectResults result2 = (SelectResults)query2.execute();
        assertEquals(queryStr, 1, result1.size());
        assertTrue(result1.asList().get(0) instanceof Integer);
        
        int count = ((Integer)result1.asList().get(0)).intValue();
        
        //Also verify with size of result2 to count
        assertEquals("COUNT(*) query result is wrong for query: " + queryStr , result2.size(), count);
        
        //assertEquals("Query: "+ queryStr, countStarQueries.get(queryStr).intValue(), count);
      }
    } catch (Exception e){
      e.printStackTrace();
      fail("Query "+ query1+" Execution Failed!");
    }
    
    //Destroy current Region for other tests
    cache.getRegion(regionName).destroyRegion();
  }

  //Test with indexes available on region
  @Test
  public void testCountStartQueriesOnLocalRegionWithIndex(){
    Cache cache = CacheUtils.getCache();

    createLocalRegion();
    assertNotNull(cache.getRegion(regionName));
    assertEquals(numElem, cache.getRegion(regionName).size());
    
    QueryService queryService = cache.getQueryService();
    
    //CReate Index on status and ID
    try {
      queryService.createIndex("sampleIndex-1", IndexType.FUNCTIONAL, "p.ID", "/"+regionName+ " p");
      queryService.createIndex("sampleIndex-2", IndexType.FUNCTIONAL, "p.status", "/"+regionName+ " p");
      queryService.createIndex("sampleIndex-3", IndexType.FUNCTIONAL, "pos.secId", "/"+regionName+" p, p.positions.values pos");
    } catch (Exception e1) {
      fail("Index Creation Failed with message: " + e1.getMessage());
    }
    
    Region region = cache.getRegion(regionName);
    //Verify Index Creation
    assertNotNull(queryService.getIndex(region, "sampleIndex-1"));
    assertNotNull(queryService.getIndex(region, "sampleIndex-2"));
    assertEquals(3, queryService.getIndexes().size());

    //Run queries
    Query query1 = null;
    Query query2 = null;
    try {
      for(String queryStr: countStarQueries.keySet()){
        query1 = queryService.newQuery(queryStr);
        query2 = queryService.newQuery(queryStr.replace("COUNT(*)", "*"));
        
        SelectResults result1 = (SelectResults)query1.execute();
        SelectResults result2 = (SelectResults)query2.execute();
        assertEquals(queryStr, 1, result1.size());
        assertTrue(result1.asList().get(0) instanceof Integer);
        
        int count = ((Integer)result1.asList().get(0)).intValue();
        
        //Also verify with size of result2 to count
        assertEquals("COUNT(*) query result is wrong for query: " + queryStr , result2.size(), count);
        
        //assertEquals("Query: "+ queryStr, countStarQueries.get(queryStr).intValue(), count);
      }
    } catch (Exception e){
      e.printStackTrace();
      fail("Query "+ query1+" Execution Failed!");
    }
    
    //Destroy current Region for other tests
    region.destroyRegion();
  }

  @Test
  public void testCountStarQueriesOnReplicatedRegionWithIndex(){
    Cache cache = CacheUtils.getCache();

    createReplicatedRegion();
    assertNotNull(cache.getRegion(regionName));
    assertEquals(numElem, cache.getRegion(regionName).size());
    
    QueryService queryService = cache.getQueryService();
    //CReate Index on status and ID
    try {
      queryService.createIndex("sampleIndex-1", IndexType.FUNCTIONAL, "p.ID", "/"+regionName+ " p");
      queryService.createIndex("sampleIndex-2", IndexType.FUNCTIONAL, "p.status", "/"+regionName+ " p");
      queryService.createIndex("sampleIndex-3", IndexType.FUNCTIONAL, "pos.secId", "/"+regionName+" p, p.positions.values pos");
    } catch (Exception e1) {
      fail("Index Creation Failed with message: " + e1.getMessage());
    }
    
    Region region = cache.getRegion(regionName);
    //Verify Index Creation
    assertNotNull(queryService.getIndex(region, "sampleIndex-1"));
    assertNotNull(queryService.getIndex(region, "sampleIndex-2"));
    assertEquals(3, queryService.getIndexes().size());


  //Run queries
    Query query1 = null;
    Query query2 = null;
    try {
      for(String queryStr: countStarQueries.keySet()){
        query1 = queryService.newQuery(queryStr);
        query2 = queryService.newQuery(queryStr.replace("COUNT(*)", "*"));
        
        SelectResults result1 = (SelectResults)query1.execute();
        SelectResults result2 = (SelectResults)query2.execute();
        assertEquals(queryStr, 1, result1.size());
        assertTrue(result1.asList().get(0) instanceof Integer);
        
        int count = ((Integer)result1.asList().get(0)).intValue();
        
        //Also verify with size of result2 to count
        assertEquals("COUNT(*) query result is wrong for query: " + queryStr , result2.size(), count);
        
        //assertEquals("Query: "+ queryStr, countStarQueries.get(queryStr).intValue(), count);
      }
    } catch (Exception e){
      e.printStackTrace();
      fail("Query "+ query1+" Execution Failed!");
    }
    
    //Destroy current Region for other tests
    region.destroyRegion();
  }

  @Test
  public void testCountStarQueriesOnPartitionedRegionWithIndex(){
    Cache cache = CacheUtils.getCache();

    createPartitionedRegion();
    assertNotNull(cache.getRegion(regionName));
    assertEquals(numElem, cache.getRegion(regionName).size());
    
    QueryService queryService = cache.getQueryService();
    //CReate Index on status and ID
    
    try {
      queryService.createIndex("sampleIndex-1", IndexType.FUNCTIONAL, "p.ID", "/"+regionName+ " p");
      //queryService.createIndex("sampleIndex-2", IndexType.FUNCTIONAL, "p.status", "/"+regionName+ " p");
      //queryService.createIndex("sampleIndex-3", IndexType.FUNCTIONAL, "pos.secId", "/"+regionName+" p, p.positions.values pos");
    } catch (Exception e1) {
      fail("Index Creation Failed with message: " + e1.getMessage());
    }
    
    
    Region region = cache.getRegion(regionName);
    //Verify Index Creation
    //assertNotNull(queryService.getIndex(region, "sampleIndex-1"));
    //assertNotNull(queryService.getIndex(region, "sampleIndex-2"));
    //assertEquals(3, queryService.getIndexes().size());

    //Run queries
    Query query1 = null;
    Query query2 = null;
    try {
      for(String queryStr: countStarQueries.keySet()){
        query1 = queryService.newQuery(queryStr);
        query2 = queryService.newQuery(queryStr.replace("COUNT(*)", "*"));
        
        SelectResults result1 = (SelectResults)query1.execute();
        SelectResults result2 = (SelectResults)query2.execute();
        assertEquals(queryStr, 1, result1.size());
        assertTrue(result1.asList().get(0) instanceof Integer);
        
        int count = ((Integer)result1.asList().get(0)).intValue();
        
        //Also verify with size of result2 to count
        assertEquals("COUNT(*) query result is wrong for query: " + queryStr , result2.size(), count);
        
        //assertEquals("Query: "+ queryStr, countStarQueries.get(queryStr).intValue(), count);
      }
    } catch (Exception e){
      e.printStackTrace();
      fail("Query "+ query1+" Execution Failed!");
    }
    //Destroy current Region for other tests
    region.destroyRegion();
  }

  @Test
  public void testEquiJoinCountStarQueries(){

    Cache cache = CacheUtils.getCache();

    createLocalRegion();
    createSecondLocalRegion();
    assertNotNull(cache.getRegion(regionName));
    assertNotNull(cache.getRegion(exampleRegionName));
    assertEquals(numElem, cache.getRegion(regionName).size());
    assertEquals(numElem, cache.getRegion(exampleRegionName).size());
    
    QueryService queryService = cache.getQueryService();
    //Run queries
    Query query1 = null;
    Query query2 = null;
    
    // Without Indexes
    try {
      for(String queryStr: countStarQueriesWithEquiJoins){
        query1 = queryService.newQuery(queryStr);
        query2 = queryService.newQuery(queryStr.replace("COUNT(*)", "*"));
        
        SelectResults result1 = (SelectResults)query1.execute();
        SelectResults result2 = (SelectResults)query2.execute();
        assertEquals(queryStr, 1, result1.size());
        assertTrue(result1.asList().get(0) instanceof Integer);
        
        int count = ((Integer)result1.asList().get(0)).intValue();
        
        //Also verify with size of result2 to count
        assertEquals("COUNT(*) query result is wrong for query: " + queryStr , result2.size(), count);
        
        //assertEquals("Query: "+ queryStr, countStarQueries.get(queryStr).intValue(), count);
      }
    } catch (Exception e){
      e.printStackTrace();
      fail("Query "+ query1+" Execution Failed!");
    }
    
    //CReate Index on status and ID
    try {
      queryService.createIndex("sampleIndex-1", IndexType.FUNCTIONAL, "p.ID", "/"+regionName+ " p");
      queryService.createIndex("sampleIndex-2", IndexType.FUNCTIONAL, "p.status", "/"+regionName+ " p");
      queryService.createIndex("sampleIndex-3", IndexType.FUNCTIONAL, "e.ID", "/"+exampleRegionName+ " e");
      queryService.createIndex("sampleIndex-4", IndexType.FUNCTIONAL, "e.status", "/"+exampleRegionName+ " e");
    } catch (Exception e1) {
      fail("Index Creation Failed with message: " + e1.getMessage());
    }
    
    Region region = cache.getRegion(regionName);
    Region region2 = cache.getRegion(exampleRegionName);
    //Verify Index Creation
    assertNotNull(queryService.getIndex(region, "sampleIndex-1"));
    assertNotNull(queryService.getIndex(region, "sampleIndex-2"));
    assertNotNull(queryService.getIndex(region2, "sampleIndex-3"));
    assertNotNull(queryService.getIndex(region2, "sampleIndex-4"));
    assertEquals(4, queryService.getIndexes().size());

    //With Indexes
    try {
      for(String queryStr: countStarQueriesWithEquiJoins){
        query1 = queryService.newQuery(queryStr);
        query2 = queryService.newQuery(queryStr.replace("COUNT(*)", "*"));
        
        SelectResults result1 = (SelectResults)query1.execute();
        SelectResults result2 = (SelectResults)query2.execute();
        assertEquals(queryStr, 1, result1.size());
        assertTrue(result1.asList().get(0) instanceof Integer);
        
        int count = ((Integer)result1.asList().get(0)).intValue();
        
        //Also verify with size of result2 to count
        assertEquals("COUNT(*) query result is wrong for query with indexes: " + queryStr , result2.size(), count);
        
        //assertEquals("Query: "+ queryStr, countStarQueries.get(queryStr).intValue(), count);
      }
    } catch (Exception e){
      e.printStackTrace();
      fail("Query "+ query1+" Execution Failed!");
    }
    
    //Destroy current Region for other tests
    region.destroyRegion();
    region2.destroyRegion();
  }
  
  @Test
  public void testCountStarOnCollection() {
    String collection = "$1";
    HashMap<String, Integer> countStarQueriesWithParms = new HashMap<String, Integer>();
    countStarQueriesWithParms.put("select COUNT(*) from " + collection , 100);
    countStarQueriesWithParms.put("select COUNT(*) from " + collection + " where ID > 0",100);
    countStarQueriesWithParms.put("select COUNT(*) from " + collection + " where ID < 0", 0);
    countStarQueriesWithParms.put("select COUNT(*) from " + collection + " where ID > 0 LIMIT 50",50);
    countStarQueriesWithParms.put("select COUNT(*) from " + collection + " where ID > 0 AND status='active'", 50);
    countStarQueriesWithParms.put("select COUNT(*) from " + collection + " where ID > 0 OR status='active'", 100);
    countStarQueriesWithParms.put("select COUNT(*) from " + collection + " where ID > 0 AND status LIKE 'act%'", 50);
    countStarQueriesWithParms.put("select COUNT(*) from " + collection + " where ID > 0 OR status LIKE 'ina%'", 100);
    countStarQueriesWithParms.put("select COUNT(*) from " + collection + " where ID IN SET(1, 2, 3, 4, 5)", 5);
    countStarQueriesWithParms.put("select COUNT(*) from " + collection + " where NOT (ID > 5)", 5);
    

    Cache cache = CacheUtils.getCache();

    QueryService queryService = cache.getQueryService();
    //Run queries
    Query query1 = null;
    Query query2 = null;
    
    //Create a collection
    ArrayList<Portfolio> portfolios = new ArrayList<Portfolio>(100);
    for (int i=1; i<=100; i++) {
      portfolios.add(new Portfolio(i, i));
    }
    // Without Indexes
    try {
      for(String queryStr: countStarQueriesWithParms.keySet()){
        query1 = queryService.newQuery(queryStr);
        query2 = queryService.newQuery(queryStr.replace("COUNT(*)", "*"));
        
        SelectResults result1 = (SelectResults)query1.execute(new Object[]{portfolios});
        SelectResults result2 = (SelectResults)query2.execute(new Object[]{portfolios});
        assertEquals(queryStr, 1, result1.size());
        assertTrue(result1.asList().get(0) instanceof Integer);
        
        int count = ((Integer)result1.asList().get(0)).intValue();
        
        //Also verify with size of result2 to count
        assertEquals("COUNT(*) query result is wrong for query: " + queryStr , result2.size(), count);
        
        assertEquals("Query: "+ queryStr, countStarQueriesWithParms.get(queryStr).intValue(), count);
      }
    } catch (Exception e){
      e.printStackTrace();
      fail("Query "+ query1+" Execution Failed!");
    }
  }
  
  @Test
  public void testCountStarWithDuplicateValues() {

    Cache cache = CacheUtils.getCache();

    createLocalRegion();
    assertNotNull(cache.getRegion(regionName));
    assertEquals(numElem, cache.getRegion(regionName).size());
    
    HashMap<String, Integer> countStarDistinctQueries = new HashMap<String, Integer>();
    countStarDistinctQueries.put("select distinct COUNT(*) from /" + regionName , 100);
    countStarDistinctQueries.put("select distinct COUNT(*) from /" + regionName + " where ID > 0",100);
    countStarDistinctQueries.put("select distinct COUNT(*) from /" + regionName + " where ID < 0", 0);
    countStarDistinctQueries.put("select distinct COUNT(*) from /" + regionName + " where ID > 0 LIMIT 50",50);
    countStarDistinctQueries.put("select distinct COUNT(*) from /" + regionName + " where ID > 0 AND status='active'", 50);
    countStarDistinctQueries.put("select distinct COUNT(*) from /" + regionName + " where ID > 0 OR status='active'", 100);
    countStarDistinctQueries.put("select distinct COUNT(*) from /" + regionName + " where ID > 0 AND status LIKE 'act%'", 50);
    countStarDistinctQueries.put("select distinct COUNT(*) from /" + regionName + " where ID > 0 OR status LIKE 'ina%'", 100);
    countStarDistinctQueries.put("select distinct COUNT(*) from /" + regionName + " where ID IN SET(1, 2, 3, 4, 5)", 5);
    countStarDistinctQueries.put("select distinct COUNT(*) from /" + regionName + " where NOT (ID > 5)", 5);
    

    QueryService queryService = cache.getQueryService();
    //Run queries
    Query query1 = null;
    Query query2 = null;
    
    //Populate the region.
    Region region = cache.getRegion(regionName);
    for (int i=1; i<=100; i++) {
      region.put(i, new Portfolio(i, i));
    }
    
    //Duplicate values
    for (int i=1; i<=100; i++) {
      region.put(i+100, new Portfolio(i, i));
    }
    // Without Indexes
    try {
      for(String queryStr: countStarDistinctQueries.keySet()){
        query1 = queryService.newQuery(queryStr);
        query2 = queryService.newQuery(queryStr.replace("COUNT(*)", "*"));
        
        SelectResults result1 = (SelectResults)query1.execute();
        SelectResults result2 = (SelectResults)query2.execute();
        assertEquals(queryStr, 1, result1.size());
        assertTrue(result1.asList().get(0) instanceof Integer);
        
        int count = ((Integer)result1.asList().get(0)).intValue();
        
        //Also verify with size of result2 to count
        assertEquals("COUNT(*) query result is wrong for query: " + queryStr , result2.size(), count);
        
        //assertEquals("Query: "+ queryStr, countStarDistinctQueries.get(queryStr).intValue(), count);
      }
    } catch (Exception e){
      e.printStackTrace();
      fail("Query "+ query1+" Execution Failed!");
    }
    
  //CReate Index on status and ID
    try {
      queryService.createIndex("sampleIndex-1", IndexType.FUNCTIONAL, "p.ID", "/"+regionName+ " p");
      queryService.createIndex("sampleIndex-2", IndexType.FUNCTIONAL, "p.status", "/"+regionName+ " p");
      queryService.createIndex("sampleIndex-3", IndexType.FUNCTIONAL, "pos.secId", "/"+regionName+" p, p.positions.values pos");
    } catch (Exception e1) {
      fail("Index Creation Failed with message: " + e1.getMessage());
    }
    
    //Verify Index Creation
    assertNotNull(queryService.getIndex(region, "sampleIndex-1"));
    assertNotNull(queryService.getIndex(region, "sampleIndex-2"));
    assertEquals(3, queryService.getIndexes().size());

 // Without Indexes
    try {
      for(String queryStr: countStarDistinctQueries.keySet()){
        query1 = queryService.newQuery(queryStr);
        query2 = queryService.newQuery(queryStr.replace("COUNT(*)", "*"));
        
        SelectResults result1 = (SelectResults)query1.execute();
        SelectResults result2 = (SelectResults)query2.execute();
        assertEquals(queryStr, 1, result1.size());
        assertTrue(result1.asList().get(0) instanceof Integer);
        
        int count = ((Integer)result1.asList().get(0)).intValue();
        
        //Also verify with size of result2 to count
        assertEquals("COUNT(*) query result is wrong for query: " + queryStr , result2.size(), count);
        
        //assertEquals("Query: "+ queryStr, countStarDistinctQueries.get(queryStr).intValue(), count);
      }
    } catch (Exception e){
      e.printStackTrace();
      fail("Query "+ query1+" Execution Failed!");
    }
  }
  private void createLocalRegion() {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setDataPolicy(DataPolicy.NORMAL);
    RegionAttributes regionAttributes = attributesFactory.create();
    Region region = cache.createRegion(regionName, regionAttributes);

    for(int i=1; i<=numElem; i++) {
      Portfolio obj = new Portfolio(i);
      region.put(i, obj);
      CacheUtils.log(obj);
    }
  }

  private void createSecondLocalRegion() {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setDataPolicy(DataPolicy.NORMAL);
    RegionAttributes regionAttributes = attributesFactory.create();
    Region region = cache.createRegion(exampleRegionName, regionAttributes);

    for(int i=1; i<=numElem; i++) {
      Portfolio obj = new Portfolio(i);
      region.put(i, obj);
      CacheUtils.log(obj);
    }
  }

  private void createPartitionedRegion() {
    Cache cache = CacheUtils.getCache();
    PartitionAttributesFactory prAttFactory = new PartitionAttributesFactory();
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setPartitionAttributes(prAttFactory.create());
    RegionAttributes regionAttributes = attributesFactory.create();
    Region region = cache.createRegion(regionName, regionAttributes);

    for(int i=1; i<=numElem; i++) {
      Portfolio obj = new Portfolio(i);
      region.put(i, obj);
      CacheUtils.log(obj);
    }
  }

  private void createReplicatedRegion() {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes regionAttributes = attributesFactory.create();
    Region region = cache.createRegion(regionName, regionAttributes);

    for(int i=1; i<=numElem; i++) {
      Portfolio obj = new Portfolio(i);
      region.put(i, obj);
      CacheUtils.log(obj);
    }
  }
}
