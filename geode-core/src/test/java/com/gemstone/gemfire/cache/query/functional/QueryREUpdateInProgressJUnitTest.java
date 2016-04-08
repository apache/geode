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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Iterator;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.partitioned.PRQueryDUnitHelper;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion.NonTXEntry;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * This tests puts some values in a Local Region and sets all
 * region entries being updated as true (Just the flag).
 * Then we run the queries with and without indexes and compare
 * their results to test DataInconsistency changes for Bug #41010
 * and #42757.
 *
 *
 */
@Category(IntegrationTest.class)
public class QueryREUpdateInProgressJUnitTest {

  private static final String exampleRegionName = "exampleRegion2";
  public static String regionName = "exampleRegion1";
  public static String regionForAsyncIndex = "exampleRegion3";
  public static int numOfEntries = 100;

  public static String[] queries =  new String[]{

        //Queries with * to be executed with corresponding result count.
          "select * from /" + regionName ,
          "select * from /" + regionName + " where ID > 0",
          "select * from /" + regionName + " where ID < 0",
          "select * from /" + regionName + " where ID > 0 AND status='active'",
          "select * from /" + regionName + " where ID > 0 OR status='active'",
          "select * from /" + regionName + " where ID > 0 AND status LIKE 'act%'",
          "select * from /" + regionName + " where ID > 0 OR status LIKE 'ina%'",
          "select * from /" + regionName + " where ID IN SET(1, 2, 3, 4, 5)",
          "select * from /" + regionName + " where NOT (ID > 5)",
          
          //StructSet queries.
          "select * from /" + regionName + " p, p.positions.values pos where p.ID > 0 AND pos.secId = 'IBM'",
          "select DISTINCT * from /" + regionName + " p, p.positions.values pos where p.ID > 0 AND pos.secId = 'IBM' ORDER BY p.ID",
          "select * from /" + regionName + " p, p.positions.values pos where p.ID > 0 AND p.status = 'active' AND pos.secId = 'IBM'",

          "select * from /" + regionName + " p, p.positions.values pos where p.ID > 0 AND p.status = 'active' OR pos.secId = 'IBM'",

          "select * from /" + regionName + " p, p.positions.values pos where p.ID > 0 OR p.status = 'active' OR pos.secId = 'IBM'",
          "select DISTINCT * from /" + regionName + " p, p.positions.values pos where p.ID > 0 OR p.status = 'active' OR pos.secId = 'IBM' ORDER BY p.ID",
          
        //EquiJoin Queries
          "select * from /" + regionName + " p, /"+ exampleRegionName +" e where p.ID = e.ID AND p.ID > 0",
          "select * from /" + regionName + " p, /"+ exampleRegionName +" e where p.ID = e.ID AND p.ID > 20 AND e.ID > 40",
          "select * from /" + regionName + " p, /"+ exampleRegionName +" e where p.ID = e.ID AND p.ID > 0 AND p.status = 'active'",
          "select * from /" + regionName + " p, /"+ exampleRegionName +" e where p.ID = e.ID OR e.status = 'active' ",

          //SelfJoin Queries
          "select * from /" + regionName + " p, /"+ regionName +" e where p.ID = e.ID AND p.ID > 0",
          "select * from /" + regionName + " p, /"+ regionName +" e where e.ID != 0 AND p.status = 'active'",
          "select * from /" + regionName + " p, /"+ regionName +" e where p.ID = e.ID AND e.ID > 20 AND p.ID > 40",
          "select * from /" + regionName + " p, /"+ regionName +" e where p.ID = e.ID AND e.ID > 0 AND p.status = 'active'",
          "select * from /" + regionName + " p, /"+ regionName +" e where p.ID = e.ID OR e.status = 'active' ",

        //EquiJoin Queries with entry iterator
          "select p_ent.key, e_ent.key from /" + regionName + ".entries p_ent, /"+ exampleRegionName +".entries e_ent where p_ent.key = e_ent.key AND p_ent.value.ID > 0",
          "select DISTINCT p_ent.key, p_ent.value, e_ent.key, e_ent.value from /"
                                                        + regionName + ".entries p_ent, p_ent.value.positions.values ppos, /"
                                                        + exampleRegionName +".entries e_ent, e_ent.value.positions.values epos "
                                                        + "WHERE ppos.secId = epos.secId AND p_ent.key = e_ent.key "
                                                        + "ORDER by p_ent.key, ppos.secId",
          "select DISTINCT p_ent.key, p_ent.value, e_ent.key, e_ent.value from /"
                                                        + regionName + ".entries p_ent, p_ent.value.positions.values ppos, /"
                                                        + exampleRegionName +".entries e_ent, e_ent.value.positions.values epos "
                                                        + "WHERE ppos.secId = epos.secId AND p_ent.key = e_ent.key ",
          "select distinct * from /" + regionForAsyncIndex + ".keys where toString > '1'",
          "select distinct key  from /" + regionForAsyncIndex + ".keys where toString > '1'"
    };
  
  public static String[] limitQueries = new String[] {
    "select * from /" + regionName + " where ID > 0 LIMIT 50",
    "select * from /" + regionName + " p, p.positions.values pos where p.ID > 0 OR p.status = 'active' OR pos.secId = 'IBM' LIMIT 150",
    "select * from /" + regionName + " p, p.positions.values pos where p.ID >= 0 AND pos.secId = 'IBM' LIMIT 5",
  };

  /**
   * This tests queries without limit clause.
   */
  @Test
  public void testQueriesOnREWhenUpdateInProgress() throws Exception {

    //Create Indexes.
    Cache cache = CacheUtils.getCache();
    QueryService qs = cache.getQueryService();
    String[] queries = getQueries(); //Get Queries.
    Object[][] results = new Object[queries.length][2];
    
    //Put values in Region.
    putREWithUpdateInProgressTrue(regionName);
    putREWithUpdateInProgressTrue(exampleRegionName);
    putREWithUpdateInProgressTrue(regionForAsyncIndex);

    //Run queries without indexes
    //Run all queries.
    for (int i=0; i < queries.length; i++) {
      try {
        results[i][0] = qs.newQuery("<trace> " +queries[i]).execute();
      } catch (Exception e) {
        throw new RuntimeException("Query execution failed for query: "+ queries[i], e);
      }
    }

    try {
      qs.createIndex("idIndex", "p.ID", "/"+regionName+" p");
      qs.createIndex("statusIndex", "p.status", "/"+regionName+" p");
      qs.createIndex("secIdIndex", "pos.secId", "/"+regionName+" p, p.positions.values pos");
      qs.createIndex("pentryKeyIndex", "p_ent.key", "/"+regionName+".entries p_ent");
      qs.createIndex("pentryValueIndex", "ppos.secId", "/"+regionName+".entries p_ent, p_ent.value.positions.values ppos");

      qs.createIndex("eidIndex", "e.ID", "/"+exampleRegionName+" e");
      qs.createIndex("estatusIndex", "e.status", "/"+exampleRegionName+" e");
      qs.createIndex("eentryKeyIndex", "e_ent.key", "/"+exampleRegionName+".entries e_ent");
      qs.createIndex("eentryValueIndex", "epos.secId", "/"+exampleRegionName+".entries e_ent, e_ent.value.positions.values epos");
      qs.createIndex("keyIndex", "toString", "/"+regionForAsyncIndex+".keys");
    } catch (Exception e) {
      throw new RuntimeException("Index creation failed!", e);
    }
    
    
    //Run all queries with Indexes.
    for (int i=0; i < queries.length; i++) {
      try {
        results[i][1] = qs.newQuery("<trace> "+queries[i]).execute();
      } catch (Exception e) {
        throw new RuntimeException("Query execution failed for query: " + queries[i]
            + "\n ResultSet 01: " + results[i][0] + "\n" + "ResultSet 02: "
            + results[i][1] + "\n", e);
      }
    }
    //Compare query results
    GemFireCacheImpl.getInstance().getLogger().fine("\n Result 01: "+ results[queries.length-1][0]+ "\n\n Result 02: "+ results[queries.length-1][1]);
    new StructSetOrResultsSet().CompareQueryResultsWithoutAndWithIndexes(results, queries.length, false, queries);
  }

  @Test
  public void testQueriesOnREWhenUpdateInProgressWithOneIndex() throws Exception {

    //Create Indexes.
    Cache cache = CacheUtils.getCache();
    QueryService qs = cache.getQueryService();
    String[] queries = getQueries(); //Get Queries.
    Object[][] results = new Object[queries.length][2];
    
    //Put values in Region.
    putREWithUpdateInProgressTrue(regionName);
    putREWithUpdateInProgressTrue(exampleRegionName);
    putREWithUpdateInProgressTrue(regionForAsyncIndex);

    //Run queries without indexes
    //Run all queries.
    for (int i=0; i < queries.length; i++) {
      try {
        results[i][0] = qs.newQuery("<trace> " +queries[i]).execute();
      } catch (Exception e) {
        throw new RuntimeException("Query executio failed for query: "+ queries[i], e);
      }
    }

    qs.createIndex("idIndex", "p.ID", "/"+regionName+" p");
    
    
    //Run all queries with Indexes.
    for (int i=0; i < queries.length; i++) {
      try {
        results[i][1] = qs.newQuery("<trace> "+queries[i]).execute();
      } catch (Exception e) {
        throw new RuntimeException("Query executio failed for query: " + queries[i]
            + "\n ResultSet 01: " + results[i][0] + "\n" + "ResultSet 02: "
            + results[i][1], e);
      }
    }
    //Compare query results
    GemFireCacheImpl.getInstance().getLogger().fine("\n Result 01: "+ results[queries.length-1][0]+ "\n\n Result 02: "+ results[queries.length-1][1]);
    new StructSetOrResultsSet().CompareQueryResultsWithoutAndWithIndexes(results, queries.length, false, queries);
  }
  
  @Test
  public void testMultiDepthQueriesOnREWhenUpdateInProgressWithOneIndex() throws Exception {

    //Create Indexes.
    Cache cache = CacheUtils.getCache();
    QueryService qs = cache.getQueryService();
    String[] queries = new String[] {"select * from /" + regionName + " z, /" + regionName + " q where z.position1.secId = 'IBM' and q.ID > 0",
        "select * from /" + regionName + " y where position1.secId='IBM'"};
    Object[][] results = new Object[queries.length][2];
    
    //Put values in Region.
    putREWithUpdateInProgressTrue(regionName);
    putREWithUpdateInProgressTrue(exampleRegionName);
    putREWithUpdateInProgressTrue(regionForAsyncIndex);

    //Run queries without indexes
    //Run all queries.
    for (int i=0; i < queries.length; i++) {
      try {
        results[i][0] = qs.newQuery("" +queries[i]).execute();
      } catch (Exception e) {
        throw new RuntimeException("Query execution failed for query: "+ queries[i], e);
      }
    }

    qs.createIndex("secIdindex", "z.position1.secId", "/" + regionName + " z " );
    
    
    //Run all queries with Indexes.
    for (int i=0; i < queries.length; i++) {
      try {
        results[i][1] = qs.newQuery(""+queries[i]).execute();
      } catch (Exception e) {
        throw new RuntimeException("Query execution failed for query: " + queries[i]
            + "\n ResultSet 01: " + results[i][0] + "\n" + "ResultSet 02: "
            + results[i][1], e);
      }
    }
    //Compare query results
    GemFireCacheImpl.getInstance().getLogger().fine("\n Result 01: "+ results[queries.length-1][0]+ "\n\n Result 02: "+ results[queries.length-1][1]);
    new StructSetOrResultsSet().CompareQueryResultsWithoutAndWithIndexes(results, queries.length, false, queries);
  }

  @Test
  public void testCompactMapIndexQueriesOnREWhenUpdateInProgressWithOneIndex() throws Exception {

    //Create Indexes.
    Cache cache = CacheUtils.getCache();
    QueryService qs = cache.getQueryService();
    String[] queries = new String[] {"select * from /" + regionName + " pf where pf.positions['IBM'] != null",
        "select * from /" + regionName + " pf, pf.positions.values pos where pf.positions['IBM'] != null"};
    Object[][] results = new Object[queries.length][2];
    
    //Put values in Region.
    putREWithUpdateInProgressTrue(regionName);
    putREWithUpdateInProgressTrue(exampleRegionName);
    putREWithUpdateInProgressTrue(regionForAsyncIndex);

    //Run queries without indexes
    //Run all queries.
    for (int i=0; i < queries.length; i++) {
      try {
        results[i][0] = qs.newQuery("" +queries[i]).execute();
      } catch (Exception e) {
        throw new RuntimeException("Query execution failed for query: "+ queries[i], e);
      }
    }


    qs.createIndex("mapIndex", "pf.positions['IBM','YHOO','SUN']", "/" + regionName + " pf");
    
    
    //Run all queries with Indexes.
    for (int i=0; i < queries.length; i++) {
      try {
        results[i][1] = qs.newQuery(""+queries[i]).execute();
      } catch (Exception e) {
        throw new RuntimeException("Query execution failed for query: " + queries[i]
            + "\n ResultSet 01: " + results[i][0] + "\n" + "ResultSet 02: "
            + results[i][1], e);
      }
    }
    //Compare query results
    GemFireCacheImpl.getInstance().getLogger().fine("\n Result 01: "+ results[queries.length-1][0]+ "\n\n Result 02: "+ results[queries.length-1][1]);
    new StructSetOrResultsSet().CompareQueryResultsWithoutAndWithIndexes(results, queries.length, false, queries);
  }
  /**
   * This tests queries without limit clause.
   */
  @Test
  public void testLimitQueriesOnREWhenUpdateInProgress() throws Exception {

    //Create Indexes.
    Cache cache = CacheUtils.getCache();
    QueryService qs = cache.getQueryService();
    String[] queries = getLimitQueries(); //Get Queries.
    Object[][] results = new Object[queries.length][2];
    
    //Put values in Region.
    putREWithUpdateInProgressTrue(regionName);
    putREWithUpdateInProgressTrue(exampleRegionName);

    //Run queries without indexes
    //Run all queries.
    for (int i=0; i < queries.length; i++) {
      try {
        results[i][0] = qs.newQuery("<trace> " +queries[i]).execute();
      } catch (Exception e) {
        throw new RuntimeException("Query executio failed for query: "+ queries[i], e);
      }
    }

    qs.createIndex("idIndex", "p.ID", "/"+regionName+" p");
    qs.createIndex("statusIndex", "p.status", "/"+regionName+" p");
    qs.createIndex("secIdIndex", "pos.secId", "/"+regionName+" p, p.positions.values pos");
    qs.createIndex("pentryKeyIndex", "p_ent.key", "/"+regionName+".entries p_ent");
    qs.createIndex("pentryValueIndex", "ppos.secId", "/"+regionName+".entries p_ent, p_ent.value.positions.values ppos");

    qs.createIndex("eidIndex", "e.ID", "/"+exampleRegionName+" e");
    qs.createIndex("estatusIndex", "e.status", "/"+exampleRegionName+" e");
    qs.createIndex("eentryKeyIndex", "e_ent.key", "/"+exampleRegionName+".entries e_ent");
    qs.createIndex("eentryValueIndex", "epos.secId", "/"+exampleRegionName+".entries e_ent, e_ent.value.positions.values epos");
    
    
    //Run all queries with Indexes.
    for (int i=0; i < queries.length; i++) {
      try {
        results[i][1] = qs.newQuery("<trace> "+queries[i]).execute();
      } catch (Exception e) {
        throw new RuntimeException("Query executio failed for query: "+ queries[i], e);
      }
    }
    //Compare query results
    GemFireCacheImpl.getInstance().getLogger().fine("\n Result 01: "+ results[queries.length-1][0]+ "\n\n Result 02: "+ results[queries.length-1][1]);
    compareLimitQueryResults(results, queries.length);
  }
 
  private void compareLimitQueryResults(Object[][] r, int len) {
    Set set1 = null;
    Set set2 = null;
    ObjectType type1, type2;

    for (int j = 0; j < len; j++) {
      if ((r[j][0] != null) && (r[j][1] != null)) {
        type1 = ((SelectResults) r[j][0]).getCollectionType().getElementType();
        assertNotNull(
            "PRQueryDUnitHelper#compareTwoQueryResults: Type 1 is NULL "
                + type1, type1);
        type2 = ((SelectResults) r[j][1]).getCollectionType().getElementType();
        assertNotNull(
            "PRQueryDUnitHelper#compareTwoQueryResults: Type 2 is NULL "
                + type2, type2);
        if ( !(type1.getClass().getName()).equals(type2.getClass().getName()) ) {
          fail("PRQueryDUnitHelper#compareTwoQueryResults: FAILED:Search result Type is different in both the cases: " 
              + type1.getClass().getName() + " "
              + type2.getClass().getName());
        }
        int size0 = ((SelectResults) r[j][0]).size();
        int size1 = ((SelectResults) r[j][1]).size();
        if (size0 != size1) {
          fail("PRQueryDUnitHelper#compareTwoQueryResults: FAILED:Search resultSet size are different in both cases; size0="
              + size0 + ";size1=" + size1 + ";j=" + j);
        }
      }
    }
  }

  private String[] getQueries() {
    //Get queries using QueryTestUtils.java
    return queries;
  }

  private String[] getLimitQueries() {
    //Get queries using QueryTestUtils.java
    return limitQueries;
  }

  private void putREWithUpdateInProgressTrue(String region) {
    Region reg = CacheUtils.getRegion(region);
    Portfolio[] values = new PRQueryDUnitHelper("").createPortfoliosAndPositions(numOfEntries);

    int i=0;
    for (Object val: values) {
      reg.put(i, val);
      i++;
    }

    //Set all RegionEntries to be updateInProgress.
    Iterator entryItr = reg.entrySet().iterator();
    while (entryItr.hasNext()) {
      Region.Entry nonTxEntry = (Region.Entry) entryItr.next();
      RegionEntry entry = ((NonTXEntry)nonTxEntry).getRegionEntry();
      entry.setUpdateInProgress(true);
      assertTrue(entry.isUpdateInProgress());
    }
  }

  @Before
  public void setUp() throws Exception {
    CacheUtils.startCache();
    CacheUtils.createRegion(regionName, null, Scope.DISTRIBUTED_ACK);
    CacheUtils.createRegion(exampleRegionName, null, Scope.DISTRIBUTED_ACK);
    AttributesFactory attr = new AttributesFactory();
    attr.setIndexMaintenanceSynchronous(false);
    CacheUtils.createRegion(regionForAsyncIndex, attr.create(), false);
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
  }
}
