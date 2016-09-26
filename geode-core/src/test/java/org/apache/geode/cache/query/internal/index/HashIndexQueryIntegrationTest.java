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
package org.apache.geode.cache.query.internal.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.text.ParseException;
import java.util.Collection;
import java.util.Iterator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexInvalidException;
import org.apache.geode.cache.query.IndexStatistics;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.QueryObserverAdapter;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.pdx.PdxInstance;

@Category(IntegrationTest.class)
public class HashIndexQueryIntegrationTest {

  private QueryService qs;
  private Region region;
  private Region joinRegion;
  private MyQueryObserverAdapter observer;
  private Index index;
  
  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
    qs = CacheUtils.getQueryService();
    observer = new MyQueryObserverAdapter();
    QueryObserverHolder.setInstance(observer);
  }
  
  private void createJoinTable(int numEntries) throws Exception {
    joinRegion = CacheUtils.createRegion("portfolios2", Portfolio.class);

    for (int i = 0; i < numEntries; i++) {
      Portfolio p = new Portfolio(i);
      joinRegion.put("" + i, p);
    }
  }

  @After
  public void tearDown() throws java.lang.Exception {
    qs.removeIndexes();
    if (joinRegion != null) {
      joinRegion.close();
      joinRegion = null;
    }
    region.close();
    CacheUtils.closeCache();
  }
  
  /**
   * Helper that tests that hash index is used and that it returns the correct result
   * @throws Exception
   */
  
  private void helpTestHashIndexForQuery(String query)throws Exception {
    helpTestHashIndexForQuery(query,"p.ID", "/portfolios p");
  }
  
  private void helpTestHashIndexForQuery(String query, String indexedExpression, String regionPath) throws Exception {
    SelectResults nonIndexedResults = (SelectResults)qs.newQuery(query).execute();
    assertFalse(observer.indexUsed);

    index = (Index)qs.createHashIndex("idHash", indexedExpression, regionPath);
    SelectResults indexedResults = (SelectResults)qs.newQuery(query).execute();
    assertEquals(nonIndexedResults.size(), indexedResults.size());
    assertTrue(observer.indexUsed);
    assertTrue(indexedResults.size() > 0);
  }
  
  private void helpTestHashIndexForQuery(String query, Object[] params, String indexedExpression, String regionPath) throws Exception {
    SelectResults nonIndexedResults = (SelectResults)qs.newQuery(query).execute(params);
    assertFalse(observer.indexUsed);

    index = (Index)qs.createHashIndex("idHash", indexedExpression, regionPath);
    SelectResults indexedResults = (SelectResults)qs.newQuery(query).execute(params);
    assertEquals(nonIndexedResults.size(), indexedResults.size());
    assertTrue(observer.indexUsed);
  }

  
  /*
   * helper method to test against a compact range index instead of hash index
   * @param query
   * @throws Exception
   */
  private void helpTestCRIndexForQuery(String query, String indexedExpression, String regionPath) throws Exception {
    SelectResults nonIndexedResults = (SelectResults)qs.newQuery(query).execute();
    assertFalse(observer.indexUsed);

    index = (Index)qs.createIndex("crIndex", indexedExpression, regionPath);
    SelectResults indexedResults = (SelectResults)qs.newQuery(query).execute();
    assertEquals(nonIndexedResults.size(), indexedResults.size());
    assertTrue(observer.indexUsed);
  }
  
  /**
   * Tests that hash index with And query for local region
   * @throws Exception
   */
  @Test
  public void testHashIndexWithORQueryForLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    int numEntries = 200;
    int numIds = 100;
    for (int i = 0; i < numEntries; i++) {
      Portfolio p = new Portfolio(i % (numIds));
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    helpTestHashIndexForQuery("SELECT * FROM /portfolios p WHERE p.ID = 1 OR p.ID = 2", "p.ID", "/portfolios p");
  }
  
  @Test
  public void testHashIndexWithNullsForLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    int numEntries = 200;
    int numIds = 100;
    for (int i = 0; i < numEntries; i++) {
      int id = i % numIds;
      Portfolio p = new Portfolio(id);
      if(id % 2 == 0) {
        p.status = null;
      }
      region.put("" + i, p);
    }
    
    helpTestHashIndexForQuery("SELECT * FROM /portfolios p WHERE p.status = 'inactive'", "p.status", "/portfolios p");
    qs.removeIndexes();
    observer = new MyQueryObserverAdapter();
    QueryObserverHolder.setInstance(observer);
    helpTestHashIndexForQuery("SELECT * FROM /portfolios p WHERE p.status != 'inactive'", "p.status", "/portfolios p");
    qs.removeIndexes();
    observer = new MyQueryObserverAdapter();
    QueryObserverHolder.setInstance(observer);
    helpTestHashIndexForQuery("SELECT * FROM /portfolios p WHERE p.status = null", "p.status", "/portfolios p");
    qs.removeIndexes();
    observer = new MyQueryObserverAdapter();
    QueryObserverHolder.setInstance(observer);
    helpTestHashIndexForQuery("SELECT * FROM /portfolios p WHERE p.status != null", "p.status", "/portfolios p");
  }
  
  /**
   * Tests that hash index with And query for local region
   * @throws Exception
   */
  @Test
  public void testHashIndexWithNestedQueryForLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    int numEntries = 200;
    int numIds = 100;
    for (int i = 0; i < numEntries; i++) {
      Portfolio p = new Portfolio(i % (numIds));
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    helpTestCRIndexForQuery("SELECT * FROM (SELECT * FROM /portfolios p WHERE p.shortID = 1)", "p.shortID", "/portfolios p");
  }
  
  /**
   * Tests that hash index with Short vs Integer comparison
   * @throws Exception
   */
  @Test
  public void testHashIndexWithNestedQueryWithShortVsIntegerCompareForLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    int numEntries = 200;
    int numIds = 100;
    for (int i = 0; i < numEntries; i++) {
      Portfolio p = new Portfolio(i % (numIds));
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    helpTestHashIndexForQuery("SELECT * FROM /portfolios p WHERE p.shortID in (SELECT p2.ID FROM /portfolios p2 WHERE p2.shortID = 1)", "p.shortID", "/portfolios p");
  }
  

  /**
   * Tests that hash index with comparison between float and integer 
   * @throws Exception
   */
//  @Test
//  public void testHashIndexQueryWithFloatVsIntegerCompareForLocalRegion() throws Exception {
//    createLocalRegion("portfolios");
//    int numEntries = 1000;
//    int numIds = 100;
//    for (int i = 0; i < numEntries; i++) {
//      Portfolio p = new Portfolio(i % (numIds));
//      p.shortID = (short)i;
//      region.put("" + i, p);
//    }
//    helpTestHashIndexForQuery("SELECT * FROM /portfolios p WHERE p.ID = 1.0f", "p.ID", "/portfolios p");
//  }
  
  /**
   * Tests that hash index with comparison between float and integer 
   * @throws Exception
   */
//  @Test
//  public void testHashIndexNotEqualsWithFloatVsIntegerLocalRegion() throws Exception {
//    createLocalRegion("portfolios");
//    int numEntries = 1000;
//    int numIds = 100;
//    for (int i = 0; i < numEntries; i++) {
//      Portfolio p = new Portfolio(i % (numIds));
//      p.shortID = (short)i;
//      region.put("" + i, p);
//    }
//    helpTestCRIndexForQuery("SELECT * FROM /portfolios p WHERE p.ID != 1.0f", "p.ID", "/portfolios p");
//  }
  
  /**
   * Tests that hash index with And query for local region
   * @throws Exception
   */
  @Test
  public void testHashIndexWithAndQueryForLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    int numEntries = 200;
    int numIds = 100;
    for (int i = 0; i < numEntries; i++) {
      Portfolio p = new Portfolio(i % (numIds));
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    helpTestHashIndexForQuery("SELECT * FROM /portfolios p WHERE p.ID = 1 AND p.shortID > 0", "p.ID", "/portfolios p");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result for local region
   * @throws Exception
   */
  @Test
  public void testHashIndexWithLimitQueryForLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    int numEntries = 200;
    int numIds = 100;
    for (int i = 0; i < numEntries; i++) {
      Portfolio p = new Portfolio(i % (numIds));
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    helpTestHashIndexForQuery("SELECT * FROM /portfolios.entries p WHERE p.key = '1' limit 3", "p.key", "/portfolios.entries p");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result for local region
   * @throws Exception
   */
  @Test
  public void testHashIndexEntriesQueryForLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexForQuery("SELECT * FROM /portfolios.entries p WHERE p.key = '1'", "p.key", "/portfolios.entries p");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result for local region
   * @throws Exception
   */
  @Test
  public void testHashIndexValueQueryForLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexForQuery("SELECT * FROM /portfolios.values p WHERE p.ID = 1", "p.ID", "/portfolios.values p");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result for local region
   * @throws Exception
   */
  @Test
  public void testHashIndexKeySetQueryForLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexForQuery("SELECT * FROM /portfolios.keySet p WHERE p = '1'", "p", "/portfolios.keySet p");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result for local region
   * @throws Exception
   */
  @Test
  public void testHashIndexEqualsForSingleResultOnLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexForQuery("Select * FROM /portfolios p where p.ID = 1");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result for replicated region
   * @throws Exception
   */
  @Test
  public void testHashIndexEqualsForSingleResultOnReplicatedRegion() throws Exception {
    createReplicatedRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexForQuery("Select * FROM /portfolios p where p.ID = 1");
  }

  /**
   * Tests that hash index is used and that it returns the correct result for partitioned region
   * @throws Exception
   */
  @Test
  public void testHashIndexEqualsForSingleResultOnPartitionedRegion() throws Exception {
    createPartitionedRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexForQuery("Select * FROM /portfolios p where p.ID = 1");
  }

  /**
   * Tests that hash index is used and that it returns the correct result
   * @throws Exception
   */
  @Test
  public void testHashIndexAndEquiJoinForSingleResultQueryWithHashIndex() throws Exception {
    createReplicatedRegion("portfolios");
    createData(region, 200);
    createJoinTable(400);
    Index index = (Index)qs.createHashIndex("index2","p2.ID", "/portfolios2 p2");
    helpTestHashIndexForQuery("Select * FROM /portfolios p, /portfolios2 p2 where (p.ID = 1 or p.ID = 2 )and p.ID = p2.ID");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result
   * @throws Exception
   */
  @Test
  public void testHashIndexAndEquiJoinForSingleResultQueryWithCompactRangeIndex() throws Exception {
    createReplicatedRegion("portfolios");
    createData(region, 200);
    createJoinTable(400);
    Index index = (Index)qs.createIndex("index2","p2.ID", "/portfolios2 p2");
    helpTestHashIndexForQuery("Select * FROM /portfolios p, /portfolios2 p2 where (p.ID = 1 or p.ID = 2 )and p.ID = p2.ID");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result
   * @throws Exception
   */
  @Test
  public void testHashIndexAndEquiJoinForSingleResultQueryWithRangeIndex() throws Exception {
    createReplicatedRegion("portfolios");
    createData(region, 200);
    createJoinTable(400);
    Index index = (Index)qs.createIndex("index2","p2.ID", "/portfolios2 p2, p2.positions.values v");
    helpTestHashIndexForQuery("Select * FROM /portfolios p, /portfolios2 p2 where (p.ID = 1 or p.ID = 2 )and p.ID = p2.ID");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result
   * @throws Exception
   */
//  @Test
//  public void testHashIndexAndEquiJoinForSingleResultQueryWithMapRangeIndex() throws Exception {
//    createReplicatedRegion("portfolios");
//    createData(region, 1000);
//    createJoinTable(2000);
//    Index index = (Index)qs.createIndex("index2","p2.names[*]", "/portfolios2 p2");
//    helpTestHashIndexForQuery("Select * FROM /portfolios p, /portfolios2 p2 where (p.names['1'] or p.names['2'] ) and p.names = p2.names", "p.names[*]", "/portfolios p");
//  }

  /**
   * Tests that hash index is used and that it returns the correct result
   * @throws Exception
   */
  @Test
  public void testHashIndexAndEquiJoinForSingleResultQueryWithHashIndexLessEntries() throws Exception {
    createReplicatedRegion("portfolios");
    createData(region, 400);
    createJoinTable(200);
    Index index = (Index)qs.createHashIndex("index2","p2.ID", "/portfolios2 p2");
    helpTestHashIndexForQuery("Select * FROM /portfolios p, /portfolios2 p2 where (p.ID = 1 or p.ID = 2 )and p.ID = p2.ID");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result
   * @throws Exception
   */
  @Test
  public void testHashIndexAndEquiJoinForSingleResultQueryWithCompactRangeIndexLessEntries() throws Exception {
    createReplicatedRegion("portfolios");
    createData(region, 400);
    createJoinTable(200);
    Index index = (Index)qs.createIndex("index2","p2.ID", "/portfolios2 p2");
    helpTestHashIndexForQuery("Select * FROM /portfolios p, /portfolios2 p2 where (p.ID = 1 or p.ID = 2 )and p.ID = p2.ID");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result
   * @throws Exception
   */
  @Test
  public void testHashIndexAndEquiJoinForSingleResultQueryWithRangeIndexLessEntries() throws Exception {
    createReplicatedRegion("portfolios");
    createData(region, 400);
    createJoinTable(200);
    Index index = (Index)qs.createIndex("index2","p2.ID", "/portfolios2 p2, p2.positions.values v");
    helpTestHashIndexForQuery("Select * FROM /portfolios p, /portfolios2 p2 where (p.ID = 1 or p.ID = 2 )and p.ID = p2.ID");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result
   * @throws Exception
   */
//  @Test
//  public void testHashIndexAndEquiJoinForSingleResultQueryWithMapRangeIndexLessEntries() throws Exception {
//    createReplicatedRegion("portfolios");
//    createData(region, 1000);
//    createJoinTable(500);
//    Index index = (Index)qs.createIndex("index2","p2.positions[*]", "/portfolios2 p2");
//    helpTestHashIndexForQuery("Select * FROM /portfolios p, /portfolios2 p2 where p.positions['IBM'] and p.positions['IBM']=p2.positions['IBM']", "p.positions[*]", "/portfolios p");
//  }
  
  /**
   * Tests that hash index is used and that it returns the correct number of results
   * on local region
   * @throws Exception
   */
  @Test
  public void testHashIndexEqualsForMultipleResultQueryOnLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    //Create the data
    int numEntries = 200;
    int numIds = 100;
    for (int i = 0; i < numEntries; i++) {
      Portfolio p = new Portfolio(i % (numIds));
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    
    helpTestHashIndexForQuery("Select * FROM /portfolios p where p.ID = 1");
  }

  /**
   * Tests that hash index is used and that it returns the correct number of results
   * on replicated region
   * @throws Exception
   */
  @Test
  public void testHashIndexEqualsForMultipleResultQueryOnReplicatedRegion() throws Exception {
    createReplicatedRegion("portfolios");
    //Create the data
    int numEntries = 200;
    int numIds = 100;
    for (int i = 0; i < numEntries; i++) {
      Portfolio p = new Portfolio(i % (numIds));
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    
    helpTestHashIndexForQuery("Select * FROM /portfolios p where p.ID = 1");
  }

  /**
   * Tests that hash index is used and that it returns the correct number of results
   * on partitioned region
   * @throws Exception
   */ 
  @Test
  public void testHashIndexEqualsForMultipleResultQueryOnPartitionedRegion() throws Exception {
    createPartitionedRegion("portfolios");
    //Create the data
    int numEntries = 200;
    int numIds = 100;
    for (int i = 0; i < numEntries; i++) {
      Portfolio p = new Portfolio(i % (numIds));
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    
    helpTestHashIndexForQuery("Select * FROM /portfolios p where p.ID = 1");
  }
  
 
  /**
   * Tests that hash index is used and that it returns the correct number of results
   * @throws Exception
   */
  @Test
  public void testHashIndexEquiJoinForMultipleResultQueryWithHashIndex() throws Exception {
    createReplicatedRegion("portfolios");
    createJoinTable(400);
    index = (HashIndex) qs.createHashIndex("idHash",
        "p.ID", "/portfolios p");
    Index index = (Index)qs.createHashIndex("index2","p2.ID", "/portfolios2 p2");

    
    int numEntries = 200;
    int numIds = 100;
    for (int i = 0; i < numEntries; i++) {
      Portfolio p = new Portfolio(i % (numIds));
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    SelectResults results = (SelectResults)qs.newQuery("Select * FROM /portfolios p, /portfolios2 p2 where p.ID = 1 and p.ID = p2.ID").execute();
    assertEquals(numEntries/numIds, results.size());
    assertTrue(observer.indexUsed);
  }
  
  /**
   * Tests that hash index is used and that the value is correctly removed from the index
   * where only 1 value is using the key for partitioned regions
   * @throws Exception
   */
  @Test
  public void testHashIndexRemoveOnLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    helpTestHashIndexRemove();
  }
  
  /**
   * Tests that hash index is used and that the value is correctly removed from the index
   * where only 1 value is using the key for replicated regions
   * @throws Exception
   */
  @Test
  public void testHashIndexRemoveOnReplicatedRegion() throws Exception {
    createReplicatedRegion("portfolios");
    helpTestHashIndexRemove();
  }
  
  @Test
  public void testHashIndexRemoveOnPartitionedRegion() throws Exception {
    createPartitionedRegion("portfolios");
    createData(region, 200);
    region.destroy("1");
    SelectResults noIndexResults = (SelectResults)qs.newQuery("Select * FROM /portfolios p where p.ID = 1").execute();

    region.destroyRegion();
    createPartitionedRegion("portfolios");
    createData(region, 200);
    region.destroy("1");
    index = (Index)qs.createHashIndex("idHash", "p.ID", "/portfolios p");
    SelectResults results = (SelectResults)qs.newQuery("Select * FROM /portfolios p where p.ID = 1").execute();
    assertEquals(noIndexResults.size(), results.size());
    assertTrue(observer.indexUsed);
  }
  /**
   * Tests that hash index is used and that the value is correctly removed from the index
   * where only 1 value is using the key for partitioned regions
   * @throws Exception
   */
  private void helpTestHashIndexRemove() throws Exception {
    createData(region, 200);
    region.destroy("1");
    SelectResults noIndexResults = (SelectResults)qs.newQuery("Select * FROM /portfolios p where p.ID = 1").execute();
    
    region.clear();
    createData(region, 200);
    region.destroy("1");
    index = (Index)qs.createHashIndex("idHash", "p.ID", "/portfolios p");
    SelectResults results = (SelectResults)qs.newQuery("Select * FROM /portfolios p where p.ID = 1").execute();
    assertEquals(noIndexResults.size(), results.size());
    assertTrue(observer.indexUsed);
  }
  
  /**
   * Tests that hash index is used and that the value is correctly removed from the index
   * where multiple entries are using the key on localRegion
   * @throws Exception
   */
  @Test
  public void testHashIndexRemoveFromCommonKeyQueryOnLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    helpTestHashIndexRemoveFromCommonKeyQuery();
  }
  
  /**
   * Tests that hash index is used and that the value is correctly removed from the index
   * where multiple entries are using the key on replicated region
   * @throws Exception
   */
  @Test
  public void testHashIndexRemoveFromCommonKeyQueryOnReplicatedRegion() throws Exception {
    createReplicatedRegion("portfolios");
    helpTestHashIndexRemoveFromCommonKeyQuery();
  }
  
  /**
   * Tests that hash index is used and that the value is correctly removed from the index
   * where multiple entries are using the key on partitioned region
   * @throws Exception
   */
  @Test
  public void testHashIndexRemoveFromCommonKeyQueryOnPartitionedRegion() throws Exception {
    createReplicatedRegion("portfolios");
    helpTestHashIndexRemoveFromCommonKeyQuery();
  }
  
  private void helpTestHashIndexRemoveFromCommonKeyQuery() throws Exception {
    int numEntries = 200;
    int numIds = 100;
    for (int i = 0; i < numEntries; i++) {
      Portfolio p = new Portfolio(i % (numIds));
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    Portfolio p2 = new Portfolio(10000);
    region.put("2", p2);
    p2.ID = 1000;
    region.put("2", p2);
    SelectResults noIndexResult = (SelectResults)qs.newQuery("Select * FROM /portfolios p where p.ID = 2").execute();
  
    region.clear();
    index = (HashIndex) qs.createHashIndex("idHash", "p.ID", "/portfolios p");
    for (int i = 0; i < numEntries; i++) {
      Portfolio p = new Portfolio(i % (numIds));
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    p2 = new Portfolio(10000);
    region.put("2", p2);
    p2.ID = 1000;
    region.put("2", p2);
    
    SelectResults results = (SelectResults)qs.newQuery("Select * FROM /portfolios p where p.ID = 2").execute();
    assertEquals(numEntries/numIds - 1, results.size());
    assertEquals(noIndexResult.size(), results.size());
    assertTrue(observer.indexUsed);
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result
   * on local region
   * @throws Exception
   */
  @Test
  public void testHashIndexNotEqualsQueryOnLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexForQuery("Select * FROM /portfolios p where p.ID != 1");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result
   * on replicated region
   * @throws Exception
   */
  @Test
  public void testHashIndexNotEqualsQueryOnReplicatedRegion() throws Exception {
    createReplicatedRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexForQuery("Select * FROM /portfolios p where p.ID != 1");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result
   * on partitioned region
   * @throws Exception
   */
  @Test
  public void testHashIndexNotEqualsQueryOnPartitionedRegion() throws Exception {
    createPartitionedRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexForQuery("Select * FROM /portfolios p where p.ID != 1");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct number of results
   * for local region
   * @throws Exception
   */
  @Test
  public void testHashIndexNotEqualsForMultipleResultQueryForLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    int numEntries = 200;
    int numIds = 100;
    for (int i = 0; i < numEntries; i++) {
      Portfolio p = new Portfolio(i % (numIds));
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    helpTestHashIndexForQuery("Select * FROM /portfolios p where p.ID != 1");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct number of results
   * for replicated region
   * @throws Exception
   */
  @Test
  public void testHashIndexNotEqualsForMultipleResultQueryForReplicatedRegion() throws Exception {
    createReplicatedRegion("portfolios");
    int numEntries = 200;
    int numIds = 100;
    for (int i = 0; i < numEntries; i++) {
      Portfolio p = new Portfolio(i % (numIds));
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    helpTestHashIndexForQuery("Select * FROM /portfolios p where p.ID != 1");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct number of results
   * for partitioned region
   * @throws Exception
   */
  @Test
  public void testHashIndexNotEqualsForMultipleResultQueryForPartitionedRegion() throws Exception {
    createPartitionedRegion("portfolios");
    int numEntries = 200;
    int numIds = 100;
    for (int i = 0; i < numEntries; i++) {
      Portfolio p = new Portfolio(i % (numIds));
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    helpTestHashIndexForQuery("Select * FROM /portfolios p where p.ID != 1");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result
   * @throws Exception
   */
  @Test
  public void testHashIndexInQueryForLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexForQuery("Select * FROM /portfolios p where p.ID in set (1)");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result
   * @throws Exception
   */
  @Test
  public void testHashIndexInQueryForReplicatedRegion() throws Exception {
    createReplicatedRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexForQuery("Select * FROM /portfolios p where p.ID in set (1)");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result
   * @throws Exception
   */
  @Test
  public void testHashIndexInQueryForPartitionedRegion() throws Exception {
    createPartitionedRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexForQuery("Select * FROM /portfolios p where p.ID in set (1)");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result
   * for local region
   * @throws Exception
   */
  @Test
  public void testHashIndexNotUsedInRangeQueryForLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexNotUsedInRangeQuery();
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result
   * for replicated region
   * @throws Exception
   */
  @Test
  public void testHashIndexNotUsedInRangeQueryForReplicatedRegion() throws Exception {
    createReplicatedRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexNotUsedInRangeQuery();
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result
   * for partitioned region
   * @throws Exception
   */
  @Test
  public void testHashIndexNotUsedInRangeQueryForPartitionedRegion() throws Exception {
    createPartitionedRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexNotUsedInRangeQuery();
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result
   * @throws Exception
   */
  private void helpTestHashIndexNotUsedInRangeQuery() throws Exception {
    SelectResults results = (SelectResults)qs.newQuery("Select * FROM /portfolios p where p.ID < 2").execute();
    assertFalse(observer.indexUsed);
  }

  /**
   * Test order by asc query for local region using hash index
   * @throws Exception
   */
  @Test
  public void testHashIndexOrderByAscQueryForLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexOrderByAscQuery();
  }
  
  /**
   * Test order by asc query for replicated region using hash index
   * @throws Exception
   */
  @Test
  public void testHashIndexOrderByAscQueryForReplicatedRegion() throws Exception {
    createReplicatedRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexOrderByAscQuery();
  }
  
  /**
   * Test order by asc query for partitioned region using hash index
   * @throws Exception
   */
  @Test
  public void testHashIndexOrderByAscQueryForPartitionedRegion() throws Exception {
    createPartitionedRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexOrderByAscQuery();
  }
  
  private void helpTestHashIndexOrderByAscQuery() throws Exception {
    index = (Index)qs.createHashIndex("idHash", "p.ID", "/portfolios p");
    SelectResults results = (SelectResults)qs.newQuery("Select * FROM /portfolios p where p.ID != 0 order by ID asc ").execute();
    assertEquals(199, results.size());
    assertTrue(observer.indexUsed);
    int countUp = 1;
    for (Object o: results) {
      Portfolio p = (Portfolio) o;
      assertEquals(countUp++, p.getID());
    }
  }
  
  /**
   * Test order by desc query for local region using hash index
   * @throws Exception
   */
  @Test
  public void testHashIndexOrderByDescQueryForLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexOrderByDescQuery();
  }
  
  /**
   * Test order by desc query for replicated region using hash index
   * @throws Exception
   */
  @Test
  public void testHashIndexOrderByDescQueryForReplicatedRegion() throws Exception {
    createReplicatedRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexOrderByDescQuery();
  }
  
  /**
   * Test order by desc query for partitioned region using hash index
   * @throws Exception
   */
  @Test
  public void testHashIndexOrderByDescQueryForPartitionedRegion() throws Exception {
    createPartitionedRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexOrderByDescQuery();
  }
  
  /**
   * Tests that hash index on non sequential hashes
   * for local region
   * @throws Exception
   */
  @Test
  public void testHashIndexOnNonSequentialHashForLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    for (int i = 0; i < 100; i++) {
      Portfolio p = new Portfolio(i);
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    
    for (int i = 200; i < 300; i++) {
      Portfolio p = new Portfolio(i);
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    
    for (int i = 500; i < 600; i++) {
      Portfolio p = new Portfolio(i);
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    helpTestHashIndexForQuery("Select * FROM /portfolios p where p.ID != 1");
  }
  
  /**
   * Tests that hash index on non sequential hashes
   * for replicated region
   * @throws Exception
   */
  @Test
  public void testHashIndexOnNonSequentialHashForReplicatedRegion() throws Exception {
    createReplicatedRegion("portfolios");
    for (int i = 0; i < 100; i++) {
      Portfolio p = new Portfolio(i);
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    
    for (int i = 200; i < 300; i++) {
      Portfolio p = new Portfolio(i);
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    
    for (int i = 500; i < 600; i++) {
      Portfolio p = new Portfolio(i);
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    helpTestHashIndexForQuery("Select * FROM /portfolios p where p.ID != 1");
  }
  
  /**
   * Tests that hash index on non sequential hashes
   * for partitioned region
   * @throws Exception
   */
  @Test
  public void testHashIndexOnNonSequentialHashForPartitionedRegion() throws Exception {
    createPartitionedRegion("portfolios");
    for (int i = 0; i < 100; i++) {
      Portfolio p = new Portfolio(i);
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    
    for (int i = 200; i < 300; i++) {
      Portfolio p = new Portfolio(i);
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    
    for (int i = 500; i < 600; i++) {
      Portfolio p = new Portfolio(i);
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    helpTestHashIndexForQuery("Select * FROM /portfolios p where p.ID != 1");
  }
  
  private void helpTestHashIndexOrderByDescQuery() throws Exception {
    index = (Index)qs.createHashIndex("idHash", "p.ID", "/portfolios p");
    SelectResults results = (SelectResults)qs.newQuery("Select * FROM /portfolios p where p.ID != 0 order by ID desc ").execute();
    assertEquals(199, results.size());
    assertTrue(observer.indexUsed);
    int countDown = 199;
    for (Object o: results) {
      Portfolio p = (Portfolio) o;
      assertEquals(countDown--, p.getID());
    }
  }
  
  /**
   * test async exception for hash index using partitioned region
   * @throws Exception
   */
  @Test
  public void testHashIndexAsyncMaintenanceExceptionForPartitionedRegion() throws Exception {
    createPartitionedRegion("portfolios_async", false);
    helpTestAsyncMaintenance();
  }
  
  private void helpTestAsyncMaintenance() throws Exception {
    boolean expected = false;
    try {
      index = qs.createHashIndex("idHash", "p.ID", "/portfolios_async p");
    }
    catch ( UnsupportedOperationException e) {
      expected = true;
    }
    catch (IndexInvalidException e) {
      //for partition region execption;
      expected = true;
    }
  
    assertTrue(expected);
  }
  
  /**
   * test multiple iterators exception for hash index using local region
   * @throws Exception
   */
  @Test
  public void testHashIndexMultipleIteratorsExceptionForLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    helpTestMultipleIteratorsException();
  }
  
  /**
   * test multiple iterators exception for hash index using replicated region
   * @throws Exception
   */
  @Test
  public void testHashIndexMultipleIteratorsExceptionForReplicatedRegion() throws Exception {
    createReplicatedRegion("portfolios");
    helpTestMultipleIteratorsException();
  }
  
  /**
   * test multiple iterators exception for hash index using partiioned region
   * @throws Exception
   */
  @Test
  public void testHashIndexMultipleIteratorsExceptionForPartitionedRegion() throws Exception {
    createPartitionedRegion("portfolios");
    helpTestMultipleIteratorsException();
  }
  
  private void helpTestMultipleIteratorsException() throws Exception {
    boolean expected = false;
    try {
      index = qs.createHashIndex("idHash",
          "p.ID", "/portfolios p, p.positions.values p");
    }
    catch ( UnsupportedOperationException e) {
      expected = true;
    }
    assertTrue(expected);
  }
  
  /**
   * test remove and not equals Query
   * @throws Exception
   */
  @Test
  public void testRemoveAndNotEqualsQuery() throws Exception {
    createReplicatedRegion("portfolios");
    helpTestRemoveAndNotEqualsQuery();
  }
  
  private void helpTestRemoveAndNotEqualsQuery() throws Exception {
    int numEntries = 200;
    index = (HashIndex) qs.createHashIndex("idHash", "p.ID", "/portfolios p");
    for (int i = 0; i < numEntries; i++) {
      Portfolio p = new Portfolio(i);
      p.shortID = (short)i;
      region.put("" + i, p);
    }

    region.destroy("1");
    
    SelectResults results = (SelectResults)qs.newQuery("Select * FROM /portfolios p where p.ID != 1").execute();
    assertEquals(numEntries - 1, results.size());
    assertTrue(observer.indexUsed);
  }
  
  @Test
  public void testHashCollisionAndProbe() throws Exception {
    createReplicatedRegion("relationships");
    int numEntries = 40000;
    int numIds = 200;
    for (int i = 0; i < numEntries; i++) {
      int ki = i % numIds;
      Object key = new RelationshipKey(ki, i);
      Object value = new Schema(new Relationship(new RelationshipKey(ki, i)));
      region.put(key, value);
    }
    Object[] params = new Object[2];
    params[0] = new Identifier("Customer" +1);
    params[1] = new Identifier("Customer" + 1);
    String query = "select * from /relationships.keySet k where k.leftKey = $1 OR k.rightKey = $2";
//    String query = "select * from /relationships r where r.relationship.key IN (select * from /relationships.keySet k where k.leftKey = $1 OR k.rightKey = $2)";
    SelectResults nonIndexedResults = (SelectResults)qs.newQuery(query).execute(params);
    assertFalse(observer.indexUsed);

    index = (Index)qs.createHashIndex("leftKey", "k.leftKey", "/relationships.keySet k");
    Index index2 = (Index)qs.createHashIndex("rightKey", "k.rightKey", "/relationships.keySet k");
    Index index3 = (Index)qs.createKeyIndex("keyIndex", "r.key", "/relationships r");
    SelectResults indexedResults = (SelectResults)qs.newQuery(query).execute(params);
    assertEquals(nonIndexedResults.size(), indexedResults.size());
    assertEquals(nonIndexedResults.size(), numEntries/numIds);
    assertTrue(observer.indexUsed);
  }
  
  @Test
  public void testHashIndexRecreateOnReplicatedRegion() throws Exception {
    createReplicatedRegion("portfolios");
    helpTestHashIndexRecreate();
  }
  
  @Test
  public void testHashIndexRecreateOnPartitionedRegion() throws Exception {
    createPartitionedRegion("portfolios");
    helpTestHashIndexRecreate();
  }
  
  private void helpTestHashIndexRecreate() throws Exception {
    index = (Index)qs.createHashIndex("idHash", "p.ID", "/portfolios p");
    createData(region, 200);
        
    SelectResults noIndexResults = (SelectResults)qs.newQuery("Select * FROM /portfolios p where p.ID = 1").execute();

    IndexStatistics ist = index.getStatistics();
    assertEquals(200, ist.getNumberOfValues());
    assertEquals(200, ist.getNumUpdates());
    assertEquals(1, ist.getTotalUses());

    region.clear();
    
    ist = index.getStatistics();
    assertEquals(0, ist.getNumberOfValues());
    assertEquals(1, ist.getTotalUses());
    assertEquals(400, ist.getNumUpdates());
    
    createData(region, 200);
        
    SelectResults results = (SelectResults)qs.newQuery("Select * FROM /portfolios p where p.ID = 1").execute();
    
    ist = index.getStatistics();
    assertEquals(200, ist.getNumberOfValues());
    assertEquals(2, ist.getTotalUses());
    assertEquals(600, ist.getNumUpdates());
    
    assertEquals(noIndexResults.size(), results.size());
    assertTrue(observer.indexUsed);
  }
  
  @Test
  public void testUpdatesOnKeyWithSameHash() throws Exception {
    createReplicatedRegion("portfolios");
    helpTestUpdatesOnKeyWithSameHash();
  }

  private void helpTestUpdatesOnKeyWithSameHash() throws Exception {
    int numEntries = 10;
    for (int i = 0; i < numEntries; i++) {
      SameHashObject p = new SameHashObject(5, i);
      region.put("" + i, p);
    }
    region.put("0", new SameHashObject(100, 100));
    SelectResults noIndexResults = (SelectResults) qs.newQuery(
        "Select * FROM /portfolios p where p.ID = 5").execute();
    region.clear();
    
    HashIndex index = (HashIndex) qs.createHashIndex("idHash", "p.ID", "/portfolios p");
    for (int i = 0; i < numEntries; i++) {
      SameHashObject p = new SameHashObject(5, i);
      region.put("" + i, p);
    }
    region.put("0", new SameHashObject(index.entriesSet.hashIndexSetProperties.set.length + 5, 100));
    
    SelectResults results = (SelectResults) qs.newQuery(
        "Select * FROM /portfolios p where p.ID = 5").execute();

    assertTrue(results.size() > 0);
    assertEquals(noIndexResults.size(), results.size());
    assertTrue(observer.indexUsed);
  }
  
  @Test
  public void testInPlaceModification() throws Exception {
    createReplicatedRegion("portfolios");
    int numEntries = 10;
    HashIndex index = (HashIndex) qs.createHashIndex("idHash", "p.ID", "/portfolios p");
    for (int i = 0; i < numEntries; i++) {
      SameHashObject p = new SameHashObject(5, i);
      region.put("" + i, p);
    }
    SameHashObject object = (SameHashObject)region.get("0");
    object.ID = 200;
    region.put("0", object);
    
    SelectResults results = (SelectResults) qs.newQuery(
        "Select * FROM /portfolios p where p.ID = 5").execute();
    assertEquals(10, index.getStatistics().getNumberOfValues());
    assertEquals(9, results.size());
    assertTrue(observer.indexUsed);
  }
  
  @Test
  public void testInPlaceModificationToSameKey() throws Exception {
    createReplicatedRegion("portfolios");
    int numEntries = 10;
    HashIndex index = (HashIndex) qs.createHashIndex("idHash", "p.ID", "/portfolios p");
    for (int i = 0; i < numEntries; i++) {
      SameHashObject p = new SameHashObject(5, i);
      region.put("" + i, p);
    }
    SameHashObject object = (SameHashObject)region.get("0");
    object.ID = 5;
    region.put("0", object);
    
    SelectResults results = (SelectResults) qs.newQuery(
        "Select * FROM /portfolios p where p.ID = 5").execute();
    assertEquals(10, index.getStatistics().getNumberOfValues());
    assertEquals(10, results.size());
    assertTrue(observer.indexUsed);
  }
  
  @Test
  public void testInPlaceModificationWithUndefinedKeys() throws Exception {
    createReplicatedRegion("portfolios");
    int numEntries = 10;
    Index index = qs.createIndex("idHash", "p.IDS", "/portfolios p");
    for (int i = 0; i < numEntries; i++) {
      SameHashObject p = new SameHashObject(5, i);
      region.put("" + i, p);
    }
    SameHashObject object = (SameHashObject)region.get("0");
    object.ID = 5;
    region.put("0", object);
    
    SelectResults results = (SelectResults) qs.newQuery(
        "Select * FROM /portfolios p where p.IDS = 5").execute();
    assertEquals(10, index.getStatistics().getNumberOfValues());
    assertEquals(0, results.size());
    assertTrue(observer.indexUsed);
  }
  
  @Test
  public void testInPlaceModificationWithNullKeys() throws Exception {
    createReplicatedRegion("portfolios");
    int numEntries = 10;
    Index index = qs.createHashIndex("idHash", "p.stringValue", "/portfolios p");
    for (int i = 0; i < numEntries; i++) {
      SameHashObject p = new SameHashObject(5, i, null);
      region.put("" + i, p);
    }
    SameHashObject object = (SameHashObject)region.get("0");
    object.stringValue = "wow";
    region.put("0", object);
    
    SelectResults results = (SelectResults) qs.newQuery(
        "Select * FROM /portfolios p where p.stringValue = 'wow'").execute();
    assertEquals(10, index.getStatistics().getNumberOfValues());
    assertEquals(1, results.size());
    assertTrue(observer.indexUsed);
  }
  
  @Test
  public void testOverflowWithRehash() throws Exception {
    try {
    final boolean[] threadCompleted = new boolean[3];
    createReplicatedRegionWithOverflow("portfolios");
    HashIndexSet.TEST_ALWAYS_REHASH = true;
    Index index = qs.createHashIndex("idHash", "p", "/portfolios p");
   
    Thread puts = new Thread(new Runnable() {
      public void run() {
        for (int j = 0; j < 20; j++) {
          for (int i = 0; i < 2; i++) {
            region.put("" + i, "SOME STRING OBJECT" + i);
          }
        }
        threadCompleted[0] = true;
      }
    });
        
    Thread morePuts = new Thread(new Runnable() {
      public void run() {
        for (int j = 0; j < 20; j++) {
          for (int i = 0; i < 1; i++) {
            region.put("" + (i + 100), "SOME OTHER STRING OBJECT" + (i+100));
          }
        }
        threadCompleted[1] = true;
      }
    });
    
    Thread evenMorePuts = new Thread(new Runnable() {
      public void run() {
        for (int j = 0; j < 20; j++) {
          for (int i = 0; i < 1; i++) {
            region.put("" + (i + 200), "ANOTHER STRING OBJECT" + (i + 200));
          }
        }
        threadCompleted[2] = true;
      }
    });
    
    evenMorePuts.start();
    morePuts.start();
    puts.start();

    puts.join(30000);
    morePuts.join(30000);
    evenMorePuts.join(30000);
    assertTrue("Thread possibly deadlocked, thread did not complete", threadCompleted[0]);
    assertTrue("Thread possibly deadlocked, thread did not complete", threadCompleted[1]);
    assertTrue("Thread possibly deadlocked, thread did not complete", threadCompleted[2]);
    
    SelectResults results = (SelectResults) qs.newQuery(
        "Select * FROM /portfolios p").execute();
    assertEquals(4, results.size());
    }
    finally {
      HashIndexSet.TEST_ALWAYS_REHASH = false;
    }
  }

  @Test
  public void testPdxWithStringIndexKeyValues() throws Exception {
    createPartitionedRegion("test_region");
    int numEntries = 10;
    Index index = qs.createHashIndex("idHash", "p.id", "/test_region p");
    for (int i = 0; i < numEntries; i++) {
      PdxInstance record = CacheUtils.getCache().createPdxInstanceFactory("test_region").writeString("id", "" + i).writeString("domain", "A").create();
      region.put("" + i, record);
    }
    
    SelectResults results = (SelectResults) qs.newQuery(
        "SELECT DISTINCT tr.domain FROM /test_region tr WHERE tr.id='1'").execute();
    assertEquals(1, results.size());
    assertTrue(observer.indexUsed);
  }

   
//  private void printIndex(Index index) {
//   if (index instanceof PartitionedIndex) {
//    Iterator it = ((PartitionedIndex)index).getBucketIndexes().iterator();
//    while (it.hasNext()) { 
//      ((HashIndex)it.next()).printAll();
//    }
//   }
//   else {
//     System.out.println(((HashIndex)index).printAll());
//   }
//  }
  
  
  private class RelationshipKey implements Comparable {
    public Identifier leftKey;
    public Identifier rightKey;
    
    public RelationshipKey(int leftKeyId, int rightKeyId) {
      this.leftKey = new Identifier("Customer" + leftKeyId);
      this.rightKey = new Identifier("Customer" + rightKeyId);
    }
    
    public Identifier getLeftKey() {
      return leftKey;
    }
    public Identifier getRightKey() {
      return rightKey;
    }
    
    public int compareTo(Object o) {
      if (o instanceof RelationshipKey) {
        return leftKey.compareTo(((RelationshipKey)o).leftKey);
      }
      throw new ClassCastException("Unable to cast " + o + " to Identifier");
    }
    
    public boolean equals(Object o) {
      if (o instanceof RelationshipKey) {
        return leftKey.equals(((RelationshipKey)o).leftKey) && rightKey.equals(((RelationshipKey)o).rightKey);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return leftKey.hashCode();
    }
  }
  
  private class Schema {
    public Relationship relationship;
    
    public Schema(Relationship relationship) {
      this.relationship = relationship;
    }
    
    public Relationship getRelationship() {
      return relationship;
    }
  }
  
  private class Relationship {
    public RelationshipKey key;
    public Relationship(RelationshipKey key) {
      this.key = key;
    }
    
    public RelationshipKey getKey() {
      return key;
    }
  }
  
  public class Identifier implements Comparable {
    private int hashCode = 0;
    public String id;
    
    public Identifier(String id) {
      this.id = id;
    }
    
    @Override
    public int compareTo(Object o) {
      if (o instanceof Identifier) {
        String otherId = ((Identifier)o).id;
        return id.compareTo(otherId);
      }
      throw new ClassCastException("Unable to cast " + o + " to Identifier");
    }
    
    public boolean equals(Object o) {
      if (o instanceof Identifier) {
        return id.equals(((Identifier)o).id);
      }
      return false;
    }

    @Override
    public int hashCode() {
      if (this.hashCode == 0) {
        this.hashCode = id.hashCode();
        this.hashCode += 7 * "something".hashCode();
    }

    return this.hashCode;
    }
  }
  
  public class SameHashObject implements Serializable {
    
    public int ID = 0;
    public String stringValue;
    private int uniqueId = 0;
    
    public SameHashObject(int i, int uniqueId, String stringValue) {
      ID = i;
      this.uniqueId = uniqueId;
      this.stringValue = stringValue;
    }
    
    public SameHashObject(int i, int uniqueId) {
      this(i, uniqueId, "" + i);
    }
    
    public boolean equals(Object o) {
      if (o instanceof SameHashObject) {
        return ID == ((SameHashObject)o).ID;
      }
      return false;
    }
    public int hashCode() {
      return 99;
    }
    
    public String toString() {
      return "SameHashObject:" + ID + ":" + uniqueId + " :" + stringValue;
    }
  }
  
  
  private void createLocalRegion(String regionName) throws ParseException {
    createLocalRegion(regionName, true);
  }
  
  private void createLocalRegion(String regionName, boolean synchMaintenance) throws ParseException {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setDataPolicy(DataPolicy.NORMAL);
    attributesFactory.setIndexMaintenanceSynchronous(synchMaintenance);
    RegionAttributes regionAttributes = attributesFactory.create();
    region = cache.createRegion(regionName, regionAttributes);
  }
  
  private void createReplicatedRegion(String regionName) throws ParseException {
    createReplicatedRegion(regionName, true);
  }
  
  private void createReplicatedRegion(String regionName, boolean synchMaintenance) throws ParseException {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setDataPolicy(DataPolicy.REPLICATE);
    attributesFactory.setIndexMaintenanceSynchronous(synchMaintenance);
    RegionAttributes regionAttributes = attributesFactory.create();
    region = cache.createRegion(regionName, regionAttributes);
  }
  
  private void createReplicatedRegionWithOverflow(String regionName) throws ParseException {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setDataPolicy(DataPolicy.REPLICATE);
    attributesFactory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK));
    RegionAttributes regionAttributes = attributesFactory.create();
    region = cache.createRegion(regionName, regionAttributes);
  }
  
  private void createPartitionedRegion(String regionName) throws ParseException {
    createLocalRegion(regionName, true);
  }

  private void createPartitionedRegion(String regionName, boolean synchMaintenance) throws ParseException {
    Cache cache = CacheUtils.getCache();
    PartitionAttributesFactory prAttFactory = new PartitionAttributesFactory();
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setPartitionAttributes(prAttFactory.create());
    attributesFactory.setIndexMaintenanceSynchronous(synchMaintenance);
    RegionAttributes regionAttributes = attributesFactory.create();
    region = cache.createRegion(regionName, regionAttributes);
  }
  
  private void createData(Region region, int numEntries) {
    for (int i = 0; i < numEntries; i++) {
      Portfolio p = new Portfolio(i);
      region.put("" + i, p);
    }
  }
  
  
  class MyQueryObserverAdapter extends QueryObserverAdapter {
    public boolean indexUsed = false;
    
    public void afterIndexLookup(Collection results){
      super.afterIndexLookup(results);
      indexUsed = true;
    }
  };
  
  
}
