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

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.*;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.functional.StructSetOrResultsSet;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.QueryExecutionContext;
import org.apache.geode.cache.query.internal.QueryObserverAdapter;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Set;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class IndexHintJUnitTest {
  private Region region;
  
  @Before
  public void setUp() throws Exception {
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "Query.VERBOSE", "true");
    CacheUtils.startCache();
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
  }

  //tests the grammar for a hint with a single index name
  @Test
  public void testSingleIndexHint() throws Exception {
    createRegion();
    QueryService qs = CacheUtils.getQueryService();
    DefaultQuery query = (DefaultQuery) qs
        .newQuery("<hint 'FirstIndex'> select * from /Portfolios p where p.ID > 10");
    QueryExecutionContext qec = new QueryExecutionContext(new Object[1],
        CacheUtils.getCache(), query);
    query.executeUsingContext(qec);

    assertTrue(qec.isHinted("FirstIndex"));
    assertEquals(-1, qec.getHintSize("FirstIndex"));
  }
  
  //Tests the grammar for a hint with two index names
  @Test
  public void testTwoIndexHint() throws Exception {
    createRegion();
    QueryService qs = CacheUtils.getQueryService();
    DefaultQuery query = (DefaultQuery) qs
        .newQuery("<hint 'FirstIndex', 'SecondIndex'> select * from /Portfolios p where p.ID > 10");
    QueryExecutionContext qec = new QueryExecutionContext(new Object[1],
        CacheUtils.getCache(), query);
    query.executeUsingContext(qec);

    assertTrue(qec.isHinted("FirstIndex"));
    assertTrue(qec.isHinted("SecondIndex"));
  }
  
  //Tests that index hints are ordered correctly
  @Test
  public void testIndexHintOrdering() throws Exception {
    createRegion();
    QueryService qs = CacheUtils.getQueryService();
    DefaultQuery query = (DefaultQuery)qs.newQuery("<hint 'FirstIndex','SecondIndex','ThirdIndex','FourthIndex'>select * from /Portfolios p where p.ID > 10");
    QueryExecutionContext qec = new QueryExecutionContext(new Object[1], CacheUtils.getCache(), query);
    query.executeUsingContext(qec);
    
    assertTrue(qec.isHinted("FirstIndex"));
    assertTrue(qec.isHinted("SecondIndex"));
    assertTrue(qec.isHinted("ThirdIndex"));
    assertTrue(qec.isHinted("FourthIndex"));
    
    assertEquals(-4, qec.getHintSize("FirstIndex"));
    assertEquals(-3, qec.getHintSize("SecondIndex"));
    assertEquals(-2, qec.getHintSize("ThirdIndex"));
    assertEquals(-1, qec.getHintSize("FourthIndex"));
  }
  
  //Tests that even though we would have chosen the index anyways, that the hint still
  //allows us to chose the same index
  @Test
  public void testIndexUsageWithIndexHint() throws Exception {
    createRegion();
    populateData(1000);
    //create index
    createIndex("IDIndex", "p.ID", "/Portfolios p");
    //set observer
    QueryObserverImpl observer = new QueryObserverImpl();
    QueryObserverHolder.setInstance(observer);
    //execute query
    QueryService qs = CacheUtils.getQueryService();
    Query query = qs.newQuery("<hint 'IDIndex'>select * from /Portfolios p where p.ID > 10");
    query.execute();
    //verify index usage
    assertTrue(observer.wasIndexUsed("IDIndex")); 
  }
  
  //Tests scenario where we provide a hint that is unuseable for the query
  //The other index should be used instead of the hint because the hint itself
  //would not have helped
  @Test
  public void testIndexUsageWithUnusableIndexHint() throws Exception {
    createRegion();
    populateData(1000);
    //create index
    createIndex("IDIndex", "p.ID", "/Portfolios p");
    createIndex("SecIndex", "p.secId", "/Portfolios p");
    //set observer
    QueryObserverImpl observer = new QueryObserverImpl();
    QueryObserverHolder.setInstance(observer);
    //execute query
    QueryService qs = CacheUtils.getQueryService();
    Query query = qs.newQuery("<hint 'SecIndex'>select * from /Portfolios p where p.ID > 10");
    query.execute();
    //verify index usage
    assertFalse(observer.wasIndexUsed("SecIndex")); 
    assertTrue(observer.wasIndexUsed("IDIndex")); 
  }
  

  //given a choice between two indexes, we hint on each one in different queries and verify
  //that both indexes are used
  @Test
  public void testMultiIndexWithSingleIndexHint() throws Exception {
    createRegion();
    populateData(1000);
    //create index
    createIndex("IDIndex", "p.ID", "/Portfolios p");
    createIndex("SecIndex", "p.status", "/Portfolios p");
    //set observer
    QueryObserverImpl observer = new QueryObserverImpl();
    QueryObserverHolder.setInstance(observer);
    //execute query
    SelectResults[][] results = new SelectResults[1][2];
    QueryService qs = CacheUtils.getQueryService();
    Query query = qs.newQuery("<hint 'IDIndex'>select * from /Portfolios p where p.ID > 10 and p.status = 'inactive'");
    results[0][0] = (SelectResults) query.execute();
    //verify index usage
    assertTrue(observer.wasIndexUsed("IDIndex")); 
    observer.reset();
    
    query = qs.newQuery("<hint 'SecIndex'>select * from /Portfolios p where p.ID > 10 and p.status = 'inactive'");
    results[0][1] = (SelectResults) query.execute();
    //verify index usage
    assertTrue(observer.wasIndexUsed("SecIndex")); 

    StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
    assertEquals(495, results[0][1].size());
    //Not really with and without index but we can use this method to verify they are the same results
    //regardless of which index used
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(results, 1, new String[]{"<query with hints>"});
  }
  
  //Using junction, we will hint and make sure single index hints are functioning
  @Test
  public void testMultiIndexWithSingleIndexHintWithRangeJunction() throws Exception {
    createRegion();
    populateData(1000);
    //create index
    createIndex("IDIndex", "p.ID", "/Portfolios p");
    createIndex("SecIndex", "p.status", "/Portfolios p");
    //set observer
    QueryObserverImpl observer = new QueryObserverImpl();
    QueryObserverHolder.setInstance(observer);
    //execute query
    SelectResults[][] results = new SelectResults[1][2];
    QueryService qs = CacheUtils.getQueryService();
    Query query = qs.newQuery("<hint 'IDIndex'>select * from /Portfolios p where p.ID > 10 and p.ID < 200 and p.status = 'inactive'");
    results[0][0] = (SelectResults) query.execute();
    //verify index usage
    assertTrue(observer.wasIndexUsed("IDIndex")); 
    observer.reset();

    query = qs.newQuery("<hint 'SecIndex'>select * from /Portfolios p where p.ID > 10 and p.ID < 200 and p.status = 'inactive'");
    results[0][1] = (SelectResults) query.execute();
    //verify index usage
    assertTrue(observer.wasIndexUsed("SecIndex")); 
    StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
    
    assertEquals(95, results[0][1].size());
    //Not really with and without index but we can use this method to verify they are the same results
    //regardless of which index used
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(results, 1, new String[]{"<query with hints>"});
  }
  
  //Using junction, we will hint and make sure multi index hints are functioning
  @Test
  public void testMultiIndexWithMultiIndexHint() throws Exception {
    createRegion();
    populateData(1000);
    //create index
    createIndex("IDIndex", "p.ID", "/Portfolios p");
    createIndex("SecIndex", "p.status", "/Portfolios p");
    //set observer
    QueryObserverImpl observer = new QueryObserverImpl();
    QueryObserverHolder.setInstance(observer);
    //execute query
    SelectResults[][] results = new SelectResults[1][2];
    QueryService qs = CacheUtils.getQueryService();
    Query query = qs.newQuery("<hint 'IDIndex', 'SecIndex'>select * from /Portfolios p where p.ID > 10 and p.ID < 200 and p.status = 'inactive'");
    results[0][0] = (SelectResults) query.execute();
    //verify index usage
    assertTrue(observer.wasIndexUsed("IDIndex")); 
    assertTrue(observer.wasIndexUsed("SecIndex")); 
    observer.reset();
    
    query = qs.newQuery("<hint 'IDIndex'>select * from /Portfolios p where p.ID > 10 and p.ID < 200 and p.status = 'inactive'");
    results[0][1] = (SelectResults) query.execute();
    //verify index usage
    assertTrue(observer.wasIndexUsed("IDIndex")); 
    StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
    
    assertEquals(95, results[0][1].size());
    //Not really with and without index but we can use this method to verify they are the same results
    //regardless of which index used
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(results, 1, new String[]{"<query with hints>"});
  }
  
  @Test
  public void testIndexWithCompiledInSet() throws Exception {
    createRegion();
    populateData(1000);
    //create index
    createIndex("IDIndex", "p.ID", "/Portfolios p");
    createIndex("SecIndex", "p.status", "/Portfolios p");
    createIndex("DescriptionIndex", "p.description", "/Portfolios p");
    //set observer
    QueryObserverImpl observer = new QueryObserverImpl();
    QueryObserverHolder.setInstance(observer);
    //execute query
    SelectResults[][] results = new SelectResults[1][2];
    QueryService qs = CacheUtils.getQueryService();
    Query query = qs.newQuery("<hint 'IDIndex'>select * from /Portfolios p where p.ID > 10 and p.ID < 200 and p.status = 'inactive' and p.description IN SET ('XXXX', 'XXXY')");
    results[0][0] = (SelectResults) query.execute();
    //verify index usage
    assertTrue(observer.wasIndexUsed("IDIndex")); 
    observer.reset();

    query = qs.newQuery("<hint 'SecIndex'>select * from /Portfolios p where p.ID > 10 and p.ID < 200 and p.status = 'inactive' and p.description IN SET ('XXXX', 'XXXY')");
    results[0][1] = (SelectResults) query.execute();
    //verify index usage
    assertTrue(observer.wasIndexUsed("SecIndex")); 
    observer.reset();

    //Compare results with the first two index queries
    StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();    
    assertEquals(95, results[0][1].size());
    //Not really with and without index but we can use this method to verify they are the same results
    //regardless of which index used
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(results, 1, new String[]{"<query with hints>"});
    
    query = qs.newQuery("<hint 'DescriptionIndex'>select * from /Portfolios p where p.ID > 10 and p.ID < 200 and p.status = 'inactive' and p.description IN SET ('XXXX', 'XXXY')");
    results[0][1] = (SelectResults) query.execute();
    //verify index usage
    assertTrue(observer.wasIndexUsed("DescriptionIndex")); 
    
    //Compare results with the final index result
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(results, 1, new String[]{"<query with hints>"});
  }
  
  @Test
  public void testHintSingleIndexWithCompiledIn() throws Exception {
    createRegion();
    populateData(1000);
    //create index
    createIndex("IDIndex", "p.ID", "/Portfolios p");
    createIndex("SecIndex", "p.status", "/Portfolios p");
    createIndex("DescriptionIndex", "p.description", "/Portfolios p");
    //set observer
    QueryObserverImpl observer = new QueryObserverImpl();
    QueryObserverHolder.setInstance(observer);
    //execute query
    SelectResults[][] results = new SelectResults[1][2];
    QueryService qs = CacheUtils.getQueryService();
    Query query = qs.newQuery("<hint 'IDIndex'>select * from /Portfolios p where p.ID > 10 and p.ID < 200 and p.status = 'inactive' and p.description IN (select p.description from /Portfolios p where p.ID > 10)");
    results[0][0] = (SelectResults) query.execute();
    //verify index usage
    assertTrue(observer.wasIndexUsed("IDIndex")); 
    observer.reset();
    
    query = qs.newQuery("<hint 'SecIndex'>select * from /Portfolios p where p.ID > 10 and p.ID < 200 and p.status = 'inactive' and p.description IN (select p.description from /Portfolios p where p.ID > 10)");
    results[0][1] = (SelectResults) query.execute();
    //verify index usage
    assertTrue(observer.wasIndexUsed("SecIndex")); 
    observer.reset();
    
    //Compare results with the first two index queries
    StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
    assertEquals(95, results[0][1].size());
    //Not really with and without index but we can use this method to verify they are the same results
    //regardless of which index used
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(results, 1, new String[]{"<query with hints>"});
    
    query = qs.newQuery("<hint 'DescriptionIndex'>select * from /Portfolios p where p.ID > 10 and p.ID < 200 and p.status = 'inactive' and p.description IN (select p.description from /Portfolios p where p.ID > 10) ");
    results[0][1] = (SelectResults) query.execute();
    //verify index usage
    assertTrue(observer.wasIndexUsed("DescriptionIndex")); 
    
    //Compare results with the final index result
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(results, 1, new String[]{"<query with hints>"});
  }
  
  @Test
  public void testHintMultiIndexWithCompiledIn() throws Exception {
    createRegion();
    populateData(1000);
    //create index
    createIndex("IDIndex", "p.ID", "/Portfolios p");
    createIndex("SecIndex", "p.status", "/Portfolios p");
    createIndex("DescriptionIndex", "p.description", "/Portfolios p");
    //set observer
    QueryObserverImpl observer = new QueryObserverImpl();
    QueryObserverHolder.setInstance(observer);
    //execute query
    SelectResults[][] results = new SelectResults[1][2];
    QueryService qs = CacheUtils.getQueryService();
    Query query = qs.newQuery("select * from /Portfolios p where p.ID > 10 and p.ID < 200 and p.status = 'inactive' and p.description IN (<hint 'IDIndex', 'SecIndex', 'DescriptionIndex'>select p.description from /Portfolios p where p.ID > 10)");
    results[0][0] = (SelectResults) query.execute();
    //verify index usage
    assertTrue(observer.wasIndexUsed("IDIndex")); 
    assertTrue(observer.wasIndexUsed("SecIndex")); 
    observer.reset();
    
    query = qs.newQuery("select * from /Portfolios p where p.ID > 10 and p.ID < 200 and p.status = 'inactive' and p.description IN (select p.description from /Portfolios p where p.ID > 10)");
    results[0][1] = (SelectResults) query.execute();
    //verify index usage
    assertTrue(observer.wasIndexUsed("SecIndex")); 
    assertFalse(observer.wasIndexUsed("DescriptionIndex"));
    //We end up using IDIndex for this case.
    observer.reset();
    
    //Compare results with the first two index queries
    StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
    assertEquals(95, results[0][1].size());
    //Not really with and without index but we can use this method to verify they are the same results
    //regardless of which index used
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(results, 1, new String[]{"<query with hints>"});
    
    query = qs.newQuery("select * from /Portfolios p where p.ID > 10 and p.ID < 200 and p.status = 'inactive' and p.description IN (<hint 'DescriptionIndex'>select p.description from /Portfolios p where p.ID > 10)");
    results[0][1] = (SelectResults) query.execute();
    //verify index usage
    assertTrue(observer.wasIndexUsed("DescriptionIndex")); 
    assertFalse(observer.wasIndexUsed("SecIndex"));
    //We end up using IDIndex for this case also

    //Compare results with the final index result
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(results, 1, new String[]{"<query with hints>"});
  }
  
  //Hints inside of a nested query will trigger index usage for that query only
  //Unusable hints should behave the same for nested queries in that they are not used if unapplicable to the query
  @Test
  public void testHintNestedCompiledIn() throws Exception {
    createRegion();
    populateData(1000);
    //create index
    createIndex("IDIndex", "p.ID", "/Portfolios p");
    createIndex("SecIndex", "p.status", "/Portfolios p");
    createIndex("DescriptionIndex", "p.description", "/Portfolios p");
    //set observer
    QueryObserverImpl observer = new QueryObserverImpl();
    QueryObserverHolder.setInstance(observer);
    //execute query
    SelectResults[][] results = new SelectResults[1][2];
    QueryService qs = CacheUtils.getQueryService();
    Query query = qs.newQuery("select * from /Portfolios p where p.ID > 10 and p.ID < 200 and p.status = 'inactive' and p.description IN (<hint 'IDIndex', 'SecIndex', 'DescriptionIndex'>select p.description from /Portfolios p where p.ID > 10)");
    results[0][0] = (SelectResults) query.execute();
    //verify index usage
    assertTrue(observer.wasIndexUsed("IDIndex")); 
    assertTrue(observer.wasIndexUsed("SecIndex")); 
    //Because it was a hint for the inner query, it was an unuseable hint for that query
    assertFalse(observer.wasIndexUsed("DescriptionIndex"));
    observer.reset();
    
    //query again with no hints for a "bare" comparison
    query = qs.newQuery("select * from /Portfolios p where p.ID > 10 and p.ID < 200 and p.status = 'inactive' and p.description IN (select p.description from /Portfolios p where p.ID > 10)");
    results[0][1] = (SelectResults) query.execute();
    observer.reset();
    
    //Compare results with the first two index queries
    StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
    assertEquals(95, results[0][1].size());
    //Not really with and without index but we can use this method to verify they are the same results
    //regardless of which index used
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(results, 1, new String[]{"<query with hints>"});
  }
  
  @Test
  public void testJoinHint() throws Exception{
    createRegion();
    Region portfolios2 = createRegion("Portfolios_2");
    populateData(1000);
    populateData(portfolios2, 1000);
    //create index
    createIndex("IDIndex", "p.ID", "/Portfolios p");
    createIndex("SecIndex", "p.status", "/Portfolios p");
    createIndex("DescriptionIndex", "p.description", "/Portfolios p");
    
    createIndex("IDIndexOnPortfolios2", "p.ID", "/Portfolios_2 p");

    //set observer
    QueryObserverImpl observer = new QueryObserverImpl();
    QueryObserverHolder.setInstance(observer);
    //execute query
    SelectResults[][] results = new SelectResults[1][2];
    QueryService qs = CacheUtils.getQueryService();
    Query query = qs.newQuery("<hint 'SecIndex'>select * from /Portfolios p, /Portfolios_2 p2 where p.ID = p2.ID and p.status = 'inactive'");
    results[0][0] = (SelectResults) query.execute();
    //verify index usage
    assertTrue(observer.wasIndexUsed("SecIndex")); 
    observer.reset();
    
    //query again with no hints for a "bare" comparison
    query = qs.newQuery("select * from /Portfolios p, /Portfolios_2 p2 where p.ID = p2.ID and p.status = 'inactive'");
    results[0][1] = (SelectResults) query.execute();
    observer.reset();
    
    //Compare results with the first two index queries
    StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
    assertEquals(500, results[0][1].size());
    //Not really with and without index but we can use this method to verify they are the same results
    //regardless of which index used
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(results, 1, new String[]{"<query with hints>"});
 
  }
  
  class QueryObserverImpl extends QueryObserverAdapter
  {
    ArrayList indexesUsed = new ArrayList();

    public void beforeIndexLookup(Index index, int oper, Object key) {
      indexesUsed.add(index.getName());
    }
    
    public void beforeIndexLookup(Index index, int lowerBoundOperator,
        Object lowerBoundKey, int upperBoundOperator, Object upperBoundKey,
        Set NotEqualKeys) {
      indexesUsed.add(index.getName());
    }
    
    public boolean wasIndexUsed(String indexName) {
      return indexesUsed.contains(indexName);
    }
    
    public void reset() {
      indexesUsed.clear();
    }
  }
  
  private Region createRegion() {
    region = createRegion("Portfolios");
    return region;
  }
  
  private Region createRegion(String regionName) {
    return CacheUtils.createRegion(regionName, Portfolio.class);
  }
  
  private void populateData(int numObjects) {
    populateData(region, numObjects);
  }
  
  private void populateData(Region region, int numObjects) {
    for (int i = 0; i < numObjects; i++) {
      region.put("" + i, new Portfolio(i));
    }
  }
  
  private void createIndex(String indexName, String indexedExpression, String regionPath) throws RegionNotFoundException, IndexExistsException, IndexNameConflictException{
    CacheUtils.getQueryService().createIndex(indexName, indexedExpression, regionPath);
  }
}
